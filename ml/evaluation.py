import json
import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

PROMPT_SYSTEM = (
    "You are a reminder extraction system. "
    "Extract text, date, time, repeat fields and return strict JSON."
)

FIELD_WEIGHTS = {
    "date": 0.4,
    "time": 0.4,
    "text": 0.1,
    "repeat": 0.1,
}


def normalize_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return " ".join(value.strip().lower().split())
    return str(value).strip().lower()


def _normalize_date(value: Any) -> str:
    text = normalize_value(value)
    if not text:
        return ""

    # Keep only date part for ISO-like payloads: 2026-03-30T10:00:00Z
    text = text.split("t")[0]
    text = text.replace("/", "-").strip()

    match = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", text)
    if not match:
        return text

    year, month, day = match.groups()
    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"


def _normalize_time(value: Any) -> str:
    text = normalize_value(value)
    if not text:
        return ""

    # Accept HH:MM and HH:MM:SS, normalize to HH:MM
    match = re.fullmatch(r"(\d{1,2}):(\d{2})(?::\d{2})?", text)
    if not match:
        return text

    hour, minute = match.groups()
    return f"{int(hour):02d}:{int(minute):02d}"


def _normalize_repeat(value: Any) -> str:
    text = normalize_value(value)
    if text in {"none", "null", "", "нет", "без повтора"}:
        return ""

    mapping = {
        "каждый день": "daily",
        "ежедневно": "daily",
        "daily": "daily",
        "каждую неделю": "weekly",
        "еженедельно": "weekly",
        "weekly": "weekly",
        "каждый месяц": "monthly",
        "ежемесячно": "monthly",
        "monthly": "monthly",
    }
    return mapping.get(text, text)


def normalize_field_value(field: str, value: Any) -> str:
    if field == "date":
        return _normalize_date(value)
    if field == "time":
        return _normalize_time(value)
    if field == "repeat":
        return _normalize_repeat(value)
    return normalize_value(value)


def _json_candidate(text: str) -> Optional[str]:
    if not text:
        return None

    text = text.strip()
    if text.startswith("{") and text.endswith("}"):
        return text

    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0)
    return None


def _decode_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None

    decoder = json.JSONDecoder()
    for index, char in enumerate(text):
        if char != "{":
            continue
        try:
            parsed, _ = decoder.raw_decode(text[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def parse_prediction(raw_response: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    parsed = _decode_first_json_object(raw_response)
    if parsed is None:
        candidate = _json_candidate(raw_response)
        if not candidate:
            return None, "no_json_object"
        try:
            parsed_obj = json.loads(candidate)
        except json.JSONDecodeError:
            return None, "invalid_json"
        if not isinstance(parsed_obj, dict):
            return None, "json_not_object"
        parsed = parsed_obj

    if not isinstance(parsed, dict):
        return None, "json_not_object"

    normalized = {
        "text": parsed.get("text"),
        "date": parsed.get("date"),
        "time": parsed.get("time"),
        "repeat": parsed.get("repeat"),
    }
    return normalized, None


def build_messages(sample: Dict[str, Any]) -> List[Dict[str, str]]:
    user_content = (
        f"Context Date: {sample['context_date']}\n"
        f"Message: \"{sample['input']}\"\n\n"
        "JSON:"
    )
    return [
        {"role": "system", "content": PROMPT_SYSTEM},
        {"role": "user", "content": user_content},
    ]


def _init_field_counters() -> Dict[str, Dict[str, float]]:
    counters = {}
    for field in FIELD_WEIGHTS:
        counters[field] = {"tp": 0.0, "fp": 0.0, "fn": 0.0}
    return counters


def _safe_div(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _field_scores(counters: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    metrics: Dict[str, float] = {}
    for field, values in counters.items():
        tp = values["tp"]
        fp = values["fp"]
        fn = values["fn"]
        precision = _safe_div(tp, tp + fp)
        recall = _safe_div(tp, tp + fn)
        f1 = 0.0
        if precision + recall > 0:
            f1 = (2.0 * precision * recall) / (precision + recall)

        metrics[f"{field}_precision"] = precision
        metrics[f"{field}_recall"] = recall
        metrics[f"{field}_f1"] = f1
    return metrics


def weighted_f1(metrics: Dict[str, float], weights: Optional[Dict[str, float]] = None) -> float:
    active_weights = weights or FIELD_WEIGHTS
    score = 0.0
    for field, weight in active_weights.items():
        score += weight * metrics.get(f"{field}_f1", 0.0)
    return score


def percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0

    sorted_values = sorted(values)
    rank = (len(sorted_values) - 1) * q
    lower = math.floor(rank)
    upper = math.ceil(rank)

    if lower == upper:
        return float(sorted_values[lower])

    lower_value = sorted_values[lower]
    upper_value = sorted_values[upper]
    return float(lower_value + (upper_value - lower_value) * (rank - lower))


def evaluate_predictions(
    gold_samples: List[Dict[str, Any]],
    predictions: List[Dict[str, Any]],
    latencies: Optional[List[float]] = None,
) -> Dict[str, float]:
    total = len(gold_samples)
    if total == 0:
        return {
            "json_valid_rate": 0.0,
            "exact_match_rate": 0.0,
            "quality_weighted_f1": 0.0,
            "latency_avg_sec": 0.0,
            "latency_p95_sec": 0.0,
        }

    counters = _init_field_counters()
    exact_match = 0.0
    valid_json = 0.0

    for gold, pred in zip(gold_samples, predictions):
        pred_obj = pred.get("parsed")
        if pred_obj is not None:
            valid_json += 1.0

        sample_match = True
        for field in FIELD_WEIGHTS:
            gold_value = normalize_field_value(field, gold.get("output", {}).get(field))
            pred_value = normalize_field_value(field, (pred_obj or {}).get(field))

            if gold_value == pred_value:
                counters[field]["tp"] += 1.0
            else:
                counters[field]["fp"] += 1.0
                counters[field]["fn"] += 1.0
                sample_match = False

        if sample_match:
            exact_match += 1.0

    metrics = _field_scores(counters)
    metrics["json_valid_rate"] = valid_json / total
    metrics["exact_match_rate"] = exact_match / total
    metrics["quality_weighted_f1"] = weighted_f1(metrics)

    latency_values = latencies or []
    metrics["latency_avg_sec"] = sum(latency_values) / len(latency_values) if latency_values else 0.0
    metrics["latency_p95_sec"] = percentile(latency_values, 0.95) if latency_values else 0.0
    return metrics


def iso_now() -> str:
    return datetime.utcnow().isoformat() + "Z"
