from dataclasses import dataclass
from typing import Dict, List


@dataclass
class QualityGateResult:
    passed: bool
    errors: List[str]


def evaluate_quality_gate(
    metrics: Dict[str, float],
    min_weighted_f1: float,
    max_latency_p95_sec: float,
    max_ram_mb: float,
    min_json_valid_rate: float,
    min_exact_match_rate: float,
) -> QualityGateResult:
    errors: List[str] = []

    weighted_f1 = metrics.get("quality_weighted_f1")
    latency_p95 = metrics.get("quality_p95_latency_sec")
    peak_ram_mb = metrics.get("quality_peak_ram_mb")
    json_valid_rate = metrics.get("quality_json_valid_rate")
    exact_match_rate = metrics.get("quality_exact_match_rate")

    if weighted_f1 is None:
        errors.append("Missing metric: quality_weighted_f1")
    elif weighted_f1 < min_weighted_f1:
        errors.append(
            f"quality_weighted_f1={weighted_f1:.4f} is below threshold {min_weighted_f1:.4f}"
        )

    if latency_p95 is None:
        errors.append("Missing metric: quality_p95_latency_sec")
    elif latency_p95 > max_latency_p95_sec:
        errors.append(
            f"quality_p95_latency_sec={latency_p95:.4f} exceeds threshold {max_latency_p95_sec:.4f}"
        )

    if peak_ram_mb is None:
        errors.append("Missing metric: quality_peak_ram_mb")
    elif peak_ram_mb > max_ram_mb:
        errors.append(
            f"quality_peak_ram_mb={peak_ram_mb:.2f} exceeds threshold {max_ram_mb:.2f}"
        )

    if json_valid_rate is None:
        errors.append("Missing metric: quality_json_valid_rate")
    elif json_valid_rate < min_json_valid_rate:
        errors.append(
            f"quality_json_valid_rate={json_valid_rate:.4f} is below threshold {min_json_valid_rate:.4f}"
        )

    if exact_match_rate is None:
        errors.append("Missing metric: quality_exact_match_rate")
    elif exact_match_rate < min_exact_match_rate:
        errors.append(
            f"quality_exact_match_rate={exact_match_rate:.4f} is below threshold {min_exact_match_rate:.4f}"
        )

    return QualityGateResult(passed=not errors, errors=errors)
