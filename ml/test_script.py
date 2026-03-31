import argparse
import json
import math
import os
import resource
import tempfile
import time
from typing import Any, Dict, List, Optional

import torch
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
import mlflow

from evaluation import build_messages, evaluate_predictions, parse_prediction

DEFAULT_WEIGHTS = {
    "date": 0.4,
    "time": 0.4,
    "text": 0.1,
    "repeat": 0.1,
}


def read_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as file_obj:
        return json.load(file_obj)


def load_model(model_id: str, adapter_path: Optional[str], baseline_only: bool):
    model_kwargs: Dict[str, Any] = {
        "device_map": "auto",
        "trust_remote_code": True,
    }

    if torch.cuda.is_available():
        model_kwargs["torch_dtype"] = torch.float16
        model_kwargs["quantization_config"] = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.float16,
            bnb_4bit_use_double_quant=True,
        )
    else:
        model_kwargs["torch_dtype"] = torch.float32

    base_model = AutoModelForCausalLM.from_pretrained(model_id, **model_kwargs)
    base_model.eval()
    if baseline_only:
        return base_model
    if not adapter_path:
        raise ValueError("adapter_path is required when baseline_only is False")
    return PeftModel.from_pretrained(base_model, adapter_path)

def generate_response(model, tokenizer, sample: Dict[str, Any], max_new_tokens: int) -> str:
    messages = build_messages(sample)
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    model_inputs = tokenizer([text], return_tensors="pt").to(model.device)

    generated_ids = model.generate(
        **model_inputs,
        max_new_tokens=max_new_tokens,
        do_sample=False,
    )
    generated_ids = [
        output_ids[len(input_ids):] for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)
    ]
    return tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]


def evaluate_dataset(
    model,
    tokenizer,
    dataset_name: str,
    samples: List[Dict[str, Any]],
    max_samples: int,
    max_new_tokens: int,
) -> Dict[str, Any]:
    selected = samples[:max_samples] if max_samples > 0 else samples
    total = len(selected)

    predictions: List[Dict[str, Any]] = []
    latencies: List[float] = []
    records: List[Dict[str, Any]] = []

    print(f"[{dataset_name}] Start evaluation: {total} samples", flush=True)
    eval_started_at = time.perf_counter()

    for index, sample in enumerate(selected, start=1):
        start = time.perf_counter()
        raw_response = generate_response(model, tokenizer, sample, max_new_tokens=max_new_tokens)
        elapsed = time.perf_counter() - start

        parsed, parse_error = parse_prediction(raw_response)
        predictions.append(
            {
                "parsed": parsed,
                "parse_error": parse_error,
                "raw_response": raw_response,
            }
        )
        latencies.append(elapsed)
        records.append(
            {
                "input": sample.get("input"),
                "expected": sample.get("output"),
                "predicted": parsed,
                "raw_response": raw_response,
                "parse_error": parse_error,
                "latency_sec": elapsed,
            }
        )

        # Heartbeat logs for CI/Airflow to make long runs observable.
        if index == 1 or index == total or index % 5 == 0:
            elapsed_total = time.perf_counter() - eval_started_at
            avg_per_item = elapsed_total / index
            eta_sec = max(0.0, (total - index) * avg_per_item)
            print(
                f"[{dataset_name}] Progress {index}/{total} "
                f"({(index / total) * 100:.1f}%) | "
                f"last={elapsed:.2f}s avg={avg_per_item:.2f}s eta={eta_sec:.1f}s",
                flush=True,
            )

    metrics = evaluate_predictions(selected, predictions, latencies=latencies)
    peak_ram_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0

    prefixed_metrics = {
        f"{dataset_name}_{metric_name}": metric_value
        for metric_name, metric_value in metrics.items()
    }
    prefixed_metrics[f"{dataset_name}_peak_ram_mb"] = peak_ram_mb

    return {
        "metrics": prefixed_metrics,
        "records": records,
        "num_samples": len(selected),
    }


def quality_from_metrics(metrics: Dict[str, float], source: str) -> Dict[str, float]:
    return {
        "quality_date_f1": metrics.get(f"{source}_date_f1", 0.0),
        "quality_time_f1": metrics.get(f"{source}_time_f1", 0.0),
        "quality_text_f1": metrics.get(f"{source}_text_f1", 0.0),
        "quality_repeat_f1": metrics.get(f"{source}_repeat_f1", 0.0),
        "quality_weighted_f1": metrics.get(f"{source}_quality_weighted_f1", 0.0),
        "quality_json_valid_rate": metrics.get(f"{source}_json_valid_rate", 0.0),
        "quality_exact_match_rate": metrics.get(f"{source}_exact_match_rate", 0.0),
        "quality_p95_latency_sec": metrics.get(f"{source}_latency_p95_sec", 0.0),
        "quality_avg_latency_sec": metrics.get(f"{source}_latency_avg_sec", 0.0),
        "quality_peak_ram_mb": metrics.get(f"{source}_peak_ram_mb", 0.0),
    }


def test_model(
    model_id,
    adapter_path,
    holdout_data_path,
    run_id_file="last_run_id.txt",
    golden_data_path=None,
    max_samples=0,
    max_new_tokens=128,
    baseline_only=False,
):
    model_variant = "baseline" if baseline_only else "finetuned"
    print(f"Loading model for testing: variant={model_variant}, model_id={model_id}")

    model = load_model(model_id, adapter_path, baseline_only=baseline_only)
    tokenizer = AutoTokenizer.from_pretrained(model_id)

    holdout_samples = read_json(holdout_data_path)
    if not holdout_samples:
        raise ValueError(f"No holdout samples found in {holdout_data_path}")

    print(
        f"Datasets loaded: holdout={len(holdout_samples)}, "
        f"golden_path={'present' if golden_data_path else 'absent'}, "
        f"max_samples={max_samples}, max_new_tokens={max_new_tokens}",
        flush=True,
    )

    all_metrics: Dict[str, float] = {}
    all_records: Dict[str, Any] = {}

    holdout_eval = evaluate_dataset(
        model,
        tokenizer,
        dataset_name="holdout",
        samples=holdout_samples,
        max_samples=max_samples,
        max_new_tokens=max_new_tokens,
    )
    all_metrics.update(holdout_eval["metrics"])
    all_records["holdout"] = holdout_eval["records"]

    source_for_quality = "holdout"
    if golden_data_path and os.path.exists(golden_data_path):
        golden_samples = read_json(golden_data_path)
        if golden_samples:
            print(f"Golden dataset detected: {len(golden_samples)} samples", flush=True)
            golden_eval = evaluate_dataset(
                model,
                tokenizer,
                dataset_name="golden",
                samples=golden_samples,
                max_samples=max_samples,
                max_new_tokens=max_new_tokens,
            )
            all_metrics.update(golden_eval["metrics"])
            all_records["golden"] = golden_eval["records"]
            source_for_quality = "golden"
        else:
            print("Golden dataset file is empty; fallback to holdout metrics", flush=True)
    elif golden_data_path:
        print(f"Golden dataset not found at path: {golden_data_path}", flush=True)

    quality_metrics = quality_from_metrics(all_metrics, source_for_quality)
    all_metrics.update(quality_metrics)

    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("reminder-bot-experiment")

    # Try to load existing run_id
    run_id = None
    try:
        with open(run_id_file, "r", encoding="utf-8") as file_obj:
            run_id = file_obj.read().strip()
            print(f"Resuming MLflow Run: {run_id}")
    except FileNotFoundError:
        print(f"No {run_id_file} found, starting new run.")

    with mlflow.start_run(run_id=run_id):
        mlflow.set_tag("pipeline_stage", "test")
        mlflow.set_tag("model_variant", model_variant)
        mlflow.log_param("evaluation_max_samples", max_samples)
        mlflow.log_param("evaluation_max_new_tokens", max_new_tokens)
        for field_name, weight in DEFAULT_WEIGHTS.items():
            mlflow.log_param(f"weight_{field_name}", weight)

        for metric_name, metric_value in all_metrics.items():
            mlflow.log_metric(metric_name, metric_value)

        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False, encoding="utf-8") as artifact_file:
            json.dump(all_records, artifact_file, ensure_ascii=False, indent=2)
            artifact_path = artifact_file.name

        mlflow.log_artifact(artifact_path, artifact_path="evaluation")
        os.remove(artifact_path)

        print(
            "Evaluation completed: "
            f"quality_weighted_f1={all_metrics.get('quality_weighted_f1', 0.0):.4f}, "
            f"quality_p95_latency_sec={all_metrics.get('quality_p95_latency_sec', 0.0):.4f}"
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_id", type=str, default="Qwen/Qwen2.5-3B-Instruct")
    parser.add_argument("--adapter_path", type=str, default=None)
    parser.add_argument("--holdout_data", type=str, required=True)
    parser.add_argument("--golden_data", type=str, default=None)
    parser.add_argument("--run_id_file", type=str, default="last_run_id.txt")
    parser.add_argument("--max_samples", type=int, default=0)
    parser.add_argument("--max_new_tokens", type=int, default=128)
    parser.add_argument("--baseline_only", action="store_true")
    args = parser.parse_args()

    test_model(
        args.model_id,
        args.adapter_path,
        args.holdout_data,
        args.run_id_file,
        args.golden_data,
        args.max_samples,
        args.max_new_tokens,
        args.baseline_only,
    )
