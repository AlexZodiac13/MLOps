import argparse
import sys

import mlflow
from mlflow.tracking import MlflowClient

from quality_gate import evaluate_quality_gate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id_file", type=str, required=True)
    parser.add_argument("--min_weighted_f1", type=float, default=0.85)
    parser.add_argument("--max_latency_p95_sec", type=float, default=1.5)
    parser.add_argument("--max_ram_mb", type=float, default=4096.0)
    parser.add_argument("--min_json_valid_rate", type=float, default=0.95)
    parser.add_argument("--min_exact_match_rate", type=float, default=0.60)
    parser.add_argument("--strict_mode", type=str, default="true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    strict_mode = str(args.strict_mode).strip().lower() in {"1", "true", "yes", "y"}

    mlflow.set_tracking_uri("http://mlflow:5000")
    client = MlflowClient()

    with open(args.run_id_file, "r", encoding="utf-8") as file_obj:
        run_id = file_obj.read().strip()

    run = client.get_run(run_id)
    eval_max_new_tokens = run.data.params.get("evaluation_max_new_tokens", "unknown")
    eval_max_samples = run.data.params.get("evaluation_max_samples", "unknown")
    print(
        "Evaluating run metrics: "
        f"run_id={run_id}, evaluation_max_new_tokens={eval_max_new_tokens}, "
        f"evaluation_max_samples={eval_max_samples}",
    )

    result = evaluate_quality_gate(
        metrics=run.data.metrics,
        min_weighted_f1=args.min_weighted_f1,
        max_latency_p95_sec=args.max_latency_p95_sec,
        max_ram_mb=args.max_ram_mb,
        min_json_valid_rate=args.min_json_valid_rate,
        min_exact_match_rate=args.min_exact_match_rate,
    )

    if result.passed:
        print(f"Quality gate passed for run_id={run_id}")
        return 0

    print(f"Quality gate failed for run_id={run_id}")
    for error in result.errors:
        print(f"- {error}")
    print(
        "Hint: retrying quality_gate for the same run_id will produce the same result. "
        "Re-run test_model (or start a new DAG run) to recalculate metrics."
    )

    if strict_mode:
        return 1

    print("Strict mode is disabled, continuing despite quality gate violations.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
