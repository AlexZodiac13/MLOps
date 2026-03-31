import argparse
import sys
import mlflow
from mlflow.tracking import MlflowClient
from mlflow.exceptions import MlflowException

from quality_gate import evaluate_quality_gate


def ranked_runs(client, experiment_id, filter_string, metric, latency_metric):
    return client.search_runs(
        experiment_ids=[experiment_id],
        filter_string=filter_string,
        order_by=[f"metrics.{metric} DESC", f"metrics.{latency_metric} ASC"],
    )


def has_gguf_artifact(client: MlflowClient, run_id: str) -> bool:
    try:
        artifacts = client.list_artifacts(run_id, path="gguf")
    except Exception:
        return False
    return len(artifacts) > 0


def has_logged_model_artifact(client: MlflowClient, run_id: str) -> bool:
    try:
        artifacts = client.list_artifacts(run_id, path="gguf_model")
    except Exception:
        return False
    return len(artifacts) > 0


def first_gguf_artifact_path(client: MlflowClient, run_id: str):
    """Return first gguf artifact path inside gguf/ folder, or None."""
    try:
        artifacts = client.list_artifacts(run_id, path="gguf")
    except Exception:
        return None

    # Usually export logs one file under gguf/, e.g. gguf/model_q4_k_m.gguf
    for item in artifacts:
        if not item.is_dir and item.path.endswith(".gguf"):
            return item.path

    for item in artifacts:
        if item.is_dir:
            try:
                nested = client.list_artifacts(run_id, path=item.path)
            except Exception:
                continue
            for child in nested:
                if not child.is_dir and child.path.endswith(".gguf"):
                    return child.path
    return None


def compare_and_register(
    model_name="reminder-bot",
    metric="quality_weighted_f1",
    latency_metric="quality_p95_latency_sec",
    min_weighted_f1=0.85,
    max_latency_p95_sec=1.5,
    max_ram_mb=4096.0,
    min_json_valid_rate=0.95,
    min_exact_match_rate=0.60,
    target_stage="Production",
    run_id_file="last_run_id.txt",
    select_best_available=True,
):
    mlflow.set_tracking_uri("http://mlflow:5000")
    client = MlflowClient()
    
    # Get all runs for the experiment
    experiment = client.get_experiment_by_name("reminder-bot-experiment")
    if not experiment:
        print("Experiment not found.")
        return False

    finetuned_filter = "attributes.status = 'FINISHED' and tags.model_variant = 'finetuned'"
    baseline_filter = "attributes.status = 'FINISHED' and tags.model_variant = 'baseline'"

    finetuned_runs = ranked_runs(
        client,
        experiment.experiment_id,
        finetuned_filter,
        metric,
        latency_metric,
    )

    if not finetuned_runs:
        print("No finetuned runs found.")
        return False

    best_finetuned = None
    best_available = None
    for run in finetuned_runs:
        run_id = run.info.run_id
        metric_val = run.data.metrics.get(metric)
        latency_val = run.data.metrics.get(latency_metric)

        if metric_val is None or latency_val is None:
            print(f"Skip run {run_id}: missing ranking metrics {metric}/{latency_metric}")
            continue

        if not has_gguf_artifact(client, run_id):
            print(f"Skip run {run_id}: missing gguf artifact")
            continue

        if best_available is None:
            best_available = run

        gate = evaluate_quality_gate(
            metrics=run.data.metrics,
            min_weighted_f1=min_weighted_f1,
            max_latency_p95_sec=max_latency_p95_sec,
            max_ram_mb=max_ram_mb,
            min_json_valid_rate=min_json_valid_rate,
            min_exact_match_rate=min_exact_match_rate,
        )
        if not gate.passed:
            print(f"Skip run {run_id}: quality gate failed")
            for error in gate.errors:
                print(f"- {error}")
            continue

        best_finetuned = run
        break

    selection_reason = "quality_gate_passed"
    if not best_finetuned:
        if select_best_available and best_available is not None:
            best_finetuned = best_available
            selection_reason = "best_available_fallback"
            print(
                "No run passed quality gate. "
                f"Fallback to best available run: {best_finetuned.info.run_id}"
            )
        else:
            print("No finetuned runs passed quality gate with required gguf artifact. Registration skipped.")
            return False

    metric_val = best_finetuned.data.metrics.get(metric)
    latency_val = best_finetuned.data.metrics.get(latency_metric)
    print(
        f"Selected finetuned run: {best_finetuned.info.run_id} "
        f"with {metric}: {metric_val}, {latency_metric}: {latency_val}"
    )

    # Установить тег stage=Product для лучшего run
    try:
        client.set_tag(best_finetuned.info.run_id, "stage", "Product")
        print(f"Tag 'stage=Product' set for run {best_finetuned.info.run_id}")
    except Exception as e:
        print(f"Failed to set tag 'stage=Product' for run {best_finetuned.info.run_id}: {e}")

    baseline_runs = ranked_runs(
        client,
        experiment.experiment_id,
        baseline_filter,
        metric,
        latency_metric,
    )
    baseline = baseline_runs[0] if baseline_runs else None
    if baseline:
        base_metric = baseline.data.metrics.get(metric, 0.0)
        delta = metric_val - base_metric
        print(
            f"Baseline run: {baseline.info.run_id}, {metric}: {base_metric}. "
            f"Delta finetuned-baseline: {delta:+.4f}"
        )
    else:
        print("Baseline run not found. Continue without baseline delta.")
    
    # Check if the current run (from run_id_file) matches the best run
    current_run_id = None
    try:
        with open(run_id_file, "r") as f:
            current_run_id = f.read().strip()
    except FileNotFoundError:
        pass

    if current_run_id:
        print(f"Current Pipeline Run ID: {current_run_id}")
        if best_finetuned.info.run_id == current_run_id:
            print("Great! The current run is the best performing model so far.")
        else:
            print(f"Current run is NOT the best. Best is {best_finetuned.info.run_id}")
    
    # Register the BEST model found (not necessarily the current one).
    # Prefer logged model path (gguf_model) if present, otherwise fallback to gguf artifact flow.
    if has_logged_model_artifact(client, best_finetuned.info.run_id):
        model_uri = f"runs:/{best_finetuned.info.run_id}/gguf_model"
    else:
        model_uri = f"runs:/{best_finetuned.info.run_id}/gguf"
    gguf_path = first_gguf_artifact_path(client, best_finetuned.info.run_id)
    normalized_stage = str(target_stage).strip().title()
    allowed_stages = {"Staging", "Production", "Archived", "None"}
    if normalized_stage not in allowed_stages:
        print(
            f"Unknown target_stage={target_stage}. "
            "Fallback to Staging. Allowed: Staging, Production, Archived, None"
        )
        normalized_stage = "Staging"
    
    try:
        # Primary path for MLflow projects that log a model entity.
        result = mlflow.register_model(model_uri, model_name)
        print(f"Model registered: {result.name} version {result.version}")

    except MlflowException as e:
        message = str(e)
        if "Unable to find a logged_model with artifact_path" not in message:
            print(f"Registration failed: {e}")
            raise

        if not gguf_path:
            print(
                "Registration failed: gguf artifact exists but no .gguf file path found "
                f"for run {best_finetuned.info.run_id}"
            )
            raise

        source_uri = f"{best_finetuned.info.artifact_uri.rstrip('/')}/{gguf_path}"
        print(
            "register_model requires logged_model in this MLflow version. "
            f"Fallback to create_model_version from source={source_uri}"
        )

        # Ensure registered model exists.
        try:
            client.get_registered_model(model_name)
        except Exception:
            client.create_registered_model(model_name)

        result = client.create_model_version(
            name=model_name,
            source=source_uri,
            run_id=best_finetuned.info.run_id,
        )
        print(f"Model version created via artifact source: {result.name} version {result.version}")

    except Exception as e:
        print(f"Registration failed (might already exist or artifact missing): {e}")
        raise

    try:

        client.set_model_version_tag(model_name, result.version, "ranking_metric", metric)
        client.set_model_version_tag(model_name, result.version, "selection_reason", selection_reason)
        client.set_model_version_tag(model_name, result.version, "quality_weighted_f1", str(metric_val))
        client.set_model_version_tag(model_name, result.version, "quality_p95_latency_sec", str(latency_val))
        client.set_model_version_tag(
            model_name,
            result.version,
            "quality_json_valid_rate",
            str(best_finetuned.data.metrics.get("quality_json_valid_rate", "")),
        )
        client.set_model_version_tag(
            model_name,
            result.version,
            "quality_exact_match_rate",
            str(best_finetuned.data.metrics.get("quality_exact_match_rate", "")),
        )
        
        # MLflow can keep only one Production version when archive_existing_versions=True.
        client.transition_model_version_stage(
            name=model_name,
            version=result.version,
            stage=normalized_stage,
            archive_existing_versions=(normalized_stage == "Production"),
        )
        print(f"Transitioned to {normalized_stage}.")

    except Exception as e:
        print(f"Post-registration actions failed (tagging/stage transition): {e}")
        raise

    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", type=str, default="reminder-bot")
    parser.add_argument("--metric", type=str, default="quality_weighted_f1")
    parser.add_argument("--latency_metric", type=str, default="quality_p95_latency_sec")
    parser.add_argument("--min_weighted_f1", type=float, default=0.85)
    parser.add_argument("--max_latency_p95_sec", type=float, default=1.5)
    parser.add_argument("--max_ram_mb", type=float, default=4096.0)
    parser.add_argument("--min_json_valid_rate", type=float, default=0.95)
    parser.add_argument("--min_exact_match_rate", type=float, default=0.60)
    parser.add_argument("--target_stage", type=str, default="Production")
    parser.add_argument("--run_id_file", type=str, default="last_run_id.txt")
    parser.add_argument("--fail_if_skipped", type=str, default="false")
    parser.add_argument("--select_best_available", type=str, default="true")
    args = parser.parse_args()

    select_best_available = str(args.select_best_available).strip().lower() in {"1", "true", "yes", "y"}
    success = compare_and_register(
        args.model_name,
        args.metric,
        args.latency_metric,
        args.min_weighted_f1,
        args.max_latency_p95_sec,
        args.max_ram_mb,
        args.min_json_valid_rate,
        args.min_exact_match_rate,
        args.target_stage,
        args.run_id_file,
        select_best_available,
    )

    fail_if_skipped = str(args.fail_if_skipped).strip().lower() in {"1", "true", "yes", "y"}
    if fail_if_skipped and not success:
        print("No model was selected/registered and fail_if_skipped=true. Failing task.")
        sys.exit(1)
