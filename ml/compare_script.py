import argparse
import mlflow
from mlflow.tracking import MlflowClient

def compare_and_register(model_name="reminder-bot", metric="test_json_valid", run_id_file="last_run_id.txt"):
    mlflow.set_tracking_uri("http://mlflow:5000")
    client = MlflowClient()
    
    # Get all runs for the experiment
    experiment = client.get_experiment_by_name("reminder-bot-experiment")
    if not experiment:
        print("Experiment not found.")
        return

    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        order_by=[f"metrics.{metric} DESC"]
    )
    
    if not runs:
        print("No runs found.")
        return

    best_run = runs[0]
    metric_val = best_run.data.metrics.get(metric)
    print(f"Best run found: {best_run.info.run_id} with {metric}: {metric_val}")
    
    # Check if the current run (from run_id_file) matches the best run
    current_run_id = None
    try:
        with open(run_id_file, "r") as f:
            current_run_id = f.read().strip()
    except FileNotFoundError:
        pass

    if current_run_id:
        print(f"Current Pipeline Run ID: {current_run_id}")
        if best_run.info.run_id == current_run_id:
            print("Great! The current run is the best performing model so far.")
        else:
            print(f"Current run is NOT the best. Best is {best_run.info.run_id}")
    
    # Register the BEST model found (not necessarily the current one)
    # The artifact path must match where export_gguf.py saved it
    # export_gguf.py uses artifact_path="gguf", so the model is inside that folder
    model_uri = f"runs:/{best_run.info.run_id}/gguf"
    
    try:
        # Register model
        result = mlflow.register_model(
            model_uri,
            model_name
        )
        print(f"Model registered: {result.name} version {result.version}")
        
        # Transition to Staging or Production
        client.transition_model_version_stage(
            name=model_name,
            version=result.version,
            stage="Staging"
        )
        print("Transitioned to Staging.")
        
    except Exception as e:
        print(f"Registration failed (might already exist or artifact missing): {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_name", type=str, default="reminder-bot")
    parser.add_argument("--metric", type=str, default="test_json_valid")
    parser.add_argument("--run_id_file", type=str, default="last_run_id.txt")
    args = parser.parse_args()
    
    compare_and_register(args.model_name, args.metric, args.run_id_file)
