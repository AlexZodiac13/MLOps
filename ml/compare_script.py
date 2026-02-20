import argparse
import mlflow
from mlflow.tracking import MlflowClient

def compare_and_register(model_name="reminder-bot", metric="test_json_valid"):
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
    print(f"Best run: {best_run.info.run_id} with {metric}: {best_run.data.metrics.get(metric)}")
    
    # Check if we should register this model version
    # (Simplified logic: if it's the best so far)
    
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
    compare_and_register()
