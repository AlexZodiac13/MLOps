import os
import json
import torch
import mlflow
import mlflow.transformers
import pandas as pd
from datasets import load_dataset
from transformers import AutoModelForCausalLM, AutoTokenizer
from sklearn.metrics import accuracy_score

# Configuration from environment (passed by Airflow)
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_S3_ENDPOINT_URL = os.getenv("MLFLOW_S3_ENDPOINT_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "ReminderBot_Training")
MLFLOW_RUN_ID = os.getenv("MLFLOW_RUN_ID") # The ID of the run we just did for training

# Set storage environment variables for MLflow artifacts (only if defined to avoid crash)
if MLFLOW_S3_ENDPOINT_URL:
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
if AWS_ACCESS_KEY_ID:
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
if AWS_SECRET_ACCESS_KEY:
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

def evaluate():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)

    # Use specified run or latest from current experiment
    if not MLFLOW_RUN_ID:
        latest_run = mlflow.search_runs(experiment_names=[MLFLOW_EXPERIMENT_NAME], order_by=["start_time DESC"], max_results=1)
        if latest_run.empty:
            raise Exception("No MLflow run found for evaluation.")
        run_id = latest_run.iloc[0].run_id
    else:
        run_id = MLFLOW_RUN_ID

    print(f"Evaluating model from MLflow run: {run_id}")
    
    with mlflow.start_run(run_id=run_id):
        # 1. Try to load GGUF model first (Target format)
        client = mlflow.tracking.MlflowClient()
        artifacts = client.list_artifacts(run_id, "gguf")
        
        gguf_path = None
        for art in artifacts:
            if art.path.endswith("q4_k_m.gguf"):
                print(f"Found GGUF artifact: {art.path}. Downloading...")
            gguf_path = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path=art.path)
            from llama_cpp import Llama
            llm = Llama(model_path=gguf_path, n_ctx=2048, n_threads=4, n_gpu_layers=0)
            
            # Logic from 3_model_testing.ipynb
            def run_inference(text):
                prompt = f"<|im_start|>user\nContext Date: 2026-02-18\nMessage: \"{text}\"\n\nJSON:<|im_end|>\n<|im_start|>assistant\n"
                output = llm(prompt, max_tokens=128, stop=["<|im_end|>"], temperature=0.1)
                return output['choices'][0]['text']
        else:
            # Fallback to HF Transformers if GGUF not found
            print("GGUF artifact not found. Falling back to HF Transformers.")
            model_uri = f"runs:/{run_id}/model_hf"
            mlflow_model = mlflow.transformers.load_model(model_uri)
            model = mlflow_model['model']
            tokenizer = mlflow_model['tokenizer']
            
            def run_inference(text):
                prompt = f"Context Date: 2026-02-18\nMessage: \"{text}\"\n\nJSON:"
                inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
                with torch.no_grad():
                    out = model.generate(**inputs, max_new_tokens=50)
                return tokenizer.decode(out[0], skip_special_tokens=True)

        # 2. Prepare small evaluation dataset
        # In Managed Airflow, relative paths should be joined with the current script location
        script_dir = os.path.dirname(os.path.abspath(__file__))
        dataset_path = os.path.join(script_dir, "labeled_dataset.json")
        dataset = load_dataset("json", data_files=dataset_path, split="train[:5]")
        
        # 3. Inference loop
        for sample in dataset:
            pred = run_inference(sample['input'])
            print(f"Input: {sample['input'][:50]}... -> Output: {pred}")

        # 4. Log final result
        mlflow.log_metric("eval_completed", 1.0)
        print("Evaluation complete.")

if __name__ == "__main__":
    evaluate()
