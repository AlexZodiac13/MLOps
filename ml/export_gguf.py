import argparse
import os
import threading
import time
import torch
import pandas as pd
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer
import subprocess
import mlflow


def _fmt_size(num_bytes):
    units = ["B", "KB", "MB", "GB", "TB"]
    size = float(num_bytes)
    for unit in units:
        if size < 1024.0 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024.0


class GGUFPointerModel(mlflow.pyfunc.PythonModel):
    """Tiny pyfunc wrapper that exposes path to logged GGUF artifact."""

    def load_context(self, context):
        self.gguf_file = context.artifacts["gguf_file"]

    def predict(self, context, model_input):
        rows = len(model_input) if hasattr(model_input, "__len__") else 1
        return pd.DataFrame({"gguf_file": [self.gguf_file] * rows})

def merge_and_export(model_id, adapter_path, output_dir, quantize_type="q4_k_m", run_id_file="last_run_id.txt"):
    print(f"Loading base model: {model_id}")
    print(f"Loading adapter: {adapter_path}")
    
    # 1. Merge Model
    # Load in FP16 to merge (requires ~6GB VRAM for 3B model)
    try:
        base_model = AutoModelForCausalLM.from_pretrained(
            model_id,
            low_cpu_mem_usage=True,
            return_dict=True,
            torch_dtype=torch.float16,
            device_map="auto",
            trust_remote_code=True
        )
        
        model = PeftModel.from_pretrained(base_model, adapter_path)
        model = model.merge_and_unload()
        
        merged_path = os.path.join(output_dir, "merged_model")
        print(f"Saving merged model to {merged_path}...")
        model.save_pretrained(merged_path)
        
        tokenizer = AutoTokenizer.from_pretrained(adapter_path)
        tokenizer.save_pretrained(merged_path)
        
        del model
        del base_model
        torch.cuda.empty_cache()
    except Exception as e:
        print(f"Error merging model: {e}")
        return

    # 2. Convert to GGUF (FP16)
    # Assuming llama.cpp is installed at /opt/llama.cpp in the container
    llama_cpp_dir = "/opt/llama.cpp"
    convert_script = os.path.join(llama_cpp_dir, "convert_hf_to_gguf.py")
    
    fp16_gguf_path = os.path.join(output_dir, "model_f16.gguf")
    
    print("Converting to GGUF (FP16)...")
    # Argument structure: python convert.py model_dir --outfile ...
    cmd_convert = [
        "python3", convert_script,
        merged_path, 
        "--outfile", fp16_gguf_path,
        "--outtype", "f16"
    ]
    subprocess.check_call(cmd_convert)
    
    # 3. Quantize
    quantize_bin = os.path.join(llama_cpp_dir, "build", "bin", "llama-quantize")
    quantized_gguf_path = os.path.join(output_dir, f"model_{quantize_type}.gguf")
    
    print(f"Quantizing to {quantize_type}...")
    cmd_quantize = [
        quantize_bin,
        fp16_gguf_path,
        quantized_gguf_path,
        quantize_type
    ]
    subprocess.check_call(cmd_quantize)
    
    # 4. Log to MLflow
    print("Logging GGUF to MLflow...")
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("reminder-bot-experiment")
    
    # Try to load existing run_id
    run_id = None
    try:
        with open(run_id_file, "r") as f:
            run_id = f.read().strip()
            print(f"Resuming MLflow Run: {run_id}")
    except FileNotFoundError:
        print(f"No {run_id_file} found, creating new run.")

    # We can attach to the existing run if we pass run_id, or just log directly
    with mlflow.start_run(run_id=run_id):
        artifact_size = os.path.getsize(quantized_gguf_path)
        print(
            f"Uploading GGUF artifact to MLflow: {quantized_gguf_path} ({_fmt_size(artifact_size)})",
            flush=True,
        )

        upload_started = time.perf_counter()
        stop_event = threading.Event()

        def _upload_heartbeat():
            while not stop_event.wait(30):
                elapsed = time.perf_counter() - upload_started
                print(f"Upload in progress... elapsed={elapsed:.1f}s", flush=True)

        heartbeat = threading.Thread(target=_upload_heartbeat, daemon=True)
        heartbeat.start()
        try:
            mlflow.log_artifact(quantized_gguf_path, artifact_path="gguf")
            # Also log a lightweight MLflow model entity to populate Models column
            # and support model registry flows that require logged_model artifacts.
            model_info = mlflow.pyfunc.log_model(
                artifact_path="gguf_model",
                python_model=GGUFPointerModel(),
                artifacts={"gguf_file": quantized_gguf_path},
            )
            print(f"Logged pyfunc model: {model_info.model_uri}", flush=True)
        finally:
            stop_event.set()
            heartbeat.join(timeout=1)

        elapsed_upload = time.perf_counter() - upload_started
        print(f"Upload finished in {elapsed_upload:.1f}s", flush=True)
        print(f"Artifact logged: {quantized_gguf_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_id", type=str, default="Qwen/Qwen2.5-3B-Instruct")
    # Adapter path is passed from the previous step output
    parser.add_argument("--adapter_path", type=str, required=True)
    parser.add_argument("--output_dir", type=str, default="./results")
    parser.add_argument("--run_id_file", type=str, default="last_run_id.txt")
    args = parser.parse_args()
    
    merge_and_export(args.model_id, args.adapter_path, args.output_dir, run_id_file=args.run_id_file)
