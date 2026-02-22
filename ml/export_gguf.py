import argparse
import os
import torch
from peft import PeftModel
from transformers import AutoModelForCausalLM, AutoTokenizer
import subprocess
import mlflow

def merge_and_export(model_id, adapter_path, output_dir, quantize_type="q4_k_m"):
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
        with open("last_run_id.txt", "r") as f:
            run_id = f.read().strip()
            print(f"Resuming MLflow Run: {run_id}")
    except FileNotFoundError:
        print("No last_run_id.txt found, creating new run.")

    # We can attach to the existing run if we pass run_id, or just log directly
    with mlflow.start_run(run_id=run_id):
        mlflow.log_artifact(quantized_gguf_path, artifact_path="gguf")
        print(f"Artifact logged: {quantized_gguf_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_id", type=str, default="Qwen/Qwen2.5-3B-Instruct")
    # Adapter path is passed from the previous step output
    parser.add_argument("--adapter_path", type=str, required=True)
    parser.add_argument("--output_dir", type=str, default="./results")
    args = parser.parse_args()
    
    merge_and_export(args.model_id, args.adapter_path, args.output_dir)
