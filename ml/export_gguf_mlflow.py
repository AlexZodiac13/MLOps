import os
import subprocess
import mlflow
import json
import urllib.request
import zipfile
import io

# --- CRITICAL S3 CONFIGURATION (Must be BEFORE mlflow imports) ---
def _get_env_robust(key, default=None):
    val = os.getenv(key, default)
    return val.strip() if val else default

os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
os.environ["AWS_S3_ADDRESSING_STYLE"] = "path"
os.environ["BOTO3_CONFIG_S3_SIGNATURE_VERSION"] = "s3v4"
os.environ["S3_USE_SIGV4"] = "True"
os.environ["AWS_SESSION_TOKEN"] = ""

# Configuration
MLFLOW_TRACKING_URI = _get_env_robust("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_S3_ENDPOINT_URL = _get_env_robust("MLFLOW_S3_ENDPOINT_URL", "https://s3.owgrant.su")
AWS_ACCESS_KEY_ID = _get_env_robust("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = _get_env_robust("AWS_SECRET_ACCESS_KEY", "")
MLFLOW_RUN_ID = _get_env_robust("MLFLOW_RUN_ID")

# Set storage environment variables for MLflow artifacts
if MLFLOW_S3_ENDPOINT_URL:
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
if AWS_ACCESS_KEY_ID:
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
if AWS_SECRET_ACCESS_KEY:
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

def export_gguf():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    
    # Use specified run or latest from current experiment
    if not MLFLOW_RUN_ID:
        latest_run = mlflow.search_runs(order_by=["start_time DESC"], max_results=1)
        if latest_run.empty:
            raise Exception("No MLflow run found.")
        run_id = latest_run.iloc[0].run_id
    else:
        run_id = MLFLOW_RUN_ID

    # Always clean work dir for GGUF export since Managed Airflow /opt/airflow/dags is read-only
    work_dir = "/tmp"
    merged_model_dir = os.path.join(work_dir, "model_merged")
    llama_cpp_dir = os.path.join(work_dir, "llama.cpp")
    
    # Use model name for artifacts
    model_name = os.getenv("MODEL_ID", "qwen2.5-0.5b").split("/")[-1].lower()
    gguf_f16 = os.path.join(work_dir, f"{model_name}-reminder-bot.gguf")
    gguf_q4 = os.path.join(work_dir, f"{model_name}-reminder-bot-q4_k_m.gguf")

    print(f"Starting GGUF Export for Run: {run_id}")

    # 1. Download llama.cpp if not present (ZIP for environments without git)
    if not os.path.exists(llama_cpp_dir):
        print(f"Downloading llama.cpp into {work_dir}...")
        zip_url = "https://github.com/ggerganov/llama.cpp/archive/refs/heads/master.zip"
        with urllib.request.urlopen(zip_url) as response:
            with zipfile.ZipFile(io.BytesIO(response.read())) as z:
                z.extractall(work_dir)
        # Rename from llama.cpp-master to llama.cpp
        os.rename(os.path.join(work_dir, "llama.cpp-master"), llama_cpp_dir)
    
    # 2. Build llama.cpp with cmake
    print("Building llama.cpp...")
    build_dir = os.path.join(llama_cpp_dir, "build")
    if not os.path.exists(build_dir):
        os.makedirs(build_dir, exist_ok=True)
    
    # Run cmake explicitly in the llama.cpp directory
    subprocess.run(["cmake", "-B", "build"], cwd=llama_cpp_dir, check=True)
    # Build with fewer threads if on c2-m4 to avoid OOM
    subprocess.run(["cmake", "--build", "build", "--config", "Release", "-j2"], cwd=llama_cpp_dir, check=True)

    # 3. Convert to GGUF (f16)
    print(f"Converting HF model to GGUF f16...")
    convert_script = os.path.join(llama_cpp_dir, "convert_hf_to_gguf.py")
    subprocess.run([
        "python3", convert_script, merged_model_dir, 
        "--outfile", gguf_f16, 
        "--outtype", "f16"
    ], check=True)

    # 4. Quantize to q4_k_m
    print(f"Quantizing to Q4_K_M...")
    quantize_bin = os.path.join(llama_cpp_dir, "build", "bin", "llama-quantize")
    subprocess.run([quantize_bin, gguf_f16, gguf_q4, "q4_k_m"], check=True)

    # 5. Upload GGUF artifacts to MLflow (MinIO)
    with mlflow.start_run(run_id=run_id):
        print(f"Uploading GGUF artifacts to MLflow run {run_id}...")
        mlflow.log_artifact(gguf_f16, artifact_path="gguf")
        mlflow.log_artifact(gguf_q4, artifact_path="gguf")
        
    print("GGUF Export Complete and Stored in MinIO.")

if __name__ == "__main__":
    export_gguf()
