from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import os
import subprocess
import time
import signal

# --- Configuration ---
# Environment variables for MLflow and MinIO (S3)
# These are used by both the MLflow server and the training/testing scripts.

MLFLOW_TRACKING_URI = Variable.get("mlflow_tracking_uri", default_var="http://localhost:5000").strip()
MLFLOW_S3_ENDPOINT_URL = Variable.get("MLFLOW_S3_ENDPOINT_URL", default_var="https://s3.owgrant.su").strip()
BUCKET_NAME = Variable.get("MLFLOW_S3_BUCKET", default_var="otus").strip()

# Cleanup: Ensure the endpoint doesn't contain the bucket name (common mistake)
if MLFLOW_S3_ENDPOINT_URL.endswith(f"/{BUCKET_NAME}"):
    MLFLOW_S3_ENDPOINT_URL = MLFLOW_S3_ENDPOINT_URL.replace(f"/{BUCKET_NAME}", "")
elif MLFLOW_S3_ENDPOINT_URL.endswith("/"):
    MLFLOW_S3_ENDPOINT_URL = MLFLOW_S3_ENDPOINT_URL[:-1]

MLFLOW_S3_IGNORE_TLS = Variable.get("MLFLOW_S3_IGNORE_TLS", default_var="false").strip()
# Using separate keys for MLflow/MinIO as requested (with defaults to avoid crash if they aren't loaded yet)
AWS_ACCESS_KEY_ID = Variable.get("MINIO_ACCESS_KEY", default_var="fake_access_key").strip()
AWS_SECRET_ACCESS_KEY = Variable.get("MINIO_SECRET_KEY", default_var="fake_secret_key").strip()
AWS_DEFAULT_REGION = Variable.get("AWS_DEFAULT_REGION", default_var="us-east-1").strip() # Region for MinIO sig verification

# Git configuration for data/code sync
GIT_REPO_URL = Variable.get("git_repo_url", default_var="https://github.com/AlexZodiac13/MLOps.git")
GIT_BRANCH = Variable.get("git_branch", default_var="project-work")

# Compute base path for Managed Airflow environment
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(DAG_DIR) # /opt/airflow/dags/
MLFLOW_PROJECT_DIR = os.path.join(PROJECT_ROOT, "ml")

# Airflow default arguments
default_args = {
    'owner': 'mlops_engineer',
    'start_date': days_ago(1),
    'retries': 0,
}

def start_mlflow_server(**kwargs):
    """
    Starts an MLflow tracking server in the background.
    Storing the process ID (PID) and URI in XCom.
    """
    # Try to kill any of our previous mlflow instances
    try:
        print("Attempting to kill previous MLflow instances...")
        subprocess.run(["pkill", "-u", os.getlogin(), "-f", "mlflow server"], check=False)
        time.sleep(2)
    except Exception as e:
        print(f"pkill failed (non-critical): {e}")

    # Find an available port
    import socket
    port = 5000
    while port < 5050:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(('127.0.0.1', port)) != 0:
                break
            print(f"Port {port} is busy, trying next...")
            port += 1
    
    tracking_uri = f"http://127.0.0.1:{port}"
    print(f"Selected Port: {port}")

    # Environment for MLflow server
    env = os.environ.copy()
    env["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
    env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    env["AWS_DEFAULT_REGION"] = AWS_DEFAULT_REGION
    env["MLFLOW_S3_IGNORE_TLS"] = MLFLOW_S3_IGNORE_TLS
    env["AWS_S3_ADDRESSING_STYLE"] = "path"
    env["MLFLOW_S3_BUCKET"] = BUCKET_NAME
    env["AWS_S3_SIGNATURE_VERSION"] = "s3v4"
    env["AWS_S3_ADDRESSING_STYLE"] = "path"

    cmd = [
        "python3", "-m", "mlflow", "server",
        "--backend-store-uri", f"sqlite:////tmp/mlflow_{port}.db",
        "--default-artifact-root", f"s3://{BUCKET_NAME}/mlflow/artifacts",
        "--host", "0.0.0.0",
        "--port", str(port)
    ]
    
    print(f"Starting MLflow server: {' '.join(cmd)}")
    log_file = open(f"/tmp/mlflow_server_{port}.log", "w")
    process = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    
    time.sleep(15)
    
    if process.poll() is not None:
        log_file.close()
        with open(f"/tmp/mlflow_server_{port}.log", "r") as f:
            print("MLflow server log output:\n", f.read())
        raise Exception(f"MLflow server failed to start on port {port}. Exit code: {process.returncode}")
        
    print(f"MLflow server started with PID: {process.pid} at {tracking_uri}")
    
    # Push both PID and the actual URI to XCom
    kwargs['ti'].xcom_push(key='mlflow_pid', value=process.pid)
    kwargs['ti'].xcom_push(key='mlflow_uri', value=tracking_uri)
    return tracking_uri

def stop_mlflow_server(**kwargs):
    """
    Kills the MLflow server process using the PID from setup task.
    """
    ti = kwargs['ti']
    pid = ti.xcom_pull(task_ids='setup_mlflow', key='mlflow_pid')
    if pid:
        print(f"Stopping MLflow server with PID: {pid}")
        try:
            os.kill(pid, signal.SIGTERM)
            print("MLflow server stopped.")
        except ProcessLookupError:
            print("Process not found.")
    else:
        print("No PID found in XCom.")

def sync_git(**kwargs):
    """
    Downloads the latest code from GitHub as a ZIP archive (bypassing git binary permissions).
    """
    import urllib.request
    import zipfile
    import io
    import shutil

    target_dir = "/tmp/ml_project"
    # Construct GitHub ZIP URL: https://github.com/USER/REPO/archive/refs/heads/BRANCH.zip
    # Support both https://github.com/user/repo and git@github.com:user/repo.git
    clean_url = GIT_REPO_URL.replace(".git", "").replace("git@github.com:", "https://github.com/")
    zip_url = f"{clean_url}/archive/refs/heads/{GIT_BRANCH}.zip"
    
    print(f"Downloading project source from: {zip_url}")
    
    if os.path.exists(target_dir):
        shutil.rmtree(target_dir)
    
    try:
        with urllib.request.urlopen(zip_url) as response:
            with zipfile.ZipFile(io.BytesIO(response.read())) as zip_ref:
                # GitHub zip contains a top-level folder like MLOps-project-work
                namelist = zip_ref.namelist()
                top_folder = namelist[0].split('/')[0]
                zip_ref.extractall("/tmp")
                # Move contents to target_dir
                shutil.move(os.path.join("/tmp", top_folder), target_dir)
        print(f"Project synced successfully to {target_dir}")
    except Exception as e:
        print(f"Sync failed: {e}")
        raise
    
    return target_dir

def run_script(script_path, extra_env=None, **kwargs):
    """
    Helper to run training/testing scripts as a separate process.
    """
    ti = kwargs['ti']
    # Get the actual URI from the setup task
    current_uri = ti.xcom_pull(task_ids='setup_mlflow', key='mlflow_uri') or MLFLOW_TRACKING_URI
    
    # Check if we have a fresh project root from sync_git
    git_synced_path = ti.xcom_pull(task_ids='sync_git')
    
    env = os.environ.copy()
    env["MLFLOW_TRACKING_URI"] = str(current_uri).strip()
    
    # Extra S3 env cleanup to fix SignatureDoesNotMatch
    env["MLFLOW_S3_ENDPOINT_URL"] = str(MLFLOW_S3_ENDPOINT_URL).strip()
    env["AWS_ACCESS_KEY_ID"] = str(AWS_ACCESS_KEY_ID).strip()
    env["AWS_SECRET_ACCESS_KEY"] = str(AWS_SECRET_ACCESS_KEY).strip()
    env["MLFLOW_S3_BUCKET"] = str(BUCKET_NAME).strip()
    env["AWS_DEFAULT_REGION"] = str(AWS_DEFAULT_REGION).strip()
    env["MLFLOW_S3_IGNORE_TLS"] = str(MLFLOW_S3_IGNORE_TLS).strip()
    # This specifically fixes many MinIO issues with signature V4 vs V2
    env["AWS_S3_ADDRESSING_STYLE"] = "path"
    env["AWS_S3_SIGNATURE_VERSION"] = "s3v4"
    env["S3_USE_SIGV4"] = "True"
    env["AWS_SESSION_TOKEN"] = ""

    if extra_env:
        env.update({k: str(v).strip() for k, v in extra_env.items()})

    python_path = "python3" 
    
    # Detect the root path
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(dag_dir)
    
    # Priority: 1. Git synced path, 2. Airflow Variable, 3. Relative to DAG
    cwd = git_synced_path or Variable.get("project_root", default_var=project_root)
    full_path = os.path.join(cwd, script_path)

    # Double check endpoint doesn't have bucket in it
    clean_endpoint = env["MLFLOW_S3_ENDPOINT_URL"].rstrip("/")
    if clean_endpoint.endswith(f"/{env['MLFLOW_S3_BUCKET']}"):
        env["MLFLOW_S3_ENDPOINT_URL"] = clean_endpoint.replace(f"/{env['MLFLOW_S3_BUCKET']}", "")

    print(f"Project root used: {cwd}")
    print(f"Running script: {full_path} against {current_uri}")
    
    # Use Popen to stream logs in real-time
    with subprocess.Popen(
        [python_path, "-u", full_path], 
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=cwd,
        bufsize=1
    ) as process:
        for line in process.stdout:
            print(line, end="")
        
        process.wait()
        if process.returncode != 0:
            raise Exception(f"Script {script_path} failed with return code {process.returncode}")

# --- DAG Definition ---
with DAG(
    'mlflow_model_cycle',
    default_args=default_args,
    description='Cycle: Scale up MLflow -> Train -> Test -> Tear down. Store in MinIO.',
    schedule_interval=None,
    catchup=False,
    tags=['mlflow', 'mlops', 'training'],
) as dag:

    # 0. Install missing dependencies on worker
    install_deps = BashOperator(
        task_id='install_dependencies',
        bash_command='pip install torch transformers datasets accelerate peft bitsandbytes trl scikit-learn mlflow boto3 cmake llama-cpp-python'
    )

    # 1. Sync Latest Code and Data from Git
    # We do this at runtime to ensure we have the absolute latest labeled_dataset.json 
    # and training script versions.
    sync_git_task = PythonOperator(
        task_id='sync_git',
        python_callable=sync_git,
    )

    # 2. Setup MLflow Tracking Server (Lifecycle start)
    setup_mlflow = PythonOperator(
        task_id='setup_mlflow',
        python_callable=start_mlflow_server,
    )

    # 3. Train Model
    train_model = PythonOperator(
        task_id='train_model',
        python_callable=run_script,
        op_kwargs={'script_path': 'ml/train_mlflow.py'}
    )

    # 4. Export to GGUF (Optimization for target hardware)
    export_gguf = PythonOperator(
        task_id='export_gguf',
        python_callable=run_script,
        op_kwargs={'script_path': 'ml/export_gguf_mlflow.py'}
    )

    # 5. Test/Evaluate Model
    test_model = PythonOperator(
        task_id='test_model',
        python_callable=run_script,
        op_kwargs={'script_path': 'ml/test_mlflow.py'}
    )

    # 6. Compare Results (Dummy compare or search MLflow)
    def compare_runs(**kwargs):
        # In a real scenario, we'd query MLflow for the best run in the experiment
        # and compare current metrics.
        # Here we just log success.
        print("Comparison logic: Current run is successful.")
        return "Success"

    compare_results = PythonOperator(
        task_id='compare_results',
        python_callable=compare_runs,
    )

    # 7. Teardown MLflow Server (Lifecycle end)
    # Using trigger_rule='all_done' to ensure it runs even if training fails
    teardown_mlflow = PythonOperator(
        task_id='teardown_mlflow',
        python_callable=stop_mlflow_server,
        trigger_rule='all_done', 
    )

    # 8. Cleanup local results
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='rm -rf /opt/airflow/results /opt/airflow/model_merged /tmp/ml_project',
        trigger_rule='all_done'
    )

    # Dependency Chain
    install_deps >> sync_git_task >> setup_mlflow >> train_model >> export_gguf >> test_model >> compare_results >> teardown_mlflow >> cleanup

