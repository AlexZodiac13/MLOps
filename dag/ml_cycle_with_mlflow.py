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

MLFLOW_TRACKING_URI = Variable.get("mlflow_tracking_uri", default_var="http://localhost:5000")
MLFLOW_S3_ENDPOINT_URL = Variable.get("MLFLOW_S3_ENDPOINT_URL", default_var="https://s3.owgrant.su/otus")
# Using separate keys for MLflow/MinIO as requested
AWS_ACCESS_KEY_ID = Variable.get("MINIO_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = Variable.get("MINIO_SECRET_KEY")

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

    cmd = [
        "python3", "-m", "mlflow", "server",
        "--backend-store-uri", f"sqlite:////tmp/mlflow_{port}.db",
        "--default-artifact-root", "s3://otus/mlflow/artifacts",
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
    Clones or pulls the latest code and data from Git to /tmp/ml_project.
    Returns the final project path.
    """
    target_dir = "/tmp/ml_project"
    print(f"Syncing Git Repo: {GIT_REPO_URL} (branch: {GIT_BRANCH}) to {target_dir}")
    
    if os.path.exists(target_dir):
        print("Target directory exists. Pulling latest changes...")
        try:
            # Re-clone if it's not a git repo or something went wrong
            subprocess.run(["git", "pull"], cwd=target_dir, check=True)
        except Exception as e:
            print(f"Pull failed, re-cloning: {e}")
            subprocess.run(["rm", "-rf", target_dir], check=True)
            subprocess.run(["git", "clone", "--branch", GIT_BRANCH, GIT_REPO_URL, target_dir], check=True)
    else:
        print("Cloning repository...")
        subprocess.run(["git", "clone", "--branch", GIT_BRANCH, GIT_REPO_URL, target_dir], check=True)
    
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
    env["MLFLOW_TRACKING_URI"] = current_uri
    env["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
    env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    if extra_env:
        env.update(extra_env)

    python_path = "python3" 
    
    # Detect the root path
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(dag_dir)
    
    # Priority: 1. Git synced path, 2. Airflow Variable, 3. Relative to DAG
    cwd = git_synced_path or Variable.get("project_root", default_var=project_root)
    full_path = os.path.join(cwd, script_path)

    print(f"Project root used: {cwd}")
    print(f"Running script: {full_path} against {current_uri}")
    result = subprocess.run([python_path, full_path], env=env, capture_output=True, text=True, cwd=cwd)
    
    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
        
    if result.returncode != 0:
        raise Exception(f"Script {script_path} failed with return code {result.returncode}")

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

