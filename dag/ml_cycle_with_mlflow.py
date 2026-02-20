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
MLFLOW_PROJECT_DIR = "/opt/airflow/ml" # Current working directory for scripts

# Airflow default arguments
default_args = {
    'owner': 'mlops_engineer',
    'start_date': days_ago(1),
    'retries': 0,
}

def start_mlflow_server(**kwargs):
    """
    Starts an MLflow tracking server in the background.
    Storing the process ID (PID) in XCom to kill it later.
    """
    # Environment for MLflow server (important for artifact store)
    env = os.environ.copy()
    env["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
    env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

    # Tracking storage: Using local sqlite for experiment metadata (volatile if ephemeral)
    # Artifact storage: Using MinIO (S3 bucket: otus)
    # Using 'python3 -m mlflow' to avoid Permission Denied on the binary
    cmd = [
        "python3", "-m", "mlflow", "server",
        "--backend-store-uri", "sqlite:////tmp/mlflow.db",
        "--default-artifact-root", "s3://otus/mlflow/artifacts", # 'otus' is the bucket
        "--host", "0.0.0.0",
        "--port", "5000"
    ]
    
    print(f"Starting MLflow server: {' '.join(cmd)}")
    
    # Redirect output to a log file in /tmp to see why it fails
    log_file = open("/tmp/mlflow_server.log", "w")
    process = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    
    # Give it a few seconds to boot up
    time.sleep(15)
    
    if process.poll() is not None:
        log_file.close()
        with open("/tmp/mlflow_server.log", "r") as f:
            content = f.read()
            print("MLflow server log output:", flush=True)
            print(content, flush=True)
        raise Exception(f"MLflow server failed to start. Exit code: {process.returncode}")
        
    print(f"MLflow server started with PID: {process.pid}", flush=True)
    return process.pid

def stop_mlflow_server(**kwargs):
    """
    Kills the MLflow server process using the PID from setup task.
    """
    ti = kwargs['ti']
    pid = ti.xcom_pull(task_ids='setup_mlflow')
    if pid:
        print(f"Stopping MLflow server with PID: {pid}")
        try:
            os.kill(pid, signal.SIGTERM)
            print("MLflow server stopped.")
        except ProcessLookupError:
            print("Process not found.")
    else:
        print("No PID found to stop.")

def run_script(script_path, extra_env=None):
    """
    Helper to run training/testing scripts as a separate process.
    """
    env = os.environ.copy()
    env["MLFLOW_TRACKING_URI"] = MLFLOW_TRACKING_URI
    env["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
    env["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    env["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY
    if extra_env:
        env.update(extra_env)

    # Use the same python as Airflow worker
    python_path = "python3" 
    
    # Run from the project root directory
    cwd = Variable.get("project_root", default_var="/opt/airflow/")
    full_path = os.path.join(cwd, script_path)

    print(f"Running script: {full_path}")
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

    # 1. Setup MLflow Tracking Server (Lifecycle start)
    setup_mlflow = PythonOperator(
        task_id='setup_mlflow',
        python_callable=start_mlflow_server,
    )

    # 2. Train Model
    train_model = PythonOperator(
        task_id='train_model',
        python_callable=run_script,
        op_kwargs={'script_path': 'ml/train_mlflow.py'}
    )

    # 3. Export to GGUF (Optimization for target hardware)
    export_gguf = PythonOperator(
        task_id='export_gguf',
        python_callable=run_script,
        op_kwargs={'script_path': 'ml/export_gguf_mlflow.py'}
    )

    # 4. Test/Evaluate Model
    test_model = PythonOperator(
        task_id='test_model',
        python_callable=run_script,
        op_kwargs={'script_path': 'ml/test_mlflow.py'}
    )

    # 5. Compare Results (Dummy compare or search MLflow)
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

    # 6. Teardown MLflow Server (Lifecycle end)
    # Using trigger_rule='all_done' to ensure it runs even if training fails
    teardown_mlflow = PythonOperator(
        task_id='teardown_mlflow',
        python_callable=stop_mlflow_server,
        trigger_rule='all_done', 
    )

    # 7. Cleanup local results
    cleanup = BashOperator(
        task_id='cleanup',
        bash_command='rm -rf /opt/airflow/results /opt/airflow/model_merged',
        trigger_rule='all_done'
    )

    # Dependency Chain
    setup_mlflow >> train_model >> export_gguf >> test_model >> compare_results >> teardown_mlflow >> cleanup

