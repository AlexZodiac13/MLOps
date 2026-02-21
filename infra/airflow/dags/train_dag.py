from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

if os.path.exists("/opt/airflow/ml_code/train_script.py"):
    # Dev mode: use mounted code
    REPO_DIR = "/opt/airflow/ml_code"
    print(f"Using local development code mounted at {REPO_DIR}")
else:
    # Prod mode: clone from git
    REPO_DIR = "/opt/airflow/repo/ml"
    
ML_HOME = "/opt/airflow/ml" # Artifacts output

MODEL_ID = "Qwen/Qwen2.5-3B-Instruct"

with DAG(
    'train_reminder_bot_cpu',
    default_args=default_args,
    description='End-to-end Pipeline: Train -> Test -> Export GGUF',
    schedule_interval=None,
    catchup=False,
    tags=['llm', 'training', 'cpu', 'gguf'],
) as dag:

    # 1. Clone/Pull Code (Conditioned)
    t1_setup_code = BashOperator(
        task_id='setup_codebase',
        bash_command=f"""
        # If we are in dev mode (REPO_DIR mounted), skip git clone entirely
        if [[ "{REPO_DIR}" == "/opt/airflow/ml_code" ]]; then
            echo "Dev Mode: Using local code at {REPO_DIR}. Skipping git clone."
            exit 0
        fi

        echo "Setting up repository at {REPO_DIR}..."
        # Используем переменные окружения, переданные в Airflow
        # Если переменная не задана, упадем с ошибкой, чтобы не клонировать дефолтный репо
        GIT_URL=$GIT_REPO_URL
        BRANCH=$GIT_BRANCH
        
        if [ -z "$GIT_URL" ]; then
            echo "ERROR: GIT_REPO_URL environment variable is not set"
            exit 1
        fi

        if [ -z "$BRANCH" ]; then
            echo "WARNING: GIT_BRANCH is not set, defaulting to 'main'"
            BRANCH="main"
        fi
        
        if [ ! -d "{REPO_DIR}/.git" ]; then
            echo "Cloning $GIT_URL (branch: $BRANCH)..."
            git clone -b $BRANCH $GIT_URL {REPO_DIR}
        else
            echo "Updating repository..."
            cd {REPO_DIR}
            git fetch origin
            git reset --hard origin/$BRANCH
            git checkout $BRANCH
            git pull origin $BRANCH
        fi
        """
    )
    
    # Create environment with AWS credentials
    env_vars = os.environ.copy()
    env_vars['PYTHONUNBUFFERED'] = '1'
    
    # 2. Train (CPU Compatible)
    t2_train = BashOperator(
        task_id='train_model',
        bash_command=f"""
        # If running from local mount (REPO_DIR == /opt/airflow/ml_code), prevent git commands from failing
        if [[ "{REPO_DIR}" == "/opt/airflow/ml_code" ]]; then
            echo "Skipping git operations for local dev mount..."
            cd {REPO_DIR}
        else
            echo "Using cloned repository at {REPO_DIR}..."
            cd {REPO_DIR}
        fi

        python3 train_script.py \\
          --data_path {REPO_DIR}/labeled_dataset.json \\
          --output_dir {ML_HOME}/results \\
          --epochs 1 \\
          --model_id "{MODEL_ID}"
        """,
        env=env_vars,
        execution_timeout=timedelta(hours=12) # CPU training is slow
    )

    # 3. Test
    t3_test = BashOperator(
        task_id='test_model',
        bash_command=f"""
        cd {REPO_DIR}/ml && \
        python3 test_script.py \
          --model_id "{MODEL_ID}" \
          --adapter_path {ML_HOME}/results/final_adapter \
          --test_data {REPO_DIR}/ml/labeled_dataset.json
        """,
        env=env_vars
    )

    # 4. Export to GGUF
    t4_export = BashOperator(
        task_id='export_gguf',
        bash_command=f"""
        cd {REPO_DIR}/ml && \
        python3 export_gguf.py \
          --model_id "{MODEL_ID}" \
          --adapter_path {ML_HOME}/results/final_adapter \
          --output_dir {ML_HOME}/results
        """,
        env=env_vars,
        execution_timeout=timedelta(hours=2)
    )

    # 5. Compare & Register Model
    t5_compare = BashOperator(
        task_id='compare_and_register',
        bash_command=f"""
        cd {REPO_DIR}/ml && \
        python3 compare_script.py
        """,
        env=env_vars
    )

    t1_setup_code >> t2_train >> t3_test >> t4_export >> t5_compare
