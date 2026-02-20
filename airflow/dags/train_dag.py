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

ML_HOME = "/opt/airflow/ml"
# When cloning inside container, we should clone into ML_HOME or a subdir
REPO_DIR = "/opt/airflow/repo"
MODEL_ID = "Qwen/Qwen2.5-3B-Instruct"

with DAG(
    'train_reminder_bot_cpu',
    default_args=default_args,
    description='End-to-end Pipeline: Train -> Test -> Export GGUF',
    schedule_interval=None,
    catchup=False,
    tags=['llm', 'training', 'cpu', 'gguf'],
) as dag:

    # 1. Clone/Pull Code
    # Assuming the repo contains 'ml/' folder with scripts and dataset
    # If not present, clone. If present, pull.
    # We use environment variables for repo config
    t1_setup_code = BashOperator(
        task_id='setup_codebase',
        bash_command=f"""
        echo "Setting up repository at {REPO_DIR}..."
        GIT_URL=${{GIT_REPO_URL:-"https://github.com/your-default/repo.git"}}
        BRANCH=${{GIT_BRANCH:-"main"}}
        
        if [ ! -d "{REPO_DIR}/.git" ]; then
            echo "Cloning $GIT_URL..."
            git clone -b $BRANCH $GIT_URL {REPO_DIR}
        else
            echo "Pulling latest changes..."
            cd {REPO_DIR} && git pull origin $BRANCH
        fi
        
        # Ensure scripts are executable or just run via python
        ls -la {REPO_DIR}/ml
        """
    )
    
    # 2. Train (CPU Compatible)
    # The script now auto-detects CPU/GPU.
    # We point to the cloned repo paths: {REPO_DIR}/ml/train_script.py
    t2_train = BashOperator(
        task_id='train_model',
        bash_command=f"""
        cd {REPO_DIR}/ml && \
        python3 train_script.py \
          --data_path {REPO_DIR}/ml/labeled_dataset.json \
          --output_dir {ML_HOME}/results \
          --epochs 1 \
          --model_id "{MODEL_ID}"
        """,
        env={
            'PYTHONUNBUFFERED': '1'
        },
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
        env={
            'PYTHONUNBUFFERED': '1'
        }
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
        env={
            'PYTHONUNBUFFERED': '1'
        },
        execution_timeout=timedelta(hours=2)
    )

    t1_setup_code >> t2_train >> t3_test >> t4_export
