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

WORKDIR = REPO_DIR
    
ML_HOME = "/opt/airflow/ml" # Artifacts output

MODEL_ID = "Qwen/Qwen2.5-3B-Instruct"
TRAIN_EPOCHS = int(os.getenv("TRAIN_EPOCHS", "3"))
# These limits are for train-time HF evaluation in Airflow worker,
# not for production GGUF service latency/RAM.
MIN_WEIGHTED_F1 = float(os.getenv("MIN_WEIGHTED_F1", "0.78"))
MAX_P95_LATENCY_SEC = float(os.getenv("MAX_P95_LATENCY_SEC", "6.0"))
MAX_RAM_MB = float(os.getenv("MAX_RAM_MB", "8192"))
MIN_JSON_VALID_RATE = float(os.getenv("MIN_JSON_VALID_RATE", "0.95"))
MIN_EXACT_MATCH_RATE = float(os.getenv("MIN_EXACT_MATCH_RATE", "0.60"))
QUALITY_GATE_STRICT = os.getenv("QUALITY_GATE_STRICT", "false")
EVAL_MAX_NEW_TOKENS = int(os.getenv("EVAL_MAX_NEW_TOKENS", "128"))
MODEL_TARGET_STAGE = os.getenv("MODEL_TARGET_STAGE", "Production")
COMPARE_FAIL_IF_SKIPPED = os.getenv("COMPARE_FAIL_IF_SKIPPED", "false")
SELECT_BEST_AVAILABLE = os.getenv("SELECT_BEST_AVAILABLE", "true")

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
                    --epochs {TRAIN_EPOCHS} \\
          --model_id "{MODEL_ID}" \\
          --run_id_file {ML_HOME}/last_run_id.txt
        """,
        env=env_vars,
        execution_timeout=timedelta(hours=12) # CPU training is slow
    )

    # 3. Test
    t3_test = BashOperator(
        task_id='test_model',
        bash_command=f"""
        cd {WORKDIR}
        
        python3 test_script.py \\
          --model_id "{MODEL_ID}" \\
          --adapter_path {ML_HOME}/results/final_adapter \\
          --holdout_data {ML_HOME}/results/holdout_dataset.json \\
          --golden_data {REPO_DIR}/labeled_dataset_golden.json \\
                    --max_new_tokens {EVAL_MAX_NEW_TOKENS} \\
          --run_id_file {ML_HOME}/last_run_id.txt
        """,
        env=env_vars
    )

    t3_quality_gate = BashOperator(
        task_id='quality_gate',
        bash_command=f"""
        cd {WORKDIR}

                                echo "Quality gate thresholds: min_weighted_f1={MIN_WEIGHTED_F1}, max_p95_latency_sec={MAX_P95_LATENCY_SEC}, max_ram_mb={MAX_RAM_MB}, min_json_valid_rate={MIN_JSON_VALID_RATE}, min_exact_match_rate={MIN_EXACT_MATCH_RATE}, strict_mode={QUALITY_GATE_STRICT}"

        python3 check_quality.py \\
          --run_id_file {ML_HOME}/last_run_id.txt \\
          --min_weighted_f1 {MIN_WEIGHTED_F1} \\
          --max_latency_p95_sec {MAX_P95_LATENCY_SEC} \\
                    --max_ram_mb {MAX_RAM_MB} \\
                                        --min_json_valid_rate {MIN_JSON_VALID_RATE} \\
                                        --min_exact_match_rate {MIN_EXACT_MATCH_RATE} \\
                    --strict_mode {QUALITY_GATE_STRICT}
        """,
        env=env_vars
    )

    # 4. Export to GGUF
    t4_export = BashOperator(
        task_id='export_gguf',
        bash_command=f"""
        cd {WORKDIR}

        python3 export_gguf.py \\
          --model_id "{MODEL_ID}" \\
          --adapter_path {ML_HOME}/results/final_adapter \\
          --output_dir {ML_HOME}/results \\
          --run_id_file {ML_HOME}/last_run_id.txt
        """,
        env=env_vars,
        execution_timeout=timedelta(hours=2)
    )

    # 5. Compare & Register Model
    t5_compare = BashOperator(
        task_id='compare_and_register',
        bash_command=f"""
        cd {WORKDIR}

        python3 compare_script.py \\
          --metric quality_weighted_f1 \\
          --latency_metric quality_p95_latency_sec \\
          --min_weighted_f1 {MIN_WEIGHTED_F1} \\
          --max_latency_p95_sec {MAX_P95_LATENCY_SEC} \\
          --max_ram_mb {MAX_RAM_MB} \\
                    --min_json_valid_rate {MIN_JSON_VALID_RATE} \\
                    --min_exact_match_rate {MIN_EXACT_MATCH_RATE} \\
                    --target_stage {MODEL_TARGET_STAGE} \\
                                        --select_best_available {SELECT_BEST_AVAILABLE} \\
                                        --fail_if_skipped {COMPARE_FAIL_IF_SKIPPED} \\
          --run_id_file {ML_HOME}/last_run_id.txt
        """,
        env=env_vars
    )

    t1_setup_code >> t2_train >> t3_test >> t3_quality_gate >> t4_export >> t5_compare
