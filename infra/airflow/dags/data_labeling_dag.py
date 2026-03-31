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
WORKDIR = "/opt/airflow/ml_code"

# Модель для лейблинга — Qwen2.5-7B-Instruct (4-bit)
# Хорошо работает на RTX 4070 Super (12GB VRAM)
LABELING_MODEL_ID = "Qwen/Qwen2.5-7B-Instruct"

# Множитель данных (аугментация)
DATA_MULTIPLY = int(os.getenv("DATA_MULTIPLY", "30"))
DATA_MULTIPLY_SEED = int(os.getenv("DATA_MULTIPLY_SEED", "42"))

# Тестовый режим (ограничение количества данных для отладки)
LABELING_LIMIT = int(os.getenv("LABELING_LIMIT", "100"))  # 0 = без лимита

with DAG(
    'data_labeling_pipeline',
    default_args=default_args,
    description='Data Augmentation + Labeling with Teacher Model',
    schedule_interval=None,
    catchup=False,
    tags=['llm', 'data', 'labeling', 'gpu'],
) as dag:

    # Генерация версии на основе execution_date (консистентна для всех задач)
    DATA_VERSION = "{{ execution_date.strftime('%Y%m%d_%H%M%S') }}"

    # 0. Скачивание исходных данных из S3
    t0_download = BashOperator(
        task_id='download_src_data',
        bash_command=f"""
        echo "Downloading source data from S3..."
        python3 -c "
import boto3
import os

s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get('MLFLOW_S3_ENDPOINT_URL'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
)

bucket = os.environ.get('MLFLOW_S3_BUCKET')
s3_key = 'src_data/user_messages.csv'
local_file = '{ML_HOME}/src_data/user_messages.csv'

os.makedirs(os.path.dirname(local_file), exist_ok=True)
print(f'Downloading s3://{{bucket}}/{{s3_key}} to {{local_file}}')
s3_client.download_file(bucket, s3_key, local_file)
print('Download complete!')
"
        """,
    )

    # 1. Аугментация данных (умножение с шумом)
    t1_augment = BashOperator(
        task_id='augment_data',
        bash_command=f"""
        cd {WORKDIR}
        
        echo "Augmenting data from user_messages.csv..."
        python3 generet_data_set.py \\
          --input {ML_HOME}/src_data/user_messages.csv \\
          --output {ML_HOME}/src_data/user_messages_augmented.csv \\
          --multiply {DATA_MULTIPLY} \\
          --seed {DATA_MULTIPLY_SEED}
        
        echo "Augmentation complete. Rows:"
        wc -l {ML_HOME}/src_data/user_messages_augmented.csv
        """,
    )

    # 2. Лейблинг данных большой моделью
    t2_label = BashOperator(
        task_id='label_data',
        bash_command=f"""
        cd {WORKDIR}
        
        echo "Labeling augmented data with {LABELING_MODEL_ID}..."
        python3 label_data_script.py \\
          --input {ML_HOME}/src_data/user_messages_augmented.csv \\
          --output {ML_HOME}/labeled_dataset_{DATA_VERSION}.json \\
          --model_id "{LABELING_MODEL_ID}" \\
          {"--limit " + str(LABELING_LIMIT) if LABELING_LIMIT > 0 else ""}
        
        echo "Labeling complete. Output size:"
        ls -lh {ML_HOME}/labeled_dataset_{DATA_VERSION}.json
        """,
        execution_timeout=timedelta(hours=1) if LABELING_LIMIT > 0 else timedelta(hours=6)
    )

    # 3. Загрузка в S3
    t3_upload = BashOperator(
        task_id='upload_to_s3',
        bash_command=f"""
        cd {ML_HOME}

        echo "Checking if labeled dataset exists..."
        if [ ! -f "{ML_HOME}/labeled_dataset_{DATA_VERSION}.json" ]; then
            echo "ERROR: File not found: {ML_HOME}/labeled_dataset_{DATA_VERSION}.json"
            ls -la {ML_HOME}/
            exit 1
        fi

        echo "Uploading labeled dataset to S3..."
        python3 -c "
import boto3
import os
from pathlib import Path

s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get('MLFLOW_S3_ENDPOINT_URL'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
)

bucket = os.environ.get('MLFLOW_S3_BUCKET')
local_file = '{ML_HOME}/labeled_dataset_{DATA_VERSION}.json'
s3_key = 'labeled_data/labeled_dataset_{DATA_VERSION}.json'

print(f'Uploading {{local_file}} to s3://{{bucket}}/{{s3_key}}')
s3_client.upload_file(local_file, bucket, s3_key)
print('Upload complete!')
"
        """,
    )

    # 4. Обновление latest ссылки
    t4_latest = BashOperator(
        task_id='update_latest_pointer',
        bash_command=f"""
        cd {ML_HOME}

        echo "Creating latest pointer..."
        python3 -c "
import boto3
import os

s3_client = boto3.client(
    's3',
    endpoint_url=os.environ.get('MLFLOW_S3_ENDPOINT_URL'),
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
)

bucket = os.environ.get('MLFLOW_S3_BUCKET')
latest_key = 'labeled_data/labeled_dataset_latest.json'
versioned_key = 'labeled_data/labeled_dataset_{DATA_VERSION}.json'

print(f'Copying s3://{{bucket}}/{{versioned_key}} to s3://{{bucket}}/{{latest_key}}')
s3_client.copy_object(
    Bucket=bucket,
    CopySource={{'Bucket': bucket, 'Key': versioned_key}},
    Key=latest_key
)
print('Latest pointer updated!')
"
        """,
    )

    t0_download >> t1_augment >> t2_label >> t3_upload >> t4_latest
