from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'model_ab_test_validation',
    default_args=default_args,
    description='A/B test validation of ML models',
    schedule_interval=timedelta(days=1),
    catchup=False
)

validate_model = BashOperator(
    task_id='validate_model_ab_test',
    bash_command='python /opt/airflow/src/ab_test.py --n-iter 2 --cv 2 --sample-frac 0.05 --auto-deploy',
    dag=dag,
    env={
        "MLFLOW_TRACKING_URI": "http://mlflow_server:5000",
        "AWS_ACCESS_KEY_ID": "minio",
        "AWS_SECRET_ACCESS_KEY": "minio123",
        "MLFLOW_S3_ENDPOINT_URL": "http://minio:9000",
        "PYTHONPATH": "/opt/airflow/src",
        "PYSPARK_PYTHON": "/usr/local/bin/python",
        "PYSPARK_DRIVER_PYTHON": "/usr/local/bin/python"
    }
)
