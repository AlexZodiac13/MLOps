"""
DAG: fraud_detection_training
Description: DAG for periodic training of fraud detection model with Dataproc and PySpark.
"""

import uuid
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.settings import Session
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator
)

# Общие переменные для вашего облака
YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

# Переменные для подключения к Object Storage
S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")
S3_INPUT_DATA_BUCKET = f"s3a://{S3_BUCKET_NAME}/input_data"     # Путь к данным
S3_OUTPUT_MODEL_BUCKET = f"s3a://{S3_BUCKET_NAME}/models"       # Путь для сохранения моделей
S3_SRC_BUCKET = f"s3a://{S3_BUCKET_NAME}/src"                   # Путь к исходному коду
S3_DP_LOGS_BUCKET = f"s3a://{S3_BUCKET_NAME}/airflow_logs/"     # Путь для логов Data Proc
S3_VENV_ARCHIVE = f"s3a://{S3_BUCKET_NAME}/venvs/venv.tar.gz"

# Переменные необходимые для создания Dataproc кластера
DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
DP_SECURITY_GROUP_ID = Variable.get("DP_SECURITY_GROUP_ID")

# MLflow переменные
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_NAME = "fraud_detection"

# Создание подключения для Object Storage
YC_S3_CONNECTION = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host=S3_ENDPOINT_URL,
    extra={
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
        "host": S3_ENDPOINT_URL,
    },
)
# Создание подключения для Dataproc
YC_SA_CONNECTION = Connection(
    conn_id="yc-sa",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY,
        "extra__yandexcloud__service_account_json": DP_SA_JSON,
    },
)

# Проверка наличия подключений в Airflow
# Если подключения отсутствуют, то они добавляются
# и сохраняются в базе данных Airflow
# Подключения используются для доступа к Object Storage и Dataproc
def setup_airflow_connections(*connections: Connection) -> None:
    """
    Check and add missing connections to Airflow.

    Parameters
    ----------
    *connections : Connection
        Variable number of Airflow Connection objects to verify and add

    Returns
    -------
    None
    """
    session = Session()
    try:
        for conn in connections:
            print("Checking connection:", conn.conn_id)
            if not session.query(Connection).filter(Connection.conn_id == conn.conn_id).first():
                session.add(conn)
                print("Added connection:", conn.conn_id)
        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


# Функция для выполнения setup_airflow_connections в рамках оператора
def run_setup_connections(**kwargs): # pylint: disable=unused-argument
    """Создает подключения внутри оператора"""
    setup_airflow_connections(YC_S3_CONNECTION, YC_SA_CONNECTION)
    return True


# Настройки DAG
default_args = {
    'owner': 'NickOsipov',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="training_pipeline",
    default_args=default_args,
    description="Periodic training of fraud detection model",
    schedule_interval=timedelta(minutes=60),  # Запуск каждые 60 минут
    start_date=datetime(2025, 3, 27),
    catchup=False,
    tags=['mlops', ],
) as dag:
    # Задача для создания подключений
    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=run_setup_connections,
    )

    # Создание Dataproc кластера
    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="spark-cluster-create-task",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-dp-training-{uuid.uuid4()}",
        cluster_description="YC Temporary cluster for model training",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=S3_DP_LOGS_BUCKET,
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone=YC_ZONE,
        cluster_image_version="2.0",

        # masternode
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=50,

        # datanodes
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=50,
        datanode_count=1,

        # computenodes
        computenode_count=0,

        # software
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],
        connection_id=YC_SA_CONNECTION.conn_id,
        dag=dag,
    )

    # Запуск PySpark задания для обучения модели
    train_model = DataprocCreatePysparkJobOperator(
        task_id="train",
        main_python_file_uri=f"{S3_SRC_BUCKET}/train.py",
        connection_id=YC_SA_CONNECTION.conn_id,
        dag=dag,
        args=[
            "--input", f"{S3_INPUT_DATA_BUCKET}/train.csv",
            "--output", f"{S3_OUTPUT_MODEL_BUCKET}/model_{datetime.now().strftime('%Y%m%d')}",
            "--tracking-uri", MLFLOW_TRACKING_URI,
            "--experiment-name", MLFLOW_EXPERIMENT_NAME,
            "--auto-register",  # Включаем автоматическую регистрацию лучшей модели
            "--s3-endpoint-url", S3_ENDPOINT_URL,
            "--s3-access-key", S3_ACCESS_KEY,
            "--s3-secret-key", S3_SECRET_KEY,
            "--run-name", f"training_{datetime.now().strftime('%Y%m%d_%H%M')}"
        ],
        properties={
            'spark.submit.deployMode': 'cluster',
            'spark.yarn.dist.archives': f'{S3_VENV_ARCHIVE}#.venv',
            'spark.yarn.appMasterEnv.PYSPARK_PYTHON': './.venv/bin/python3',
            'spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON': './.venv/bin/python3',
        },
    )

    # Удаление Dataproc кластера
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="spark-cluster-delete-task",
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    # Определение последовательности выполнения задач
    # pylint: disable=pointless-statement
    setup_connections >> create_spark_cluster >> train_model >> delete_spark_cluster
