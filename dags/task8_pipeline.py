from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


NETWORK = 'mlops-task8_default'


with DAG(
  dag_id='task8_streaming_pipeline',
  start_date=datetime(2024, 1, 1),
  schedule=None,
  catchup=False,
  tags=['task8'],
):
  register_model = DockerOperator(
    task_id='register_model_mlflow',
    image='mlops-task8-spark:latest',
    network_mode=NETWORK,
    auto_remove=True,
    command=(
      'bash -lc '
      '"python3 /opt/app/src/register_model_mlflow.py '
      '--model-path /opt/app/data/MNISTClassifier.pt '
      '--model-name mnist --alias champion"'
    ),
    environment={
      'MLFLOW_TRACKING_URI': 'http://mlflow:5000',
      'MLFLOW_S3_ENDPOINT_URL': 'http://minio:9000',
      'AWS_ACCESS_KEY_ID': 'minio',
      'AWS_SECRET_ACCESS_KEY': 'minio123456',
      'AWS_DEFAULT_REGION': 'us-east-1',
    },
  )

  produce_load = DockerOperator(
    task_id='produce_retro_data_to_kafka',
    image='mlops-task8-spark:latest',
    network_mode=NETWORK,
    auto_remove=True,
    command=(
      'bash -lc '
      '"python3 /opt/app/src/producer.py '
      '--data-path /opt/app/data/test_data.npy '
      '--tps 200 --duration-s 30"'
    ),
    environment={
      'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
      'KAFKA_SECURITY_PROTOCOL': 'PLAINTEXT',
      'KAFKA_INPUT_TOPIC': 'inputs',
    },
  )

  lag_sweep = DockerOperator(
    task_id='lag_sweep_tps',
    image='mlops-task8-spark:latest',
    network_mode=NETWORK,
    auto_remove=True,
    command=(
      'bash -lc '
      '"python3 /opt/app/src/load_test_lag.py '
      '--tps-list \\\"50,100,200,300,400\\\" '
      '--duration-s 30 --warmup-s 10 --sample-every-s 2 '
      '--consumer-group spark-infer '
      '--kafka-bootstrap kafka:29092 '
      '--input-topic inputs"'
    ),
  )

  evaluate = DockerOperator(
    task_id='evaluate_quality_and_perf',
    image='mlops-task8-spark:latest',
    network_mode=NETWORK,
    auto_remove=True,
    command='bash -lc "python3 /opt/app/src/evaluate_predictions.py --limit 2000 --timeout-s 60"',
    environment={
      'KAFKA_BOOTSTRAP_SERVERS': 'kafka:29092',
      'KAFKA_SECURITY_PROTOCOL': 'PLAINTEXT',
      'KAFKA_OUTPUT_TOPIC': 'predictions',
    },
  )

  register_model >> lag_sweep >> produce_load >> evaluate
