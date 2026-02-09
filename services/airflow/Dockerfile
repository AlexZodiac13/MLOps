FROM apache/airflow:2.9.3-python3.11

USER root

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    docker.io \
  && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
  apache-airflow-providers-docker==3.11.0 \
  mlflow==2.13.2 \
  boto3==1.34.160 \
  kafka-python-ng==2.2.3 \
  numpy==1.26.4

