FROM apache/spark:3.5.1

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      python3 \
      python3-pip \
      curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*


RUN pip3 install --no-cache-dir \
  numpy==1.24.4 \
  pandas==2.0.3 \
  pyarrow==14.0.2 \
  mlflow==2.13.2 \
  boto3==1.34.160 \
  kafka-python-ng==2.2.3 \
  typer==0.12.3 \
  torch==2.2.2


RUN mkdir -p /opt/app/src

COPY src/ /opt/app/src/
COPY data/ /opt/app/data/

ENV PYTHONPATH=/opt/app/src

USER spark
