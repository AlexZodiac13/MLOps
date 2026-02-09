FROM python:3.11-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
  mlflow==2.13.2 \
  boto3==1.34.160 \
  psycopg2-binary==2.9.9

EXPOSE 5000

CMD ["mlflow", "--help"]
