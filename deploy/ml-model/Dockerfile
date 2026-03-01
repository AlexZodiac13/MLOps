# Dockerfile для FastAPI приложения с загрузкой модели
FROM python:3.10-slim

# Установить системные зависимости
RUN sed -i 's/deb.debian.org/mirror.yandex.ru/g' /etc/apt/sources.list.d/debian.sources || true && \
    apt-get update -o Acquire::Retries=3 && apt-get install -y \
    build-essential \
    libopenblas-dev \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Копировать requirements.txt и установить зависимости
COPY src/requirements.txt ./src/requirements.txt
RUN pip install --upgrade pip && pip install -r ./src/requirements.txt -i https://pypi.org/simple/ --extra-index-url https://mirrors.aliyun.com/pypi/simple/

# Копировать исходный код
COPY src/ ./src/

# Build-time args (можно задавать через build --build-arg или docker-compose build.args)
ARG MLFLOW_TRACKING_URI="http://mlflow:5000"
ARG MLFLOW_EXPERIMENT_NAME="reminder_parser"
ARG MLFLOW_MODEL_STAGE="Production"
ARG S3_ENDPOINT_URL="http://minio:9000"
ARG S3_ACCESS_KEY
ARG S3_SECRET_KEY
ARG S3_REGION="us-east-1"
ARG MODEL_PATH="/app/model/gguf"

# Expose args as env so RUN steps can see them
ENV MLFLOW_TRACKING_URI=${MLFLOW_TRACKING_URI}
ENV MLFLOW_EXPERIMENT_NAME=${MLFLOW_EXPERIMENT_NAME}
ENV MLFLOW_MODEL_STAGE=${MLFLOW_MODEL_STAGE}
ENV S3_ENDPOINT_URL=${S3_ENDPOINT_URL}
ENV S3_ACCESS_KEY=${S3_ACCESS_KEY}
ENV S3_SECRET_KEY=${S3_SECRET_KEY}
ENV S3_REGION=${S3_REGION}
ENV MODEL_PATH=${MODEL_PATH}

# Запустить download_model.py для скачивания актуальной модели (build-time)
RUN python ./src/download_model.py

EXPOSE 8001

CMD ["python", "./src/main.py"]
