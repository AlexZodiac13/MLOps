#!/usr/bin/env python3
"""
Скрипт для скачивания модели из MLflow/S3 на этапе сборки Docker образа.
Используется в Dockerfile при сборке образа.
"""

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Загрузка переменных окружения из .env
# Сначала попробуем загрузить `.env` на уровень выше (обычно рядом с docker-compose),
# затем — обычный поиск по путям.
env_path = Path(__file__).resolve().parents[1] / ".env"
if env_path.exists():
    load_dotenv(env_path)
else:
    load_dotenv()

import mlflow
import boto3
from loguru import logger

# Настройка логгера
logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> - <level>{level}</level> - <level>{message}</level>")


def get_production_model_info(
    tracking_uri: str,
    experiment_name: str,
    stage: str = "Production"
) -> tuple[str, str]:
    """
    Получение информации о модели с тегом Production.

    Returns:
        tuple: (run_id, artifact_uri)
    """
    logger.info(f"Подключение к MLflow: {tracking_uri}")
    mlflow.set_tracking_uri(tracking_uri)

    client = mlflow.tracking.MlflowClient()

    # Поиск эксперимента
    experiment = client.get_experiment_by_name(experiment_name)
    if not experiment:
        raise ValueError(f"Эксперимент '{experiment_name}' не найден")

    logger.info(f"Эксперимент найден: {experiment.experiment_id}")

    # Поиск runs с тегом stage=Production
    filter_string = f"tags.stage = '{stage}'"
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=filter_string,
        order_by=["start_time DESC"],
        max_results=1
    )

    if not runs:
        raise ValueError(
            f"Не найдено моделей с тегом stage='{stage}' в эксперименте '{experiment_name}'"
        )

    run_id = runs[0].info.run_id
    artifact_uri = runs[0].info.artifact_uri

    logger.info(f"Найдена модель: run_id={run_id}, artifact_uri={artifact_uri}")

    return run_id, artifact_uri


def download_from_s3(
    s3_uri: str,
    local_path: str,
    endpoint_url: str = None,
    access_key: str = None,
    secret_key: str = None,
    region_name: str = "us-east-1"
):
    """Скачивание модели из S3."""
    # Парсинг S3 URI
    path_without_scheme = s3_uri[5:]
    parts = path_without_scheme.split("/", 1)
    bucket_name = parts[0]
    object_key = parts[1] if len(parts) > 1 else ""

    logger.info(f"S3 bucket: {bucket_name}, key: {object_key}")

    # Создание S3 клиента
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region_name
    )

    # Создание локальной директории
    Path(local_path).mkdir(parents=True, exist_ok=True)

    # Список объектов
    logger.info(f"Поиск объектов в S3...")
    response = s3_client.list_objects_v2(
        Bucket=bucket_name,
        Prefix=object_key
    )

    if "Contents" not in response:
        raise ValueError(f"Не найдено объектов в S3: {s3_uri}")

    # Скачивание каждого объекта
    downloaded_count = 0
    for obj in response["Contents"]:
        key = obj["Key"]
        relative_path = key.replace(object_key, "").lstrip("/")
        local_file_path = os.path.join(local_path, relative_path)

        # Создание директории
        Path(local_file_path).parent.mkdir(parents=True, exist_ok=True)

        # Скачивание файла
        s3_client.download_file(bucket_name, key, local_file_path)
        logger.info(f"Скачан: {key} -> {local_file_path}")
        downloaded_count += 1

    logger.info(f"Всего скачано файлов: {downloaded_count}")


def main():
    """Основная функция."""
    # Получение настроек из переменных окружения
    tracking_uri = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000123")
    experiment_name = os.environ.get("MLFLOW_EXPERIMENT_NAME", "reminder_parser")
    model_stage = os.environ.get("MLFLOW_MODEL_STAGE", "Production")

    s3_endpoint_url = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
    s3_access_key = os.environ.get("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key = os.environ.get("S3_SECRET_KEY", "minioadmin")
    s3_region = os.environ.get("S3_REGION", "us-east-1")

    local_model_path = os.environ.get("MODEL_PATH", "model")

    try:
        # Получение информации о модели
        run_id, artifact_uri = get_production_model_info(
            tracking_uri=tracking_uri,
            experiment_name=experiment_name,
            stage=model_stage
        )

        # Скачивание модели
        download_from_s3(
            s3_uri=artifact_uri,
            local_path=local_model_path,
            endpoint_url=s3_endpoint_url,
            access_key=s3_access_key,
            secret_key=s3_secret_key,
            region_name=s3_region
        )

        logger.success(f"Модель успешно загружена в: {local_model_path}")

    except Exception as e:
        logger.error(f"Ошибка загрузки модели: {e}")
        # Не завершаем с ошибкой, чтобы можно было использовать fallback
        logger.warning("Модель не загружена. Приложение будет использовать эвристический парсинг.")
        sys.exit(0)  # Выходим с 0, чтобы сборка не прерывалась


if __name__ == "__main__":
    main()
