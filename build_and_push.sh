#!/bin/bash
set -e

# ==========================================
# Скрипт сборки и публикации образов в Docker Hub
# ==========================================

# Укажите ваш логин (username) на Docker Hub
DOCKER_USER="alexzodiac"

if [ "$DOCKER_USER" = "your_dockerhub_username" ]; then
    echo "❌ ОШИБКА: Пожалуйста, откройте скрипт build_and_push.sh и замените 'your_dockerhub_username' на ваш реальный логин Docker Hub."
    exit 1
fi

echo "🔑 Авторизация в Docker Hub..."
docker login

echo "🚀 Сборка и отправка: Custom Airflow (включает MLflow и нужные ML-библиотеки)..."
docker build -t $DOCKER_USER/mlops-airflow:latest -f infra/airflow/Dockerfile.ml infra/airflow
docker push $DOCKER_USER/mlops-airflow:latest

echo "🚀 Сборка и отправка: DB Backup (скрипты бэкапа базы)..."
docker build -t $DOCKER_USER/mlops-db-backup:latest -f infra/docker-scripts/Dockerfile.backup infra/docker-scripts
docker push $DOCKER_USER/mlops-db-backup:latest

# echo "🚀 Сборка и отправка: Telegram Bot..."
# docker build -t $DOCKER_USER/mlops-bot:latest -f deploy/bot/Dockerfile deploy/bot
# docker push $DOCKER_USER/mlops-bot:latest

# echo "🚀 Сборка и отправка: ML Model API..."
# docker build -t $DOCKER_USER/mlops-ml-model:latest -f deploy/ml-model/Dockerfile deploy/ml-model
# docker push $DOCKER_USER/mlops-ml-model:latest

echo "✅ Все образы успешно собраны и отправлены в Docker Hub: https://hub.docker.com/u/$DOCKER_USER"
