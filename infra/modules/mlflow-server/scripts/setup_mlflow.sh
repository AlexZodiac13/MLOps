#!/bin/bash

function log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')]: $1"
}

path_to_user="/home/ubuntu"
path_to_venv="$path_to_user/venv"

# Обновляем пакеты
log "Обновление пакетов"
sudo apt-get update

# Устанавливаем необходимые пакеты
log "Установка необходимых пакетов"
sudo apt-get install -y python3-pip python3-venv

# Создаем виртуальное окружение для MLflow
log "Настройка виртуального окружения Python"
mkdir -p $path_to_venv
python3 -m venv $path_to_venv
$path_to_venv/bin/pip install --upgrade pip
$path_to_venv/bin/pip install mlflow==2.21.0 psycopg2-binary==2.9.10 boto3==1.37.16

# Загружаем сертификаты для подключения к PostgreSQL
log "Загрузка сертификатов PostgreSQL"
mkdir -p $path_to_user/.postgresql
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
    --output-document $path_to_user/.postgresql/root.crt
chmod 0600 $path_to_user/.postgresql/root.crt

# Копируем конфигурационный файл
log "Копирование конфигурационного файла"
cp $path_to_user/mlflow.conf $path_to_user/.mlflow.conf
chmod 600 $path_to_user/.mlflow.conf

# Копируем systemd сервис для запуска от имени пользователя
log "Настройка пользовательского systemd сервиса"
sudo cp mlflow.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable mlflow.service
sudo systemctl start mlflow.service

# Настраиваем systemd для запуска сервисов пользователя при загрузке системы
log "Настройка автозапуска пользовательских сервисов"
sudo loginctl enable-linger ubuntu

log "Установка MLflow завершена успешно" 