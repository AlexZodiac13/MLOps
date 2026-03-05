# MLOps: Инфраструктура и пайплайн для Telegram-бота «Напоминалка»

Данный репозиторий содержит инфраструктурный и MLOps пайплайн для развертывания Telegram-бота. 
Бот использует локальную LLM-модель (на базе Llama.cpp) для извлечения параметров (время, дата, текст) из сообщений на естественном языке.

## Структура репозитория

- `deploy/bot/` — исходный код Telegram-бота (Python, aiogram).
- `deploy/ml-model/` — микросервис на FastAPI для взаимодействия с LLM (inference).
- `ml/` — скрипты для подготовки данных, обучения (fine-tuning) и тестирования моделей.
- `infra/terraform/` — IaaC конфигурации Terraform для создания ресурсов в Yandex Cloud.
- `infra/ansible/` — Ansible-роли для настройки виртуальной машины.
- `infra/airflow/` — DAG-и Airflow для пайплайна машинного обучения.
- `k8s/` — манифесты Kubernetes (Deployments, Services, PVC) для деплоя бота, модели и базы данных.
- `.github/workflows/` — CI/CD пайплайны GitHub Actions.

## Архитектура системы

Проект поддерживает два типа развертывания:
1. **Docker Compose**: Для локальной разработки и тестирования (`deploy/docker-compose.yaml`, `infra/docker-compose.yml`).
2. **Kubernetes (Yandex Cloud)**: Продакшн-развертывание через Managed Service for Kubernetes. Связь бота и модели происходит через внутренний Service Discovery K8s. Данные пользователей хранятся в PostgreSQL (Stateful).

## CI/CD 

Пайплайны GitHub Actions разделены на несколько процессов:

- **test-infra.yml**: Запуск инфраструктурного тестирования. Включает в себя Plan-тесты Terraform кода с использованием Terratest, тестирование Ansible с помощью Molecule (docker driver), линтинг Python-кода (flake8) и проверку валидности Docker Compose файлов. Запускается при Push и Pull Requests.
- **apply.yml & destroy.yml**: Ручное управление инфраструктурой Terraform (Apply / Destroy) в облаке. State хранится в S3-бакете Minio.
- **deploy-k8s.yml**: Полноценный CD-пайплайн для Kubernetes. Собирает Docker контейнеры (bot, ml-model), пушит их в Docker Hub и автоматически деплоит (через `kubectl apply`) новые образы в преднастроенный кластер Yandex Cloud, подтягивая секреты из GitHub.
- **deploy-bot.yml**: Выполняет деплой на классическую (не-K8s) виртуальную машину по SSH (для тестовых нужд).

## Развертывание (Быстрый старт)

### Локальный запуск
1. Скопируйте `.env.example` в `.env` внутри директорий `deploy/` и `infra/` и укажите актуальные токены.
2. Для запуска инфраструктурной части перейдите в `/infra` и выполните `docker compose up -d`.
3. Для запуска приложения (Бота и ML-модели) перейдите в `/deploy` и выполните `docker compose up -d`.

### Инфраструктура в облаке (Terraform)
1. Установите необходимые переменные окружения для Yandex Cloud (через `TF_VAR_yc_token` и т.д.).
2. Перейдите в каталог `infra/terraform`.
3. Выполните:
   ```bash
   terraform init -backend-config=backend.conf  # конфигурация S3
   terraform plan
   terraform apply
   ```

### Деплой в K8s
При наличии поднятого Kubernetes кластера:
1. Настройте конфигурацию `kubectl` через YC CLI:
   ```bash
   yc managed-kubernetes cluster get-credentials --id <CLUSTER_ID> --external
   ```
2. Примените манифесты:
   ```bash
   kubectl apply -f k8s/postgres.yaml
   kubectl apply -f k8s/model.yaml
   kubectl apply -f k8s/bot.yaml
   ```

## MLOps составляющая

Проект включает в себя наработки для:
- Сбора исторического датасета пользовательских сообщений.
- Генерации синтетических данных и их разметки.
- Скрипты (GGUF Export) для упаковки обученных весов в оптимизированные форматы, пригодные для CPU-инференса (llama.cpp) внутри микросервиса.

# Очистить кэш сборщика перед сборкой
docker builder prune --all --force

# Сборка образа с ML библиотеками (займет время, компиляция llama.cpp)
docker-compose up --build -d

Доступ к интерфейсам:

Airflow: http://localhost:8080 (логин/пароль: airflow/airflow)
MLflow: http://localhost:5000


# Terraform local

terraform -chdir=infra/terraform init -reconfigure -backend-config="access_key=user" -backend-config="secret_key=password"
terraform -chdir=infra/terraform apply -auto-approve

# GPU Configuration
Коментирую и раскоментирую данный блок в docker compose, включаем и выключаем использование GPU в зависимости от его наличия