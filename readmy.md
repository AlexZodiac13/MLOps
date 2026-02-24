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