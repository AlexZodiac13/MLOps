# Сборка образа с ML библиотеками (займет время, компиляция llama.cpp)
docker-compose up --build -d


# Создание базы mlflow_db
docker-compose exec postgres /bin/bash /opt/airflow/init_db.sh
# Инициализация Airflow (если не сработал авто-инит)
docker-compose run --rm airflow-init

Доступ к интерфейсам:

Airflow: http://localhost:8080 (логин/пароль: airflow/airflow)
MLflow: http://localhost:5000



# Terraform local

terraform -chdir=infra/terraform init -reconfigure -backend-config="access_key=user" -backend-config="secret_key=password"
terraform -chdir=infra/terraform apply -auto-approve