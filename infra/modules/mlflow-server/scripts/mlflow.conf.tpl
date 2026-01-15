# MLflow конфигурация
MLFLOW_PORT=${mlflow_port}
MLFLOW_HOST=0.0.0.0
MLFLOW_BACKEND_STORE_URI=postgresql://${postgres_user}:${postgres_password}@${postgres_host}:${postgres_port}/${postgres_db}?sslmode=verify-full
MLFLOW_DEFAULT_ARTIFACT_ROOT=s3://${s3_bucket_name}/mlflow/artifacts

# S3 конфигурация
AWS_ACCESS_KEY_ID=${s3_access_key}
AWS_SECRET_ACCESS_KEY=${s3_secret_key}
MLFLOW_S3_ENDPOINT_URL=${s3_endpoint_url}

# Postgresql сертификаты
PGSSLROOTCERT=/home/ubuntu/.postgresql/root.crt
PGSSLMODE=verify-full 