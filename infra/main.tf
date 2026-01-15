# main.tf

module "iam" {
  source          = "./modules/iam"
  name            = var.yc_service_account_name
  provider_config = var.yc_config
}

module "network" {
  source          = "./modules/network"
  network_name    = var.yc_network_name
  subnet_name     = var.yc_subnet_name
  provider_config = var.yc_config
}

module "storage" {
  source          = "./modules/storage"
  name            = var.yc_bucket_name
  provider_config = var.yc_config
  access_key      = module.iam.access_key
  secret_key      = module.iam.secret_key
}

module "airflow-cluster" {
  source             = "./modules/airflow-cluster"
  instance_name      = var.yc_instance_name
  subnet_id          = module.network.subnet_id
  service_account_id = module.iam.service_account_id
  admin_password     = var.admin_password
  bucket_name        = module.storage.bucket
  provider_config    = var.yc_config
}

module "postgres-cluster" {
  source             = "./modules/postgres-cluster"
  cluster_name       = var.yc_postgres_cluster_name
  network_id         = module.network.network_id
  security_group_id  = module.network.security_group_id
  subnet_id          = module.network.subnet_id
  postgres_password  = var.postgres_password
  provider_config    = var.yc_config
}

module "mlflow-server" {
  source             = "./modules/mlflow-server"
  instance_name      = var.yc_mlflow_instance_name
  subnet_id          = module.network.subnet_id
  service_account_id = module.iam.service_account_id
  ubuntu_image_id    = var.ubuntu_image_id
  public_key_path    = var.public_key_path
  private_key_path   = var.private_key_path
  s3_endpoint_url    = var.yc_storage_endpoint_url
  s3_bucket_name     = module.storage.bucket
  s3_access_key      = module.iam.access_key
  s3_secret_key      = module.iam.secret_key
  postgres_password  = var.postgres_password
  postgres_host      = module.postgres-cluster.postgres_host
  postgres_port      = module.postgres-cluster.postgres_port
  postgres_db        = module.postgres-cluster.postgres_db
  postgres_user      = module.postgres-cluster.postgres_user
  provider_config    = var.yc_config
}

resource "local_file" "variables_file" {
  content = jsonencode({
    # общие переменные
    YC_ZONE           = var.yc_config.zone
    YC_FOLDER_ID      = var.yc_config.folder_id
    YC_SUBNET_ID      = module.network.subnet_id
    YC_SSH_PUBLIC_KEY = trimspace(file(var.public_key_path))
    # S3
    S3_ENDPOINT_URL = var.yc_storage_endpoint_url
    S3_ACCESS_KEY   = module.iam.access_key
    S3_SECRET_KEY   = module.iam.secret_key
    S3_BUCKET_NAME  = module.storage.bucket
    # Data Proc
    DP_SECURITY_GROUP_ID      = module.network.security_group_id
    DP_SA_ID                  = module.iam.service_account_id
    DP_SA_AUTH_KEY_PUBLIC_KEY = module.iam.public_key
    DP_SA_JSON = jsonencode({
      id                 = module.iam.auth_key_id
      service_account_id = module.iam.service_account_id
      created_at         = module.iam.auth_key_created_at
      public_key         = module.iam.public_key
      private_key        = module.iam.private_key
    })
    # MLflow
    MLFLOW_TRACKING_URI = module.mlflow-server.mlflow_tracking_uri
    # PostgreSQL
    POSTGRES_HOST = module.postgres-cluster.postgres_host
    POSTGRES_PORT = module.postgres-cluster.postgres_port
    POSTGRES_DB   = module.postgres-cluster.postgres_db
    POSTGRES_USER = module.postgres-cluster.postgres_user
  })
  filename        = "./variables.json"
  file_permission = "0600"
}

# Запись переменных в .env файл
resource "null_resource" "update_env" {
  provisioner "local-exec" {
    command = <<EOT
      # Определяем переменные
      AIRFLOW_ADMIN_PASSWORD=${var.admin_password}
      STORAGE_ENDPOINT_URL=${var.yc_storage_endpoint_url}
      BUCKET_NAME=${module.storage.bucket}
      ACCESS_KEY=${module.iam.access_key}
      SECRET_KEY=${module.iam.secret_key}
      MLFLOW_TRACKING_URI=${module.mlflow-server.mlflow_tracking_uri}
      POSTGRES_HOST=${module.postgres-cluster.postgres_host}
      POSTGRES_PORT=${module.postgres-cluster.postgres_port}
      POSTGRES_DB=${module.postgres-cluster.postgres_db}
      POSTGRES_USER=${module.postgres-cluster.postgres_user}
      POSTGRES_PASSWORD=${var.postgres_password}
      POSTGRES_CONNECTION_STRING=${module.postgres-cluster.postgres_connection_string}

      # Замена пустых переменных в .env
      sed -i "s|^AIRFLOW_ADMIN_PASSWORD=.*|AIRFLOW_ADMIN_PASSWORD=$AIRFLOW_ADMIN_PASSWORD|" ../.env
      sed -i "s|^S3_ENDPOINT_URL=.*|S3_ENDPOINT_URL=$STORAGE_ENDPOINT_URL|" ../.env
      sed -i "s|^S3_BUCKET_NAME=.*|S3_BUCKET_NAME=$BUCKET_NAME|" ../.env
      sed -i "s|^S3_ACCESS_KEY=.*|S3_ACCESS_KEY=$ACCESS_KEY|" ../.env
      sed -i "s|^S3_SECRET_KEY=.*|S3_SECRET_KEY=$SECRET_KEY|" ../.env

      # Добавляем или обновляем переменную MLFLOW_TRACKING_URI
      if grep -q "^MLFLOW_TRACKING_URI=" ../.env; then
        sed -i "s|^MLFLOW_TRACKING_URI=.*|MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI|" ../.env
      else
        echo "MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI" >> ../.env
      fi

      # Добавляем или обновляем переменные PostgreSQL
      if grep -q "^POSTGRES_HOST=" ../.env; then
        sed -i "s|^POSTGRES_HOST=.*|POSTGRES_HOST=$POSTGRES_HOST|" ../.env
      else
        echo "POSTGRES_HOST=$POSTGRES_HOST" >> ../.env
      fi

      if grep -q "^POSTGRES_PORT=" ../.env; then
        sed -i "s|^POSTGRES_PORT=.*|POSTGRES_PORT=$POSTGRES_PORT|" ../.env
      else
        echo "POSTGRES_PORT=$POSTGRES_PORT" >> ../.env
      fi

      if grep -q "^POSTGRES_DB=" ../.env; then
        sed -i "s|^POSTGRES_DB=.*|POSTGRES_DB=$POSTGRES_DB|" ../.env
      else
        echo "POSTGRES_DB=$POSTGRES_DB" >> ../.env
      fi

      if grep -q "^POSTGRES_USER=" ../.env; then
        sed -i "s|^POSTGRES_USER=.*|POSTGRES_USER=$POSTGRES_USER|" ../.env
      else
        echo "POSTGRES_USER=$POSTGRES_USER" >> ../.env
      fi

      if grep -q "^POSTGRES_PASSWORD=" ../.env; then
        sed -i "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$POSTGRES_PASSWORD|" ../.env
      else
        echo "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" >> ../.env
      fi

      if grep -q "^POSTGRES_CONNECTION_STRING=" ../.env; then
        sed -i "s|^POSTGRES_CONNECTION_STRING=.*|POSTGRES_CONNECTION_STRING=$POSTGRES_CONNECTION_STRING|" ../.env
      else
        echo "POSTGRES_CONNECTION_STRING=$POSTGRES_CONNECTION_STRING" >> ../.env
      fi
    EOT
  }

  depends_on = [
    module.iam,
    module.storage,
    module.mlflow-server,
    module.postgres-cluster
  ]
}
