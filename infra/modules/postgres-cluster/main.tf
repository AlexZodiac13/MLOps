resource "yandex_mdb_postgresql_cluster" "postgres_cluster" {
  name                = var.cluster_name
  environment         = "PRODUCTION"
  network_id          = var.network_id
  security_group_ids  = [var.security_group_id]
  deletion_protection = var.deletion_protection

  config {
    version = var.postgres_version
    resources {
      resource_preset_id = var.resource_preset_id
      disk_type_id       = var.disk_type_id
      disk_size          = var.disk_size
    }

    access {
      data_lens     = false
      web_sql       = true
      serverless    = false
      data_transfer = false
    }

    performance_diagnostics {
      enabled = true
      sessions_sampling_interval  = 60
      statements_sampling_interval = 600
    }

    pooler_config {
      pooling_mode = "TRANSACTION"
      pool_discard = true
    }
  }

  host {
    zone             = var.provider_config.zone
    subnet_id        = var.subnet_id
    assign_public_ip = var.assign_public_ip
  }

  maintenance_window {
    type = "WEEKLY"
    day  = "SAT"
    hour = 12
  }
}

resource "yandex_mdb_postgresql_database" "mlflow_db" {
  cluster_id = yandex_mdb_postgresql_cluster.postgres_cluster.id
  name       = var.postgres_db
  owner      = yandex_mdb_postgresql_user.mlflow_user.name
}

resource "yandex_mdb_postgresql_user" "mlflow_user" {
  cluster_id = yandex_mdb_postgresql_cluster.postgres_cluster.id
  name       = var.postgres_user
  password   = var.postgres_password

  settings = {
    default_transaction_isolation = "read committed"
    log_min_duration_statement    = 5000
  }

}

# Обновляем .env файл с данными подключения к PostgreSQL
resource "null_resource" "update_env_postgres" {
  triggers = {
    cluster_id = yandex_mdb_postgresql_cluster.postgres_cluster.id
  }

  provisioner "local-exec" {
    command = <<EOT
      # Определяем переменные
      POSTGRES_HOST=${yandex_mdb_postgresql_cluster.postgres_cluster.host[0].fqdn}
      POSTGRES_PORT=6432
      POSTGRES_DB=${var.postgres_db}
      POSTGRES_USER=${var.postgres_user}
      POSTGRES_PASSWORD=${var.postgres_password}
      POSTGRES_CONNECTION_STRING=postgresql://${var.postgres_user}:${var.postgres_password}@${yandex_mdb_postgresql_cluster.postgres_cluster.host[0].fqdn}:6432/${var.postgres_db}

      # Добавляем или обновляем переменные в .env файле
      if grep -q "^POSTGRES_HOST=" ../../.env; then
        sed -i "s|^POSTGRES_HOST=.*|POSTGRES_HOST=$POSTGRES_HOST|" ../../.env
      else
        echo "POSTGRES_HOST=$POSTGRES_HOST" >> ../../.env
      fi

      if grep -q "^POSTGRES_PORT=" ../../.env; then
        sed -i "s|^POSTGRES_PORT=.*|POSTGRES_PORT=$POSTGRES_PORT|" ../../.env
      else
        echo "POSTGRES_PORT=$POSTGRES_PORT" >> ../../.env
      fi

      if grep -q "^POSTGRES_DB=" ../../.env; then
        sed -i "s|^POSTGRES_DB=.*|POSTGRES_DB=$POSTGRES_DB|" ../../.env
      else
        echo "POSTGRES_DB=$POSTGRES_DB" >> ../../.env
      fi

      if grep -q "^POSTGRES_USER=" ../../.env; then
        sed -i "s|^POSTGRES_USER=.*|POSTGRES_USER=$POSTGRES_USER|" ../../.env
      else
        echo "POSTGRES_USER=$POSTGRES_USER" >> ../../.env
      fi

      if grep -q "^POSTGRES_PASSWORD=" ../../.env; then
        sed -i "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$POSTGRES_PASSWORD|" ../../.env
      else
        echo "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" >> ../../.env
      fi

      if grep -q "^POSTGRES_CONNECTION_STRING=" ../../.env; then
        sed -i "s|^POSTGRES_CONNECTION_STRING=.*|POSTGRES_CONNECTION_STRING=$POSTGRES_CONNECTION_STRING|" ../../.env
      else
        echo "POSTGRES_CONNECTION_STRING=$POSTGRES_CONNECTION_STRING" >> ../../.env
      fi
    EOT
  }

  depends_on = [
    yandex_mdb_postgresql_cluster.postgres_cluster,
    yandex_mdb_postgresql_database.mlflow_db,
    yandex_mdb_postgresql_user.mlflow_user
  ]
}
