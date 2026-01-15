# modules/airflow-cluster/main.tf

resource "yandex_airflow_cluster" "airflow_cluster" {
  name               = var.instance_name
  subnet_ids         = [var.subnet_id]
  service_account_id = var.service_account_id
  admin_password     = var.admin_password

  code_sync = {
    s3 = {
      bucket = var.bucket_name
    }
  }

  webserver = {
    count              = 1
    resource_preset_id = "c1-m4"
  }

  scheduler = {
    count              = 1
    resource_preset_id = "c1-m4"
  }

  worker = {
    min_count          = 1
    max_count          = 2
    resource_preset_id = "c1-m4"
  }

  airflow_config = {
    "api" = {
      "auth_backends" = "airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session"
    }
    "scheduler" = {
      "dag_dir_list_interval" = "10"
    }
  }

  logging = {
    enabled   = true
    folder_id = var.provider_config.folder_id
    min_level = "INFO"
  }
}