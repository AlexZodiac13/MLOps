provider "yandex" {
  token     = var.yc_token
  cloud_id  = var.cloud_id
  folder_id = var.folder_id
  zone      = var.zone
}

resource "yandex_storage_bucket" "airflow_bucket" {
  bucket = var.bucket_name
  acl    = "private"
  # ...доп. настройки при необходимости...
}

# TODO: убедитесь в правильности названия ресурса/атрибутов для mdb airlfow в провайдере yandex.
# Ниже — шаблонный ресурс для Managed Service for Apache Airflow, проверьте поля/версии в документации.
resource "yandex_mdb_airflow_cluster" "airflow" {
  name      = var.cluster_name
  folder_id = var.folder_id
  network_id = var.network_id
  subnet_ids = var.subnet_ids

  environment {
    version = var.airflow_version
    # ... другие опции
  }

  webserver {
    public_ip = var.web_ui_public_ip
  }

  labels = var.labels
}
