output "bucket_name" {
  value = yandex_storage_bucket.airflow_bucket.bucket
}

output "airflow_cluster_id" {
  value = yandex_mdb_airflow_cluster.airflow.id
}

output "airflow_web_ui_url" {
  # TODO: если у ресурса есть поле web_ui_url — замените на правильный атрибут
  value = yandex_mdb_airflow_cluster.airflow.web_ui_url
}
