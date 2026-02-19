output "bucket_name" {
  value = yandex_storage_bucket.airflow_bucket.bucket
}

output "airflow_cluster_id" {
  value = yandex_airflow_cluster.airflow.id
}

output "airflow_service_account_id" {
  value = try(yandex_iam_service_account.airflow_sa[0].id, "")
}

output "airflow_service_account_access_key" {
  value     = try(yandex_iam_service_account_static_access_key.airflow_sa_key[0].access_key, "")
  sensitive = true
}

output "airflow_service_account_secret_key" {
  value     = try(yandex_iam_service_account_static_access_key.airflow_sa_key[0].secret_key, "")
  sensitive = true
}

output "airflow_network_id" {
  value = try(yandex_vpc_network.airflow_network[0].id, "")
}

output "airflow_subnet_id" {
  value = try(yandex_vpc_subnet.airflow_subnet[0].id, "")
}

output "airflow_web_url" {
  description = "URL for Airflow Web UI"
  value       = "https://c-${yandex_airflow_cluster.airflow.id}.airflow.yandexcloud.net"
}

output "airflow_admin_password" {
  description = "Password for Airflow Admin (user: admin)"
  value       = var.admin_password
  sensitive   = true
}
