# Выходные данные
output "bucket_name" {
  value = yandex_storage_bucket.data_bucket.bucket
}

output "cluster_name" {
  description = "Name of the Dataproc cluster"
  value       = yandex_dataproc_cluster.dataproc_cluster.name
}