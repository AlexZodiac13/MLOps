# Выходные данные
output "bucket_name" {
  value       = yandex_storage_bucket.data_bucket.bucket
  description = "S3 bucket name for data storage"
}

output "cluster_name" {
  description = "Name of the Dataproc cluster"
  value       = yandex_dataproc_cluster.dataproc_cluster.name
}

output "s3_access_key" {
  value       = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  description = "Static access key for S3 bucket"
}

output "s3_secret_key" {
  value       = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  description = "Static secret key for S3 bucket. To view, run: terraform output -raw s3_secret_key"
  sensitive   = true
}

output "jupyter_access" {
  description = "Jupyter access information"
  value       = "После завершения terraform apply, токен будет в файле: log/terraform_summary.txt"
}