# Выходные данные

output "cluster_name" {
  description = "Name of the Dataproc cluster"
  value       = yandex_dataproc_cluster.dataproc_cluster.name
}


output "jupyter_access" {
  description = "Jupyter access information"
  value       = "После завершения terraform apply, токен будет в файле: log/terraform_summary.txt"
}
