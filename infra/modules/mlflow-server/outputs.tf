output "external_ip_address" {
  value = yandex_compute_instance.mlflow_server.network_interface.0.nat_ip_address
}

output "instance_id" {
  value = yandex_compute_instance.mlflow_server.id
}

output "mlflow_tracking_uri" {
  value = "http://${yandex_compute_instance.mlflow_server.network_interface.0.nat_ip_address}:${var.mlflow_port}"
}
