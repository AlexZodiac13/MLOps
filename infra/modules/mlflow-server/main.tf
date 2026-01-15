resource "yandex_compute_instance" "mlflow_server" {
  name               = var.instance_name
  service_account_id = var.service_account_id

  scheduling_policy {
    preemptible = true
  }

  resources {
    cores  = 2
    memory = 8
    core_fraction = 20 # 20% vCPU
  }

  boot_disk {
    initialize_params {
      image_id = var.ubuntu_image_id
      size     = 30 # GB
    }
  }

  network_interface {
    subnet_id = var.subnet_id
    nat       = true
  }

  metadata = {
    ssh-keys = "${var.instance_user}:${file(var.public_key_path)}"
    serial-port-enable = "1"
  }

  connection {
    type        = "ssh"
    user        = var.instance_user
    private_key = file(var.private_key_path)
    host        = self.network_interface.0.nat_ip_address
  }

  # Копируем скрипт установки MLflow
  provisioner "file" {
    source      = "${path.module}/scripts/setup_mlflow.sh"
    destination = "/home/${var.instance_user}/setup_mlflow.sh"
  }

  # Копируем конфигурационный файл для MLflow
  provisioner "file" {
    content     = templatefile("${path.module}/scripts/mlflow.conf.tpl", {
      s3_endpoint_url = var.s3_endpoint_url
      s3_bucket_name  = var.s3_bucket_name
      s3_access_key   = var.s3_access_key
      s3_secret_key   = var.s3_secret_key
      mlflow_port     = var.mlflow_port
      postgres_host   = var.postgres_host
      postgres_port   = var.postgres_port
      postgres_db     = var.postgres_db
      postgres_user   = var.postgres_user
      postgres_password = var.postgres_password
    })
    destination = "/home/${var.instance_user}/mlflow.conf"
  }

  # Копируем systemd сервис для MLflow
  provisioner "file" {
    source      = "${path.module}/scripts/mlflow.service"
    destination = "/home/${var.instance_user}/mlflow.service"
  }

  # Запускаем скрипт установки
  provisioner "remote-exec" {
    inline = [
      "chmod +x /home/${var.instance_user}/setup_mlflow.sh",
      "/home/${var.instance_user}/setup_mlflow.sh"
    ]
  }
}

# Обновляем .env файл с URL MLflow сервера
resource "null_resource" "update_env_mlflow" {
  triggers = {
    mlflow_server_ip = yandex_compute_instance.mlflow_server.network_interface.0.nat_ip_address
  }

  provisioner "local-exec" {
    command = <<EOT
      # Определяем переменные
      MLFLOW_TRACKING_URI=http://${yandex_compute_instance.mlflow_server.network_interface.0.nat_ip_address}:${var.mlflow_port}

      # Добавляем или обновляем переменную MLFLOW_TRACKING_URI в .env файле
      if grep -q "^MLFLOW_TRACKING_URI=" ../../.env; then
        sed -i "s|^MLFLOW_TRACKING_URI=.*|MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI|" ../../.env
      else
        echo "MLFLOW_TRACKING_URI=$MLFLOW_TRACKING_URI" >> ../../.env
      fi
    EOT
  }

  depends_on = [
    yandex_compute_instance.mlflow_server
  ]
}
