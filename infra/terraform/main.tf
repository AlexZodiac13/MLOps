# Core provider configuration

terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 0.13"

  backend "s3" {
    endpoints = {
      s3 = "https://home.owgrant.su:9005"
    }
    bucket = "otus"
    region = "us-east-1"
    key    = "terraform/terraform.tfstate"

    skip_region_validation      = true
    skip_credentials_validation = true
    skip_requesting_account_id  = true
    skip_s3_checksum            = true
    skip_metadata_api_check     = true
    use_path_style              = true
  }
}

provider "yandex" {
  token     = var.yc_token
  cloud_id  = var.cloud_id
  folder_id = var.folder_id
  zone      = var.zone
}

# VM Configuration

# 1. Network (We need a network for the VM)
resource "yandex_vpc_network" "vm_network" {
  name = "gpu-vm-network"
}

resource "yandex_vpc_subnet" "vm_subnet" {
  name           = "gpu-vm-subnet-${var.zone}"
  zone           = var.zone
  network_id     = yandex_vpc_network.vm_network.id
  v4_cidr_blocks = ["10.0.0.0/24"]
}

# 2. Disk Image (Debian 12 Bookworm) - Newer, stable repositories
data "yandex_compute_image" "debian" {
  family = "debian-12"
}

# 3. CPU VM Instance (Preemptible, White IP, Standard v3)
resource "yandex_compute_instance" "ml_vm" {
  name        = "cpu-ml-instance"
  platform_id = "standard-v3" # Intel Ice Lake
  zone        = var.zone

  scheduling_policy {
    preemptible = true
  }

  resources {
    cores         = 8
    memory        = 32
    core_fraction = 100
  }

  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.debian.id
      size     = 93
      type     = "network-ssd-nonreplicated"
    }
  }

  network_interface {
    subnet_id = yandex_vpc_subnet.vm_subnet.id
    nat       = true # White IP
  }

  metadata = {
    user-data = <<EOF
#cloud-config
users:
  - name: zodiac
    groups: sudo
    shell: /bin/bash
    sudo: ['ALL=(ALL) NOPASSWD:ALL']
    ssh-authorized-keys:
      - ${var.public_key}
EOF
  }

  allow_stopping_for_update = true
}

output "vm_public_ip" {
  value = yandex_compute_instance.ml_vm.network_interface[0].nat_ip_address
}

resource "local_file" "ansible_inventory" {
  content = templatefile("${path.module}/inventory.tftpl", {
    ip_address = yandex_compute_instance.ml_vm.network_interface[0].nat_ip_address
  })
  filename = "${path.module}/../../infra/ansible/inventory.yml"
}

resource "null_resource" "run_ansible" {
  depends_on = [yandex_compute_instance.ml_vm, local_file.ansible_inventory]

  provisioner "local-exec" {
    command = "sleep 60 && ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i ${path.module}/../../infra/ansible/inventory.yml ${path.module}/../../infra/ansible/playbook.yml"
  }
}

