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

# --- Kubernetes Cluster Configuration ---

# Service Account for K8s
resource "yandex_iam_service_account" "k8s_sa" {
  name        = "k8s-service-account"
  description = "Service account for Kubernetes cluster"
}

# Roles for Service Account
resource "yandex_resourcemanager_folder_iam_member" "k8s_editor" {
  folder_id = var.folder_id
  role      = "editor"
  member    = "serviceAccount:${yandex_iam_service_account.k8s_sa.id}"
}

resource "yandex_resourcemanager_folder_iam_member" "k8s_images_puller" {
  folder_id = var.folder_id
  role      = "container-registry.images.puller"
  member    = "serviceAccount:${yandex_iam_service_account.k8s_sa.id}"
}

# K8s Cluster (Zonal)
resource "yandex_kubernetes_cluster" "k8s_cluster" {
  name        = "k8s-cluster"
  network_id  = yandex_vpc_network.vm_network.id

  master {
    version   = "1.33"
    public_ip = true

    zonal {
      zone      = yandex_vpc_subnet.vm_subnet.zone
      subnet_id = yandex_vpc_subnet.vm_subnet.id
    }
  }

  service_account_id      = yandex_iam_service_account.k8s_sa.id
  node_service_account_id = yandex_iam_service_account.k8s_sa.id

  depends_on = [
    yandex_resourcemanager_folder_iam_member.k8s_editor,
    yandex_resourcemanager_folder_iam_member.k8s_images_puller
  ]
}

# Single Node Group
resource "yandex_kubernetes_node_group" "k8s_node_group" {
  cluster_id  = yandex_kubernetes_cluster.k8s_cluster.id
  name        = "k8s-node-group"
  version     = "1.33"

  instance_template {
    platform_id = "standard-v3"

    network_interface {
      nat        = true
      subnet_ids = [yandex_vpc_subnet.vm_subnet.id]
    }

    resources {
      memory = 16
      cores  = 8
    }

    boot_disk {
      type = "network-ssd"
      size = 64
    }

    metadata = {
      ssh-keys = "zodiac:${var.public_key}"
    }
  }

  scale_policy {
    fixed_scale {
      size = 1
    }
  }

  allocation_policy {
    location {
      zone = var.zone
    }
  }
}

output "k8s_cluster_id" {
  value = yandex_kubernetes_cluster.k8s_cluster.id
}

output "k8s_external_v4_endpoint" {
  value = yandex_kubernetes_cluster.k8s_cluster.master[0].external_v4_endpoint
}

