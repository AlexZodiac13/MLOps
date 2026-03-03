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

# 1. Network
resource "yandex_vpc_network" "k8s_network" {
  name = "k8s-network"
}

resource "yandex_vpc_subnet" "k8s_subnet" {
  name           = "k8s-subnet-${var.zone}"
  zone           = var.zone
  network_id     = yandex_vpc_network.k8s_network.id
  v4_cidr_blocks = ["10.0.0.0/24"]
}

# 2. Service Accounts for K8s
resource "yandex_iam_service_account" "k8s_cluster_sa" {
  name        = "k8s-cluster-sa"
  description = "Service account for Kubernetes cluster"
}

resource "yandex_resourcemanager_folder_iam_member" "clusters_editor" {
  folder_id = var.folder_id
  role      = "editor"
  member    = "serviceAccount:${yandex_iam_service_account.k8s_cluster_sa.id}"
}

resource "yandex_iam_service_account" "k8s_node_sa" {
  name        = "k8s-node-sa"
  description = "Service account for Kubernetes nodes"
}

resource "yandex_resourcemanager_folder_iam_member" "nodes_puller" {
  folder_id = var.folder_id
  role      = "container-registry.images.puller"
  member    = "serviceAccount:${yandex_iam_service_account.k8s_node_sa.id}"
}

# 3. Kubernetes Cluster
resource "yandex_kubernetes_cluster" "mks_cluster" {
  name        = "mlops-mks-cluster"
  network_id  = yandex_vpc_network.k8s_network.id
  
  master {
    version   = "1.33" # Replace with needed or leave default
    public_ip = true   # Public IP for the master

    zonal {
      zone      = yandex_vpc_subnet.k8s_subnet.zone
      subnet_id = yandex_vpc_subnet.k8s_subnet.id
    }
  }

  service_account_id      = yandex_iam_service_account.k8s_cluster_sa.id
  node_service_account_id = yandex_iam_service_account.k8s_node_sa.id

  depends_on = [
    yandex_resourcemanager_folder_iam_member.clusters_editor,
    yandex_resourcemanager_folder_iam_member.nodes_puller
  ]
}

# 4. Kubernetes Node Group (16 vCPU, 64 GB RAM)
resource "yandex_kubernetes_node_group" "mks_node_group" {
  cluster_id  = yandex_kubernetes_cluster.mks_cluster.id
  name        = "mlops-node-group"
  version     = "1.33"

  instance_template {
    platform_id = "standard-v3" # Intel Ice Lake (good for ML)

    network_interface {
      nat        = true
      subnet_ids = [yandex_vpc_subnet.k8s_subnet.id]
    }

    resources {
      memory        = 64
      cores         = 16
      core_fraction = 100
    }

    boot_disk {
      type = "network-ssd"
      size = 93
    }

    scheduling_policy {
      preemptible = false
    }

    metadata = {
      ssh-keys = "ubuntu:${var.public_key}"
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

  maintenance_policy {
    auto_upgrade = true
    auto_repair  = true
  }
}

output "k8s_cluster_ip" {
  value = yandex_kubernetes_cluster.mks_cluster.master[0].external_v4_endpoint
}

