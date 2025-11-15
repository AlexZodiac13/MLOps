# Storage ресурсы
resource "yandex_storage_bucket" "data_bucket" {
  depends_on  = [yandex_resourcemanager_folder_iam_member.sa_roles]
  bucket        = "${var.yc_bucket_name}-${var.yc_folder_id}"
  access_key    = yandex_iam_service_account_static_access_key.sa_static_key.access_key
  secret_key    = yandex_iam_service_account_static_access_key.sa_static_key.secret_key
  force_destroy = true
  acl           = "public-read" # Делаем бакет публичным для чтения
}

# IAM ресурсы
resource "yandex_iam_service_account" "sa" {
  name        = var.yc_service_account_name
  description = "Service account for Dataproc cluster and related services"
}

resource "yandex_resourcemanager_folder_iam_member" "sa_roles" {
  for_each = toset([
    "storage.admin",
    "dataproc.editor",
    "compute.admin",
    "dataproc.agent",
    "mdb.dataproc.agent",
    "vpc.user",
    "iam.serviceAccounts.user",
    "storage.uploader",
    "storage.viewer",
    "storage.editor"
  ])

  folder_id = var.yc_folder_id
  role      = each.key
  member    = "serviceAccount:${yandex_iam_service_account.sa.id}"
}

resource "yandex_iam_service_account_static_access_key" "sa_static_key" {
  service_account_id = yandex_iam_service_account.sa.id
  description        = "Static access key for object storage"
}

# Network ресурсы
resource "yandex_vpc_network" "network" {
  name = var.yc_network_name
}

resource "yandex_vpc_subnet" "subnet" {
  name           = var.yc_subnet_name
  zone           = var.yc_zone
  network_id     = yandex_vpc_network.network.id
  v4_cidr_blocks = [var.yc_subnet_range]
  route_table_id = yandex_vpc_route_table.route_table.id
}

resource "yandex_vpc_gateway" "nat_gateway" {
  name = var.yc_nat_gateway_name
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "route_table" {
  name       = var.yc_route_table_name
  network_id = yandex_vpc_network.network.id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.nat_gateway.id
  }
}

resource "yandex_vpc_security_group" "security_group" {
  name        = var.yc_security_group_name
  description = "Security group for Dataproc cluster"
  network_id  = yandex_vpc_network.network.id

  ingress {
    protocol       = "ANY"
    description    = "Allow all incoming traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    protocol       = "TCP"
    description    = "UI"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 443
  }

  ingress {
    protocol       = "TCP"
    description    = "SSH"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 22
  }

  ingress {
    protocol       = "TCP"
    description    = "Jupyter"
    v4_cidr_blocks = ["0.0.0.0/0"]
    port           = 8888
  }

  egress {
    protocol       = "ANY"
    description    = "Allow all outgoing traffic"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol       = "ANY"
    description    = "Allow all outgoing traffic"
    from_port      = 0
    to_port        = 65535
    v4_cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    protocol          = "ANY"
    description       = "Internal"
    from_port         = 0
    to_port           = 65535
    predefined_target = "self_security_group"
  }
}

# Dataproc ресурсы
resource "yandex_dataproc_cluster" "dataproc_cluster" {
  depends_on  = [yandex_resourcemanager_folder_iam_member.sa_roles]
  bucket      = yandex_storage_bucket.data_bucket.bucket
  description = "Dataproc Cluster created by Terraform for OTUS project"
  name        = var.yc_dataproc_cluster_name
  labels = {
    created_by = "terraform"
  }
  service_account_id = yandex_iam_service_account.sa.id
  zone_id            = var.yc_zone
  security_group_ids = [yandex_vpc_security_group.security_group.id]


  cluster_config {
    version_id = var.yc_dataproc_version

    hadoop {
      services = ["HDFS", "YARN", "SPARK", "HIVE", "TEZ"]
      properties = {
        "yarn:yarn.resourcemanager.am.max-attempts" = 5
      }
      ssh_public_keys = [file(var.public_key_path)]
    }

    subcluster_spec {
      name = "master"
      role = "MASTERNODE"
      resources {
        resource_preset_id = var.dataproc_master_resources.resource_preset_id
        disk_type_id       = "network-ssd"
        disk_size          = var.dataproc_master_resources.disk_size
      }
      subnet_id        = yandex_vpc_subnet.subnet.id
      hosts_count      = 1
      assign_public_ip = true
    }

    subcluster_spec {
      name = "data"
      role = "DATANODE"
      resources {
        resource_preset_id = var.dataproc_data_resources.resource_preset_id
        disk_type_id       = "network-hdd"
        disk_size          = var.dataproc_data_resources.disk_size
      }
      subnet_id   = yandex_vpc_subnet.subnet.id
      hosts_count = var.dataproc_data_hosts_count
    }
  }
}

# Provisioner для настройки кластера после его создания
resource "null_resource" "cluster_provisioner" {
  depends_on = [yandex_dataproc_cluster.dataproc_cluster]

  provisioner "local-exec" {
    command = <<-EOT
      set -e
      
      LOG_FILE="terraform_provision.log"
      echo "Starting provisioner script..." > $LOG_FILE

      # --- Ожидание появления ВМ и получение IP ---
      echo "Waiting for master node VM to appear..." >> $LOG_FILE
      MASTER_IP=""
      i=0
      while [ $i -lt 30 ]; do
        # Логируем полный вывод yc для отладки
        echo "--- YC compute instance list output (Attempt $((i+1))) ---" >> $LOG_FILE
        yc --token ${var.yc_token} --folder-id ${var.yc_folder_id} compute instance list --format json >> $LOG_FILE 2>&1 || true
        echo "--- End of YC output ---" >> $LOG_FILE

        # Ищем ВМ по меткам кластера и роли мастер-ноды
        MASTER_IP=$(yc --token ${var.yc_token} --folder-id ${var.yc_folder_id} compute instance list --format json | jq -r '.[] | select(.labels.cluster_id == "${yandex_dataproc_cluster.dataproc_cluster.id}" and .labels.subcluster_role == "masternode") | .network_interfaces[0].primary_v4_address.one_to_one_nat.address // ""' 2>>$LOG_FILE)
        if [ -n "$MASTER_IP" ]; then
          echo "VM found, IP: $MASTER_IP" >> $LOG_FILE
          break
        fi
        i=$((i+1))
        echo "Attempt $i: Master node VM not found, sleeping 10s..." >> $LOG_FILE
        sleep 10
      done

      if [ -z "$MASTER_IP" ]; then
        echo "Error: Master node VM not found or has no public IP." >> $LOG_FILE
        exit 1
      fi
      echo "Master node IP: $MASTER_IP" >> $LOG_FILE

      # --- Запуск Ansible ---
      echo "Running Ansible playbook..." >> $LOG_FILE
      PYTHONUNBUFFERED=1 ansible-playbook -v -i "$MASTER_IP," \
        --private-key ${var.private_key_path} \
        -u ubuntu \
        --ssh-common-args '-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null' \
        --extra-vars 'access_key_var=${yandex_iam_service_account_static_access_key.sa_static_key.access_key} secret_key_var=${yandex_iam_service_account_static_access_key.sa_static_key.secret_key} s3_bucket_var=${yandex_storage_bucket.data_bucket.bucket}' \
        ${path.root}/ansible/master_playbook.yml >>$LOG_FILE 2>&1
      
      echo "Provisioning finished." >> $LOG_FILE
    EOT
    
    environment = {
      YC_TOKEN     = var.yc_token
      YC_CLOUD_ID  = var.yc_cloud_id
      YC_FOLDER_ID = var.yc_folder_id
    }
  }
}
