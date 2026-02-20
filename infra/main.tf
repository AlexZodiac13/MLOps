
resource "yandex_storage_bucket" "airflow_bucket" {
  bucket        = var.bucket_name
  force_destroy = true
  # ACL deprecated — manage access via grants/iam
  # Явно указываем папку
  folder_id  = var.folder_id
}

# Grant для доступа SA к бакету (если SA создаётся)
resource "yandex_storage_bucket_grant" "airflow_bucket_sa_grant" {
  count  = var.create_airflow_service_account ? 1 : 0
  bucket = yandex_storage_bucket.airflow_bucket.id

  grant {
    permissions = ["WRITE", "READ"]
    type        = "CanonicalUser"
    id          = try(yandex_iam_service_account.airflow_sa[0].id, "")
  }
}

# Optionally create a dedicated VPC network + subnet for Airflow
resource "yandex_vpc_network" "airflow_network" {
  count     = var.create_network ? 1 : 0
  name      = var.network_name
  folder_id = var.folder_id
}

resource "yandex_vpc_subnet" "airflow_subnet" {
  count          = var.create_network ? 1 : 0
  name           = var.subnet_name
  zone           = var.zone
  folder_id      = var.folder_id
  network_id     = yandex_vpc_network.airflow_network[0].id
  v4_cidr_blocks = [var.subnet_cidr]
  route_table_id = yandex_vpc_route_table.airflow_rt[0].id
}

resource "yandex_vpc_gateway" "airflow_nat" {
  count = var.create_network ? 1 : 0
  name  = "${var.network_name}-nat"
  shared_egress_gateway {}
}

resource "yandex_vpc_route_table" "airflow_rt" {
  count      = var.create_network ? 1 : 0
  name       = "${var.network_name}-rt"
  network_id = yandex_vpc_network.airflow_network[0].id

  static_route {
    destination_prefix = "0.0.0.0/0"
    gateway_id         = yandex_vpc_gateway.airflow_nat[0].id
  }
}


locals {
  # Корневая папка исходников: родительская папка (локально)
  source_root = "${path.module}/.."
  mlops_root  = "${path.module}/.."
}

# Загрузка файлов в S3 нативными средствами Terraform.
# Это НЕ требует наличия awscli или pip на локальной машине.
resource "yandex_storage_object" "project_files" {
  # Перебираем все файлы в папках dag и ml относительно корня проекта
  for_each = fileset("${path.module}/..", "{dag,ml}/**/*")
  
  bucket = yandex_storage_bucket.airflow_bucket.bucket
  key    = "${var.dags_bucket_path}/${each.value}"
  source = "${path.module}/../${each.value}"
  
  depends_on = [
    yandex_storage_bucket.airflow_bucket
  ]
}

resource "yandex_storage_object" "mlops_sentinel" {
  count  = var.upload_with_terraform ? 1 : 0
  bucket = yandex_storage_bucket.airflow_bucket.bucket
  key    = "mlops/.upload_complete"
  source = "${local.mlops_root}/deploy-sentinel.txt"
}

resource "yandex_airflow_cluster" "airflow" {
  name       = var.cluster_name
  folder_id  = var.folder_id
  subnet_ids = var.create_network ? [yandex_vpc_subnet.airflow_subnet[0].id] : var.subnet_ids


  depends_on = [
    yandex_resourcemanager_folder_iam_member.airflow_sa_roles,
    yandex_storage_object.mlops_sentinel,
  ]

  airflow_version = var.airflow_version
  python_version  = var.python_version

  pip_packages = [
    "mlflow==2.10.2",
    "boto3",
    "botocore",
    "llama-cpp-python",
    "cmake"
  ]


  webserver = {
    resource_preset_id = var.worker_resource_preset
    count              = 1
    public_ip          = var.web_ui_public_ip
  }


  scheduler = {
    resource_preset_id = var.scheduler_resource_preset
    count              = 1
  }

  worker = {
    resource_preset_id = var.worker_resource_preset
    min_count          = 1
    max_count          = 2
  }

  # code_sync — синхронизация DAG'ов с Object Storage
  code_sync = {
    s3 = {
      bucket     = yandex_storage_bucket.airflow_bucket.bucket
      path       = var.dags_bucket_path
      access_key = local.sa_access_key
      secret_key = local.sa_secret_key
    }
  }

  # Админ-пароль (передавайте в `terraform.tfvars` переменной `admin_password`)
  admin_password = var.admin_password

  labels = var.labels

  service_account_id = var.create_airflow_service_account ? yandex_iam_service_account.airflow_sa[0].id : null
}

resource "yandex_iam_service_account" "airflow_sa" {
  count       = var.create_airflow_service_account ? 1 : 0
  name        = var.airflow_service_account_name
  description = "Service account for Managed Airflow"
}

# Делаем привязки ролей на уровне папки к сервисному аккаунту
resource "yandex_resourcemanager_folder_iam_member" "airflow_sa_roles" {
  count = var.create_airflow_service_account ? length(local.airflow_roles) : 0

  folder_id = var.folder_id
  role      = local.airflow_roles[count.index]
  member    = "serviceAccount:${yandex_iam_service_account.airflow_sa[0].id}"
}

resource "yandex_iam_service_account_static_access_key" "airflow_sa_key" {
  count = var.create_airflow_service_account ? 1 : 0

  service_account_id = yandex_iam_service_account.airflow_sa[0].id
  description        = "Static access key for Airflow to access Object Storage"
}

locals {
  airflow_roles = [
    "storage.admin",
    "iam.serviceAccounts.user",
    "viewer",
    # Required to allow the managed Airflow service to write metrics and integrate with monitoring
    "managed-airflow.integrationProvider"
  ]
  
  # Determine which keys to use: provided vars (from GitHub secrets) or generated resource
  # We prefer the generated SA key for Yandex-specific tasks (like sync) to avoid conflicts with backend secrets
  sa_access_key = try(yandex_iam_service_account_static_access_key.airflow_sa_key[0].access_key, var.aws_access_key_id)
  sa_secret_key = try(yandex_iam_service_account_static_access_key.airflow_sa_key[0].secret_key, var.aws_secret_access_key)
}

resource "local_file" "variables_json" {
  content = <<EOF
{
  "AWS_ACCESS_KEY_ID": ${jsonencode(coalesce(local.sa_access_key, "no_key"))},
  "AWS_SECRET_ACCESS_KEY": ${jsonencode(coalesce(local.sa_secret_key, "no_secret"))},
  "MLFLOW_S3_ENDPOINT_URL": ${jsonencode(var.minio_endpoint_url)},
  "MINIO_ACCESS_KEY": ${jsonencode(var.minio_access_key)},
  "MINIO_SECRET_KEY": ${jsonencode(var.minio_secret_key)},
  "airflow-bucket-name": ${jsonencode(yandex_storage_bucket.airflow_bucket.bucket)},
  "yc_token": ${jsonencode(var.yc_token)},
  "cloud_id": ${jsonencode(var.cloud_id)},
  "folder_id": ${jsonencode(var.folder_id)},
  "zone": ${jsonencode(var.zone)},
  "yc_subnet_name": ${jsonencode(var.subnet_name)},
  "yc_service_account_name": ${jsonencode(var.airflow_service_account_name)},
  "yc_network_name": ${jsonencode(var.network_name)},
  "yc_subnet_range": ${jsonencode(var.subnet_cidr)},
  "yc_dataproc_cluster_name": ${jsonencode(var.yc_dataproc_cluster_name)},
  "yc_dataproc_version": ${jsonencode(var.yc_dataproc_version)},
  "public_key": ${jsonencode(var.public_key)},
  "private_key": ${jsonencode(var.private_key)},
  "dataproc_master_resources": ${jsonencode(var.dataproc_master_resources)},
  "dataproc_data_resources": ${jsonencode(var.dataproc_data_resources)},
  "dataproc_data_hosts_count": ${jsonencode(var.dataproc_data_hosts_count)},
  "yc_security_group_name": ${jsonencode(var.yc_security_group_name)},
  "yc_nat_gateway_name": ${jsonencode(var.yc_nat_gateway_name)},
  "yc_route_table_name": ${jsonencode(var.yc_route_table_name)}
}
EOF
  filename = "variables.json"
}

resource "yandex_storage_object" "airflow_variables_json" {
  bucket = yandex_storage_bucket.airflow_bucket.bucket
  # Загружаем в папку dag/, чтобы файл лежал рядом с import_variables.py
  key    = "dag/variables.json"
  source = local_file.variables_json.filename
  
  depends_on = [
    local_file.variables_json,
    yandex_storage_bucket.airflow_bucket
  ]
}

