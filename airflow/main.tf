provider "yandex" {
  token     = var.yc_token
  cloud_id  = var.cloud_id
  folder_id = var.folder_id
  zone      = var.zone
}

resource "yandex_storage_bucket" "airflow_bucket" {
  bucket        = var.bucket_name
  force_destroy = true
  # ACL deprecated — manage access via grants/iam
  # Явно указываем папку и опционально используем статические ключи SA
  folder_id  = var.folder_id
  access_key = try(yandex_iam_service_account_static_access_key.airflow_sa_key[0].access_key, null)
  secret_key = try(yandex_iam_service_account_static_access_key.airflow_sa_key[0].secret_key, null)
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
}

locals {
  mlops_root = "${path.module}/.."
  mlops_all_files = fileset(local.mlops_root, "**")
  mlops_files = [for f in local.mlops_all_files : f
    if !(can(regex("^\\.terraform(/|$)", f)) || can(regex("^.*\\.tfstate$", f)) || can(regex("^.*\\.terraform\\/.*$", f)) || can(regex("^\\.git(/|$)", f)) )]
  mlops_files_map = { for f in local.mlops_files : f => f }
}

resource "yandex_storage_object" "mlops_files" {
  for_each = var.upload_with_terraform ? local.mlops_files_map : {}

  bucket = yandex_storage_bucket.airflow_bucket.bucket
  key    = each.key
  source = "${local.mlops_root}/${each.key}"
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
      access_key = try(yandex_iam_service_account_static_access_key.airflow_sa_key[0].access_key, "")
      secret_key = try(yandex_iam_service_account_static_access_key.airflow_sa_key[0].secret_key, "")
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
    # Required to allow the managed Airflow service to write metrics and integrate with monitoring
    "managed-airflow.integrationProvider"
  ]
}
