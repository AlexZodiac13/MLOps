variable "yc_token" {
  type        = string
  description = "YC OAuth token (can be passed as env or in terraform.tfvars)"
}

variable "cloud_id" {
  type        = string
  description = "Yandex Cloud ID"
}

variable "folder_id" {
  type        = string
  description = "Yandex Cloud folder id"
}

variable "zone" {
  type        = string
  description = "Zone for resources"
  default     = "ru-central1-a"
}

variable "network_id" {
  type        = string
  description = "VPC network id to create Airflow in"
  default     = ""
}

variable "create_network" {
  type        = bool
  description = "Create a new VPC network and subnet for Airflow. If false, use existing `network_id` and `subnet_ids`."
  default     = true
}

variable "subnet_ids" {
  type        = list(string)
  description = "List of subnet resource IDs to use when `create_network=false`. Leave empty to use created subnet when `create_network=true`."
  default     = []
}


variable "upload_with_terraform" {
  type        = bool
  description = "If true, upload repository files to Object Storage using Terraform resources (`yandex_storage_object`). If false, use local sync script or DAG-based upload."
  default     = true
}

variable "network_name" {
  type        = string
  description = "Name for the VPC network to create when `create_network=true`."
  default     = "airflow-network"
}

variable "subnet_name" {
  type        = string
  description = "Name for the subnet to create when `create_network=true`."
  default     = "airflow-subnet"
}

variable "subnet_cidr" {
  type        = string
  description = "CIDR block for the subnet to create when `create_network=true`."
  default     = "10.0.0.0/24"
}


variable "bucket_name" {
  type        = string
  description = "S3 (Object storage) bucket name for dags/airflow files"
}

variable "cluster_name" {
  type        = string
  description = "Managed Airflow cluster name"
  default     = "airflow-managed"
}

variable "airflow_version" {
  type    = string
  default = "2.8" 
}

variable "python_version" {
  type    = string
  default = "3.10"
  description = "Python runtime version for Airflow (e.g. 3.10, 3.11). Must be in form of Major.Minor."

  validation {
    condition     = can(regex("^3\\.(8|9|10|11)$", var.python_version))
    error_message = "python_version must be in the 3.8, 3.9, 3.10, or 3.11 family (e.g. 3.10)."
  }
}

variable "web_ui_public_ip" {
  type    = bool
  default = true
}

variable "labels" {
  type    = map(string)
  default = {}
}

variable "create_airflow_service_account" {
  type    = bool
  default = true
  description = "Create a dedicated service account for Airflow and bind basic roles (storage)."
}

variable "airflow_service_account_name" {
  type    = string
  default = "airflow-sa"
}

variable "scheduler_resource_preset" {
  type    = string
  default = "c2-m4"
}

variable "worker_resource_preset" {
  type    = string
  default = "c2-m4"
}

variable "worker_disk_size_gb" {
  type    = number
  default = 20
}

variable "dags_bucket_path" {
  type    = string
  default = "dags"
}

variable "admin_password" {
  type        = string
  description = "Admin password for Airflow web UI (set in terraform.tfvars)"
  sensitive   = true
}
