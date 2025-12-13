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
}

variable "subnet_ids" {
  type        = list(string)
  description = "List of subnet ids"
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
  default = "2.6" # поменяйте при необходимости
}

variable "web_ui_public_ip" {
  type    = bool
  default = true
}

variable "labels" {
  type    = map(string)
  default = {}
}
