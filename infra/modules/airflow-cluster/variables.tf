variable "instance_name" {
  description = "Name of the compute instance"
  type        = string
}

variable "admin_password" {
  description = "Admin password for the Airflow web interface"
  type        = string
}

variable "service_account_id" {
  description = "ID of the service account"
  type        = string
}

variable "subnet_id" {
  description = "ID of the subnet"
  type        = string
}

variable "bucket_name" {
  description = "Name of the bucket"
  type        = string
}

variable "provider_config" {
  description = "Yandex Cloud configuration"
  type = object({
    zone      = string
    folder_id = string
    token     = string
    cloud_id  = string
  })
}

