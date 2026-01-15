variable "subnet_id" {
  description = "ID of the subnet"
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

variable "postgres_password" {
  description = "Password for PostgreSQL database"
  type        = string
  sensitive   = true
}

variable "postgres_db" {
  description = "Name of the PostgreSQL database"
  type        = string
  default     = "mlflow"
}

variable "postgres_user" {
  description = "Username for PostgreSQL database"
  type        = string
  default     = "mlflow"
}

variable "postgres_max_connections" {
  description = "Maximum number of connections for PostgreSQL"
  type        = number
  default     = 100
}

variable "postgres_shared_buffers" {
  description = "Shared buffers for PostgreSQL"
  type        = string
  default     = "128MB"
}

variable "mlflow_server_ip" {
  description = "IP address of the MLflow server"
  type        = string
  default     = "0.0.0.0/0"  # По умолчанию разрешаем подключения с любого IP
}

variable "cluster_name" {
  description = "Name of the PostgreSQL cluster"
  type        = string
}

variable "network_id" {
  description = "ID of the network"
  type        = string
}

variable "security_group_id" {
  description = "ID of the security group"
  type        = string
}

variable "deletion_protection" {
  description = "Protection from accidental deletion"
  type        = bool
  default     = false
}

variable "postgres_version" {
  description = "PostgreSQL version"
  type        = string
  default     = "15"
}

variable "resource_preset_id" {
  description = "Resource preset ID for PostgreSQL hosts"
  type        = string
  default     = "s2.micro" # Минимальный размер
}

variable "disk_type_id" {
  description = "Disk type ID for PostgreSQL hosts"
  type        = string
  default     = "network-ssd"
}

variable "disk_size" {
  description = "Disk size in GB"
  type        = number
  default     = 20 # Минимальный размер
}

variable "assign_public_ip" {
  description = "Assign public IP to PostgreSQL host"
  type        = bool
  default     = true
}
