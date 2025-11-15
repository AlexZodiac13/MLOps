variable "yc_token" {
  type        = string
  description = "Yandex Cloud OAuth token"
  sensitive   = true
}

variable "yc_cloud_id" {
  type        = string
  description = "Yandex Cloud Cloud ID"
}

variable "yc_folder_id" {
  type        = string
  description = "Yandex Cloud Folder ID"
}

variable "yc_zone" {
  type        = string
  description = "Yandex Cloud Zone"
}

variable "yc_subnet_name" {
  type        = string
  description = "VPC Subnet name"
}

variable "yc_service_account_name" {
  type        = string
  description = "Service Account name"
}

variable "yc_bucket_name" {
  type        = string
  description = "S3 Bucket name"
}

variable "yc_network_name" {
  type        = string
  description = "VPC Network name"
}

variable "yc_route_table_name" {
  type        = string
  description = "VPC Route table name"
}

variable "yc_nat_gateway_name" {
  type        = string
  description = "VPC NAT Gateway name"
}

variable "yc_security_group_name" {
  type        = string
  description = "VPC Security Group name"
}

variable "yc_subnet_range" {
  type        = string
  description = "VPC Subnet CIDR range"
}

variable "yc_dataproc_cluster_name" {
  type        = string
  description = "Dataproc cluster name"
}

variable "yc_dataproc_version" {
  type        = string
  description = "Dataproc version"
}

variable "public_key_path" {
  type        = string
  description = "Path to the public SSH key"
}

variable "private_key_path" {
  type        = string
  description = "Path to the private SSH key"
}

variable "dataproc_master_resources" {
  type = object({
    resource_preset_id = string
    disk_size          = number
  })
  description = "Resources for the Dataproc master node."
}

variable "dataproc_data_resources" {
  type = object({
    resource_preset_id = string
    disk_size          = number
  })
  description = "Resources for the Dataproc data nodes."
}

variable "dataproc_data_hosts_count" {
  type        = number
  description = "Number of hosts in the data subcluster."
}
