
variable "yc_cloud_id" {
  type    = string
  default = null
}

variable "yc_folder_id" {
  type    = string
  default = null
}

variable "yc_zone" {
  type    = string
  default = null
}

variable "yc_dataproc_cluster_name" {
  type    = string
  default = null
}

variable "yc_dataproc_version" {
  type    = string
  default = null
}

variable "public_key" {
  type    = string
  default = null
}

variable "private_key" {
  type      = string
  default   = null
  sensitive = true
}

variable "dataproc_master_resources" {
  type = object({
    resource_preset_id = string
    disk_size          = number
  })
  default = null
}

variable "dataproc_data_resources" {
  type = object({
    resource_preset_id = string
    disk_size          = number
  })
  default = null
}

variable "dataproc_data_hosts_count" {
  type    = number
  default = null
}

variable "yc_security_group_name" {
  type    = string
  default = null
}

variable "yc_nat_gateway_name" {
  type    = string
  default = null
}

variable "yc_route_table_name" {
  type    = string
  default = null
}

variable "yc_subnet_range" {
  type    = string
  default = null
}

variable "yc_network_name" {
  type    = string
  default = null
}

variable "yc_subnet_name" {
  type    = string
  default = null
}

variable "yc_service_account_name" {
  type    = string
  default = null
}
