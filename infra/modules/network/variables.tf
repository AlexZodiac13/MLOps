variable "network_name" {
  description = "Name of the VPC network"
  type        = string
  default     = "otus-network"
}

variable "subnet_name" {
  description = "Name of the subnet"
  type        = string
  default     = "otus-subnet"
}

variable "subnet_range" {
  description = "Subnet range"
  type        = string
  default     = "10.0.0.0/24"
}

variable "route_table_name" {
  description = "Name of the route table"
  type        = string
  default     = "otus-route-table"
}

variable "nat_gateway_name" {
  description = "Name of the NAT gateway"
  type        = string
  default     = "otus-nat-gateway"
}

variable "security_group_name" {
  description = "Name of the security group"
  type        = string
  default     = "otus-security-group"
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
