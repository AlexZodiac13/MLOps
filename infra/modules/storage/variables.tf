variable "provider_config" {
  description = "Yandex Cloud configuration"
  type        = object({
    zone      = string
    folder_id = string
    token     = string
    cloud_id  = string
  })
}

variable "access_key" {
  description = "Access key for the bucket"
  type        = string
}

variable "secret_key" {
  description = "Secret key for the bucket"
  type        = string
}

variable "name" {
  description = "Name of the bucket"
  type        = string
}
