variable "yc_token" {
  type = string
}

variable "cloud_id" {
  type = string
}

variable "folder_id" {
  type = string
}

variable "zone" {
  type    = string
  default = "ru-central1-b"
}

variable "minio_access_key" { type = string }
variable "minio_secret_key" { type = string }
variable "bucket_name" { type = string }

variable "public_key" {
  type        = string
  description = "SSH public key"
}

variable "private_key" {
  type        = string
  description = "SSH private key for login"
}
