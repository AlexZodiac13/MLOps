terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
    }
  }
  required_version = ">= 1.00"
}

provider "yandex" {
  zone      = var.provider_config.zone
  folder_id = var.provider_config.folder_id
  token     = var.provider_config.token
  cloud_id  = var.provider_config.cloud_id
}
