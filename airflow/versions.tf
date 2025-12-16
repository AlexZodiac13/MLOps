terraform {
  required_version = ">= 1.0.0"

  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      # соответствие доступным локально релизам (см. .terraform.lock.hcl)
      version = ">= 0.170.0, <= 0.173.0"
    }
    null = {
      source  = "hashicorp/null"
      version = ">= 3.2.4"
    }
  }
}
