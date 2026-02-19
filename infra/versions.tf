terraform {
  backend "s3" {
    endpoints = {
      s3  = "https://s3.owgrant.su"
      sts = "https://s3.owgrant.su"
      iam = "https://s3.owgrant.su"
    }
    bucket     = "otus"
    region     = "us-east-1" # MinIO требует регион, даже фиктивный
    key        = "state/airflow.tfstate" # Путь к файлу состояния
    
    # Чтобы работало с MinIO (самоподписанные сертификаты или http)
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    use_path_style              = true # instead of force_path_style
    skip_s3_checksum            = true
  }
  required_version = ">= 1.0.0"

  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      # соответствие доступным локально релизам (см. .terraform.lock.hcl)
      version = ">= 0.170.0, <= 0.174.0"
    }
    null = {
      source  = "hashicorp/null"
      version = ">= 3.2.4"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.0"
    }
  }
}
