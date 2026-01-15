resource "random_id" "bucket_id" {
  byte_length = 8
}

resource "yandex_storage_bucket" "bucket" {
  bucket        = "${var.name}-${random_id.bucket_id.hex}"
  access_key    = var.access_key
  secret_key    = var.secret_key
  force_destroy = true
}