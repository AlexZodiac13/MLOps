output "cluster_id" {
  value = yandex_mdb_postgresql_cluster.postgres_cluster.id
}

output "fqdn" {
  value = yandex_mdb_postgresql_cluster.postgres_cluster.host[0].fqdn
}

output "postgres_connection_string" {
  value     = "postgresql://${yandex_mdb_postgresql_user.mlflow_user.name}:${var.postgres_password}@${yandex_mdb_postgresql_cluster.postgres_cluster.host[0].fqdn}:6432/${yandex_mdb_postgresql_database.mlflow_db.name}"
  sensitive = true
}

output "postgres_host" {
  value = yandex_mdb_postgresql_cluster.postgres_cluster.host[0].fqdn
}

output "postgres_port" {
  value = 6432
}

output "postgres_db" {
  value = yandex_mdb_postgresql_database.mlflow_db.name
}

output "postgres_user" {
  value = yandex_mdb_postgresql_user.mlflow_user.name
}
