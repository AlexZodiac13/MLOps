output "subnet_id" {
  value = yandex_vpc_subnet.subnet.id
}

output "network_id" {
  value = yandex_vpc_network.network.id
}

output "security_group_id" {
  value = yandex_vpc_security_group.security_group.id
}
