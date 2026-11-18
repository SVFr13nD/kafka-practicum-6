terraform {
  required_version = ">= 1.9.0"
  required_providers {
    yandex = {
      source  = "yandex-cloud/yandex"
      version = "~> 0.140"
    }
  }
}
provider "yandex" {
  folder_id = var.folder_id
}
variable "folder_id" { type = string }
variable "password" {
  type      = string
  sensitive = true
}
variable "ssh_public_key" { type = string }
variable "admin_cidr" {
  type        = string
  description = "Your external IPv4 /32 for SSH"
}
locals {
  zones = { "ru-central1-a" = "10.71.0.0/24", "ru-central1-b" = "10.72.0.0/24", "ru-central1-d" = "10.73.0.0/24" }
}
resource "yandex_vpc_network" "lab" { name = "practicum7" }
resource "yandex_vpc_subnet" "lab" {
  for_each       = local.zones
  name           = "practicum7-${each.key}"
  zone           = each.key
  network_id     = yandex_vpc_network.lab.id
  v4_cidr_blocks = [each.value]
}
resource "yandex_vpc_security_group" "lab" {
  name       = "practicum7"
  network_id = yandex_vpc_network.lab.id
  ingress {
    protocol          = "ANY"
    predefined_target = "self_security_group"
  }
  ingress {
    protocol       = "TCP"
    port           = 22
    v4_cidr_blocks = [var.admin_cidr]
  }
  egress {
    protocol       = "ANY"
    v4_cidr_blocks = ["0.0.0.0/0"]
  }
}
resource "yandex_mdb_kafka_cluster" "lab" {
  name               = "practicum7"
  environment        = "PRODUCTION"
  network_id         = yandex_vpc_network.lab.id
  subnet_ids         = [for s in yandex_vpc_subnet.lab : s.id]
  security_group_ids = [yandex_vpc_security_group.lab.id]
  config {
    version          = "3.9"
    brokers_count    = 1
    zones            = keys(local.zones)
    assign_public_ip = false
    kafka {
      resources {
        resource_preset_id = "s3-c2-m8"
        disk_type_id       = "network-ssd"
        disk_size          = 32
      }
      kafka_config {
        auto_create_topics_enable = false
      }
    }
    zookeeper {
      resources {
        resource_preset_id = "s3-c2-m8"
        disk_type_id       = "network-ssd"
        disk_size          = 10
      }
    }
  }
}
resource "yandex_mdb_kafka_topic" "orders" {
  cluster_id         = yandex_mdb_kafka_cluster.lab.id
  name               = "orders"
  partitions         = 3
  replication_factor = 3
  topic_config {
    cleanup_policy      = "CLEANUP_POLICY_DELETE"
    retention_ms        = 86400000
    segment_bytes       = 134217728
    min_insync_replicas = 2
  }
}
resource "yandex_mdb_kafka_topic" "schemas" {
  cluster_id         = yandex_mdb_kafka_cluster.lab.id
  name               = "_schemas"
  partitions         = 1
  replication_factor = 3
  topic_config {
    cleanup_policy      = "CLEANUP_POLICY_COMPACT"
    min_insync_replicas = 2
  }
}
resource "yandex_mdb_kafka_user" "lab" {
  cluster_id = yandex_mdb_kafka_cluster.lab.id
  name       = "lab"
  password   = var.password
  permission {
    topic_name = yandex_mdb_kafka_topic.orders.name
    role       = "ACCESS_ROLE_PRODUCER"
  }
  permission {
    topic_name = yandex_mdb_kafka_topic.orders.name
    role       = "ACCESS_ROLE_CONSUMER"
  }
  permission {
    topic_name = yandex_mdb_kafka_topic.schemas.name
    role       = "ACCESS_ROLE_PRODUCER"
  }
  permission {
    topic_name = yandex_mdb_kafka_topic.schemas.name
    role       = "ACCESS_ROLE_CONSUMER"
  }
}
data "yandex_compute_image" "ubuntu" { family = "ubuntu-2404-lts" }
resource "yandex_compute_instance" "integration" {
  name        = "practicum7-integration"
  zone        = "ru-central1-a"
  platform_id = "standard-v3"
  resources {
    cores  = 4
    memory = 8
  }
  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.ubuntu.id
      size     = 40
      type     = "network-ssd"
    }
  }
  network_interface {
    subnet_id          = yandex_vpc_subnet.lab["ru-central1-a"].id
    nat                = true
    security_group_ids = [yandex_vpc_security_group.lab.id]
  }
  metadata = {
    ssh-keys  = "ubuntu:${var.ssh_public_key}"
    user-data = "#cloud-config\npackage_update: true\npackages:\n  - docker.io\n  - docker-compose-v2\n  - git\n  - curl\n  - python3\n"
  }
}
output "cluster_id" { value = yandex_mdb_kafka_cluster.lab.id }
output "vm_ip" { value = yandex_compute_instance.integration.network_interface[0].nat_ip_address }
output "bootstrap_servers" {
  value = join(",", [for h in yandex_mdb_kafka_cluster.lab.host : "${h.name}:9091" if h.role == "KAFKA"])
}
