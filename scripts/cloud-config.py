"""Prepare the Kafka CLI, Python client and Schema Registry on the integration VM."""
import os
import re
import ssl
from pathlib import Path
from urllib.request import urlopen

CA_URL = {
    "kz": "https://storage.yandexcloud.kz/cloud-certs/CA.pem",
    "ru": "https://storage.yandexcloud.net/cloud-certs/CA.pem",
}

password = os.environ["KAFKA_PASSWORD"]
servers = os.environ["BOOTSTRAP_SERVERS"].split(",")
if not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", password):
    raise ValueError("KAFKA_PASSWORD must contain 16–128 letters, digits, underscores or hyphens")
if len(set(servers)) != 3 or len(servers) != 3 or not all(
    re.fullmatch(r"[A-Za-z0-9.-]+:9091", server) for server in servers
):
    raise ValueError("BOOTSTRAP_SERVERS must list the three broker hostnames with port 9091")

directory = Path(".runtime")
directory.mkdir(exist_ok=True)
directory.chmod(0o755)
region = os.getenv("YC_REGION", "kz")
with urlopen(CA_URL[region], timeout=30) as response:
    ca_pem = response.read().decode("ascii")
ssl.create_default_context(cadata=ca_pem)
ca_path = directory / "ca.pem"
ca_path.write_text(ca_pem)
ca_path.chmod(0o644)

os.umask(0o077)
login = (
    'org.apache.kafka.common.security.scram.ScramLoginModule required '
    f'username="lab" password="{password}";'
)

def write_config(name, settings):
    path = directory / name
    if path.exists():
        path.chmod(0o600)
    with path.open("w") as file:
        for key, value in settings.items():
            file.write(f"{key}={value}\n")

write_config("client.env", {
    "BOOTSTRAP_SERVERS": ",".join(servers),
    "KAFKA_SECURITY_PROTOCOL": "SASL_SSL",
    "KAFKA_USERNAME": "lab",
    "KAFKA_PASSWORD": password,
})
write_config("client.properties", {
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.jaas.config": login,
    "ssl.truststore.type": "PEM",
    "ssl.truststore.location": "/certs/ca.pem",
    "ssl.endpoint.identification.algorithm": "https",
})
write_config("registry.env", {
    "SCHEMA_REGISTRY_HOST_NAME": "schema-registry",
    "SCHEMA_REGISTRY_LISTENERS": "http://0.0.0.0:8081",
    "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": ",".join(f"SASL_SSL://{s}" for s in servers),
    "SCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL": "SASL_SSL",
    "SCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM": "SCRAM-SHA-512",
    "SCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG": login,
    "SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_TYPE": "PEM",
    "SCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION": "/certs/ca.pem",
    "SCHEMA_REGISTRY_KAFKASTORE_TOPIC": "orders-schemas",
    "SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR": 3,
})
print("Created Kafka client and Schema Registry settings in .runtime/")
