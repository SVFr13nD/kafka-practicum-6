"""Run on the integration VM. Reads connection settings from environment."""
import os
import re
from pathlib import Path
import requests
p = Path(".runtime")
p.mkdir(exist_ok=True, mode=0o755)
p.chmod(0o755)  # CA must be readable by the Schema Registry container user
password = os.environ["KAFKA_PASSWORD"]
if not re.fullmatch(r"[A-Za-z0-9_-]{16,}", password):
    raise ValueError("Use a random password of at least 16 letters/digits/_/- for safe env/JAAS rendering")
bootstrap = os.environ["BOOTSTRAP_SERVERS"]
r = requests.get("https://storage.yandexcloud.net/cloud-certs/CA.pem", timeout=30)
r.raise_for_status()
(p / "ca.pem").write_bytes(r.content)
(p / "client.env").write_text(f"BOOTSTRAP_SERVERS={bootstrap}\nKAFKA_SECURITY_PROTOCOL=SASL_SSL\nKAFKA_USERNAME=lab\nKAFKA_PASSWORD={password}\n")
(p / "client.properties").write_text(f"security.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"lab\" password=\"{password}\";\nssl.truststore.type=PEM\nssl.truststore.location=/certs/ca.pem\nssl.endpoint.identification.algorithm=https\n")
(p / "registry.env").write_text(f"SCHEMA_REGISTRY_HOST_NAME=schema-registry\nSCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081\nSCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS={bootstrap}\nSCHEMA_REGISTRY_KAFKASTORE_SECURITY_PROTOCOL=SASL_SSL\nSCHEMA_REGISTRY_KAFKASTORE_SASL_MECHANISM=SCRAM-SHA-512\nSCHEMA_REGISTRY_KAFKASTORE_SASL_JAAS_CONFIG=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"lab\" password=\"{password}\";\nSCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_TYPE=PEM\nSCHEMA_REGISTRY_KAFKASTORE_SSL_TRUSTSTORE_LOCATION=/certs/ca.pem\nSCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR=3\n")
for f in p.iterdir():
    f.chmod(0o644 if f.name == "ca.pem" else 0o600)
print("Cloud client configuration created; credentials were not printed")
