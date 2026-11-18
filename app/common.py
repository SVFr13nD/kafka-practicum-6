import json
import os
from pathlib import Path
import requests
from confluent_kafka import TopicPartition
from jsonschema import validate
TOPIC = "orders"
REGISTRY = os.getenv("REGISTRY_URL", "http://schema-registry:8081")
SCHEMA = json.loads((Path(__file__).resolve().parents[1] / "schemas/order.json").read_text())
def kafka():
    config = {"bootstrap.servers": os.environ["BOOTSTRAP_SERVERS"]}
    if os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT") == "SASL_SSL":
        config.update({"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512",
                       "sasl.username": os.environ["KAFKA_USERNAME"], "sasl.password": os.environ["KAFKA_PASSWORD"],
                       "ssl.ca.location": "/certs/ca.pem", "ssl.endpoint.identification.algorithm": "https"})
    return config
def registered_schema():
    r = requests.get(f"{REGISTRY}/subjects/{TOPIC}-value/versions/latest", timeout=20)
    r.raise_for_status()
    return json.loads(r.json()["schema"])
def decode(message, schema):
    if message.error():
        raise RuntimeError(str(message.error()))
    value = json.loads(message.value())
    validate(value, schema)
    return value


def read_orders(consumer):
    """For a verification batch, begin at the offsets recorded before sending."""
    start = os.getenv("START_OFFSETS")
    if not start:
        consumer.subscribe([TOPIC])
        return
    offsets = [int(value) for value in start.split(",")]
    if len(offsets) != 3 or min(offsets) < 0:
        raise ValueError("Expected three non-negative start offsets for orders")
    consumer.assign([TopicPartition(TOPIC, p, offset) for p, offset in enumerate(offsets)])
