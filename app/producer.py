import json
import os
from confluent_kafka import Producer
from jsonschema import validate
from common import kafka, TOPIC, registered_schema
schema = registered_schema()
p = Producer({**kafka(), "acks": "all", "enable.idempotence": False, "delivery.timeout.ms": 30000})
errors = []
def delivered(error, msg):
    if error:
        errors.append(str(error))
    else:
        print(f"DELIVERED topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} value={msg.value().decode()}")
for i in range(6):
    value = {"id": f"{os.getenv('RUN_ID', 'demo')}-{i}", "product": "book", "quantity": i + 1}
    validate(value, schema)
    p.produce(TOPIC, partition=i % 3, key=value["id"], value=json.dumps(value), on_delivery=delivered)
remaining = p.flush(40)
if remaining or errors:
    raise RuntimeError(f"Delivery failed: pending={remaining}, errors={errors}")
