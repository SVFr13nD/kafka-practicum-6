import json
import os
import time
from confluent_kafka import Consumer
from common import kafka, TOPIC, registered_schema, decode, expected_ids
c = Consumer({**kafka(), "group.id": "reader-" + os.getenv("RUN_ID", "demo"),
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
schema = registered_schema()
c.subscribe([TOPIC])
count = 0
remaining = expected_ids()
try:
    deadline = time.monotonic() + 90
    while count < 6 and time.monotonic() < deadline:
        msg = c.poll(1)
        if msg is None:
            continue
        value = decode(msg, schema)
        if value["id"] not in remaining:
            continue
        print(f"READ partition={msg.partition()} offset={msg.offset()} value={json.dumps(value)}")
        c.commit(message=msg, asynchronous=False)
        remaining.remove(value["id"])
        count += 1
    if count != 6:
        raise RuntimeError(f"Expected 6 records, received {count}")
finally:
    c.close()
