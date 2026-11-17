"""Kafka -> HDFS. Offset commits only after a successful WebHDFS write."""
import json
import os
import time
import requests
from confluent_kafka import Consumer
from common import kafka, TOPIC, registered_schema, decode
base = "http://namenode:9870/webhdfs/v1"
c = Consumer({**kafka(), "group.id": "hdfs-" + os.getenv("RUN_ID", "demo"),
              "auto.offset.reset": "earliest", "enable.auto.commit": False})
schema = registered_schema()
c.subscribe([TOPIC])
count = 0
try:
    deadline = time.monotonic() + 90
    while count < 6 and time.monotonic() < deadline:
        msg = c.poll(1)
        if msg is None:
            continue
        value = decode(msg, schema)
        directory = f"/events/{TOPIC}/{msg.partition()}"
        r = requests.put(base + directory, params={"op": "MKDIRS", "user.name": "root"}, timeout=20)
        r.raise_for_status()
        path = f"{directory}/{msg.offset()}.json"
        r = requests.put(base + path, params={"op": "CREATE", "overwrite": "true", "user.name": "root"}, allow_redirects=False, timeout=20)
        if r.status_code != 307:
            raise RuntimeError(f"Expected WebHDFS redirect, got {r.status_code}: {r.text}")
        response = requests.put(r.headers["Location"], data=(json.dumps(value) + "\n").encode(), timeout=30)
        response.raise_for_status()
        c.commit(message=msg, asynchronous=False)
        count += 1
        print(f"HDFS SAVED {path} id={value['id']} OFFSET COMMITTED")
    if count != 6:
        raise RuntimeError(f"Only {count}/6 records saved")
finally:
    c.close()
