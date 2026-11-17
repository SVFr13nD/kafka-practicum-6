import json
import requests
from common import REGISTRY, TOPIC, SCHEMA
r = requests.post(f"{REGISTRY}/subjects/{TOPIC}-value/versions",
                  json={"schemaType": "JSON", "schema": json.dumps(SCHEMA)}, timeout=20)
r.raise_for_status()
print("REGISTERED", r.json())
