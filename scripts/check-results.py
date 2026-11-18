"""Compare the producer acknowledgments, two Kafka reads and the files read back from HDFS."""
import json
import re
import sys
from pathlib import Path

directory = Path(sys.argv[1])
run_id = sys.argv[2]

def lines(name):
    return (directory / name).read_text().splitlines()

def values(name, prefix):
    return [json.loads(row.split("value=", 1)[1]) for row in lines(name) if row.startswith(prefix)]

def matching(records):
    return sorted(records, key=lambda record: record["id"])

subjects = json.loads((directory / "subjects.json").read_text())
versions = json.loads((directory / "versions.json").read_text())
assert "orders-value" in subjects and any(isinstance(v, int) and v > 0 for v in versions)

description = (directory / "topic-describe.txt").read_text()
assert "PartitionCount: 3" in description and "ReplicationFactor: 3" in description
for setting in ["cleanup.policy=delete", "retention.ms=86400000", "segment.bytes=134217728", "min.insync.replicas=2"]:
    assert setting in description, setting
partitions = re.findall(r"Partition: (\d+)\s+Leader: (\d+)\s+Replicas: ([\d,]+)\s+Isr: ([\d,]+)", description)
assert {part for part, *_ in partitions} == {"0", "1", "2"}
for part, leader, replicas, isr in partitions:
    replica_ids = set(replicas.split(","))
    assert len(replica_ids) == 3 and leader in replica_ids and replica_ids == set(isr.split(",")), part

sent = matching(values("producer.txt", "DELIVERED"))
ids = {row["id"] for row in sent}
assert ids == {f"{run_id}-{i}" for i in range(6)} and len(sent) == 6
assert matching(values("consumer.txt", "READ")) == sent
assert matching(json.loads(line) for line in lines("console-consumer.txt") if line.strip()) == sent
hdfs = [json.loads(line) for line in lines("hdfs-data.jsonl") if line.strip()]
assert matching(row for row in hdfs if row["id"] in ids) == sent
assert re.search(r"\bHEALTHY\b", (directory / "hdfs-fsck.txt").read_text())

result = "PASS: schema registered; 3 partitions; RF=3; ISR=3; six records delivered, read and verified in HDFS."
print(result)
(directory / "result.txt").write_text(result + "\n")
