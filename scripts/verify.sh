#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
MODE=${1:-local}
export RUN_ID=${RUN_ID:-$(python3 -c 'import uuid; print(uuid.uuid4().hex)')}
OUT="evidence/$MODE"
mkdir -p "$OUT" .runtime
rm -f "$OUT/result.txt"
printf 'Environment: %s\nRun ID: %s\nCommit: %s\n' "$MODE" "$RUN_ID" "$(git rev-parse HEAD)" > "$OUT/run.txt"
collect_logs() {
  docker compose logs --no-color > "$OUT/docker.log" 2>&1 || true
  docker compose ps -a > "$OUT/services.txt" 2>&1 || true
}
trap collect_logs EXIT
if [ "$MODE" = local ]; then
  export COMPOSE_FILE=docker-compose.yml:compose.local.yml
  export BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
  : > .runtime/client.properties
elif [ "$MODE" = cloud ]; then
  export COMPOSE_FILE=docker-compose.yml
  export CLIENT_ENV=.runtime/client.env REGISTRY_ENV=.runtime/registry.env
  : "${BOOTSTRAP_SERVERS:?Set cloud broker FQDNs}"
else
  echo 'Mode must be local or cloud' >&2; exit 1
fi
cli() { docker compose run --rm -T cli "$1"; }
docker compose up -d --build

ready=false
for i in $(seq 1 45); do
  if cli "kafka-broker-api-versions --bootstrap-server $BOOTSTRAP_SERVERS --command-config /certs/client.properties" > "$OUT/brokers.txt" 2>&1; then ready=true; break; fi
  sleep 4
done
[ "$ready" = true ] || { cat "$OUT/brokers.txt"; exit 1; }
if [ "$MODE" = local ]; then
  cli "kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS --create --if-not-exists --topic orders --partitions 3 --replication-factor 3 --config cleanup.policy=delete --config retention.ms=86400000 --config segment.bytes=134217728 --config min.insync.replicas=2"
fi
ready=false
for i in $(seq 1 60); do
  if curl -fsS http://localhost:8081/subjects >/dev/null; then ready=true; break; fi
  sleep 3
done
[ "$ready" = true ]
ready=false
for i in $(seq 1 45); do
  if docker compose exec -T namenode hdfs dfsadmin -report 2>/dev/null | grep -q 'Live datanodes (1)'; then ready=true; break; fi
  sleep 3
done
[ "$ready" = true ]
docker compose exec -T namenode hdfs dfsadmin -safemode wait
docker compose build app
docker compose run --rm -T app python app/register.py | tee "$OUT/register.txt"
curl -fsS http://localhost:8081/subjects | tee "$OUT/subjects.json"
curl -fsS http://localhost:8081/subjects/orders-value/versions | tee "$OUT/versions.json"
cli "kafka-topics --bootstrap-server $BOOTSTRAP_SERVERS --command-config /certs/client.properties --describe --topic orders" | tee "$OUT/topic-describe.txt"
docker compose run --rm -T app python app/producer.py | tee "$OUT/producer.txt"
docker compose run --rm -T app python app/consumer.py | tee "$OUT/consumer.txt"
# Read exact offsets acknowledged by this producer, including on repeated runs.
python3 - "$OUT/producer.txt" <<'OFFSETS' > "$OUT/offsets.tsv"
import re,sys
for line in open(sys.argv[1]):
    m = re.search(r'^DELIVERED .*partition=(\d+) offset=(\d+)', line)
    if m:
        print(*m.groups(), sep='\t')
OFFSETS
: > "$OUT/console-consumer.txt"
while IFS=$'\t' read -r partition offset; do
  cli "kafka-console-consumer --bootstrap-server $BOOTSTRAP_SERVERS --consumer.config /certs/client.properties --topic orders --partition $partition --offset $offset --max-messages 1 --timeout-ms 60000" | tee -a "$OUT/console-consumer.txt"
done < "$OUT/offsets.tsv"
docker compose run --rm -T app python app/hdfs_sink.py | tee "$OUT/hdfs-sink.txt"
docker compose exec -T namenode hdfs dfs -cat '/events/orders/*/*.json' | tee "$OUT/hdfs-data.jsonl"
docker compose exec -T namenode hdfs dfs -ls -R /events | tee "$OUT/hdfs-files.txt"
docker compose exec -T namenode hdfs fsck /events | tee "$OUT/hdfs-fsck.txt"
docker compose ps -a > "$OUT/services.txt"
python3 - "$OUT" <<'CHECK'
import json,sys,re
from pathlib import Path
p=Path(sys.argv[1])
assert 'orders-value' in json.loads((p/'subjects.json').read_text())
assert json.loads((p/'versions.json').read_text())
producer=[json.loads(x.split('value=',1)[1]) for x in (p/'producer.txt').read_text().splitlines() if x.startswith('DELIVERED')]
consumer=[json.loads(x.split('value=',1)[1]) for x in (p/'consumer.txt').read_text().splitlines() if x.startswith('READ')]
console=[json.loads(x) for x in (p/'console-consumer.txt').read_text().splitlines()]
hdfs=[json.loads(x) for x in (p/'hdfs-data.jsonl').read_text().splitlines()]
assert len(producer)==len(consumer)==len(console)==6
assert len({x['id'] for x in producer}) == 6
assert {x['id'] for x in producer} == {x['id'] for x in console}
assert {x['id'] for x in producer} == {x['id'] for x in consumer}
assert {x['id'] for x in producer} <= {x['id'] for x in hdfs}
text=(p/'topic-describe.txt').read_text()
assert re.search(r'PartitionCount:\s+3\b', text) and re.search(r'ReplicationFactor:\s+3\b', text)
assert len(re.findall(r'Isr: \d+,\d+,\d+',text)) == 3
assert 'HEALTHY' in (p/'hdfs-fsck.txt').read_text()
(p/'result.txt').write_text('PASS: schema registered; 3 partitions; RF=3; ISR=3; six records delivered, read and verified in HDFS.\n')
CHECK
