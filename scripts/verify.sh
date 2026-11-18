#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

mode=${1:-local}
bash scripts/check-prerequisites.sh "$mode"
export RUN_ID=${RUN_ID:-$(date -u +%s)-$$}
case "$mode" in
  local)
    export COMPOSE_FILE=docker-compose.yml:compose.local.yml
    export CERTS_DIR=./.runtime/local
    export CLIENT_ENV=config/client-local.env REGISTRY_ENV=config/registry-local.env
    export BOOTSTRAP_SERVERS=kafka-1:9092,kafka-2:9092,kafka-3:9092
    mkdir -p "$CERTS_DIR"
    : > "$CERTS_DIR/client.properties"
    docker compose up -d kafka-1 kafka-2 kafka-3
    ;;
  cloud)
    export COMPOSE_FILE=docker-compose.yml CERTS_DIR=./.runtime
    export CLIENT_ENV=.runtime/client.env REGISTRY_ENV=.runtime/registry.env
    : "${BOOTSTRAP_SERVERS:?Set the three cloud broker addresses}"
    ;;
  *) echo 'Use local or cloud' >&2; exit 2 ;;
esac

out="evidence/$mode"
mkdir -p "$out"
rm -f "$out/result.txt"
printf 'Environment: %s\nRun ID: %s\nCommit: %s\n' "$mode" "$RUN_ID" "$(git rev-parse HEAD)" > "$out/run.txt"
collect_logs() {
  docker compose logs --no-color > "$out/docker.log" 2>&1 || true
  docker compose ps -a > "$out/services.txt" 2>&1 || true
}
trap collect_logs EXIT
cli() {
  local tool=$1
  shift
  timeout 90s docker compose run --rm --no-deps -T --entrypoint "/opt/kafka/bin/$tool.sh" cli \
    --bootstrap-server "$BOOTSTRAP_SERVERS" "$@"
}
auth=(--command-config /certs/client.properties)

for _ in $(seq 1 45); do
  if cli kafka-broker-api-versions "${auth[@]}" > "$out/brokers.txt" 2>&1 &&
     [ "$(grep -c ' -> (' "$out/brokers.txt")" -eq 3 ]; then break; fi
  sleep 4
done
[ "$(grep -c ' -> (' "$out/brokers.txt")" -eq 3 ] || { cat "$out/brokers.txt"; exit 1; }

if [[ "$mode" == local ]]; then
  cli kafka-topics "${auth[@]}" --create --if-not-exists --topic orders --partitions 3 \
    --replication-factor 3 --config cleanup.policy=delete --config retention.ms=86400000 \
    --config segment.bytes=134217728 --config min.insync.replicas=2
  cli kafka-topics "${auth[@]}" --create --if-not-exists --topic orders-schemas --partitions 1 \
    --replication-factor 3 --config cleanup.policy=compact --config min.insync.replicas=2
  cli kafka-metadata-quorum "${auth[@]}" describe --status | tee "$out/quorum.txt"
fi
docker compose up -d schema-registry namenode datanode
curl -fsS --retry 50 --retry-delay 3 --retry-all-errors --max-time 5 \
  http://localhost:8081/subjects > /dev/null
for _ in $(seq 1 45); do
  if docker compose exec -T namenode hdfs dfsadmin -report > "$out/hdfs-report.txt" 2>&1 &&
     grep -q 'Live datanodes (1)' "$out/hdfs-report.txt"; then break; fi
  sleep 3
done
grep -q 'Live datanodes (1)' "$out/hdfs-report.txt" || { cat "$out/hdfs-report.txt"; exit 1; }
timeout 180s docker compose exec -T namenode hdfs dfsadmin -safemode wait
docker compose build app

docker compose run --rm -T app python app/register.py | tee "$out/register.txt"
curl -fsS http://localhost:8081/subjects | tee "$out/subjects.json"; echo
curl -fsS -X GET http://localhost:8081/subjects/orders-value/versions | tee "$out/versions.json"; echo
cli kafka-topics "${auth[@]}" --describe --topic orders | tee "$out/topic-describe.txt"

# Capture each log end before producing, so the test can be repeated on the same topic.
cli kafka-get-offsets "${auth[@]}" --topic orders --time -1 | sort -t: -k2,2n > "$out/start-offsets.txt"
start=$(awk -F: '$1 == "orders" && $2 == NR-1 && $3 ~ /^[0-9]+$/ {print $3}' "$out/start-offsets.txt" | paste -sd, -)
[[ "$start" =~ ^[0-9]+,[0-9]+,[0-9]+$ ]] || { cat "$out/start-offsets.txt"; exit 1; }
export START_OFFSETS="$start"
docker compose run --rm -T app python app/producer.py | tee "$out/producer.txt"
docker compose run --rm -T app python app/consumer.py | tee "$out/consumer.txt"

IFS=, read -r -a offsets <<< "$start"
for part in 0 1 2; do
  cli kafka-console-consumer --consumer.config /certs/client.properties \
    --topic orders --partition "$part" --offset "${offsets[$part]}" --max-messages 2 --timeout-ms 60000
done | tee "$out/console-consumer.txt"

docker compose run --rm -T app python app/hdfs_sink.py | tee "$out/hdfs-sink.txt"
docker compose exec -T namenode hdfs dfs -cat '/events/orders/*/*.json' | tee "$out/hdfs-data.jsonl"
docker compose exec -T namenode hdfs dfs -ls -R /events/orders | tee "$out/hdfs-files.txt"
docker compose exec -T namenode hdfs fsck /events/orders | tee "$out/hdfs-fsck.txt"
docker compose ps > "$out/services.txt"
python3 scripts/check-results.py "$out" "$RUN_ID"

printf "\nVerification passed. Results: %s/result.txt\n" "$out"
