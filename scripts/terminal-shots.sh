#!/usr/bin/env bash
# Run inside Xvfb after verifying the local stand. Each PNG is a screenshot of an xterm.
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_FILE=docker-compose.yml:compose.local.yml
out=evidence/local

run() {
  printf '$'
  printf ' %q' "$@"
  printf '\n'
  "$@"
  printf '\n'
}

if [[ ${1:-} == --show ]]; then
  printf 'Kafka / Hadoop   |   Docker Compose'
  [[ ${GITHUB_ACTIONS:-} != true ]] || printf '   |   GitHub Actions #%s' "$GITHUB_RUN_NUMBER"
  printf '\n\n'
  case "$2" in
    registry)
      run curl -fsS http://localhost:8081/subjects
      run curl -fsS -X GET http://localhost:8081/subjects/orders-value/versions
      run docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka-1:9092 --describe --topic orders
      ;;
    clients)
      run cat "$out/producer.txt"
      run cat "$out/consumer.txt"
      offset=$(awk -F: '$2 == 0 {print $3}' "$out/start-offsets.txt")
      run docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka-1:9092 --topic orders --partition 0 --offset "$offset" \
        --max-messages 2 --timeout-ms 20000
      ;;
    hadoop)
      run docker compose ps --format 'table {{.Service}}\t{{.Status}}'
      run cat "$out/hdfs-sink.txt"
      run docker compose exec -T namenode hdfs dfs -ls -R /events/orders
      ;;
  esac
  exit 0
fi

temp=$(mktemp -d)
pid=
cleanup() {
  if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  rm -rf "$temp"
}
trap cleanup EXIT

for view in registry clients hadoop; do
  case "$view" in
    registry) rows=18 ;;
    clients) rows=29 ;;
    hadoop) rows=37 ;;
  esac
  marker="$temp/$view"
  xterm -geometry "140x${rows}+0+0" -fa 'DejaVu Sans Mono' -fs 11 -b 10 +sb \
    -bg '#182027' -fg '#f1f4f6' -title "$view" -e bash -c '
      bash scripts/terminal-shots.sh --show "$1" 2>&1 | tee "$2.log"
      rc=${PIPESTATUS[0]}
      printf "%s %s\n" "$rc" "$WINDOWID" > "$2.status"
      mv "$2.status" "$2.done"
      read -r _
    ' screenshot "$view" "$marker" &
  pid=$!
  for _ in $(seq 1 600); do
    [[ ! -f "$marker.done" ]] || break
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.2
  done
  if [[ ! -f "$marker.done" ]]; then cat "$marker.log" >&2 || true; exit 1; fi
  read -r rc window < "$marker.done"
  if (( rc != 0 )); then cat "$marker.log" >&2; exit "$rc"; fi
  sleep 0.3
  import -silent -window "$window" "$out/terminal-$view.png"
  identify "$out/terminal-$view.png"
  kill "$pid"
  wait "$pid" || true
  pid=
done
