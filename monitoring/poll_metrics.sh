#!/usr/bin/env bash
# ---------------------------------------------------------------
# Continuously polls /metrics from the db-server and appends
# a timestamped snapshot to metrics_log.jsonl (one JSON per line).
# Usage: ./monitoring/poll_metrics.sh [db_server_ip] [interval_sec]
# ---------------------------------------------------------------
DB_HOST="${1:-localhost}"
INTERVAL="${2:-5}"
OUT="$(dirname "$0")/metrics_log.jsonl"

echo "Polling http://$DB_HOST:8081/metrics every ${INTERVAL}s → $OUT"
echo "Press Ctrl-C to stop."

while true; do
  TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  PAYLOAD=$(curl -sf "http://$DB_HOST:8081/metrics" 2>/dev/null || echo '{"error":"unreachable"}')
  echo "{\"polledAt\":\"$TS\",\"data\":$PAYLOAD}" >> "$OUT"
  sleep "$INTERVAL"
done
