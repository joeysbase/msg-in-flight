#!/usr/bin/env bash
# =============================================================
# collect_metrics.sh  —  parallel metrics collector for a load test
#
# Writes three JSONL files while the test runs:
#   metrics_log.jsonl   write throughput + latency from /metrics API
#   docker_stats.jsonl  PostgreSQL container CPU % + memory
#   jvm_stats.jsonl     db-server JVM process CPU % + RSS memory
#
# Usage:
#   ./monitoring/collect_metrics.sh [db_server_ip] [interval_sec] [run_label]
#
# Examples:
#   ./monitoring/collect_metrics.sh                          # all defaults
#   ./monitoring/collect_metrics.sh localhost 3 test1_baseline
#
# Stop: Ctrl-C  (all background collectors stop automatically)
# =============================================================

DB_HOST="${1:-localhost}"
INTERVAL="${2:-5}"
LABEL="${3:-run_$(date +%Y%m%d_%H%M%S)}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/results/${LABEL}"
mkdir -p "$OUT_DIR"

METRICS_LOG="${OUT_DIR}/metrics_log.jsonl"
DOCKER_LOG="${OUT_DIR}/docker_stats.jsonl"
JVM_LOG="${OUT_DIR}/jvm_stats.jsonl"

echo "============================================"
echo "  collect_metrics  label=${LABEL}"
echo "  db-server : http://${DB_HOST}:8081/metrics"
echo "  interval  : ${INTERVAL}s"
echo "  output    : ${OUT_DIR}/"
echo "  Stop with : Ctrl-C"
echo "============================================"

# ── write a test line immediately so the file exists ─────────────────────
echo '{"polledAt":"start","data":{"note":"collector started"}}' >> "$METRICS_LOG"
echo '{"polledAt":"start","stats":{"note":"collector started"}}' >> "$DOCKER_LOG"
echo '{"polledAt":"start","jvm":{"note":"collector started"}}' >> "$JVM_LOG"
echo "Output files created in ${OUT_DIR}/"

# ─────────────────────────────────────────────────────────────────────────
# Collector 1: /metrics API
# ─────────────────────────────────────────────────────────────────────────
collect_metrics_api() {
    local count=0
    while true; do
        local ts
        ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

        local payload
        if payload=$(curl -sf --max-time 5 "http://${DB_HOST}:8081/metrics" 2>/dev/null); then
            echo "{\"polledAt\":\"${ts}\",\"data\":${payload}}" >> "$METRICS_LOG"
        else
            echo "{\"polledAt\":\"${ts}\",\"data\":{\"error\":\"unreachable\"}}" >> "$METRICS_LOG"
        fi

        count=$((count + 1))
        # Print a heartbeat every 12 samples (~1 min at 5s interval)
        if (( count % 12 == 0 )); then
            echo "[metrics] ${ts}  sample #${count}"
        fi
        sleep "$INTERVAL"
    done
}

# ─────────────────────────────────────────────────────────────────────────
# Collector 2: docker stats for the postgres container
# ─────────────────────────────────────────────────────────────────────────
collect_docker_stats() {
    while true; do
        local ts
        ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

        # Docker Compose v2 names containers  <project>-<service>-<n>
        # grep for 'postgres' to match both 'postgres' and 'chat-flow-postgres-1'
        local stats
        stats=$(docker stats --no-stream --format \
            '{"container":"{{.Name}}","cpuPct":"{{.CPUPerc}}","memUsage":"{{.MemUsage}}","memPct":"{{.MemPerc}}"}' \
            2>/dev/null | grep -i postgres | head -1) || true

        if [ -n "$stats" ]; then
            echo "{\"polledAt\":\"${ts}\",\"stats\":${stats}}" >> "$DOCKER_LOG"
        else
            echo "{\"polledAt\":\"${ts}\",\"stats\":{\"error\":\"postgres container not found\"}}" >> "$DOCKER_LOG"
        fi
        sleep "$INTERVAL"
    done
}

# ─────────────────────────────────────────────────────────────────────────
# Collector 3: db-server JVM process (ps)
# ─────────────────────────────────────────────────────────────────────────
collect_jvm_stats() {
    while true; do
        local ts
        ts=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

        # Find the DbServerApp process; 'grep -v grep' removes the grep itself.
        # '|| true' prevents set -e from killing this function if there's no match.
        local jvm_line
        jvm_line=$(ps aux 2>/dev/null \
            | grep "[D]bServerApp" \
            | awk '{printf "{\"pid\":%s,\"cpuPct\":%s,\"rssKb\":%s,\"vszKb\":%s}", $2, $3, $6, $5}' \
            | head -1) || true

        if [ -n "$jvm_line" ]; then
            echo "{\"polledAt\":\"${ts}\",\"jvm\":${jvm_line}}" >> "$JVM_LOG"
        else
            echo "{\"polledAt\":\"${ts}\",\"jvm\":{\"error\":\"DbServerApp process not found\"}}" >> "$JVM_LOG"
        fi
        sleep "$INTERVAL"
    done
}

# ─────────────────────────────────────────────────────────────────────────
# Start all three collectors as background jobs
# ─────────────────────────────────────────────────────────────────────────
collect_metrics_api  &
PID_METRICS=$!

collect_docker_stats &
PID_DOCKER=$!

collect_jvm_stats    &
PID_JVM=$!

echo "Collectors started (PIDs: metrics=${PID_METRICS} docker=${PID_DOCKER} jvm=${PID_JVM})"

cleanup() {
    echo ""
    echo "Stopping collectors..."
    kill "$PID_METRICS" "$PID_DOCKER" "$PID_JVM" 2>/dev/null || true
    wait "$PID_METRICS" "$PID_DOCKER" "$PID_JVM" 2>/dev/null || true

    local lines
    lines=$(wc -l < "$METRICS_LOG" 2>/dev/null || echo 0)
    echo "Done. ${lines} metrics samples collected."
    echo "Results : ${OUT_DIR}/"
    echo "Summarize: python3 monitoring/summarize.py '${OUT_DIR}'"
}
trap cleanup EXIT INT TERM

# Wait for background jobs (runs until Ctrl-C triggers the trap)
wait
