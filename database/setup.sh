#!/usr/bin/env bash
# ---------------------------------------------------------------
# Chat-Flow Database Setup Script
# Starts PostgreSQL via Docker Compose and applies the schema.
# Usage: ./database/setup.sh [up|down|reset]
# ---------------------------------------------------------------
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CMD="${1:-up}"

case "$CMD" in
  up)
    echo "Starting infrastructure..."
    docker compose -f "$ROOT/docker-compose.yml" up -d postgres rabbitmq
    echo "Waiting for PostgreSQL to be ready..."
    until docker compose -f "$ROOT/docker-compose.yml" exec -T postgres \
        pg_isready -U chatflow -d chatflow >/dev/null 2>&1; do
      sleep 1
    done
    echo "Applying schema..."
    docker compose -f "$ROOT/docker-compose.yml" exec -T postgres \
        psql -U chatflow -d chatflow < "$ROOT/database/schema.sql"
    echo "Done. PostgreSQL ready at localhost:5432"
    ;;
  down)
    docker compose -f "$ROOT/docker-compose.yml" down
    ;;
  reset)
    docker compose -f "$ROOT/docker-compose.yml" down -v
    "$0" up
    ;;
  *)
    echo "Usage: $0 [up|down|reset]"
    exit 1
    ;;
esac
