#!/bin/bash

DB_CMD="cargo run"
DB_PORT=8000

CRASHES=0
MAX_CRASHES=2

echo "Starting DB supervisor..."

while true; do
  echo "Starting DB..."
  $DB_CMD &
  DB_PID=$!

  # If we've already crashed twice, never crash again
  if [ "$CRASHES" -ge "$MAX_CRASHES" ]; then
    echo "No more crashes. DB will stay up."
    wait $DB_PID
    exit 0
  fi

  # Random uptime before crash (10–30s)
  sleep $((RANDOM % 21 + 10))

  echo "Crashing DB (crash #$((CRASHES + 1)))..."
  kill -9 $DB_PID

  CRASHES=$((CRASHES + 1))

  # Random downtime (2–5s)
  sleep $((RANDOM % 4 + 2))
done
