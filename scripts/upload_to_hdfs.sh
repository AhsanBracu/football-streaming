#!/usr/bin/env bash
# Copy the downloaded StatsBomb files from data/statsbomb into HDFS at /statsbomb.
# Run after: docker compose up -d
set -euo pipefail
cd "$(dirname "$0")/.."

# Run an "hdfs dfs" command inside the namenode container.
dfs() { docker compose exec -T namenode hdfs dfs "$@"; }

echo "Waiting for HDFS to be ready..."
# Retry: right after "docker compose up" the namenode isn't listening yet,
# and "safemode wait" fails immediately instead of waiting.
until docker compose exec -T namenode hdfs dfsadmin -safemode wait >/dev/null 2>&1; do sleep 2; done

dfs -mkdir -p /statsbomb
dfs -put -f /input/statsbomb/matches /input/statsbomb/events /statsbomb/

echo "Files in HDFS:"
dfs -ls -R /statsbomb
