#!/usr/bin/env bash
# Copy the downloaded StatsBomb files from data/statsbomb into HDFS at /statsbomb.
# Run after: docker compose up -d
set -euo pipefail
cd "$(dirname "$0")/.."

# Run an "hdfs dfs" command inside the namenode container.
dfs() { docker compose exec -T namenode hdfs dfs "$@"; }

echo "Waiting for HDFS to be ready..."
docker compose exec -T namenode hdfs dfsadmin -safemode wait

dfs -mkdir -p /statsbomb
dfs -put -f /input/statsbomb/matches /input/statsbomb/events /statsbomb/

echo "Files in HDFS:"
dfs -ls -R /statsbomb
