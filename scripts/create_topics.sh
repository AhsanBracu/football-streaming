#!/usr/bin/env bash
# Create the Kafka topic that the producer writes to and Spark reads from.
# Run after: docker compose up -d
# Safe to run again: --if-not-exists skips a topic that already exists.
set -euo pipefail
cd "$(dirname "$0")/.."

# Run one of Kafka's command-line tools inside the kafka container.
kafka() { docker compose exec -T kafka "/opt/kafka/bin/$1" --bootstrap-server kafka:9092 "${@:2}"; }

echo "Waiting for Kafka to be ready..."
until kafka kafka-topics.sh --list >/dev/null 2>&1; do sleep 2; done

# 6 partitions: messages are keyed by match_id, so each match stays in order
# inside one partition, and the matches are spread over the partitions.
# Replication factor 1, because there is only one broker.
kafka kafka-topics.sh --create --if-not-exists \
  --topic football-events --partitions 6 --replication-factor 1

kafka kafka-topics.sh --describe --topic football-events
