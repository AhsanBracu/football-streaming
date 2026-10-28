#!/usr/bin/env bash
# Submit the streaming job (spark/streaming_job.py) to the Spark cluster.
# Run after: docker compose up -d. Stop the job with Ctrl+C.
#
#   ./scripts/run_spark_job.sh           # continue from the last checkpoint
#   ./scripts/run_spark_job.sh --reset   # forget the checkpoint: start fresh at the newest Kafka messages
set -euo pipefail
cd "$(dirname "$0")/.."

CHECKPOINT=/checkpoints/window-stats
hdfs() { docker compose exec -T namenode hdfs dfs "$@"; }

if [[ "${1:-}" == "--reset" ]]; then
  echo "Deleting checkpoint $CHECKPOINT"
  hdfs -rm -r -f "$CHECKPOINT"
fi

# The checkpoint folder in HDFS. Spark runs as user "spark", but HDFS folders
# belong to "hadoop", so make /checkpoints writable for everyone (local dev setup).
hdfs -mkdir -p /checkpoints
hdfs -chmod 777 /checkpoints

# --packages: download the Kafka and MongoDB connectors (must match Spark 3.5.3 / Scala 2.12).
# --driver-memory / --executor-memory: 512 MB each (default 1 GB) so everything fits in Docker.
# spark.jars.ivy: where the download goes; the image's "spark" user has no home folder.
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --driver-memory 512m \
  --executor-memory 512m \
  /opt/spark-apps/streaming_job.py
