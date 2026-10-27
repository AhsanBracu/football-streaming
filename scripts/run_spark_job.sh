#!/usr/bin/env bash
# Submit the streaming job (spark/streaming_job.py) to the Spark cluster.
# Run after: docker compose up -d. Stop the job with Ctrl+C.
set -euo pipefail
cd "$(dirname "$0")/.."

# --packages: download the Kafka connector (must match Spark 3.5.3 / Scala 2.12).
# --driver-memory / --executor-memory: 512 MB each (default 1 GB) so everything fits in Docker.
# spark.jars.ivy: where the download goes; the image's "spark" user has no home folder.
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --driver-memory 512m \
  --executor-memory 512m \
  /opt/spark-apps/streaming_job.py
