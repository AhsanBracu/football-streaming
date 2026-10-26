"""Spark Structured Streaming job: read football events from Kafka and process them live.

Step 5b: read the topic, parse the JSON and print each event to the console.
Run it with scripts/run_spark_job.sh, then start the producer.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "football-events"


def named(field):
    """StatsBomb stores things like type and team as {"id": ..., "name": ...}; we only need the name."""
    return StructField(field, StructType([StructField("name", StringType())]))


# The shape of one Kafka message (see producer/replay.py). Only the fields we
# use are listed; Spark ignores everything else in the JSON.
MESSAGE_SCHEMA = StructType([
    StructField("match_id", LongType()),
    StructField("event_time", TimestampType()),
    StructField("match_minute", IntegerType()),
    StructField("sent_at", LongType()),
    StructField("event", StructType([
        named("type"),
        named("team"),
        named("player"),
        StructField("shot", StructType([
            StructField("statsbomb_xg", DoubleType()),
            named("outcome"),
        ])),
    ])),
])


def main():
    spark = SparkSession.builder.appName("football-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")  # hide Spark's INFO messages

    # 1. Read: an endless table with one row per Kafka message.
    #    "latest": only messages sent after the job starts (not old test replays).
    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    # 2. Parse: Kafka gives the message as bytes; turn it into columns.
    events = (raw
              .select("partition", F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m"))
              .select(
                  "partition",
                  F.col("m.match_id").alias("match_id"),
                  F.col("m.event_time").alias("event_time"),
                  F.col("m.match_minute").alias("minute"),
                  F.col("m.event.type.name").alias("type"),
                  F.col("m.event.team.name").alias("team"),
                  F.col("m.event.player.name").alias("player"),
                  F.col("m.event.shot.statsbomb_xg").alias("xg"),
                  F.col("m.event.shot.outcome.name").alias("shot_outcome"),
              ))

    # 3. Output: print each new batch of events to the terminal.
    query = (events.writeStream
             .format("console")
             .option("truncate", False)
             .option("numRows", 20)
             .outputMode("append")
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
