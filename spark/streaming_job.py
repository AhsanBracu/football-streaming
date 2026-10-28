"""Spark Structured Streaming job: read football events from Kafka and process them live.

Reads the topic, parses the JSON, and computes team stats per sliding window of
match time (shots, shots on target, goals, xG, pressures, passes). Each window is
stored as one MongoDB document that is updated in place while the window changes.
Run it with scripts/run_spark_job.sh, then start the producer.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "football-events"
MONGO_URI = "mongodb://mongodb:27017/?replicaSet=rs0"
MONGO_DB = "football"
MONGO_COLLECTION = "window_stats"
# Spark's progress (Kafka offsets + open windows). On HDFS, because the driver
# and the executor run in different containers and both need the same folder.
CHECKPOINT = "hdfs://namenode:8020/checkpoints/window-stats"

# All in match time (event_time), not wall-clock time.
WINDOW_LENGTH = "5 minutes"   # each window covers 5 minutes of the match
WINDOW_SLIDE = "1 minute"     # a new window starts every minute
WATERMARK = "1 minute"        # how late an event may arrive and still be counted

# StatsBomb shot outcomes that count as "on target".
ON_TARGET = ["Goal", "Saved", "Saved to Post"]


def named(field):
    """StatsBomb stores things like type and team as {"id": ..., "name": ...}; we only need the name."""
    return StructField(field, StructType([StructField("name", StringType())]))


def is_(condition):
    """1 if the condition is true, else 0 (also 0 when the field is missing)."""
    return F.when(condition, 1).otherwise(0)


# The shape of one Kafka message (see producer/replay.py). Only the fields we
# use are listed; Spark ignores everything else in the JSON.
MESSAGE_SCHEMA = StructType([
    StructField("match_id", LongType()),
    StructField("event_time", TimestampType()),
    StructField("match_minute", IntegerType()),
    StructField("sent_at", LongType()),
    StructField("kickoff", TimestampType()),
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


def write_to_mongo(batch, batch_id):
    """Write one micro-batch of changed windows to MongoDB.

    "replace" + "upsertDocument": the document with the same _id is replaced by
    the new numbers, or created if the window is new. Writing the same batch
    twice (e.g. after a crash) gives the same result, so nothing is double counted.
    """
    batch.persist()  # used twice below (write + count): compute it only once
    (batch.write
     .format("mongodb")
     .mode("append")
     .option("connection.uri", MONGO_URI)
     .option("database", MONGO_DB)
     .option("collection", MONGO_COLLECTION)
     .option("operationType", "replace")
     .option("upsertDocument", "true")
     .save())
    print(f"batch {batch_id}: {batch.count()} windows written")
    batch.unpersist()


def main():
    spark = (SparkSession.builder
             .appName("football-streaming")
             # groupBy reshuffles the data into this many pieces; Spark's default
             # of 200 makes 200 tiny tasks per batch for our small stream.
             .config("spark.sql.shuffle.partitions", 6)
             .getOrCreate())
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
                  F.col("m.kickoff").alias("kickoff"),
                  F.col("m.sent_at").alias("sent_at"),
                  F.col("m.event.type.name").alias("type"),
                  F.col("m.event.team.name").alias("team"),
                  F.col("m.event.player.name").alias("player"),
                  F.col("m.event.shot.statsbomb_xg").alias("xg"),
                  F.col("m.event.shot.outcome.name").alias("shot_outcome"),
              ))

    # 3. Flags: turn each event into numbers that can simply be added up.
    flagged = events.where(F.col("team").isNotNull()).select(
        "match_id", "event_time", "team", "kickoff", "sent_at",
        is_(F.col("type") == "Shot").alias("is_shot"),
        is_(F.col("shot_outcome").isin(*ON_TARGET)).alias("is_on_target"),
        is_(F.col("shot_outcome") == "Goal").alias("is_goal"),
        F.coalesce(F.col("xg"), F.lit(0.0)).alias("xg"),
        is_(F.col("type") == "Pressure").alias("is_pressure"),
        is_(F.col("type") == "Pass").alias("is_pass"),
    )

    # 4. Windows: add up the flags per match, team and sliding window.
    #    The watermark lets Spark finish (and forget) a window once the match
    #    clock is more than WATERMARK past its end.
    stats = (flagged
             .withWatermark("event_time", WATERMARK)
             .groupBy("match_id", "team", F.window("event_time", WINDOW_LENGTH, WINDOW_SLIDE))
             .agg(F.sum("is_shot").alias("shots"),
                  F.sum("is_on_target").alias("on_target"),
                  F.sum("is_goal").alias("goals"),
                  F.round(F.sum("xg"), 2).alias("xg"),
                  F.sum("is_pressure").alias("pressures"),
                  F.sum("is_pass").alias("passes"),
                  F.max("kickoff").alias("kickoff"),            # the same for every event of a replay
                  F.max("sent_at").alias("last_sent_at"))       # newest event in this window
             .select(
                 # One document per match + team + window: the key for the upsert.
                 F.concat_ws("|", "match_id", "team",
                             F.date_format("window.start", "yyyy-MM-dd'T'HH:mm:ss")).alias("_id"),
                 "match_id", "team",
                 F.col("window.start").alias("window_start"),
                 F.col("window.end").alias("window_end"),
                 # Window position in the match (0, 1, 2 ...): the dashboard's x-axis.
                 ((F.unix_timestamp("window.start") - F.unix_timestamp("kickoff")) / 60)
                 .cast("int").alias("minute"),
                 "kickoff",
                 "shots", "on_target", "goals", "xg", "pressures", "passes",
                 # For measuring latency: producer send time -> written by Spark.
                 "last_sent_at",
                 F.current_timestamp().alias("processed_at")))

    # 5. Output: "update" gives each batch only the windows that changed;
    #    foreachBatch writes them to MongoDB.
    query = (stats.writeStream
             .outputMode("update")
             .foreachBatch(write_to_mongo)
             .option("checkpointLocation", CHECKPOINT)
             .start())
    query.awaitTermination()


if __name__ == "__main__":
    main()
