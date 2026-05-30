import sys
import os
sys.stdout.reconfigure(encoding='utf-8')

# Fix for Windows: point Hadoop to winutils
os.environ["HADOOP_HOME"] = "C:\\hadoop"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType,
    BooleanType, ArrayType, TimestampType
)

# ── Config ────────────────────────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC          = "lol-match-events"
PG_URL   = "jdbc:postgresql://localhost:5432/asiastats"
PG_PROPS = {
    "user":     "asiastats",
    "password": "asiastats123",
    "driver":   "org.postgresql.Driver",
    "TimeZone": "UTC",                    
}
PG_PROPS       = {"user": "asiastats", "password": "asiastats123", "driver": "org.postgresql.Driver"}
CHECKPOINT_DIR = "./checkpoints"

os.makedirs(CHECKPOINT_DIR, exist_ok=True)

# ── Spark session ─────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("AsiaStats-LoL-Streaming")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")   # ← add this
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC") # ← add this
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("[OK] Spark session started")

# ── Schema ────────────────────────────────────────────────────────────
team_schema = ArrayType(StructType([
    StructField("id",      IntegerType()),
    StructField("name",    StringType()),
    StructField("acronym", StringType()),
]))

result_schema = ArrayType(StructType([
    StructField("team_id", IntegerType()),
    StructField("score",   IntegerType()),
]))

event_schema = StructType([
    StructField("event_id",             StringType()),
    StructField("event_type",           StringType()),
    StructField("match_id",             IntegerType()),
    StructField("match_name",           StringType()),
    StructField("match_status",         StringType()),
    StructField("match_type",           StringType()),
    StructField("number_of_games",      IntegerType()),
    StructField("begin_at",             StringType()),
    StructField("end_at",               StringType()),
    StructField("league_name",          StringType()),
    StructField("league_id",            IntegerType()),
    StructField("tournament_name",      StringType()),
    StructField("tournament_tier",      StringType()),
    StructField("tournament_region",    StringType()),
    StructField("serie_name",           StringType()),
    StructField("game_title",           StringType()),
    StructField("teams",                team_schema),
    StructField("results",              result_schema),
    StructField("games_count",          IntegerType()),
    StructField("avg_game_length_mins", FloatType()),
    StructField("simulated",            BooleanType()),
    StructField("_source",              StringType()),
    StructField("_ingested_at",         StringType()),
])

# ── Read from Kafka ───────────────────────────────────────────────────
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

parsed = (
    raw_stream
    .select(
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), event_schema).alias("e")
    )
    .select(
        "kafka_timestamp",
        "e.*",
        F.to_timestamp("e._ingested_at").alias("ingested_at"),
        F.to_timestamp("e.begin_at").alias("match_begin_at"),
    )
    .withWatermark("ingested_at", "10 minutes")
)

print("[OK] Kafka stream parsed")

# ── Postgres writer ───────────────────────────────────────────────────
def write_to_postgres(df, table):
    df.write.jdbc(url=PG_URL, table=table, mode="append", properties=PG_PROPS)

# ── Sink 1: Raw events ────────────────────────────────────────────────
raw_events_df = parsed.select(
    "event_id", "event_type", "match_id", "match_name",
    "match_status", "league_name", "league_id",
    "tournament_name", "tournament_tier", "tournament_region",
    "game_title", "games_count", "avg_game_length_mins",
    "simulated", "_source", "ingested_at", "match_begin_at",
)

def write_raw_events(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
    print(f"[Sink 1] Batch {batch_id} -- {count} raw events")
    write_to_postgres(batch_df, "match_events_raw")

query_raw = (
    raw_events_df.writeStream
    .foreachBatch(write_raw_events)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw_events")
    .trigger(processingTime="15 seconds")
    .start()
)
print("[OK] Sink 1 started -- match_events_raw")

# ── Explode teams + results (one at a time -- Spark streaming rule) ───
teams_exploded = (
    parsed
    .filter(F.col("event_type") == "match_finished")
    .filter(F.col("results").isNotNull())
    # Step 1: explode results only
    .select(
        "ingested_at", "match_id", "league_name",
        "tournament_tier", "tournament_region", "_source", "teams",
        F.explode("results").alias("result"),
    )
    # Step 2: explode teams only
    .select(
        "ingested_at", "match_id", "league_name",
        "tournament_tier", "tournament_region", "_source",
        "result",
        F.explode("teams").alias("team"),
    )
    # Step 3: keep matching pairs
    .filter(F.col("team.id") == F.col("result.team_id"))
    .select(
        "ingested_at", "match_id", "league_name",
        "tournament_tier", "tournament_region", "_source",
        F.col("team.id").alias("team_id"),
        F.col("team.name").alias("team_name"),
        F.col("team.acronym").alias("team_acronym"),
        F.col("result.score").alias("series_score"),
    )
)

# ── Sink 2: Windowed team win rate ────────────────────────────────────
# Fix: use approx_count_distinct instead of countDistinct (streaming limitation)
win_rate_window = (
    teams_exploded
    .groupBy(
        F.window("ingested_at", "10 minutes", "2 minutes").alias("window"),
        "team_id", "team_name", "team_acronym",
        "league_name", "tournament_tier", "tournament_region",
    )
    .agg(
        F.count("match_id").alias("matches_in_window"),
        F.sum("series_score").alias("total_score"),
        F.avg("series_score").alias("avg_score"),
        F.approx_count_distinct("match_id").alias("unique_matches"),  # fixed
        F.first("_source").alias("data_source"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "team_id", "team_name", "team_acronym",
        "league_name", "tournament_tier", "tournament_region",
        "matches_in_window", "total_score",
        F.round("avg_score", 3).alias("avg_score"),
        "unique_matches", "data_source",
        F.current_timestamp().alias("computed_at"),
    )
)

def write_win_rate(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
    print(f"[Sink 2] Batch {batch_id} -- {count} win-rate rows")
    write_to_postgres(batch_df, "team_win_rate_window")

query_winrate = (
    win_rate_window.writeStream
    .outputMode("update")
    .foreachBatch(write_win_rate)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/win_rate")
    .trigger(processingTime="30 seconds")
    .start()
)
print("[OK] Sink 2 started -- team_win_rate_window")

# ── Sink 3: League activity ───────────────────────────────────────────
league_activity = (
    parsed
    .groupBy(
        F.window("ingested_at", "5 minutes").alias("window"),
        "league_name", "tournament_tier", "tournament_region",
    )
    .agg(
        F.count("event_id").alias("event_count"),
        F.approx_count_distinct("match_id").alias("unique_matches"),  # fixed
        F.avg("avg_game_length_mins").alias("avg_game_length_mins"),
        F.sum(F.when(F.col("simulated") == True, 1).otherwise(0)).alias("simulated_count"),
        F.sum(F.when(F.col("simulated") == False, 1).otherwise(0)).alias("live_count"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "league_name", "tournament_tier", "tournament_region",
        "event_count", "unique_matches",
        F.round("avg_game_length_mins", 1).alias("avg_game_length_mins"),
        "simulated_count", "live_count",
        F.current_timestamp().alias("computed_at"),
    )
)

def write_league_activity(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
    print(f"[Sink 3] Batch {batch_id} -- {count} league-activity rows")
    write_to_postgres(batch_df, "league_activity")

query_league = (
    league_activity.writeStream
    .outputMode("update")
    .foreachBatch(write_league_activity)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/league_activity")
    .trigger(processingTime="30 seconds")
    .start()
)
print("[OK] Sink 3 started -- league_activity")

# ── Sink 4: Anomaly detection ─────────────────────────────────────────
anomaly_window = (
    teams_exploded
    .groupBy(
        F.window("ingested_at", "5 minutes").alias("window"),
        "team_id", "team_name", "team_acronym",
        "league_name", "tournament_region",
    )
    .agg(
        F.count("match_id").alias("match_count"),
        F.avg("series_score").alias("avg_score_window"),
        F.stddev("series_score").alias("stddev_score"),
        F.max("series_score").alias("max_score"),
    )
    .withColumn(
        "anomaly_flag",
        (F.col("avg_score_window") > 1.5) & (F.col("match_count") >= 2)
    )
    .filter(F.col("anomaly_flag") == True)
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "team_id", "team_name", "team_acronym",
        "league_name", "tournament_region",
        "match_count",
        F.round("avg_score_window", 3).alias("avg_score_window"),
        F.round("stddev_score", 3).alias("stddev_score"),
        "max_score",
        F.lit("HIGH_WIN_RATE").alias("anomaly_type"),
        F.current_timestamp().alias("detected_at"),
    )
)

def write_anomalies(batch_df, batch_id):
    count = batch_df.count()
    if count == 0:
        return
    print(f"[Sink 4] ALERT Batch {batch_id} -- {count} anomalies detected")
    batch_df.show(truncate=False)
    write_to_postgres(batch_df, "anomaly_alerts")

query_anomaly = (
    anomaly_window.writeStream
    .outputMode("update")
    .foreachBatch(write_anomalies)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/anomalies")
    .trigger(processingTime="30 seconds")
    .start()
)
print("[OK] Sink 4 started -- anomaly_alerts")

# ── Wait ──────────────────────────────────────────────────────────────
print("\n[RUNNING] All 4 streaming queries active. Ctrl+C to stop.\n")
for q in spark.streams.active:
    print(f"  Query: {q.id} -- {q.status['message']}")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nShutting down...")
    for q in spark.streams.active:
        q.stop()
    spark.stop()
    print("Done.")