# Databricks notebook source
# MAGIC %md
# MAGIC ####6A — Read Bronze stream

# COMMAND ----------

bronze_location = (
    "/Volumes/industrial_sensor/sensor_data/"
    "industrial_sensor_data/delta/bronze_telemetry"
)

bronze_stream = (
    spark.readStream
        .format("delta")
        .load(bronze_location)
)

# COMMAND ----------

bronze_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6B-Convert event and ingestion timestamps

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

silver_stream_typed = (
    bronze_stream
        .withColumn(
            "event_ts",
            to_timestamp(col("event_timestamp"))
        )
        .withColumn(
            "ingestion_ts",
            col("ingestion_timestamp").cast("timestamp")
        )
)

# COMMAND ----------

silver_stream_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6C-Apply Watermark

# COMMAND ----------

silver_watermarked = (
    silver_stream_typed
        .withWatermark("event_ts", "10 minutes")
)

# COMMAND ----------

silver_watermarked.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6D - Streaming Deduplication

# COMMAND ----------

silver_stream_deduplicated = (
    silver_watermarked
        .dropDuplicates(["event_id"])
)

# COMMAND ----------

silver_stream_deduplicated.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6E - Add Data-Quality Flags

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit

silver_quality_stream = (
    silver_stream_deduplicated

    .withColumn(
        "invalid_identity",
        coalesce(
            (
                col("event_id").isNull()
                | col("site_id").isNull()
                | col("asset_id").isNull()
                | col("asset_type").isNull()
            ),
            lit(False)
        )
    )

    .withColumn(
        "missing_measurement",
        coalesce(
            (
                col("temperature").isNull()
                | col("pressure").isNull()
                | col("vibration").isNull()
                | col("flow_rate").isNull()
                | col("power_kw").isNull()
            ),
            lit(False)
        )
    )

    .withColumn(
        "invalid_measurement",
        coalesce(
            (
                (col("temperature") < -100)
                | (col("pressure") < -100)
                | (col("vibration") < 0)
                | (col("flow_rate") < 0)
                | (col("power_kw") < 0)
            ),
            lit(False)
        )
    )

    .withColumn(
        "invalid_timestamp",
        coalesce(
            col("event_ts").isNull(),
            lit(False)
        )
    )
)

# COMMAND ----------

silver_quality_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6F - Classify Records as VALID or INVALID

# COMMAND ----------

from pyspark.sql.functions import when, lit

silver_quality_final = (
    silver_quality_stream
    .withColumn(
        "data_quality_status",
        when(
            col("invalid_identity")
            | col("missing_measurement")
            | col("invalid_measurement")
            | col("invalid_timestamp"),
            lit("INVALID")
        )
        .otherwise(lit("VALID"))
    )
)

# COMMAND ----------

silver_quality_final.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6G — Write valid records → Silver Delta

# COMMAND ----------

# create the valid-record stream
silver_valid_stream = (
    silver_quality_final
        .filter(col("data_quality_status") == "VALID")
)

# COMMAND ----------

#define the Silver destination and checkpoin
silver_location = (
    "/Volumes/industrial_sensor/sensor_data/"
    "industrial_sensor_data/delta/silver_telemetry"
)

silver_checkpoint = (
    "/Volumes/industrial_sensor/silver_checkpoints/"
    "silver_telemetry"
)

# COMMAND ----------

# Delta streaming write:
silver_query = (
    silver_valid_stream
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", silver_checkpoint)
        .option("path", silver_location)
        .trigger(availableNow=True)
        .start()
)

silver_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6H — Write invalid records → Quarantine Delta

# COMMAND ----------

# Create the quarantine stream
quarantine_stream = (
    silver_quality_final
        .filter(col("data_quality_status") == "INVALID")
)

# COMMAND ----------

# Define the destination and checkpoint
quarantine_location = (
    "/Volumes/industrial_sensor/sensor_data/"
    "industrial_sensor_data/delta/quarantine_telemetry"
)

quarantine_checkpoint = (
    "/Volumes/industrial_sensor/silver_checkpoints/"
    "quarantine_telemetry"
)

# COMMAND ----------

# Write to Delta
quarantine_query = (
    quarantine_stream
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", quarantine_checkpoint)
        .option("path", quarantine_location)
        .trigger(availableNow=True)
        .start()
)

quarantine_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ####6I — Validate both Delta tables

# COMMAND ----------

# Read both Delta tables
silver_df = (
    spark.read
        .format("delta")
        .load(silver_location)
)

quarantine_df = (
    spark.read
        .format("delta")
        .load(quarantine_location)
)

# COMMAND ----------

# Check row counts
print("Silver rows:", silver_df.count())
print("Quarantine rows:", quarantine_df.count())

# COMMAND ----------

# Verify the known HIGH_VIBRATION event
silver_df.filter(
    col("event_id") == "evt-9ea65316ec5f"
).select(
    "event_id",
    "asset_id",
    "anomaly_type",
    "data_quality_status",
    "event_ts",
    "ingestion_ts"
).show(truncate=False)

# COMMAND ----------

# Verify the known MISSING_VALUE event
quarantine_df.filter(
    col("event_id") == "evt-23738a7b4539"
).select(
    "event_id",
    "asset_id",
    "vibration",
    "anomaly_type",
    "missing_measurement",
    "data_quality_status",
    "event_ts",
    "ingestion_ts"
).show(truncate=False)

# COMMAND ----------

# Verify the duplicate event
silver_df.filter(
    col("event_id") == "e2e-test-000001"
).count()