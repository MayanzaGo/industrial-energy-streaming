# Databricks notebook source
# MAGIC %md
# MAGIC #### Read the Bronze stream

# COMMAND ----------

bronze_location = (
    "/Volumes/industrial_sensor/"
    "sensor_data/industrial_sensor_data/"
    "delta/bronze_telemetry"
)

bronze_df = (
    spark.read
        .format("delta")
        .load(bronze_location)
)

print("Bronze records:", bronze_df.count())

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####5A — Profiling and rule discovery

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5A.1 — Inspect the actual telemetry

# COMMAND ----------

display(
    bronze_df.select(
        "event_id",
        "site_id",
        "asset_id",
        "asset_type",
        "event_timestamp",
        "temperature",
        "pressure",
        "vibration",
        "flow_rate",
        "power_kw",
        "status",
        "anomaly_type",
        "ingestion_timestamp"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5A.2 — Inspect the anomalies

# COMMAND ----------

display(
    bronze_df
        .groupBy("anomaly_type")
        .count()
        .orderBy("anomaly_type")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5A.3 — Inspect data-quality problems

# COMMAND ----------

from pyspark.sql.functions import col, sum

display(
    bronze_df.select(
        sum(col("temperature").isNull().cast("int")).alias("null_temperature"),
        sum(col("pressure").isNull().cast("int")).alias("null_pressure"),
        sum(col("vibration").isNull().cast("int")).alias("null_vibration"),
        sum(col("flow_rate").isNull().cast("int")).alias("null_flow_rate"),
        sum(col("power_kw").isNull().cast("int")).alias("null_power_kw")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5A.4 — Check duplicate IDs

# COMMAND ----------

duplicate_events = (
    bronze_df
        .groupBy("event_id")
        .count()
        .filter(col("count") > 1)
)

print(
    "Duplicated event IDs:",
    duplicate_events.count()
)

display(
    duplicate_events.orderBy(
        col("count").desc()
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5A.5 — Inspect timestamps

# COMMAND ----------

from pyspark.sql.functions import to_timestamp

bronze_timestamp_df = (
    bronze_df
        .withColumn(
            "event_ts",
            to_timestamp(col("event_timestamp"))
        )
)

display(
    bronze_timestamp_df.select(
        "event_id",
        "event_timestamp",
        "event_ts",
        "kinesis_arrival_timestamp",
        "ingestion_timestamp",
        "anomaly_type"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5A.6 — Standardize the Silver schema

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    to_timestamp
)

silver_typed = (
    bronze_df
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

silver_typed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5A.7 — Define data-quality rules

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    when,
    lit
)

silver_quality = (
    silver_typed

    # Required identity fields
    .withColumn(
        "invalid_identity",
        (
            col("event_id").isNull()
            | col("site_id").isNull()
            | col("asset_id").isNull()
            | col("asset_type").isNull()
        )
    )

    # Required telemetry measurements
    .withColumn(
        "missing_measurement",
        (
            col("temperature").isNull()
            | col("pressure").isNull()
            | col("vibration").isNull()
            | col("flow_rate").isNull()
            | col("power_kw").isNull()
        )
    )

    # Clearly invalid sensor values
    .withColumn(
        "invalid_measurement",
        (
            (col("temperature") < -100)
            | (col("pressure") < -100)
            | (col("vibration") < 0)
            | (col("flow_rate") < 0)
            | (col("power_kw") < 0)
        )
    )

    # Timestamp validation
    .withColumn(
        "invalid_timestamp",
        col("event_ts").isNull()
    )
)

# COMMAND ----------

display(
    silver_quality.select(
        "event_id",
        "asset_id",
        "temperature",
        "pressure",
        "vibration",
        "flow_rate",
        "power_kw",
        "invalid_identity",
        "missing_measurement",
        "invalid_measurement",
        "invalid_timestamp",
        "anomaly_type"
    )
)

# COMMAND ----------

# Let's inspect the results

display(
    silver_quality.select(
        "event_id",
        "asset_id",
        "temperature",
        "pressure",
        "vibration",
        "flow_rate",
        "power_kw",
        "invalid_identity",
        "missing_measurement",
        "invalid_measurement",
        "invalid_timestamp",
        "anomaly_type"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5A.8 — Deduplicate the telemetry

# COMMAND ----------

# MAGIC %md
# MAGIC ######5A.8.1 Add a row number

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# COMMAND ----------

dedup_window = (
    Window.partitionBy("event_id")
          .orderBy(col("ingestion_ts").desc())        
)

# COMMAND ----------

silver_dedup_ranked = (
    silver_quality.withColumn(
        "duplicate_rank",
        row_number().over(dedup_window)
    )
)

# COMMAND ----------

display(
    silver_dedup_ranked.select(
        "event_id",
        "asset_id",
        "event_ts",
        "ingestion_ts",
        "duplicate_rank",
        "anomaly_type"
    )
    .orderBy(
        col("event_id"),
        col("duplicate_rank")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ######5A.8.2 Keep only rank 1

# COMMAND ----------

silver_deduplicated = (
    silver_dedup_ranked
              .filter(col("duplicate_rank") == 1)
              .drop("duplicate_rank")
    
)

# COMMAND ----------

display(silver_deduplicated)

# COMMAND ----------

print(
    "Records before deduplication:",
    silver_quality.count()
)

print(
    "Records after deduplication:",
    silver_deduplicated.count()
)

# COMMAND ----------

display(
    silver_deduplicated.select(
        "event_id",
        "site_id",
        "asset_id",
        "event_ts",
        "ingestion_ts",
        "temperature",
        "pressure",
        "vibration",
        "flow_rate",
        "power_kw",
        "status",
        "anomaly_type"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5A.9 — Event-time watermarking and late events

# COMMAND ----------

# MAGIC %md
# MAGIC ######Step 5A.9.1 — Calculate event delay

# COMMAND ----------

from pyspark.sql.functions import (
    unix_timestamp,
    round
)

silver_with_delay = (
    silver_deduplicated
        .withColumn(
            "delay_seconds",
            unix_timestamp("ingestion_ts") - unix_timestamp("event_ts")
        )
)

# COMMAND ----------

display(
    silver_with_delay.select(
        "event_id",
        "asset_id",
        "event_ts",
        "ingestion_ts",
        "delay_seconds",
        "anomaly_type"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Step 5A.9.2 — Why these numbers are unusual
# MAGIC we should not use ingestion_ts - event_ts alone as our production definition of sensor lateness
# MAGIC For the real pipeline, we care about the relationship between: event timestamp - Kinesis arrival - Databricks processing

# COMMAND ----------

# MAGIC %md
# MAGIC ######Step 5A.9.3 — Calculate sensor-to-Kinesis delay

# COMMAND ----------

silver_with_delay = (
    silver_deduplicated
        .withColumn(
            "sensor_to_kinesis_delay_seconds",
            unix_timestamp("kinesis_arrival_timestamp") - unix_timestamp("event_ts")
        )
)

# COMMAND ----------

display(
    silver_with_delay.select(
        "event_id",
        "asset_id",
        "event_ts",
        "kinesis_arrival_timestamp",
        "ingestion_ts",
        "sensor_to_kinesis_delay_seconds",
        "anomaly_type"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Step 5A.9.4 — Define our watermark policy
# MAGIC  We wil not apply .withWatermark() to our current static silver_deduplicated DataFrame. Watermarks are a Structured Streaming concept, so we'll apply the watermark when we build the actual streaming Silver pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5B — Streaming transformations
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5B.1 — Read Bronze as a stream

# COMMAND ----------

silver_stream = (
    spark.readStream
        .format("delta")
        .load(bronze_location)
)

# COMMAND ----------

silver_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5B.2 — Convert the event timestamp

# COMMAND ----------

silver_stream_typed = (
    silver_stream
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

# MAGIC %md
# MAGIC #####Step 5B.3 — Apply the watermark

# COMMAND ----------

silver_watermarked = (
    silver_stream_typed
        .withWatermark(
            "event_ts",
            "10 minutes"
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5B.4 — Streaming deduplication

# COMMAND ----------

silver_stream_deduplicated = (
    silver_watermarked
        .dropDuplicates(["event_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 5B.5 — Inspect the streaming Silver schema

# COMMAND ----------

silver_stream_deduplicated.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####Then Step 5B.6 — Inspect the streaming query without writing Silver

# COMMAND ----------

# Let's use a dedicated checkpoint path rather than our eventual production Silver checkpoint
test_checkpoint = "/Volumes/industrial_sensor/silver_checkpoints/silver_test_data/step5b_test"

# COMMAND ----------

#test_query = (
#    silver_stream_deduplicated
#        .writeStream
#        .format("memory")
#        .queryName("silver_stream_test")
#        .outputMode("append")
#        .option("checkpointLocation", test_checkpoint)
#        .trigger(availableNow=True)
#        .start()
#)

# COMMAND ----------

#if test_query.isActive:
#    test_query.stop()

# COMMAND ----------

#display(
#    spark.sql("""
#        SELECT
#            event_id,
#            asset_id,
#            event_ts,
#            temperature,
#            pressure,
#            vibration,
#            anomaly_type
#        FROM silver_stream_test
#        ORDER BY event_ts
#    """)
#)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5C — Data-quality classification and quarantine

# COMMAND ----------

# MAGIC %md
# MAGIC #####5C.1 — Add quality flags

# COMMAND ----------

from pyspark.sql.functions import col

silver_quality_stream = (
    silver_stream_deduplicated
    .withColumn(
        "invalid_identity",
        (
            col("event_id").isNull()
            | col("site_id").isNull()
            | col("asset_id").isNull()
            | col("asset_type").isNull()
        )
    )
    .withColumn(
        "missing_measurement",
        (
            col("temperature").isNull()
            | col("pressure").isNull()
            | col("vibration").isNull()
            | col("flow_rate").isNull()
            | col("power_kw").isNull()
        )
    )
    .withColumn(
        "invalid_measurement",
        (
            (col("temperature") < -100)
            | (col("pressure") < -100)
            | (col("vibration") < 0)
            | (col("flow_rate") < 0)
            | (col("power_kw") < 0)
        )
    )
    .withColumn(
        "invalid_timestamp",
        col("event_ts").isNull()
    )
)

# COMMAND ----------

silver_quality_stream.printSchema()

# COMMAND ----------

# Test the current record
#quality_test_checkpoint = (
#    "/Volumes/industrial_sensor/silver_checkpoints/"
#    "silver_test_data/step5c_quality"
#)

#quality_query = (
#    silver_quality_stream
#        .writeStream
#        .format("memory")
#        .queryName("silver_quality_test")
#        .outputMode("append")
#        .option("checkpointLocation", quality_test_checkpoint)
#        .trigger(availableNow=True)
#        .start()
#)

#quality_query.awaitTermination()

# COMMAND ----------

#spark.sql("""
#SELECT
#    event_id,
#    asset_id,
#    anomaly_type,
#    invalid_identity,
#    missing_measurement,
#    invalid_measurement,
#    invalid_timestamp
#FROM silver_quality_test
#ORDER BY event_id
#""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #####5C.2 — Create data_quality_status

# COMMAND ----------

from pyspark.sql.functions import when, lit

silver_quality_classified = (
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

#quality_status_checkpoint = (
#    "/Volumes/industrial_sensor/silver_checkpoints/"
#    "silver_test_data/step5c_quality_status"
#)

#quality_status_query = (
#    silver_quality_classified
#        .writeStream
#        .format("memory")
#        .queryName("silver_quality_status_test")
#        .outputMode("append")
#        .option("checkpointLocation", quality_status_checkpoint)
#        .trigger(availableNow=True)
#        .start()
#)

#quality_status_query.awaitTermination()

# COMMAND ----------

#spark.sql("""
#SELECT
#    event_id,
#    asset_id,
#    anomaly_type,
#    invalid_identity,
#    missing_measurement,
#    invalid_measurement,
#    invalid_timestamp,
#   data_quality_status
#FROM silver_quality_status_test
#ORDER BY event_id
#""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #####5C.3 — Test an actual bad telemetry event

# COMMAND ----------

from simulator import generate_event

# COMMAND ----------

from simulator import create_assets, generate_event, inject_missing_value

# COMMAND ----------

import inspect

print(inspect.signature(generate_event))

# COMMAND ----------

print(inspect.getsource(generate_event))

# COMMAND ----------

# create an asset
assets = create_assets(assets_per_site=10)

asset = assets[0]

print(asset)

# COMMAND ----------

# generate a normal event and deliberately inject a missing measurement
event = generate_event(asset)

event = inject_missing_value(event)

print(event)

# COMMAND ----------

# MAGIC %md
# MAGIC ######5C.3.1 — Publish this event through the real pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ######5C.3.1.1 Create the MQTT connection

# COMMAND ----------

# MAGIC %pip install awscrt

# COMMAND ----------

# MAGIC %pip install awsiotsdk

# COMMAND ----------

from mqtt_publisher import create_mqtt_connection, publish_event

# COMMAND ----------

mqtt_connection = create_mqtt_connection(
    "industrial-silver-quality-test-client"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ######5C.3.1.1.2 Publish the controlled event

# COMMAND ----------

publish_event(
    mqtt_connection,
    event
)

# COMMAND ----------

# MAGIC %md
# MAGIC ######5C.3.1.1.3 — Verify Silver classification

# COMMAND ----------

#silver_invalid_test_checkpoint = (
#    "/Volumes/industrial_sensor/silver_checkpoints/"
#    "silver_test_data/step5c_invalid_event1"
#)

#invalid_event_query = (
#    silver_quality_classified
#        .writeStream
#        .format("memory")
#        .queryName("silver_invalid_event_test")
#        .outputMode("append")
#        .option(
#            "checkpointLocation",
#            silver_invalid_test_checkpoint
#        )
#        .trigger(availableNow=True)
#        .start()
#)

#invalid_event_query.awaitTermination()

# COMMAND ----------

#spark.sql("""
#SELECT
#    event_id,
#    asset_id,
#    temperature,
#    pressure,
#    vibration,
#    flow_rate,
#    power_kw,
#    anomaly_type,
#    missing_measurement,
#    invalid_measurement,
#    data_quality_status
#FROM silver_invalid_event_test
#WHERE event_id = 'evt-23738a7b4539'
#""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ######5C.3.1.1.4  — Normalize the quality flags

# COMMAND ----------

from pyspark.sql.functions import coalesce, lit

silver_quality_normalized = (
    silver_quality_stream
    .withColumn(
        "invalid_identity",
        coalesce(col("invalid_identity"), lit(False))
    )
    .withColumn(
        "missing_measurement",
        coalesce(col("missing_measurement"), lit(False))
    )
    .withColumn(
        "invalid_measurement",
        coalesce(col("invalid_measurement"), lit(False))
    )
    .withColumn(
        "invalid_timestamp",
        coalesce(col("invalid_timestamp"), lit(False))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ######5C.3.1.1.5 — Validate normalized quality flags

# COMMAND ----------

#normalized_quality_checkpoint = (
#    "/Volumes/industrial_sensor/silver_checkpoints/"
#    "silver_test_data/step5c_normalized_quality1"
#)

#normalized_quality_query = (
#    silver_quality_normalized
#        .writeStream
#        .format("memory")
#        .queryName("silver_normalized_quality_test")
#        .outputMode("append")
#        .option(
#            "checkpointLocation",
#            normalized_quality_checkpoint
#        )
#        .trigger(availableNow=True)
#        .start()
#)

#normalized_quality_query.awaitTermination()

# COMMAND ----------

#spark.sql("""
#SELECT
#    event_id,
#    asset_id,
#    anomaly_type,
#    invalid_identity,
#    missing_measurement,
#    invalid_measurement,
#    invalid_timestamp
#FROM silver_normalized_quality_test
#WHERE event_id = 'evt-23738a7b4539'
#""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ######5C.3.1.1.6 — Create the final quality classification

# COMMAND ----------

silver_quality_final = (
    silver_quality_normalized
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
# MAGIC #####5C.6 — Split valid and invalid records

# COMMAND ----------

silver_valid_stream = (
    silver_quality_final
    .filter(
        col("data_quality_status") == "VALID"
    )
)

silver_quarantine_stream = (
    silver_quality_final
    .filter(
        col("data_quality_status") == "INVALID"
    )
)

# COMMAND ----------

print("VALID STREAM")
silver_valid_stream.printSchema()

print("\nQUARANTINE STREAM")
silver_quarantine_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####5C.7 — Validate VALID vs QUARANTINE routing

# COMMAND ----------

# MAGIC %md
# MAGIC ######1-Test the VALID stream

# COMMAND ----------

#valid_test_checkpoint = (
#    "/Volumes/industrial_sensor/silver_checkpoints/"
#    "silver_test_data/step5c_valid_routing"
#)

#valid_test_query = (
#    silver_valid_stream
#        .writeStream
#        .format("memory")
#        .queryName("silver_valid_routing_test")
#        .outputMode("append")
#        .option(
#            "checkpointLocation",
#            valid_test_checkpoint
#        )
#        .trigger(availableNow=True)
#        .start()
#)

#valid_test_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ######2-Test the QUARANTINE stream

# COMMAND ----------

#quarantine_test_checkpoint = (
#    "/Volumes/industrial_sensor/silver_checkpoints/"
#    "silver_test_data/step5c_quarantine_routing"
#)

#quarantine_test_query = (
#    silver_quarantine_stream
#        .writeStream
#        .format("memory")
#        .queryName("silver_quarantine_routing_test")
#        .outputMode("append")
#        .option(
#            "checkpointLocation",
#            quarantine_test_checkpoint
#        )
#        .trigger(availableNow=True)
#        .start()
#)

#quarantine_test_query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ######3-Check our two test events

# COMMAND ----------

# Valid events
#spark.sql("""
#SELECT
#    event_id,
#    asset_id,
#    anomaly_type,
#    vibration,
#    data_quality_status
#FROM silver_valid_routing_test
#WHERE event_id = 'evt-9ea65316ec5f'
#""").show(truncate=False)

# COMMAND ----------

# Invalid events to sent to quarantine
#spark.sql("""
#SELECT
#    event_id,
#    asset_id,
#    anomaly_type,
#    vibration,
#    missing_measurement,
#    data_quality_status
#FROM silver_quarantine_routing_test
#WHERE event_id = 'evt-23738a7b4539'
#""").show(truncate=False)