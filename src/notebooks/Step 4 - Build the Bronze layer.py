# Databricks notebook source
# MAGIC %md
# MAGIC ####Step 8A - Create the Bronze streaming DataFrame

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# 1. Define the expected telemetry structure
telemetry_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("site_id", StringType(), True),
    StructField("asset_id", StringType(), True),
    StructField("asset_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("flow_rate", DoubleType(), True),
    StructField("power_kw", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("anomaly_type", StringType(), True)
])

# 2. Read new records from Kinesis
kinesis_stream = (
    spark.readStream
        .format("kinesis")
        .option("streamName", "industrial-telemetry")
        .option("region", "us-east-1")
        .option("serviceCredential", "industrial-kinesis-read")
        .option("initialPosition", "earliest")
        .load()
)


# 3. Convert Kinesis binary payload into JSON string
bronze = (
    kinesis_stream
        .select(
            col("partitionKey"),
            col("data").cast("string").alias("raw_payload"),
            col("stream").alias("kinesis_stream"),
            col("shardId").alias("kinesis_shard_id"),
            col("sequenceNumber").alias("kinesis_sequence_number"),
            col("approximateArrivalTimestamp")
                .alias("kinesis_arrival_timestamp")
        )
        .withColumn(
            "telemetry",
            from_json(col("raw_payload"), telemetry_schema)
        )
        .select(
            "telemetry.*",
            "raw_payload",
            "partitionKey",
            "kinesis_stream",
            "kinesis_shard_id",
            "kinesis_sequence_number",
            "kinesis_arrival_timestamp"
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
)

bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 8B — Create the Bronze Delta table stored by path

# COMMAND ----------

bronze_checkpoint = (
    "/Volumes/industrial_sensor/"
    "sensor_data/industrial_sensor_data/"
    "checkpoints/bronze_telemetry_v2"
)

bronze_location = (
    "/Volumes/industrial_sensor/"
    "sensor_data/industrial_sensor_data/"
    "delta/bronze_telemetry"
)

bronze_query = (
    bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", bronze_checkpoint)
        .option("path", bronze_location)
        .trigger(availableNow=True)
        .start()
)

bronze_query.awaitTermination()

print("Bronze ingestion completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 8C- Read/validate Bronze Delta

# COMMAND ----------

bronze_df = spark.read.format("delta").load(
    bronze_location
)

print("Bronze records:", bronze_df.count())

display(
    bronze_df.select(
        "event_id",
        "site_id",
        "asset_id",
        "asset_type",
        "temperature",
        "pressure",
        "vibration",
        "flow_rate",
        "power_kw",
        "status",
        "anomaly_type",
        "kinesis_shard_id",
        "kinesis_sequence_number"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 8D — Verify the actual record

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
        "raw_payload",
        "kinesis_shard_id",
        "kinesis_sequence_number",
        "kinesis_arrival_timestamp",
        "ingestion_timestamp"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step 8E — Send real simulator telemetry

# COMMAND ----------

# MAGIC %pip install awscrt

# COMMAND ----------

# MAGIC %pip install awsiotsdk

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 8F — Create mqtt_publisher.py

# COMMAND ----------

import os

print(os.listdir(
    "/Workspace/Users/mayanzaouamba@yahoo.fr/Industrial_sensor_simulator"
))

# COMMAND ----------

print(
    open(
        "/Workspace/Users/mayanzaouamba@yahoo.fr/"
        "Industrial_sensor_simulator/mqtt_publisher.py"
    ).read()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ######Add the project directory to Python's path

# COMMAND ----------

import sys

project_path = (
    "/Workspace/Users/"
    "mayanzaouamba@yahoo.fr/"
    "Industrial_sensor_simulator"
)

sys.path.insert(0, project_path)

print(sys.path[:3])

# COMMAND ----------

# MAGIC %md
# MAGIC ######Step 1 — Load mqtt_publisher.py

# COMMAND ----------

import runpy

publisher = runpy.run_path(
    "/Workspace/Users/mayanzaouamba@yahoo.fr/"
    "Industrial_sensor_simulator/mqtt_publisher.py"
)

print("Functions loaded:")
print([
    name for name in publisher.keys()
    if not name.startswith("__")
])

# COMMAND ----------

# MAGIC %md
# MAGIC ######Step 2 — Use the functions

# COMMAND ----------

create_mqtt_connection = publisher["create_mqtt_connection"]
publish_event = publisher["publish_event"]

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 8G — Test the publisher with ONE event

# COMMAND ----------

from simulator import create_assets, generate_event

assets = create_assets(assets_per_site=10)

event = generate_event(assets[0])

mqtt_connection = create_mqtt_connection(
    "industrial-bronze-test-client"
)

publish_event(
    mqtt_connection,
    event
)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 8H — Consume the new event into Bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ######1. Recreate the Kinesis source

# COMMAND ----------

kinesis_stream = (
    spark.readStream
        .format("kinesis")
        .option("streamName", "industrial-telemetry")
        .option("region", "us-east-1")
        .option("serviceCredential", "industrial-kinesis-read")
        .option("initialPosition", "earliest")
        .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ######2. Recreate the Bronze transformation

# COMMAND ----------

bronze = (
    kinesis_stream
        .select(
            col("partitionKey"),
            col("data").cast("string").alias("raw_payload"),
            col("stream").alias("kinesis_stream"),
            col("shardId").alias("kinesis_shard_id"),
            col("sequenceNumber").alias("kinesis_sequence_number"),
            col("approximateArrivalTimestamp")
                .alias("kinesis_arrival_timestamp")
        )
        .withColumn(
            "telemetry",
            from_json(col("raw_payload"), telemetry_schema)
        )
        .select(
            "telemetry.*",
            "raw_payload",
            "partitionKey",
            "kinesis_stream",
            "kinesis_shard_id",
            "kinesis_sequence_number",
            "kinesis_arrival_timestamp"
        )
        .withColumn(
            "ingestion_timestamp",
            current_timestamp()
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ######3. Run the Bronze stream again
# MAGIC Same Bronze Delta location, but a new checkpoint

# COMMAND ----------

bronze_checkpoint = (
    "/Volumes/industrial_sensor/"
    "sensor_data/industrial_sensor_data/"
    "checkpoints/bronze_telemetry_v3"
)

bronze_location = (
    "/Volumes/industrial_sensor/"
    "sensor_data/industrial_sensor_data/"
    "delta/bronze_telemetry"
)

bronze_query = (
    bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", bronze_checkpoint)
        .option("path", bronze_location)
        .trigger(availableNow=True)
        .start()
)

bronze_query.awaitTermination()

print("Bronze ingestion completed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ######4. Check the Bronze table

# COMMAND ----------

bronze_df = spark.read.format("delta").load(
    bronze_location
)

print("Bronze records:", bronze_df.count())

display(
    bronze_df.select(
        "event_id",
        "asset_id",
        "asset_type",
        "temperature",
        "pressure",
        "vibration",
        "anomaly_type",
        "kinesis_sequence_number",
        "ingestion_timestamp"
    ).orderBy(
        col("ingestion_timestamp").desc()
    )
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

