# Databricks notebook source
# MAGIC %md
# MAGIC ####7A - Read Silver Telemetry

# COMMAND ----------

silver_location = (
    "/Volumes/industrial_sensor/sensor_data/"
    "industrial_sensor_data/delta/silver_telemetry"
)

silver_df = (
    spark.read
        .format("delta")
        .load(silver_location)
)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df.select(
    "event_id",
    "site_id",
    "asset_id",
    "asset_type",
    "event_ts",
    "temperature",
    "pressure",
    "vibration",
    "flow_rate",
    "power_kw",
    "status",
    "anomaly_type",
    "data_quality_status"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 7B - Define Asset-Health Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC We'll use metrics such as:
# MAGIC
# MAGIC - average temperature
# MAGIC - maximum temperature
# MAGIC - average pressure
# MAGIC - maximum pressure
# MAGIC - average vibration
# MAGIC - maximum vibration
# MAGIC - average flow rate
# MAGIC - average power consumption
# MAGIC - number of anomaly events
# MAGIC - most recent telemetry timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ####7B.1 - Create the asset-health aggregatio

# COMMAND ----------

from pyspark.sql.functions import (
    avg,
    max,
    count,
    when,
    col,
    max_by
)

asset_health_metrics = (
    silver_df
    .groupBy(
        "site_id",
        "asset_id",
        "asset_type"
    )
    .agg(
        avg("temperature").alias("avg_temperature"),
        max("temperature").alias("max_temperature"),

        avg("pressure").alias("avg_pressure"),
        max("pressure").alias("max_pressure"),

        avg("vibration").alias("avg_vibration"),
        max("vibration").alias("max_vibration"),

        avg("flow_rate").alias("avg_flow_rate"),

        avg("power_kw").alias("avg_power_kw"),

        count("*").alias("telemetry_count"),

        count(when(col("anomaly_type").isNotNull(),True )).alias("anomaly_count"),

        max("event_ts").alias("last_event_ts")
    )
)

# COMMAND ----------

asset_health_metrics.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####7C - Add Operational Alert Classification

# COMMAND ----------

from pyspark.sql.functions import when, lit

asset_health_gold = (
    asset_health_metrics
    .withColumn(
        "health_status",
        when(col("anomaly_count") > 0, lit("ATTENTION")).otherwise(lit("NORMAL"))
    )
)

# COMMAND ----------

asset_health_gold.select(
    "site_id",
    "asset_id",
    "asset_type",
    "telemetry_count",
    "anomaly_count",
    "health_status",
    "last_event_ts"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####7D - Build an Anomaly Summary

# COMMAND ----------

# MAGIC %md
# MAGIC we'll create a second Gold dataset focused specifically on operational anomalies, to answer questions such as:
# MAGIC
# MAGIC - Which assets are generating anomalies?
# MAGIC - What type of anomaly occurred?
# MAGIC - How many times did each anomaly occur?
# MAGIC - When was the latest occurrence?

# COMMAND ----------

# MAGIC %md
# MAGIC #####7D.1 — Create the anomaly summary

# COMMAND ----------

anomaly_summary = (
    silver_df
    .filter(col("anomaly_type").isNotNull())
    .groupBy(
        "site_id",
        "asset_id",
        "asset_type",
        "anomaly_type"
    )
    .agg(
        count("*").alias("anomaly_count"),
        max("event_ts").alias("last_anomaly_ts")
    )
)

# COMMAND ----------

anomaly_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####7E - Prepare the Gold Delta Tables

# COMMAND ----------

# MAGIC %md
# MAGIC We now have two Gold datasets:
# MAGIC
# MAGIC - asset_health_gold — asset-level operational health
# MAGIC - anomaly_summary — anomaly-level history

# COMMAND ----------

# MAGIC %md
# MAGIC #####7E.1 - Inspect datasets

# COMMAND ----------

asset_health_gold.printSchema()

# COMMAND ----------

anomaly_summary.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #####7E.2 - Write Asset Health Gold → Delta

# COMMAND ----------

# Define the destination
asset_health_location = (
    "/Volumes/industrial_sensor/sensor_data/"
    "industrial_sensor_data/delta/gold_asset_health"
)

# COMMAND ----------

# Write the DataFrame
(
    asset_health_gold
        .write
        .format("delta")
        .mode("overwrite")
        .save(asset_health_location)
)

# COMMAND ----------

# verify that the Delta table can be read
asset_health_check = (
    spark.read
        .format("delta")
        .load(asset_health_location)
)

asset_health_check.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #####7E.3 - Write Anomaly Summary → Delta

# COMMAND ----------

# Define the destination:
anomaly_summary_location = (
    "/Volumes/industrial_sensor/sensor_data/"
    "industrial_sensor_data/delta/gold_anomaly_summary"
)

# COMMAND ----------

# Write the DataFrame
(
    anomaly_summary
        .write
        .format("delta")
        .mode("overwrite")
        .save(anomaly_summary_location)
)

# COMMAND ----------

# # verify that the Delta table can be read
anomaly_summary_check = (
    spark.read
        .format("delta")
        .load(anomaly_summary_location)
)

anomaly_summary_check.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ####7F - Final Gold Validation

# COMMAND ----------

# MAGIC %md
# MAGIC #####7F.1 - Read both Gold tables

# COMMAND ----------

asset_health_gold_df = (
    spark.read
        .format("delta")
        .load(asset_health_location)
)

anomaly_summary_gold_df = (
    spark.read
        .format("delta")
        .load(anomaly_summary_location)
)

# COMMAND ----------

# MAGIC %md
# MAGIC #####7F.2 - Check row counts

# COMMAND ----------

print("Asset Health rows:", asset_health_gold_df.count())
print("Anomaly Summary rows:", anomaly_summary_gold_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #####7F.3 - Final business validation

# COMMAND ----------

asset_health_gold_df.select(
    "asset_id",
    "asset_type",
    "telemetry_count",
    "anomaly_count",
    "health_status",
    "last_event_ts"
).show(truncate=False)

# COMMAND ----------

anomaly_summary_gold_df.select(
    "asset_id",
    "anomaly_type",
    "anomaly_count",
    "last_anomaly_ts"
).show(truncate=False)