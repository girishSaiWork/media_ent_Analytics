# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Device Analytics
# MAGIC Business analytics-ready data for device ecosystem analysis

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum, avg, round, current_timestamp, current_date,
    when, coalesce
)
from config import TARGET_CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

# COMMAND ----------

def create_device_analytics():
    """
    Create device analytics table
    Combines devices and metrics for device performance analysis
    
    Dimensions: device_ecosystem, device_form_factor, device_capability_tier, browser, os_version
    Metrics: active_devices, avg_performance_score, completion_rate, bitrate_quality
    """
    
    # Read silver tables
    devices = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.devices")
    metrics = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.metrics")
    
    # Join devices with metrics
    combined = devices.join(metrics, "device_id", "left")
    
    # Aggregate by device characteristics
    gold = combined.groupBy(
        "device_ecosystem",
        "device_form_factor",
        "device_capability_tier",
        "browser",
        "os_support_status",
        "browser_support_status",
        "screen_resolution_category"
    ).agg(
        count("device_id").alias("active_devices"),
        avg("performance_score").alias("avg_performance_score"),
        avg("bit_rate").alias("avg_bitrate_kbps"),
        avg("rebuffering_count").alias("avg_buffering_events"),
        count(when(col("is_bot_likely") == True, 1)).alias("bot_traffic_count")
    )
    
    # Add calculated metrics
    gold = gold.withColumn("bot_traffic_percentage",
        round((col("bot_traffic_count") / col("active_devices")) * 100, 2)
    )
    
    gold = gold.withColumn("device_health_score",
        round(
            (col("avg_performance_score") / 100 * 0.4) +
            (when(col("avg_buffering_events") < 2, 1.0).otherwise(0.5) * 0.2),
            2
        )
    )
    
    gold = gold.withColumn("device_health_status",
        when(col("device_health_score") >= 0.8, "Excellent")
        .when(col("device_health_score") >= 0.6, "Good")
        .when(col("device_health_score") >= 0.4, "Fair")
        .otherwise("Poor")
    )
    
    # Optimization recommendation
    gold = gold.withColumn("optimization_needed",
        when((col("avg_buffering_events") > 3), True)
        .otherwise(False)
    )
    
    # Fill nulls
    gold = gold.fillna(0, ["bot_traffic_count", "avg_buffering_events"])
    gold = gold.fillna(0.0, ["avg_performance_score", "avg_bitrate_kbps"])
    
    # Add metadata
    gold = gold.withColumn("analytics_timestamp", current_timestamp()) \
               .withColumn("analytics_date", current_date())
    
    # Write to gold
    target_table = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.device_analytics"
    gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Device Analytics: {gold.count()} rows")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
create_device_analytics()
