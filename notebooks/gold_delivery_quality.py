# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Delivery Quality Analytics
# MAGIC Business analytics-ready data for streaming quality and infrastructure analysis

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum, avg, round, current_timestamp, current_date,
    when, coalesce
)
from config import TARGET_CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

# COMMAND ----------

def create_delivery_quality():
    """
    Create delivery quality analytics table
    Combines delivery and metrics for QoS and infrastructure analysis
    
    Dimensions: isp_tier, cdn, connection_type, streaming_protocol, cache_status
    Metrics: total_streams, avg_delivery_quality, cache_hit_rate, performance_score
    """
    
    # Read silver tables
    delivery = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.delivery")
    metrics = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.metrics")
    
    # Join delivery with metrics
    combined = delivery.join(metrics, "delivery_id", "left")
    
    # Aggregate by delivery characteristics
    gold = combined.groupBy(
        "isp_tier",
        "cdn",
        "connection_type",
        "speed_category",
        "streaming_protocol",
        "cache_effectiveness",
        "bandwidth_utilization_category"
    ).agg(
        count("delivery_id").alias("total_streams"),
        avg("delivery_quality_score").alias("avg_delivery_quality_score"),
        avg("connection_quality_score").alias("avg_connection_quality_score"),
        sum(when(col("cache_effectiveness") == "Effective", 1)).alias("cache_hits"),
        count("delivery_id").alias("total_deliveries"),
        avg("performance_score").alias("avg_performance_score"),
        avg("bit_rate").alias("avg_bitrate_kbps"),
        sum("rebuffering_count").alias("total_buffering_events")
    )
    
    # Calculate cache hit rate
    gold = gold.withColumn("cache_hit_rate",
        round((col("cache_hits") / col("total_deliveries")) * 100, 2)
    )
    
    # Calculate buffering rate
    gold = gold.withColumn("buffering_rate_per_stream",
        round(col("total_buffering_events") / col("total_streams"), 2)
    )
    
    # QoS score (0-100)
    gold = gold.withColumn("qos_score",
        round(
            (col("avg_delivery_quality_score") * 40) +
            (col("cache_hit_rate") / 100 * 30) +
            (when(col("buffering_rate_per_stream") < 1, 30)
             .when(col("buffering_rate_per_stream") < 3, 20)
             .otherwise(10)),
            2
        )
    )
    
    # QoS status
    gold = gold.withColumn("qos_status",
        when(col("qos_score") >= 85, "Excellent")
        .when(col("qos_score") >= 70, "Good")
        .when(col("qos_score") >= 50, "Fair")
        .otherwise("Poor")
    )
    
    # Infrastructure health
    gold = gold.withColumn("infrastructure_health",
        when((col("cache_hit_rate") >= 80) & (col("buffering_rate_per_stream") < 1), "Optimal")
        .when((col("cache_hit_rate") >= 60) & (col("buffering_rate_per_stream") < 2), "Healthy")
        .when((col("cache_hit_rate") >= 40) | (col("buffering_rate_per_stream") < 3), "Acceptable")
        .otherwise("Needs_Attention")
    )
    
    # ISP performance ranking
    gold = gold.withColumn("isp_performance_rank",
        when(col("avg_delivery_quality_score") >= 0.8, "Top_Performer")
        .when(col("avg_delivery_quality_score") >= 0.6, "Good_Performer")
        .otherwise("Needs_Improvement")
    )
    
    # Fill nulls
    gold = gold.fillna(0, ["cache_hits", "total_buffering_events"])
    gold = gold.fillna(0.0, ["avg_delivery_quality_score", "avg_performance_score"])
    
    # Add metadata
    gold = gold.withColumn("analytics_timestamp", current_timestamp()) \
               .withColumn("analytics_date", current_date())
    
    # Write to gold
    target_table = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.delivery_quality"
    gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Delivery Quality: {gold.count()} rows")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
create_delivery_quality()
