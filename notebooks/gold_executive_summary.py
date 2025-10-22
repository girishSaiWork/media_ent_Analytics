# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Executive Summary
# MAGIC High-level KPIs and metrics for executive dashboards

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum, avg, max, min, round, current_timestamp, current_date,
    when, lit, coalesce
)
from pyspark.sql import Window
from config import TARGET_CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

# COMMAND ----------

def create_executive_summary():
    """
    Create executive summary table with key business metrics
    
    Metrics: total_views, avg_completion_rate, avg_performance_score,
             active_devices, active_locations, qos_score, market_potential
    """
    
    # Read all silver tables
    assets = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.assets")
    metrics = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.metrics")
    devices = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.devices")
    geo = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.geo")
    delivery = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.delivery")
    
    # Calculate overall metrics
    metrics_summary = metrics.agg(
        avg("performance_score").alias("avg_performance_score"),
        count("*").alias("total_streams"),
        sum("rebuffering_count").alias("total_buffering_events")
    )
    
    # Device metrics
    device_summary = devices.agg(
        count("device_id").alias("active_devices"),
        count(when(col("device_form_factor") == "Mobile", 1)).alias("mobile_devices"),
        count(when(col("device_form_factor") == "Desktop", 1)).alias("desktop_devices"),
        count(when(col("is_bot_likely") == True, 1)).alias("bot_devices")
    )
    
    # Geographic metrics
    geo_summary = geo.agg(
        count("location_id").alias("active_locations"),
        sum("population").alias("total_population"),
        count(when(col("population_density_tier") == "Megacity", 1)).alias("megacity_locations"),
        count(when(col("coordinates_valid") == False, 1)).alias("invalid_coordinates")
    )
    
    # Content metrics
    content_summary = assets.agg(
        count("content_id").alias("total_content"),
        count(when(col("content_freshness") == "New", 1)).alias("new_content"),
        count(when(col("content_freshness") == "Archive", 1)).alias("archive_content"),
        avg("content_completeness_score").alias("avg_content_completeness")
    )
    
    # Delivery metrics
    delivery_summary = delivery.agg(
        avg("delivery_quality_score").alias("avg_delivery_quality_score"),
        count(when(col("cache_effectiveness") == "Effective", 1)).alias("effective_cache_count"),
        count("delivery_id").alias("total_deliveries")
    )
    
    # Combine all metrics
    gold = metrics_summary.crossJoin(device_summary) \
                          .crossJoin(geo_summary) \
                          .crossJoin(content_summary) \
                          .crossJoin(delivery_summary)
    
    # Calculate derived metrics
    gold = gold.withColumn("cache_hit_rate",
        round((col("effective_cache_count") / col("total_deliveries")) * 100, 2)
    )
    
    gold = gold.withColumn("buffering_rate",
        round(col("total_buffering_events") / col("total_streams"), 2)
    )
    
    gold = gold.withColumn("mobile_percentage",
        round((col("mobile_devices") / col("active_devices")) * 100, 2)
    )
    
    gold = gold.withColumn("bot_percentage",
        round((col("bot_devices") / col("active_devices")) * 100, 2)
    )
    
    gold = gold.withColumn("data_quality_score",
        round(
            (when(col("invalid_coordinates") == 0, 1.0).otherwise(0.8)) * 0.3 +
            (col("avg_content_completeness") * 0.3) +
            (when(col("bot_percentage") < 5, 1.0).otherwise(0.7) * 0.4),
            2
        )
    )
    
    # Overall health score
    gold = gold.withColumn("overall_health_score",
        round(
            (col("avg_delivery_quality_score") * 0.2) +
            (col("data_quality_score") * 0.2),
            2
        )
    )
    
    # Health status
    gold = gold.withColumn("overall_health_status",
        when(col("overall_health_score") >= 0.8, "Excellent")
        .when(col("overall_health_score") >= 0.6, "Good")
        .when(col("overall_health_score") >= 0.4, "Fair")
        .otherwise("Poor")
    )
    
    # Add metadata
    gold = gold.withColumn("analytics_timestamp", current_timestamp()) \
               .withColumn("analytics_date", current_date())
    
    # Write to gold
    target_table = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.executive_summary"
    gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Executive Summary created")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
create_executive_summary()
