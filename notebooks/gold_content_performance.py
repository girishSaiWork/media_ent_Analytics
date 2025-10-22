# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Content Performance Analytics
# MAGIC Business analytics-ready data for Power BI dashboards

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum, avg, max, min, round, current_timestamp, current_date,
    when, coalesce, concat_ws
)
from config import TARGET_CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

# COMMAND ----------

def create_content_performance():
    """
    Create content performance analytics table
    Combines assets and metrics for comprehensive content analysis
    
    Dimensions: content_id, video_title, content_type, genre, language, rating
    Metrics: total_views, avg_completion_rate, avg_bitrate, buffering_events, performance_score
    """
    
    # Read silver tables
    assets = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.assets")
    metrics = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.metrics")
    
    # Aggregate metrics by content
    metrics_agg = metrics.groupBy("content_id").agg(
        count("*").alias("total_streams"),
        count("session_id").alias("total_views"),
        avg("bit_rate").alias("avg_bitrate_kbps"),
        sum("rebuffering_count").alias("total_buffering_events"))
    
    # Join with assets
    assets_agg = assets.select(
        "content_id",
        "video_title",
        "content_category",
        "content_freshness",
        "genre",
        "language",
        "rating_category",
        "content_duration_category",
        "content_completeness_score",
        "release_date"
    ).groupBy("content_id").agg(
        avg("content_completeness_score").alias("avg_completion_rate"),
        max("content_completeness_score").alias("max_completion_rate"),
        min("content_completeness_score").alias("min_completion_rate")
    )
    
    gold = assets_agg.join(metrics_agg, "content_id", "left")
    
    # Add calculated metrics
    gold = gold.withColumn("avg_buffering_per_stream",
        round(col("total_buffering_events") / col("total_streams"), 2)
    )
    
    gold = gold.withColumn("completion_rate_category",
        when(col("avg_completion_rate") >= 0.8, "Excellent")
        .when(col("avg_completion_rate") >= 0.6, "Good")
        .when(col("avg_completion_rate") >= 0.4, "Fair")
        .otherwise("Poor")
    )
    
    gold = gold.withColumn("content_performance_tier",
        when((col("avg_completion_rate") >= 0.7), "Premium")
        .when((col("avg_completion_rate") >= 0.5), "Standard")
        .otherwise("Basic")
    )
    
    # Fill nulls for content without metrics
    gold = gold.fillna(0, ["total_streams", "total_views", "total_buffering_events"])
    gold = gold.fillna(0.0, ["avg_completion_rate", "avg_bitrate_kbps"])
    
    # Add metadata
    gold = gold.withColumn("analytics_timestamp", current_timestamp()) \
               .withColumn("analytics_date", current_date())
    
    # Write to gold
    target_table = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.content_performance"
    gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Content Performance: {gold.count()} rows")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
create_content_performance()
