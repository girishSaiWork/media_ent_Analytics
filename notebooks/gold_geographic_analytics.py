# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Geographic Analytics
# MAGIC Business analytics-ready data for regional performance and market analysis

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, sum, avg, round, current_timestamp, current_date,
    when, coalesce
)
from config import TARGET_CATALOG, SILVER_SCHEMA, GOLD_SCHEMA

# COMMAND ----------

def create_geographic_analytics():
    """
    Create geographic analytics table
    Combines geo and metrics for regional performance analysis
    
    Dimensions: regional_market, country, population_density_tier, market_size_segment
    Metrics: active_locations, avg_performance_score, total_views, market_potential
    """
    
    # Read silver tables
    geo = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.geo")
    metrics = spark.table(f"{TARGET_CATALOG}.{SILVER_SCHEMA}.metrics")
    
    # Join geo with metrics
    combined = geo.join(metrics, "location_id", "left")
    
    # Aggregate by geographic dimensions
    gold = combined.groupBy(
        "regional_market",
        "country",
        "population_density_tier",
        "market_size_segment",
        "time_zone_standardized",
        "is_metro_area"
    ).agg(
        count("location_id").alias("active_locations"),
        sum("population").alias("total_population"),
        avg("performance_score").alias("avg_performance_score"),
        count("*").alias("total_views"),
        avg("bit_rate").alias("avg_bitrate_kbps"),
        count(when(col("coordinates_valid") == False, 1)).alias("invalid_coordinates_count")
    )
    
    # Calculate market potential score
    gold = gold.withColumn("market_potential_score",
        round(
            (when(col("total_population") >= 5000000, 1.0)
             .when(col("total_population") >= 1000000, 0.8)
             .when(col("total_population") >= 100000, 0.6)
             .otherwise(0.4)) * 0.4 +
            (col("avg_performance_score") / 100 * 0.3),
            2
        )
    )
    
    # Market tier classification
    gold = gold.withColumn("market_tier",
        when(col("market_potential_score") >= 0.8, "Tier1_Priority")
        .when(col("market_potential_score") >= 0.6, "Tier2_Growth")
        .when(col("market_potential_score") >= 0.4, "Tier3_Emerging")
        .otherwise("Tier4_Monitor")
    )
    
    # Regional performance status
    gold = gold.withColumn("regional_performance_status",
        when(col("avg_performance_score") >= 85, "Excellent")
        .when(col("avg_performance_score") >= 70, "Good")
        .when(col("avg_performance_score") >= 50, "Fair")
        .otherwise("Poor")
    )
    
    # Data quality flag
    gold = gold.withColumn("data_quality_flag",
        when(col("invalid_coordinates_count") > 0, "Review_Coordinates")
        .otherwise("Clean")
    )
    
    # Fill nulls
    gold = gold.fillna(0, ["total_population", "total_views", "invalid_coordinates_count"])
    gold = gold.fillna(0.0, ["avg_performance_score", "avg_bitrate_kbps"])
    
    # Add metadata
    gold = gold.withColumn("analytics_timestamp", current_timestamp()) \
               .withColumn("analytics_date", current_date())
    
    # Write to gold
    target_table = f"{TARGET_CATALOG}.{GOLD_SCHEMA}.geographic_analytics"
    gold.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Geographic Analytics: {gold.count()} rows")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{GOLD_SCHEMA}")
create_geographic_analytics()
