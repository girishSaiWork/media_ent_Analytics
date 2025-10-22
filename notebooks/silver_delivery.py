# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Delivery Transformation
# MAGIC Business transformations for streaming delivery optimization and performance analysis

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, lower, coalesce, when, current_timestamp, current_date,
    lit, regexp_replace, split, concat_ws
)
from config import TARGET_CATALOG, BRONZE_SCHEMA

# COMMAND ----------

SILVER_SCHEMA = f"silver_schema"

# COMMAND ----------

def transform_delivery():
    """
    Transform delivery with business logic:
    - ISP and CDN performance classification
    - Connection quality scoring
    - Streaming protocol optimization
    - Cache efficiency analysis
    - Bandwidth utilization categorization
    - Player compatibility assessment
    """
    
    df = spark.table(f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.delivery")
    
    # Standardize column names
    df = df.select([col(c).alias(c.lower()) for c in df.columns])
    
    # Trim string columns
    string_cols = ['isp', 'isp_type', 'cdn', 'cdn_edge_location', 'connection_type', 
                   'video_player', 'player_version', 'streaming_protocol', 'cache_status']
    for col_name in string_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))
    
    # ISP tier classification for performance analysis
    df = df.withColumn("isp_tier",
        when(upper(col("isp_type")).contains("TIER1"), "Tier1")
            .when(upper(col("isp_type")).contains("TIER2"), "Tier2")
            .when(upper(col("isp_type")).contains("TIER3"), "Tier3")
            .otherwise("Other")
    )
    
    # Connection quality scoring
    df = df.withColumn("connection_quality_score",
        when(upper(col("connection_type")) == "FIBER", 5)
            .when(upper(col("connection_type")) == "CABLE", 4)
            .when(upper(col("connection_type")) == "DSL", 3)
            .when(upper(col("connection_type")) == "MOBILE", 2)
            .otherwise(1)
    )
    
    # Connection speed categorization
    df = df.withColumn("speed_category",
        when(col("connection_speed") >= 100, "Ultra_High")
            .when(col("connection_speed") >= 50, "Very_High")
            .when(col("connection_speed") >= 25, "High")
            .when(col("connection_speed") >= 10, "Medium")
            .when(col("connection_speed") >= 5, "Low")
            .otherwise("Very_Low")
    )
    
    # Streaming protocol optimization
    df = df.withColumn("protocol_efficiency",
        when(upper(col("streaming_protocol")).contains("DASH"), "High")
            .when(upper(col("streaming_protocol")).contains("HLS"), "High")
            .when(upper(col("streaming_protocol")).contains("SMOOTH"), "Medium")
            .otherwise("Low")
    )
    
    # Cache effectiveness analysis
    df = df.withColumn("cache_effectiveness",
        when(upper(col("cache_status")) == "HIT", "Effective")
            .when(upper(col("cache_status")) == "PARTIAL_HIT", "Partial")
            .when(upper(col("cache_status")) == "MISS", "Ineffective")
            .otherwise("Unknown")
    )
    
    # Bandwidth utilization classification
    df = df.withColumn("bandwidth_utilization_category",
        when(col("bandwidth_limit").isNull(), "Unlimited")
            .when(col("bandwidth_limit") >= 500, "Premium")
            .when(col("bandwidth_limit") >= 100, "Standard")
            .when(col("bandwidth_limit") >= 25, "Basic")
            .otherwise("Limited")
    )
    
    # Player version compatibility assessment
    df = df.withColumn("player_compatibility",
        when(col("player_version").isNull(), "Unknown")
            .when(col("player_version") >= "5.0", "Latest")
            .when(col("player_version") >= "4.0", "Current")
            .when(col("player_version") >= "3.0", "Legacy")
            .otherwise("Outdated")
    )
    
    # Overall delivery quality score
    df = df.withColumn("delivery_quality_score",
        (col("connection_quality_score") / 5.0 * 0.4) +
        (when(col("cache_effectiveness") == "Effective", 1.0)
         .when(col("cache_effectiveness") == "Partial", 0.5)
         .otherwise(0.0) * 0.3) +
        (when(col("protocol_efficiency") == "High", 1.0)
         .when(col("protocol_efficiency") == "Medium", 0.5)
         .otherwise(0.0) * 0.3)
    )
    
    # Remove duplicates
    df = df.dropDuplicates(["delivery_id"])
    
    # Add transformation metadata
    df = df.withColumn("transformed_timestamp", current_timestamp()) \
           .withColumn("transformed_date", current_date())
    
    # Write to silver
    target_table = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.delivery"
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Delivery transformed: {df.count()} rows")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{SILVER_SCHEMA}")
transform_delivery()
