# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Metrics Transformation
# MAGIC Business transformations for performance analytics and KPI calculation

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, lower, coalesce, when, current_timestamp, current_date,
     lit, round, abs, sqrt, pow
)
from config import TARGET_CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

# COMMAND ----------

def transform_metrics():
    """
    Transform metrics with business logic:
    - Performance KPI calculation and normalization
    - Metric quality validation
    - Anomaly detection flags
    - Performance tier classification
    - Trend indicators
    - Business metric aggregation
    """
    
    df = spark.table(f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.metrics")
    
    # Standardize column names
    df = df.select([col(c).alias(c.lower()) for c in df.columns])
    
    # Trim string columns
    string_cols = [f.name for f in df.schema.fields if f.dataType.typeName() == 'string']
    for col_name in string_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))
    
    # Get numeric columns for metric processing
    numeric_cols = [f.name for f in df.schema.fields if f.dataType.typeName() in ['double', 'float', 'long', 'integer']]
    
    # Round numeric metrics to 2 decimal places
    for col_name in numeric_cols:
        if col_name not in ['load_timestamp', 'load_date', 'data_quality_score']:
            df = df.withColumn(col_name, round(col(col_name), 2))
    
    # Validate metric values - filter out negative values for KPIs
    for col_name in numeric_cols:
        if col_name not in ['load_timestamp', 'load_date', 'data_quality_score']:
            df = df.filter(col(col_name) >= 0)
    
    # Performance tier classification based on typical streaming metrics
    # Assuming columns like views, completion_rate, avg_bitrate, etc.
    
    # Views/Engagement tier
    if "views" in df.columns:
        df = df.withColumn("engagement_tier",
            when(col("views") >= 100000, "High")
                .when(col("views") >= 10000, "Medium")
                .when(col("views") >= 1000, "Low")
                .otherwise("Minimal")
        )
    
    # Completion rate classification
    if "completion_rate" in df.columns:
        df = df.withColumn("completion_rate_category",
            when(col("completion_rate") >= 0.8, "Excellent")
                .when(col("completion_rate") >= 0.6, "Good")
                .when(col("completion_rate") >= 0.4, "Fair")
                .otherwise("Poor")
        )
    
    # Bitrate quality classification
    if "avg_bitrate" in df.columns:
        df = df.withColumn("bitrate_quality",
            when(col("avg_bitrate") >= 5000, "4K")
                .when(col("avg_bitrate") >= 2500, "1080p")
                .when(col("avg_bitrate") >= 1500, "720p")
                .when(col("avg_bitrate") >= 800, "480p")
                .otherwise("Low")
        )
    
    # Buffering impact assessment
    if "buffering_events" in df.columns:
        df = df.withColumn("buffering_impact",
            when(col("buffering_events") == 0, "None")
                .when(col("buffering_events") <= 2, "Minimal")
                .when(col("buffering_events") <= 5, "Moderate")
                .otherwise("Severe")
        )
    
    # Anomaly detection - flag unusual metric values
    if "avg_bitrate" in df.columns:
        df = df.withColumn("bitrate_anomaly",
            when((col("avg_bitrate") > 10000) | (col("avg_bitrate") < 100), True).otherwise(False)
        )
    
    if "completion_rate" in df.columns:
        df = df.withColumn("completion_rate_anomaly",
            when((col("completion_rate") > 1.0) | (col("completion_rate") < 0), True).otherwise(False)
        )
    
    # Performance score calculation (0-100)
    performance_score = lit(0)
    
    if "completion_rate" in df.columns:
        performance_score = performance_score + (col("completion_rate") * 40)
    
    if "avg_bitrate" in df.columns:
        performance_score = performance_score + (when(col("avg_bitrate") >= 2500, 30).otherwise(col("avg_bitrate") / 2500 * 30))
    
    if "buffering_events" in df.columns:
        performance_score = performance_score + (when(col("buffering_events") == 0, 30).otherwise(30 - (col("buffering_events") * 5)))
    
    df = df.withColumn("performance_score", round(performance_score, 2))
    
    # Performance health status
    df = df.withColumn("performance_health",
        when(col("performance_score") >= 85, "Excellent")
            .when(col("performance_score") >= 70, "Good")
            .when(col("performance_score") >= 50, "Fair")
            .otherwise("Poor")
    )
    
    # Remove duplicates
    if "metric_id" in df.columns:
        df = df.dropDuplicates(["metric_id"])
    
    # Add transformation metadata
    df = df.withColumn("transformed_timestamp", current_timestamp()) \
           .withColumn("transformed_date", current_date())
    
    # Write to silver
    target_table = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.metrics"
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Metrics transformed: {df.count()} rows")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{SILVER_SCHEMA}")
transform_metrics()
