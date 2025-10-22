# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Assets Transformation
# MAGIC Business transformations for content asset enrichment and quality scoring

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, lower, coalesce, when, to_date, current_timestamp, current_date,
    year, month, datediff, lit, concat_ws, regexp_replace, length
)
from config import TARGET_CATALOG, BRONZE_SCHEMA

# COMMAND ----------

SILVER_SCHEMA = "silver_schema"

# COMMAND ----------

def transform_assets():
    """
    Transform assets with business logic:
    - Content age calculation for relevance scoring
    - Content categorization by type and genre
    - Series/episode hierarchy validation
    - Content completeness scoring
    - Language standardization
    - Rating classification
    """
    
    df = spark.table(f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.assets")
    
    # Standardize column names
    df = df.select([col(c).alias(c.lower()) for c in df.columns])
    
    # Trim and standardize text fields
    string_cols = ['video_title', 'ad_creative_name', 'rendition', 'cms', 'content_type', 'genre', 'language', 'rating']
    for col_name in string_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))
    
    # Parse and standardize release_date
    df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))
    
    # Content age calculation for relevance
    df = df.withColumn("content_age_days", datediff(current_date(), col("release_date")))
    
    # Content freshness category
    df = df.withColumn(
        "content_freshness", 
            when(col("content_age_days") <= 30, "New")
            .when(col("content_age_days") <= 90, "Recent")
            .when(col("content_age_days") <= 365, "Standard")
            .otherwise("Archive")
    )
    
    # Content type standardization and categorization
    df = df.withColumn(
    "content_category",
    when(upper(col("content_type")).contains("MOVIE"), "Movie")
    .when(upper(col("content_type")).contains("SERIES"), "Series")
    .when(upper(col("content_type")).contains("EPISODE"), "Episode")
    .when(upper(col("content_type")).contains("DOCUMENTARY"), "Documentary")
    .otherwise("Other")
)
    
    # Series/Episode hierarchy validation
    df = df.withColumn("is_series_content", 
        when((col("series_id").isNotNull()) & (col("season_number") > 0), True).otherwise(False)
    )
    
    # Content completeness scoring
    df = df.withColumn("content_completeness_score",
        (
            when(col("video_title").isNotNull(), 1).otherwise(0) +
            when(col("content_length").isNotNull() & (col("content_length") > 0), 1).otherwise(0) +
            when(col("genre").isNotNull(), 1).otherwise(0) +
            when(col("release_date").isNotNull(), 1).otherwise(0) +
            when(col("language").isNotNull(), 1).otherwise(0) +
            when(col("rating").isNotNull(), 1).otherwise(0)
        ) / 6.0
    )
    
    # Language standardization
    df = df.withColumn("language_code",
        when(upper(col("language")).contains("ENGLISH"), "EN")
            .when(upper(col("language")).contains("SPANISH"), "ES")
            .when(upper(col("language")).contains("FRENCH"), "FR")
            .when(upper(col("language")).contains("GERMAN"), "DE")
            .when(upper(col("language")).contains("PORTUGUESE"), "PT")
            .otherwise("OTHER")
    )
    
    # Rating classification for content filtering
    df = df.withColumn("rating_category",
                       when(upper(col("rating")).isin("G", "PG"), "Family")
                        .when(upper(col("rating")).isin("PG-13", "12"), "Teen")
                        .when(upper(col("rating")).isin("R", "15", "16"), "Mature")
                        .when(upper(col("rating")).isin("NC-17", "18"), "Adult")
                        .otherwise("Unrated")
    )
    
    # Content length validation and categorization
    df = df.withColumn("content_duration_category",
        when(col("content_length") < 5, "Short")
            .when(col("content_length") < 30, "Medium")
            .when(col("content_length") < 120, "Long")
            .otherwise("Feature")
    )
    
    # Remove duplicates by content_id
    df = df.dropDuplicates(["content_id"])
    
    # Add transformation metadata
    df = df.withColumn("transformed_timestamp", current_timestamp()) \
           .withColumn("transformed_date", current_date())
    
    # Write to silver
    target_table = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.assets"
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Assets transformed: {df.count()} rows")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS media_ent_catalog.silver_schema")
transform_assets()
