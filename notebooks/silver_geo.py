# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Geo Transformation
# MAGIC Business transformations for geographic analysis and regional performance insights

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, lower, coalesce, when, current_timestamp, current_date,
     lit, round, sqrt, pow
)
from config import TARGET_CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

# COMMAND ----------

def transform_geo():
    """
    Transform geo with business logic:
    - Geographic region hierarchy (country, state, city)
    - Population density classification
    - Time zone standardization
    - Metro area identification
    - Geographic coordinate validation
    - Regional market segmentation
    """
    
    df = spark.table(f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.geo")
    
    # Standardize column names
    df = df.select([col(c).alias(c.lower()) for c in df.columns])
    
    # Trim string columns
    string_cols = ['ip_address', 'city', 'state', 'state_code', 'country', 'country_code', 
                   'postal_code', 'time_zone', 'region', 'metro_area']
    for col_name in string_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))
    
    # Standardize country and region names
    df = df.withColumn("country", upper(col("country")))
    df = df.withColumn("region", upper(col("region")))
    df = df.withColumn("state", upper(col("state")))
    
    # Geographic coordinate validation
    df = df.withColumn("coordinates_valid",
        when((col("latitude") >= -90) & (col("latitude") <= 90) &
             (col("longitude") >= -180) & (col("longitude") <= 180), True).otherwise(False)
    )
    
    # Population density classification
    df = df.withColumn("population_density_tier",
        when(col("population") >= 10000000, "Megacity")
            .when(col("population") >= 1000000, "Major_City")
            .when(col("population") >= 100000, "City")
            .when(col("population") >= 10000, "Town")
            .otherwise("Rural")
    )
    
    # Market size classification for revenue potential
    df = df.withColumn("market_size_segment",
        when(col("population") >= 5000000, "Tier1")
            .when(col("population") >= 1000000, "Tier2")
            .when(col("population") >= 100000, "Tier3")
            .otherwise("Tier4")
    )
    
    # Time zone standardization and UTC offset calculation
    df = df.withColumn("time_zone_standardized",
        when(upper(col("time_zone")).contains("EST"), "EST")
            .when(upper(col("time_zone")).contains("CST"), "CST")
            .when(upper(col("time_zone")).contains("MST"), "MST")
            .when(upper(col("time_zone")).contains("PST"), "PST")
            .when(upper(col("time_zone")).contains("GMT"), "GMT")
            .when(upper(col("time_zone")).contains("CET"), "CET")
            .when(upper(col("time_zone")).contains("IST"), "IST")
            .otherwise(col("time_zone"))
    )
    
    # Metro area identification for regional analytics
    df = df.withColumn("is_metro_area",
        when(col("metro_area").isNotNull() & (col("metro_area") != ""), True).otherwise(False)
    )
    
    # Regional market classification
    df = df.withColumn("regional_market",
        when(upper(col("country")).isin("US", "CA", "MX"), "North_America")
            .when(upper(col("country")).isin("BR", "AR", "CL", "CO"), "South_America")
            .when(upper(col("country")).isin("GB", "DE", "FR", "IT", "ES"), "Europe")
            .when(upper(col("country")).isin("IN", "CN", "JP", "KR", "SG"), "Asia_Pacific")
            .when(upper(col("country")).isin("AU", "NZ"), "Oceania")
            .when(upper(col("country")).isin("ZA", "EG", "NG"), "Africa")
            .when(upper(col("country")).isin("AE", "SA", "IL"), "Middle_East")
            .otherwise("Other")
    )
    
    # Geographic diversity score for content localization
    df = df.withColumn("localization_priority",
        when(col("population_density_tier").isin("Megacity", "Major_City"), "High")
            .when(col("population_density_tier") == "City", "Medium")
            .otherwise("Low")
    )
    
    # IP address validation flag
    df = df.withColumn("ip_address_valid",
        when(col("ip_address").isNotNull() & (col("ip_address") != ""), True).otherwise(False)
    )
    
    # Remove duplicates
    df = df.dropDuplicates(["location_id"])
    
    # Add transformation metadata
    df = df.withColumn("transformed_timestamp", current_timestamp()) \
           .withColumn("transformed_date", current_date())
    
    # Write to silver
    target_table = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.geo"
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
    
    print(f"Geo transformed: {df.count()} rows")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{SILVER_SCHEMA}")
transform_geo()
