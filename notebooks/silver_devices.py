# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Devices Transformation
# MAGIC Business transformations for device ecosystem analysis and user experience optimization

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, lower, coalesce, when, current_timestamp, current_date
    , lit, split, regexp_replace, concat_ws
)
from config import TARGET_CATALOG, BRONZE_SCHEMA, SILVER_SCHEMA

# COMMAND ----------

def transform_devices():
    """
    Transform devices with business logic:
    - Device ecosystem classification
    - OS and browser version compatibility assessment
    - Screen resolution categorization for content delivery
    - User agent parsing for device intelligence
    - Mobile vs desktop segmentation
    - Browser capability scoring
    """

    df = spark.table(f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.devices")

    # Standardize column names
    df = df.select([col(c).alias(c.lower()) for c in df.columns])

    # Trim string columns
    string_cols = ['user_agent', 'device_type', 'device_model', 'operating_system',
                   'os_version', 'browser', 'browser_version', 'screen_resolution']
    for col_name in string_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, trim(col(col_name)))

    # Device ecosystem classification
    df = df.withColumn("device_ecosystem",
        when(upper(col("operating_system")).contains("WINDOWS"), "Windows")
            .when(upper(col("operating_system")).contains("MAC"), "macOS")
            .when(upper(col("operating_system")).contains("LINUX"), "Linux")
            .when(upper(col("operating_system")).contains("IOS"), "iOS")
            .when(upper(col("operating_system")).contains("ANDROID"), "Android")
            .otherwise("Other")
    )

    # Device form factor classification
    df = df.withColumn("device_form_factor",
        when(col("is_mobile") == True, "Mobile")
            .when(upper(col("device_type")).contains("TABLET"), "Tablet")
            .when(upper(col("device_type")).contains("TV"), "SmartTV")
            .otherwise("Desktop")
    )

    # OS version compatibility assessment
    df = df.withColumn("os_support_status",
        when(col("os_version").isNull(), "Unknown")
            .when(col("os_version") >= "10", "Current")
            .when(col("os_version") >= "8", "Supported")
            .when(col("os_version") >= "6", "Legacy")
            .otherwise("Outdated")
    )

    # Browser capability scoring
    df = df.withColumn("browser_capability_score",
        when(upper(col("browser")).contains("CHROME"), 5)
            .when(upper(col("browser")).contains("FIREFOX"), 4)
            .when(upper(col("browser")).contains("SAFARI"), 4)
            .when(upper(col("browser")).contains("EDGE"), 4)
            .when(upper(col("browser")).contains("OPERA"), 3)
            .otherwise(2)
    )

    # Browser version support status
    df = df.withColumn("browser_support_status",
        when(col("browser_version").isNull(), "Unknown")
            .when(col("browser_version") >= "90", "Latest")
            .when(col("browser_version") >= "80", "Current")
            .when(col("browser_version") >= "70", "Supported")
            .otherwise("Outdated")
    )

    # Screen resolution categorization for adaptive streaming
    df = df.withColumn("screen_resolution_category",
        when(upper(col("screen_resolution")).contains("1920X1080"), "FullHD")
            .when(upper(col("screen_resolution")).contains("1280X720"), "HD")
            .when(upper(col("screen_resolution")).contains("854X480"), "SD")
            .when(upper(col("screen_resolution")).contains("640X360"), "Mobile")
            .otherwise("Other")
    )

    # Device capability tier for content delivery optimization
    df = df.withColumn("device_capability_tier",
        when((col("browser_capability_score") >= 4) & (col("os_support_status") == "Current"), "Premium")
            .when((col("browser_capability_score") >= 3) & (col("os_support_status").isin("Current", "Supported")), "Standard")
            .when((col("browser_capability_score") >= 2) & (col("os_support_status").isin("Supported", "Legacy")), "Basic")
            .otherwise("Limited")
    )

    # Mobile optimization flag
    df = df.withColumn("requires_mobile_optimization",
        when((col("is_mobile") == True) | (col("device_form_factor") == "Tablet"), True).otherwise(False)
    )

    # User agent intelligence for bot detection
    df = df.withColumn("is_bot_likely",
        when(upper(col("user_agent")).contains("BOT") |
             upper(col("user_agent")).contains("CRAWLER") |
             upper(col("user_agent")).contains("SPIDER"), True).otherwise(False)
    )

    # Remove duplicates
    df = df.dropDuplicates(["device_id"])

    # Add transformation metadata
    df = df.withColumn("transformed_timestamp", current_timestamp()) \
           .withColumn("transformed_date", current_date())

    # Write to silver
    target_table = f"{TARGET_CATALOG}.{SILVER_SCHEMA}.devices"
    df.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)

    print(f"Devices transformed: {df.count()} rows")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{SILVER_SCHEMA}")
transform_devices()
