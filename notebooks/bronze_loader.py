# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Loader

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, current_date, lit, when, count, isnull
from config import SRC_CATALOG, SRC_SCHEMA, TARGET_CATALOG, BRONZE_SCHEMA, VOLUME_PATH, DATA_SOURCES, BRONZE_COLUMNS

# COMMAND ----------

def calculate_quality_score(df):
    """Calculate data quality score based on null values"""
    total_rows = df.count()
    if total_rows == 0:
        return 0.0
    
    null_counts = df.select([count(when(isnull(col(c)), 1)).alias(c) for c in df.columns])
    null_row = null_counts.collect()[0]
    
    total_cells = total_rows * len(df.columns)
    null_cells = sum([null_row[c] for c in df.columns])
    
    return round((total_cells - null_cells) / total_cells, 4) if total_cells > 0 else 0.0

# COMMAND ----------

def load_csv_to_bronze(csv_file, table_name):
    """Load CSV file to bronze table with quality score"""
    try:
        file_path = f"{VOLUME_PATH}{csv_file}"
        
        # Read CSV
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        
        # Calculate quality score
        quality_score = calculate_quality_score(df)
        
        # Add bronze columns
        df_bronze = df.withColumn("load_timestamp", current_timestamp()) \
                      .withColumn("load_date", current_date()) \
                      .withColumn("data_quality_score", lit(quality_score))
        
        # Write to delta table
        target_table = f"{TARGET_CATALOG}.{BRONZE_SCHEMA}.{table_name}"
        df_bronze.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(target_table)
        
        print(f"Loaded {table_name}: {df_bronze.count()} rows, Quality Score: {quality_score}")
        return True
        
    except Exception as e:
        print(f"Error loading {csv_file}: {str(e)}")
        raise

# COMMAND ----------

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{BRONZE_SCHEMA}")

# Load all data sources
for csv_file, table_name in DATA_SOURCES.items():
    load_csv_to_bronze(csv_file, table_name)

print("Bronze layer ingestion completed")
