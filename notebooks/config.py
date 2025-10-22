# Configuration for Bronze Layer Ingestion

# Catalog and Schema Configuration
SRC_CATALOG = "media_ent_catalog"
SRC_SCHEMA = "media_schema"
TARGET_CATALOG = "media_ent_catalog"
BRONZE_SCHEMA = "bronze_schema"
SILVER_SCHEMA = "silver_schema"
GOLD_SCHEMA = "gold_schema"

# Volume Path
VOLUME_PATH = "/Volumes/media_ent_catalog/media_schema/video_asset_data/raw_csv/"

# Data Source Mapping - Maps CSV files to table names
DATA_SOURCES = {
    "assets.csv": "assets",
    "delivery.csv": "delivery",
    "devices.csv": "devices",
    "geo.csv": "geo",
    "metrics.csv": "metrics"
}

# Bronze Layer Columns
BRONZE_COLUMNS = {
    "load_timestamp": "timestamp",
    "load_date": "date",
    "data_quality_score": "double"
}

