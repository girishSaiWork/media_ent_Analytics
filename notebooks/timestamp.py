from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, when, regexp_extract
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType

# ─────────────────────────────────────────────────────────────────────────────
# UTC Timestamp Validation in PySpark
# ─────────────────────────────────────────────────────────────────────────────
#
# PROBLEM:
#   - Raw Kafka data arrives as:  2026-02-15T14:30:00Z
#   - After PySpark schema read:  2026-02-15T14:30:00.000+00:00  (auto-converted)
#
# GOAL:
#   - Validate that the timestamp string represents a VALID UTC timestamp.
#   - A valid UTC timestamp must have either:
#       a) A 'Z' suffix               (e.g., 2026-02-15T14:30:00Z)
#       b) A '+00:00' UTC offset      (e.g., 2026-02-15T14:30:00.000+00:00)
#   - A timestamp with a non-UTC offset (e.g., +05:30) is NOT valid UTC.
#
# STRATEGY:
#   Step 1: Parse the string into a Timestamp using coalesce across known formats.
#   Step 2: Check that the original string has a UTC indicator (Z or +00:00).
#           This ensures a non-UTC offset like +05:30 is correctly rejected.
# ─────────────────────────────────────────────────────────────────────────────


def is_valid_utc_timestamp(df, col_name: str, output_col: str = "is_utc_valid"):
    """
    Validates whether a string column contains a valid UTC timestamp.

    A timestamp is considered valid UTC if:
      1. It can be parsed into a Timestamp type (structurally valid).
      2. The original string explicitly carries a UTC indicator:
         - Ends with 'Z'           e.g., 2026-02-15T14:30:00Z
         - Contains '+00:00'       e.g., 2026-02-15T14:30:00.000+00:00
         - Contains '-00:00'       e.g., (edge case) 

    Args:
        df:         Input DataFrame
        col_name:   Name of the string column holding the timestamp value
        output_col: Name of the new boolean column (True = valid UTC)

    Returns:
        DataFrame with an added boolean column `output_col`
    """

    # ── Step 1: Try to parse across known formats ────────────────────────────
    # PySpark's to_timestamp returns NULL if the format doesn't match.
    # coalesce picks the first non-null result.
    parsed_ts = coalesce(
        # Format A: After PySpark schema parsing  → 2026-02-15T14:30:00.000+00:00
        to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),

        # Format B: High-precision microseconds   → 2026-02-15T23:53:16.716804+00:00
        to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX"),

        # Format C: Raw Kafka with Z suffix       → 2026-02-15T14:30:00Z
        to_timestamp(col(col_name), "yyyy-MM-dd'T'HH:mm:ss'Z'"),

        # Format D: Local / no timezone           → 2026-02-15 18:53:38
        to_timestamp(col(col_name)),
    )

    # ── Step 2: Check UTC indicator in the original string ───────────────────
    # Even if a timestamp parses OK, we reject non-UTC offsets like +05:30.
    utc_pattern = r"(Z$|\+00:00$|-00:00$)"
    has_utc_indicator = regexp_extract(col(col_name), utc_pattern, 0) != ""

    # ── Step 3: Combine both conditions ────────────────────────────────────
    return df.withColumn(
        output_col,
        when(parsed_ts.isNotNull() & has_utc_indicator, lit(True)).otherwise(lit(False))
    )


# ─────────────────────────────────────────────────────────────────────────────
# QUICK TEST (Run this in Databricks notebook / local Spark)
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    spark = SparkSession.builder.appName("UTCTimestampValidation").getOrCreate()

    # Sample data covering all the formats you encounter
    test_data = [
        ("2026-02-15T14:30:00Z",),               # ✅ Raw Kafka - UTC with Z
        ("2026-02-15T14:30:00.000+00:00",),       # ✅ After PySpark schema read
        ("2026-02-15T23:53:16.716804+00:00",),    # ✅ High-precision UTC
        ("2026-02-15 18:53:38",),                 # ❌ Local timestamp - No UTC indicator
        ("2026-02-15T14:30:00+05:30",),           # ❌ IST offset - NOT UTC
        ("not-a-timestamp",),                     # ❌ Malformed
        (None,),                                  # ❌ Null value
    ]

    test_df = spark.createDataFrame(test_data, ["event_timestamp"])

    result_df = is_valid_utc_timestamp(test_df, col_name="event_timestamp")
    result_df.show(truncate=False)

    # ── Expected Output ──────────────────────────────────────────────────────
    # +--------------------------------+-------------+
    # |event_timestamp                 |is_utc_valid |
    # +--------------------------------+-------------+
    # |2026-02-15T14:30:00Z            |true         |  ✅
    # |2026-02-15T14:30:00.000+00:00   |true         |  ✅
    # |2026-02-15T23:53:16.716804+00:00|true         |  ✅
    # |2026-02-15 18:53:38             |false        |  ❌ no UTC indicator
    # |2026-02-15T14:30:00+05:30       |false        |  ❌ not UTC offset
    # |not-a-timestamp                 |false        |  ❌ can't parse
    # |null                            |false        |  ❌ null
    # +--------------------------------+-------------+
