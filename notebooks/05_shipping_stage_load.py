# Databricks notebook source
# ============================================================
# 05_shipping_stage_load.py
# Reads raw shipping CSV → validates → enriches → shipping_stage
# ============================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/<your-repo>/ecomm-etl-pipeline")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DecimalType, TimestampType
)
from datetime import datetime

from configs.pipeline_config import PATHS, TABLES, CSV_READ_OPTIONS
from utils.pipeline_utils import archive_csv_files, write_valid_and_errors, log_summary

# COMMAND ----------
source_dir  = PATHS["shipping"]["source"]
archive_dir = PATHS["shipping"]["archive"]
stage_table = TABLES["shipping_stage"]
error_table = TABLES["shipping_errors"]

# COMMAND ----------
shipping_schema = StructType([
    StructField("shipping_id",          StringType(),      False),
    StructField("order_id",             StringType(),      False),
    StructField("tracking_number",      StringType(),      False),
    StructField("carrier",              StringType(),      False),
    StructField("service_type",         StringType(),      False),
    StructField("origin_warehouse",     StringType(),      False),
    StructField("destination_address",  StringType(),      False),
    StructField("shipping_cost",        DecimalType(10,2), False),
    StructField("currency",             StringType(),      False),
    StructField("estimated_delivery",   DateType(),        False),
    StructField("actual_delivery",      DateType(),        True),   # nullable
    StructField("shipping_status",      StringType(),      False),
    StructField("package_weight",       DecimalType(8,2),  False),
    StructField("package_dimensions",   StringType(),      False),
    StructField("insurance_value",      DecimalType(10,2), False),
    StructField("created_timestamp",    TimestampType(),   False),
])

# COMMAND ----------
try:
    df_shipping = (spark.read.schema(shipping_schema).csv(source_dir, **CSV_READ_OPTIONS))
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_shipping = (df_shipping
                   .withColumn("processed_timestamp", F.current_timestamp())
                   .withColumn("batch_id",     F.lit(batch_id))
                   .withColumn("source_system", F.lit("ecommerce_shipping")))

    df_shipping.cache()
    total_records    = df_shipping.count()
    null_ship_ids    = df_shipping.filter(F.col("shipping_id").isNull()).count()
    invalid_costs    = df_shipping.filter(F.col("shipping_cost") < 0).count()
    negative_weights = df_shipping.filter(F.col("package_weight") <= 0).count()
    invalid_insur    = df_shipping.filter(F.col("insurance_value") < 0).count()

    print(f"Total: {total_records} | Null IDs: {null_ship_ids} | Bad cost: {invalid_costs} | Neg weight: {negative_weights} | Bad insurance: {invalid_insur}")

    valid_filter = (
        F.col("shipping_id").isNotNull() &
        (F.col("shipping_cost") >= 0) &
        (F.col("package_weight") > 0) &
        (F.col("insurance_value") >= 0)
    )
    df_valid   = df_shipping.filter(valid_filter)
    df_invalid = df_shipping.filter(~valid_filter)
    valid_records   = df_valid.count()
    invalid_records = df_invalid.count()

except Exception as e:
    print(f"Error: {e}"); raise

# COMMAND ----------
try:
    order_date_col = F.col("created_timestamp").cast("date")

    df_valid = (df_valid
        .withColumn("delivery_days",
            F.when(F.col("actual_delivery").isNotNull(),
                   F.datediff(F.col("actual_delivery"), order_date_col))
             .otherwise(F.lit(None)))
        .withColumn("estimated_delivery_days",
            F.datediff(F.col("estimated_delivery"), order_date_col))
        .withColumn("delivery_performance",
            F.when(F.col("actual_delivery").isNull(), "Pending")
             .when(F.col("delivery_days") <= F.col("estimated_delivery_days"), "On Time")
             .when(F.col("delivery_days") <= F.col("estimated_delivery_days") + 1, "Slightly Delayed")
             .otherwise("Delayed"))
        .withColumn("cost_category",
            F.when(F.col("shipping_cost") < 10, "Low Cost")
             .when(F.col("shipping_cost") < 20, "Medium Cost")
             .otherwise("High Cost"))
        .withColumn("cost_per_kg", F.col("shipping_cost") / F.col("package_weight"))
    )
    print("Shipping enrichment complete.")
except Exception as e:
    print(f"Error in enrichment: {e}"); raise

# COMMAND ----------
try:
    write_valid_and_errors(df_valid, df_invalid, stage_table, error_table, "shipping", invalid_records)
except Exception as e:
    print(f"Error writing: {e}"); raise

# COMMAND ----------
try:
    archived_count = archive_csv_files(source_dir, archive_dir, dbutils)
except Exception as e:
    print(f"Error archiving: {e}"); raise

# COMMAND ----------
log_summary(spark, "shipping_stage_load", total_records, valid_records, invalid_records, archived_count, TABLES["process_log"])
