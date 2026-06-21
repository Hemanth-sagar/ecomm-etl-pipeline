# Databricks notebook source
# ============================================================
# 04_inventory_stage_load.py
# Reads raw inventory CSV → validates → enriches → inventory_stage
# ============================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/<your-repo>/ecomm-etl-pipeline")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, IntegerType, TimestampType
)
from datetime import datetime

from configs.pipeline_config import PATHS, TABLES, CSV_READ_OPTIONS
from utils.pipeline_utils import archive_csv_files, write_valid_and_errors, log_summary

# COMMAND ----------
source_dir  = PATHS["inventory"]["source"]
archive_dir = PATHS["inventory"]["archive"]
stage_table = TABLES["inventory_stage"]
error_table = TABLES["inventory_errors"]

# COMMAND ----------
inventory_schema = StructType([
    StructField("inventory_id",       StringType(),  False),
    StructField("product_id",         StringType(),  False),
    StructField("warehouse_id",       StringType(),  False),
    StructField("warehouse_name",     StringType(),  False),
    StructField("location",           StringType(),  False),
    StructField("stock_quantity",     IntegerType(), False),
    StructField("reserved_quantity",  IntegerType(), False),
    StructField("available_quantity", IntegerType(), False),
    StructField("reorder_level",      IntegerType(), False),
    StructField("last_restocked",     DateType(),    False),
    StructField("last_audit",         DateType(),    False),
    StructField("created_timestamp",  TimestampType(), False),
])

# COMMAND ----------
try:
    df_inventory = (spark.read.schema(inventory_schema).csv(source_dir, **CSV_READ_OPTIONS))
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_inventory = (df_inventory
                    .withColumn("processed_timestamp", F.current_timestamp())
                    .withColumn("batch_id",     F.lit(batch_id))
                    .withColumn("source_system", F.lit("ecommerce_inventory")))

    df_inventory.cache()
    total_records     = df_inventory.count()
    null_inv_ids      = df_inventory.filter(F.col("inventory_id").isNull()).count()
    negative_stock    = df_inventory.filter(F.col("stock_quantity") < 0).count()
    negative_reserved = df_inventory.filter(F.col("reserved_quantity") < 0).count()
    invalid_available = df_inventory.filter(F.col("available_quantity") < 0).count()

    print(f"Total: {total_records} | Null IDs: {null_inv_ids} | Neg stock: {negative_stock} | Neg reserved: {negative_reserved} | Invalid avail: {invalid_available}")

    valid_filter = (
        F.col("inventory_id").isNotNull() &
        (F.col("stock_quantity") >= 0) &
        (F.col("reserved_quantity") >= 0) &
        (F.col("available_quantity") >= 0)
    )
    df_valid   = df_inventory.filter(valid_filter)
    df_invalid = df_inventory.filter(~valid_filter)
    valid_records   = df_valid.count()
    invalid_records = df_invalid.count()

except Exception as e:
    print(f"Error: {e}"); raise

# COMMAND ----------
try:
    df_valid = (df_valid
        .withColumn("stock_utilization_rate",
            F.when(F.col("stock_quantity") > 0,
                   F.col("reserved_quantity") / F.col("stock_quantity"))
             .otherwise(F.lit(0)))
        .withColumn("stock_status",
            F.when(F.col("available_quantity") == 0,                              "Out of Stock")
             .when(F.col("available_quantity") <= F.col("reorder_level"),         "Reorder Required")
             .when(F.col("available_quantity") <= F.col("reorder_level") * 2,     "Low Stock")
             .otherwise("In Stock"))
        .withColumn("days_since_restock", F.datediff(F.current_date(), F.col("last_restocked")))
        .withColumn("days_since_audit",   F.datediff(F.current_date(), F.col("last_audit")))
        .withColumn("audit_status",
            F.when(F.col("days_since_audit") > 90, "Overdue")
             .when(F.col("days_since_audit") > 60, "Due Soon")
             .otherwise("Current"))
    )
    print("Inventory enrichment complete.")
except Exception as e:
    print(f"Error in enrichment: {e}"); raise

# COMMAND ----------
try:
    write_valid_and_errors(df_valid, df_invalid, stage_table, error_table, "inventory", invalid_records)
except Exception as e:
    print(f"Error writing: {e}"); raise

# COMMAND ----------
try:
    archived_count = archive_csv_files(source_dir, archive_dir, dbutils)
except Exception as e:
    print(f"Error archiving: {e}"); raise

# COMMAND ----------
log_summary(spark, "inventory_stage_load", total_records, valid_records, invalid_records, archived_count, TABLES["process_log"])
