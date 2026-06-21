# Databricks notebook source
# ============================================================
# 01_orders_stage_load.py
# Reads raw orders CSV → validates → writes to orders_stage
# ============================================================

# COMMAND ----------
# %md
# ## Orders Data Stage Load
# Reads orders data from the Unity Catalog Volume, validates it,
# writes valid records to the staging Delta table, and archives source files.

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/<your-repo>/ecomm-etl-pipeline")  # update path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType,
    DecimalType, TimestampType
)
from datetime import datetime
import json

from configs.pipeline_config import PATHS, TABLES, CSV_READ_OPTIONS
from utils.pipeline_utils import archive_csv_files, write_valid_and_errors, log_summary

# COMMAND ----------
# ---- Configuration ------------------------------------------
source_dir  = PATHS["orders"]["source"]
archive_dir = PATHS["orders"]["archive"]
stage_table = TABLES["orders_stage"]
error_table = TABLES["orders_errors"]

print(f"Source  : {source_dir}")
print(f"Staging : {stage_table}")

# COMMAND ----------
# ---- Schema Definition --------------------------------------
orders_schema = StructType([
    StructField("order_id",          StringType(),      False),
    StructField("customer_id",       StringType(),      False),
    StructField("product_id",        StringType(),      False),
    StructField("order_date",        DateType(),        False),
    StructField("order_amount",      DecimalType(10,2), False),
    StructField("currency",          StringType(),      False),
    StructField("payment_method",    StringType(),      False),
    StructField("shipping_address",  StringType(),      False),
    StructField("order_status",      StringType(),      False),
    StructField("created_timestamp", TimestampType(),   False),
])

# COMMAND ----------
# ---- Read & Validate ----------------------------------------
try:
    df_orders = (spark.read
                 .schema(orders_schema)
                 .csv(source_dir, **CSV_READ_OPTIONS))

    # Add processing metadata columns
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_orders = (df_orders
                 .withColumn("processed_timestamp", F.current_timestamp())
                 .withColumn("batch_id",     F.lit(batch_id))
                 .withColumn("source_system", F.lit("ecommerce_orders")))

    # --- Data quality metrics (single pass using cache) ---
    df_orders.cache()
    total_records       = df_orders.count()
    null_order_ids      = df_orders.filter(F.col("order_id").isNull()).count()
    null_customer_ids   = df_orders.filter(F.col("customer_id").isNull()).count()
    invalid_amounts     = df_orders.filter(F.col("order_amount") <= 0).count()

    print(f"Total records     : {total_records}")
    print(f"Null order_id     : {null_order_ids}")
    print(f"Null customer_id  : {null_customer_ids}")
    print(f"Invalid amounts   : {invalid_amounts}")

    # --- Split valid / invalid ---
    valid_filter = (
        F.col("order_id").isNotNull() &
        F.col("customer_id").isNotNull() &
        (F.col("order_amount") > 0)
    )
    df_valid   = df_orders.filter(valid_filter)
    df_invalid = df_orders.filter(~valid_filter)

    valid_records   = df_valid.count()
    invalid_records = df_invalid.count()

    print(f"Valid records     : {valid_records}")
    print(f"Invalid records   : {invalid_records}")

except Exception as e:
    print(f"Error reading orders data: {e}")
    raise

# COMMAND ----------
# ---- Write to Delta -----------------------------------------
try:
    write_valid_and_errors(df_valid, df_invalid,
                           stage_table, error_table,
                           entity="orders", invalid_count=invalid_records)
except Exception as e:
    print(f"Error writing to Delta: {e}")
    raise

# COMMAND ----------
# ---- Archive Source Files -----------------------------------
try:
    archived_count = archive_csv_files(source_dir, archive_dir, dbutils)
except Exception as e:
    print(f"Error archiving files: {e}")
    raise

# COMMAND ----------
# ---- Log Summary --------------------------------------------
log_summary(spark, "orders_stage_load",
            total=total_records, valid=valid_records,
            invalid=invalid_records, archived=archived_count,
            process_log_table=TABLES["process_log"])
