# Databricks notebook source
# ============================================================
# 03_products_stage_load.py
# Reads raw products CSV → validates → enriches → products_stage
# ============================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/<your-repo>/ecomm-etl-pipeline")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DecimalType,
    IntegerType, BooleanType, TimestampType
)
from datetime import datetime

from configs.pipeline_config import PATHS, TABLES, CSV_READ_OPTIONS
from utils.pipeline_utils import archive_csv_files, write_valid_and_errors, log_summary

# COMMAND ----------
source_dir  = PATHS["products"]["source"]
archive_dir = PATHS["products"]["archive"]
stage_table = TABLES["products_stage"]
error_table = TABLES["products_errors"]

# COMMAND ----------
products_schema = StructType([
    StructField("product_id",         StringType(),      False),
    StructField("product_name",       StringType(),      False),
    StructField("category",           StringType(),      False),
    StructField("subcategory",        StringType(),      False),
    StructField("brand",              StringType(),      False),
    StructField("price",              DecimalType(10,2), False),
    StructField("currency",           StringType(),      False),
    StructField("stock_quantity",     IntegerType(),     False),
    StructField("weight_kg",          DecimalType(8,2),  False),
    StructField("dimensions_cm",      StringType(),      False),
    StructField("color",              StringType(),      False),
    StructField("material",           StringType(),      False),
    StructField("description",        StringType(),      False),
    StructField("launch_date",        DateType(),        False),
    StructField("discontinued",       BooleanType(),     False),
    StructField("created_timestamp",  TimestampType(),   False),
])

# COMMAND ----------
try:
    df_products = (spark.read.schema(products_schema).csv(source_dir, **CSV_READ_OPTIONS))
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_products = (df_products
                   .withColumn("processed_timestamp", F.current_timestamp())
                   .withColumn("batch_id",     F.lit(batch_id))
                   .withColumn("source_system", F.lit("ecommerce_products")))

    df_products.cache()
    total_records       = df_products.count()
    null_product_ids    = df_products.filter(F.col("product_id").isNull()).count()
    invalid_prices      = df_products.filter(F.col("price") <= 0).count()
    negative_stock      = df_products.filter(F.col("stock_quantity") < 0).count()
    future_launch_dates = df_products.filter(F.col("launch_date") > F.current_date()).count()

    print(f"Total: {total_records} | Null IDs: {null_product_ids} | Bad price: {invalid_prices} | Neg stock: {negative_stock} | Future launch: {future_launch_dates}")

    valid_filter = (
        F.col("product_id").isNotNull() &
        (F.col("price") > 0) &
        (F.col("stock_quantity") >= 0) &
        (F.col("launch_date") <= F.current_date())
    )
    df_valid   = df_products.filter(valid_filter)
    df_invalid = df_products.filter(~valid_filter)

    valid_records   = df_valid.count()
    invalid_records = df_invalid.count()

except Exception as e:
    print(f"Error: {e}"); raise

# COMMAND ----------
# ---- Enrichment: price segment, stock status, lifecycle, volume/density ---
try:
    df_valid = (df_valid
        .withColumn("price_segment",
            F.when(F.col("price") < 50,  "Budget")
             .when(F.col("price") < 150, "Mid-range")
             .when(F.col("price") < 300, "Premium")
             .otherwise("Luxury"))
        .withColumn("stock_status",
            F.when(F.col("stock_quantity") == 0,  "Out of Stock")
             .when(F.col("stock_quantity") < 10,  "Low Stock")
             .when(F.col("stock_quantity") < 50,  "Medium Stock")
             .otherwise("High Stock"))
        .withColumn("days_since_launch", F.datediff(F.current_date(), F.col("launch_date")))
        .withColumn("lifecycle_stage",
            F.when(F.col("days_since_launch") < 30,  "New")
             .when(F.col("days_since_launch") < 365, "Growth")
             .when(F.col("discontinued") == True,    "Discontinued")
             .otherwise("Mature"))
        .withColumn("dimensions_array", F.split(F.col("dimensions_cm"), "x"))
        .withColumn("volume_cm3",
            F.when(F.size("dimensions_array") == 3,
                   F.col("dimensions_array")[0].cast("double") *
                   F.col("dimensions_array")[1].cast("double") *
                   F.col("dimensions_array")[2].cast("double"))
             .otherwise(F.lit(0)))
        .withColumn("density_kg_cm3",
            F.when(F.col("volume_cm3") > 0, F.col("weight_kg") / F.col("volume_cm3"))
             .otherwise(F.lit(0)))
    )
    print("Products enrichment complete.")
except Exception as e:
    print(f"Error in enrichment: {e}"); raise

# COMMAND ----------
try:
    write_valid_and_errors(df_valid, df_invalid, stage_table, error_table, "products", invalid_records)
except Exception as e:
    print(f"Error writing: {e}"); raise

# COMMAND ----------
try:
    archived_count = archive_csv_files(source_dir, archive_dir, dbutils)
except Exception as e:
    print(f"Error archiving: {e}"); raise

# COMMAND ----------
log_summary(spark, "products_stage_load", total_records, valid_records, invalid_records, archived_count, TABLES["process_log"])
