# Databricks notebook source
# ============================================================
# 02_customers_stage_load.py
# Reads raw customers CSV → validates → enriches → writes to customers_stage
# ============================================================

# COMMAND ----------
# %md
# ## Customers Data Stage Load
# Reads customer data, applies validation rules, enriches with
# age/segment/lifecycle columns, and writes to the staging Delta table.

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/<your-repo>/ecomm-etl-pipeline")

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, TimestampType
)
from datetime import datetime

from configs.pipeline_config import PATHS, TABLES, CSV_READ_OPTIONS
from utils.pipeline_utils import archive_csv_files, write_valid_and_errors, log_summary

# COMMAND ----------
source_dir  = PATHS["customers"]["source"]
archive_dir = PATHS["customers"]["archive"]
stage_table = TABLES["customers_stage"]
error_table = TABLES["customers_errors"]

print(f"Source  : {source_dir}")
print(f"Staging : {stage_table}")

# COMMAND ----------
customers_schema = StructType([
    StructField("customer_id",         StringType(),  False),
    StructField("first_name",          StringType(),  False),
    StructField("last_name",           StringType(),  False),
    StructField("email",               StringType(),  False),
    StructField("phone",               StringType(),  False),
    StructField("date_of_birth",       DateType(),    False),
    StructField("registration_date",   DateType(),    False),
    StructField("address",             StringType(),  False),
    StructField("city",                StringType(),  False),
    StructField("state",               StringType(),  False),
    StructField("zip_code",            StringType(),  False),
    StructField("country",             StringType(),  False),
    StructField("customer_tier",       StringType(),  False),
    StructField("last_login",          TimestampType(), False),
    StructField("created_timestamp",   TimestampType(), False),
])

# COMMAND ----------
# ---- Read & Validate ----------------------------------------
try:
    df_customers = (spark.read
                    .schema(customers_schema)
                    .csv(source_dir, **CSV_READ_OPTIONS))

    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    df_customers = (df_customers
                    .withColumn("processed_timestamp", F.current_timestamp())
                    .withColumn("batch_id",     F.lit(batch_id))
                    .withColumn("source_system", F.lit("ecommerce_customers")))

    df_customers.cache()
    total_records      = df_customers.count()
    null_customer_ids  = df_customers.filter(F.col("customer_id").isNull()).count()
    null_emails        = df_customers.filter(F.col("email").isNull()).count()
    future_birth_dates = df_customers.filter(F.col("date_of_birth") > F.current_date()).count()

    # Email format: must contain "@" and "."
    invalid_emails = df_customers.filter(
        F.col("email").isNotNull() &
        (~F.col("email").contains("@") | ~F.col("email").contains("."))
    ).count()

    # Phone format: must contain "-" and be 12 chars (e.g. 123-456-7890)
    invalid_phones = df_customers.filter(
        F.col("phone").isNotNull() &
        (~F.col("phone").contains("-") | (F.length("phone") != 12))
    ).count()

    print(f"Total            : {total_records}")
    print(f"Null customer_id : {null_customer_ids}")
    print(f"Null email       : {null_emails}")
    print(f"Invalid emails   : {invalid_emails}")
    print(f"Invalid phones   : {invalid_phones}")
    print(f"Future DOB       : {future_birth_dates}")

    valid_filter = (
        F.col("customer_id").isNotNull() &
        F.col("email").isNotNull() &
        F.col("phone").isNotNull() &
        F.col("email").contains("@") &
        F.col("email").contains(".") &
        (F.col("date_of_birth") <= F.current_date())
    )
    df_valid   = df_customers.filter(valid_filter)
    df_invalid = df_customers.filter(~valid_filter)

    valid_records   = df_valid.count()
    invalid_records = df_invalid.count()

    print(f"Valid   : {valid_records}")
    print(f"Invalid : {invalid_records}")

except Exception as e:
    print(f"Error reading customers data: {e}")
    raise

# COMMAND ----------
# ---- Data Enrichment ----------------------------------------
try:
    # Age from date of birth
    df_valid = df_valid.withColumn(
        "age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365)
    )

    # Generational segment
    df_valid = df_valid.withColumn(
        "age_segment",
        F.when(F.col("age") < 25, "Gen Z")
         .when(F.col("age") < 40, "Millennial")
         .when(F.col("age") < 55, "Gen X")
         .otherwise("Boomer")
    )

    # Days since registration & lifecycle stage
    df_valid = df_valid.withColumn(
        "days_since_registration",
        F.datediff(F.current_date(), F.col("registration_date"))
    ).withColumn(
        "lifecycle_stage",
        F.when(F.col("days_since_registration") < 30,  "New")
         .when(F.col("days_since_registration") < 365, "Active")
         .otherwise("Established")
    )

    print("Customer enrichment complete.")

except Exception as e:
    print(f"Error in data enrichment: {e}")
    raise

# COMMAND ----------
# ---- Write to Delta -----------------------------------------
try:
    write_valid_and_errors(df_valid, df_invalid,
                           stage_table, error_table,
                           entity="customers", invalid_count=invalid_records)
except Exception as e:
    print(f"Error writing to Delta: {e}")
    raise

# COMMAND ----------
# ---- Archive ------------------------------------------------
try:
    archived_count = archive_csv_files(source_dir, archive_dir, dbutils)
except Exception as e:
    print(f"Error archiving files: {e}")
    raise

# COMMAND ----------
log_summary(spark, "customers_stage_load",
            total=total_records, valid=valid_records,
            invalid=invalid_records, archived=archived_count,
            process_log_table=TABLES["process_log"])
