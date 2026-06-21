# Databricks notebook source
# MAGIC %md
# MAGIC # Orders Data Stage Load
# MAGIC This notebook processes orders data from source files and loads it into the staging table.

# COMMAND ----------

# Configuration
source_dir = "/Volumes/accenture/hemanth/incremental_load/orders_data/source/"
archive_dir = "/Volumes/accenture/hemanth/incremental_load/orders_data/archive/"
stage_table = "`accenture`.hemanth.orders_stage"
error_table = "`accenture`.hemanth.orders_errors"

print(f"Processing orders data from: {source_dir}")
print(f"Staging table: {stage_table}")

# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import json

# Define schema for orders data
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("order_date", DateType(), False),
    StructField("order_amount", DecimalType(10,2), False),
    StructField("currency", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("shipping_address", StringType(), False),
    StructField("order_status", StringType(), False),
    StructField("created_timestamp", TimestampType(), False)
])

print("Schema defined for orders data")

# COMMAND ----------

# Read and validate orders data
try:
    # Read CSV files with Schema validation
    df_orders = spark.read.schema(orders_schema).csv(source_dir, header=True,
                                                      dateFormat="yyyy-MM-dd", timestampFormat="yyyy-MM-dd HH:mm:ss")
    
    # Add processing metadata
    df_orders = df_orders.withColumn("processed_timestamp", F.current_timestamp())\
                        .withColumn("batch_id",F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))\
                        .withColumn("source_system", F.lit("ecommerce_orders"))

    # Data quality checks
    total_records = df_orders.count()
    null_order_ids = df_orders.filter(F.col("order_id").isNull()).count()
    null_customer_ids = df_orders.filter(F.col("customer_id").isNull()).count()
    invalid_amounts = df_orders.filter(F.col("order_amount")<=0).count()

    print(f"Total records processed: {total_records}")
    print(f"Records with null order_id: {null_order_ids}")
    print(f"Records with null customer_id: {null_customer_ids}")
    print(f"Records with invalid amounts: {invalid_amounts}")

    # Filter out valid records - Fixed boolean logic
    df_valid_orders = df_orders.filter(
                                    (F.col("order_id").isNotNull()) &
                                    (F.col("customer_id").isNotNull()) &
                                    (F.col("order_amount") > 0)
                                )
    
    # Capture invalid records for error handling - Fixed boolean logic
    df_invalid_orders = df_orders.filter(
                                    (F.col("order_id").isNull()) |
                                    (F.col("customer_id").isNull()) |
                                    (F.col("order_amount") <= 0)
                                )

    valid_records = df_valid_orders.count()
    invalid_records = df_invalid_orders.count()

    print(f"Valid records: {valid_records}")
    print(f"Invalid records: {invalid_records}")

except Exception as e:
    print(f"Error reading orders data: {str(e)}")
    raise
# here raise once it raise error it makes it fail the pipeline so next cell won't run
    

# COMMAND ----------

# Write valid data to staging table
try:
    # Create or overwrite staging table
    df_valid_orders.write.format("delta").mode("overwrite").saveAsTable(stage_table)
    print(f"Successfully loaded {valid_records} valid orders to staging table")

    # Write invalid data to error table for investigation
    if invalid_records>0:
        df_invalid_orders.withColumn("error_reason", F.lit("Data quality validation failed"))\
                        .withColumn("error_timestamp", F.current_timestamp())\
                        .write.format("delta").mode("append").saveAsTable(error_table)
        print(f"Logged {invalid_records} invalid orders to error table")
        
except Exception as e:
    print("Error writing to staging table: {str(e)}")
    raise

# COMMAND ----------

# Archive processed files
try:
    # List all files in the source directory
    files = dbutils.fs.ls(source_dir)

    archived_count = 0
    for file in files:
        if file.name.endswith(".csv"):
            src_path = file.path
            archive_path = archive_dir + file.name

            # Move the file to archive
            dbutils.fs.mv(src_path, archive_path)
            archived_count+=1
            print(f"Archived: {file.name}")

    print(f"Successfully archived {archived_count} files")

except Exception as e:
    print(f"Error archiving files: {str(e)}")
    raise


# COMMAND ----------

# Log processing summary
processing_summary ={
    "task": "orders_stage_load",
    "timestamp": datetime.now().isoformat(),
    "total_records": total_records,
    "valid_records": valid_records,
    "invalid_records": invalid_records,
    "archived_files": archived_count,
    "status": "SUCCESS" if invalid_records==0 else "SUCCESS_WITH_WARNINGS"
}

print("Processing Summary:")
print(json.dumps(processing_summary, indent=2))

# Store summary in a table for monitoring
summary_df = spark.createDataFrame([processing_summary])
summary_df.write.format("delta").mode("append").saveAsTable("`accenture`.hemanth.process_log")



# COMMAND ----------

