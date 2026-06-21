# Databricks notebook source
# MAGIC %md
# MAGIC # Inventory Data Stage Load
# MAGIC This notebook processes inventory data from source files and loads it into the staging table.

# COMMAND ----------

# Configuration
source_dir = "/Volumes/accenture/hemanth/incremental_load/inventory_data/source/"
archive_dir = "/Volumes/accenture/hemanth/incremental_load/inventory_data/archive/"
stage_table = "`accenture`.hemanth.inventory_stage"
error_table = "`accenture`.hemanth.inventory_errors"

print(f"Processing inventory data from: {source_dir}")
print(f"Staging table: {stage_table}")

# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import json

# Define schema for inventory data
inventory_schema = StructType([
    StructField("inventory_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("warehouse_id", StringType(), False),
    StructField("warehouse_name", StringType(), False),
    StructField("location", StringType(), False),
    StructField("stock_quantity", IntegerType(), False),
    StructField("reserved_quantity", IntegerType(), False),
    StructField("available_quantity", IntegerType(), False),
    StructField("reorder_level", IntegerType(), False),
    StructField("last_restocked", DateType(), False),
    StructField("last_audit", DateType(), False),
    StructField("created_timestamp", TimestampType(), False)
])

print("Schema defined for inventory data")


# COMMAND ----------

# Read and validate inventory data
try:
    # Read CSV files with schema validation
    df_inventory = spark.read.schema(inventory_schema).csv(source_dir, header=True, dateFormat="yyyy-MM-dd", timestampFormat="yyyy-MM-dd HH:mm:ss")
    
    # Add processing metadata
    df_inventory = df_inventory.withColumn("processed_timestamp", F.current_timestamp()) \
                              .withColumn("batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
                              .withColumn("source_system", F.lit("ecommerce_inventory"))
    
    # Data quality checks
    total_records = df_inventory.count()
    null_inventory_ids = df_inventory.filter(F.col("inventory_id").isNull()).count()
    negative_stock = df_inventory.filter(F.col("stock_quantity") < 0).count()
    negative_reserved = df_inventory.filter(F.col("reserved_quantity") < 0).count()
    invalid_available = df_inventory.filter(F.col("available_quantity") < 0).count()
    
    print(f"Total records processed: {total_records}")
    print(f"Records with null inventory_id: {null_inventory_ids}")
    print(f"Records with negative stock: {negative_stock}")
    print(f"Records with negative reserved: {negative_reserved}")
    print(f"Records with invalid available: {invalid_available}")
    
    # Filter out invalid records - Fixed boolean logic
    df_valid_inventory = df_inventory.filter(
        (F.col("inventory_id").isNotNull()) & 
        (F.col("stock_quantity") >= 0) & 
        (F.col("reserved_quantity") >= 0) & 
        (F.col("available_quantity") >= 0)
    )
    
    # Capture invalid records for error handling - Fixed boolean logic
    df_invalid_inventory = df_inventory.filter(
        (F.col("inventory_id").isNull()) | 
        (F.col("stock_quantity") < 0) | 
        (F.col("reserved_quantity") < 0) | 
        (F.col("available_quantity") < 0)
    )
    
    valid_records = df_valid_inventory.count()
    invalid_records = df_invalid_inventory.count()
    
    print(f"Valid records: {valid_records}")
    print(f"Invalid records: {invalid_records}")
    
except Exception as e:
    print(f"Error reading inventory data: {str(e)}")
    raise


# COMMAND ----------

# Data enrichment - Inventory analytics
try:
    # Calculate inventory turnover metrics
    df_valid_inventory = df_valid_inventory.withColumn(
        "stock_utilization_rate",
        F.when(F.col("stock_quantity") > 0, F.col("reserved_quantity")/F.col("stock_quantity"))
         .otherwise(F.lit(0))
    )
    
    # Create stock status categories
    df_valid_inventory = df_valid_inventory.withColumn(
        "stock_status",
        F.when(F.col("available_quantity") == 0, "Out of Stock")
         .when(F.col("available_quantity") <= F.col("reorder_level"), "Reorder Required")
         .when(F.col("available_quantity") <= F.col("reorder_level") * 2, "Low Stock")
         .otherwise("In Stock")
    )


    # Calculate days since last restock
    df_valid_inventory = df_valid_inventory.withColumn(
        "days_since_restock",
        F.datediff(F.current_date(), F.col("last_restocked"))
    )

    # Calculate days since last audit
    df_valid_inventory = df_valid_inventory.withColumn(
        "days_since_audit",
        F.datediff(F.current_date(), F.col("last_audit"))
    )

    # Create audit status
    df_valid_inventory = df_valid_inventory.withColumn(
        "audit_status",
        F.when(F.col("days_since_audit") > 90, "Overdue")
         .when(F.col("days_since_audit") > 60, "Due Soon")
         .otherwise("Current")
    )

    print("Data enrichment completed")

except Exception as e:
    print(F"Error in data enrichment: {str(e)}")
    raise

# COMMAND ----------

# Write valid data to staging table
try:
    # Create or overwrite staging table
    df_valid_inventory.write.format("delta").mode("overwrite").saveAsTable(stage_table)
    print(f"Successfully loaded {valid_records} valid inventory records to staging table")
    
    # Write invalid records to error table for investigation
    if invalid_records > 0:
        df_invalid_inventory.withColumn("error_reason", F.lit("Data quality validation failed")) \
                           .withColumn("error_timestamp", F.current_timestamp()) \
                           .write.format("delta").mode("append").saveAsTable(error_table)
        print(f"Logged {invalid_records} invalid records to error table")
    
except Exception as e:
    print(f"Error writing to staging table: {str(e)}")
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
    "task": "inventory_stage_load",
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

