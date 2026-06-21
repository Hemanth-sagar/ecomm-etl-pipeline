# Databricks notebook source
# Configuration
source_dir = "/Volumes/accenture/hemanth/incremental_load/shipping_file/source/"
archive_dir = "/Volumes/accenture/hemanth/incremental_load/shipping_file/archive/"
stage_table = "`accenture`.hemanth.shipping_stage"
error_table = "`accenture`.hemanth.shipping_errors"

print(f"Processing shipping data from: {source_dir}")
print(f"Staging table: {stage_table}")


# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import json

# Define schema for shipping data
shipping_schema = StructType([
    StructField("shipping_id", StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("tracking_number", StringType(), False),
    StructField("carrier", StringType(), False),
    StructField("service_type", StringType(), False),
    StructField("origin_warehouse", StringType(), False),
    StructField("destination_address", StringType(), False),
    StructField("shipping_cost", DecimalType(10,2), False),
    StructField("currency", StringType(), False),
    StructField("estimated_delivery", DateType(), False),
    StructField("actual_delivery", DateType(), True),
    StructField("shipping_status", StringType(), False),
    StructField("package_weight", DecimalType(8,2), False),
    StructField("package_dimensions", StringType(), False),
    StructField("insurance_value", DecimalType(10,2), False),
    StructField("created_timestamp", TimestampType(), False)
])

print("Schema defined for shipping data")


# COMMAND ----------

# Read and validate shipping data
try:
    # Read CSV files with schema validation
    df_shipping = spark.read.schema(shipping_schema).csv(source_dir, header=True, dateFormat="yyyy-MM-dd", timestampFormat="yyyy-MM-dd HH:mm:ss")
    
    # Add processing metadata
    df_shipping = df_shipping.withColumn("processed_timestamp", F.current_timestamp()) \
                            .withColumn("batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
                            .withColumn("source_system", F.lit("ecommerce_shipping"))
    
    # Data quality checks
    total_records = df_shipping.count()
    null_shipping_ids = df_shipping.filter(F.col("shipping_id").isNull()).count()
    invalid_costs = df_shipping.filter(F.col("shipping_cost") < 0).count()
    negative_weights = df_shipping.filter(F.col("package_weight") <= 0).count()
    invalid_insurance = df_shipping.filter(F.col("insurance_value") < 0).count()
    
    print(f"Total records processed: {total_records}")
    print(f"Records with null shipping_id: {null_shipping_ids}")
    print(f"Records with invalid costs: {invalid_costs}")
    print(f"Records with negative weights: {negative_weights}")
    print(f"Records with invalid insurance: {invalid_insurance}")
    
    # Filter out invalid records - Fixed boolean logic
    df_valid_shipping = df_shipping.filter(
        (F.col("shipping_id").isNotNull()) & 
        (F.col("shipping_cost") >= 0) & 
        (F.col("package_weight") > 0) & 
        (F.col("insurance_value") >= 0)
    )
    
    # Capture invalid records for error handling - Fixed boolean logic
    df_invalid_shipping = df_shipping.filter(
        (F.col("shipping_id").isNull()) | 
        (F.col("shipping_cost") < 0) | 
        (F.col("package_weight") <= 0) | 
        (F.col("insurance_value") < 0)
    )
    
    valid_records = df_valid_shipping.count()
    invalid_records = df_invalid_shipping.count()
    
    print(f"Valid records: {valid_records}")
    print(f"Invalid records: {invalid_records}")
    
except Exception as e:
    print(f"Error reading shipping data: {str(e)}")
    raise


# COMMAND ----------

# Data enrichment - Shipping analytics
try:
    # Calculate delivery performance metrics
    df_valid_shipping = df_valid_shipping.withColumn(
        "delivery_days",
        F.when(F.col("actual_delivery").isNotNull(),
               F.datediff(F.col('actual_delivery'), F.col('created_timestamp').cast("date")))
         .otherwise(F.lit(None))
    )

    # Calculate estimated delivery days
    df_valid_shipping = df_valid_shipping.withColumn(
        "estimated_delivery_days",
        F.datediff(F.col("estimated_delivery"), F.col("created_timestamp").cast("date"))
    )

    # Calculate delivery performance
    df_valid_shipping = df_valid_shipping.withColumn(
        "delivery_performance",
        F.when(F.col("actual_delivery").isNull(), "Pending")
         .when(F.col("delivery_days") <= F.col("estimated_delivery_days"), "On Time")
         .when(F.col("delivery_days") <= F.col("estimated_delivery_days") + 1, "Slightly Delayed")
         .otherwise("Delayed")
    )
    
    # Create shipping cost categories
    df_valid_shipping = df_valid_shipping.withColumn(
        "cost_category",
        F.when(F.col("shipping_cost") < 10, "Low Cost")
         .when(F.col("shipping_cost") < 20, "Medium Cost")
         .otherwise("High Cost")
    )
    
    # Calculate cost per weight ratio
    df_valid_shipping = df_valid_shipping.withColumn(
        "cost_per_kg",
        F.col("shipping_cost") / F.col("package_weight")
    )

    print("Data enrichment completed")

except Exception as e:
    print(f"Error in data enrichment: {str(e)}")
    raise

# COMMAND ----------

# Write valid data to staging table
try:
    # Create or overwrite staging table
    df_valid_shipping.write.format("delta").mode("overwrite").saveAsTable(stage_table)
    print(f"Successfully loaded {valid_records} valid shipping records to staging table")
    
    # Write invalid records to error table for investigation
    if invalid_records > 0:
        df_invalid_shipping.withColumn("error_reason", F.lit("Data quality validation failed")) \
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
        if file.name.endswith('.csv'):
            src_path = file.path
            archive_path = archive_dir + file.name
            
            # Move the file to archive
            dbutils.fs.mv(src_path, archive_path)
            archived_count += 1
            print(f"Archived: {file.name}")
    
    print(f"Successfully archived {archived_count} files")
    
except Exception as e:
    print(f"Error archiving files: {str(e)}")
    raise


# COMMAND ----------

# Log processing summary
processing_summary = {
    "task": "shipping_stage_load",
    "timestamp": datetime.now().isoformat(),
    "total_records": total_records,
    "valid_records": valid_records,
    "invalid_records": invalid_records,
    "archived_files": archived_count,
    "status": "SUCCESS" if invalid_records == 0 else "SUCCESS_WITH_WARNINGS"
}

print("Processing Summary:")
print(json.dumps(processing_summary, indent=2))

# Store summary in a table for monitoring
summary_df = spark.createDataFrame([processing_summary])
summary_df.write.format("delta").mode("append").saveAsTable("`accenture`.hemanth.process_log")

# COMMAND ----------

