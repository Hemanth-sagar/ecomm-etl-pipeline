# Databricks notebook source
# MAGIC %md
# MAGIC # Customers Data Stage Load
# MAGIC This notebook processes customers data from source files and loads it into the staging table.

# COMMAND ----------

# Configuration
source_dir = "/Volumes/accenture/hemanth/incremental_load/customers_data/source/"
archive_dir = "/Volumes/accenture/hemanth/incremental_load/customers_data/archive/"
stage_table = "`accenture`.hemanth.customers_stage"
error_table = "`accenture`.hemanth.customers_errors"

print(f"Processing orders data from: {source_dir}")
print(f"Staging table: {stage_table}")

# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import json

# Define schema for orders data
customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("email", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("date_of_birth", DateType(), False),
    StructField("registration_date", DateType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("zip_code", StringType(), False),
    StructField("country", StringType(), False),
    StructField("customer_tier", StringType(), False),
    StructField("last_login", TimestampType(), False),
    StructField("created_timestamp", TimestampType(), False)
])


print("Schema defined for customers data")

# COMMAND ----------

# Read and validate orders data
try:
    # Read CSV files with Schema validation
    df_customers = spark.read.schema(customers_schema).csv(source_dir, header=True,
                                                      dateFormat="yyyy-MM-dd", timestampFormat="yyyy-MM-dd HH:mm:ss")
    
    # Add processing metadata
    df_customers = df_customers.withColumn("processed_timestamp", F.current_timestamp())\
                        .withColumn("batch_id",F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))\
                        .withColumn("source_system", F.lit("ecommerce_orders"))

    # Data quality checks - Simplified validation
    total_records = df_customers.count()
    null_customer_ids = df_customers.filter(F.col("customer_id").isNull()).count()
    null_emails = df_customers.filter(F.col("email").isNull()).count()
    null_phones = df_customers.filter(F.col("phone").isNull()).count()
    future_birth_dates = df_customers.filter(F.col("date_of_birth")>F.current_date()).count()

    # Simple email validation (contains @ and .)
    invalid_emails = df_customers.filter(
        (F.col("email").isNotNull()) &
        (~F.col("email").contains("@")) |
        (~F.col("email").contains("."))
    ).count()

    # Simple phone validation (contains - and has 12 characters)
    invalid_phones = df_customers.filter(
        (F.col("phone").isNotNull()) &
        (~F.col("phone").contains("-")) |
        (F.length(F.col("phone")) != 12)
    ).count()
    
    print(f"Total records processed: {total_records}")
    print(f"Records with null customer_id: {null_customer_ids}")
    print(f"Records with null email: {null_emails}")
    print(f"Records with null phone: {null_phones}")
    print(f"Records with null email: {invalid_emails}")
    print(f"Records with null email: {invalid_phones}")
    print(f"Records with future birth dates: {future_birth_dates}")

    # Filter out valid records - Fixed boolean logic
    df_valid_customers = df_customers.filter(
                                    (F.col("customer_id").isNotNull()) &
                                    (F.col("email").isNotNull()) &
                                    (F.col("phone").isNotNull()) &
                                    (F.col("email").contains("@")) &
                                    (F.col("email").contains(".")) &
                                    (F.col("date_of_birth") <= F.current_date())
                                )
    
    # Capture invalid records for error handling - Fixed boolean logic
    df_invalid_customers = df_customers.filter(
                                    (F.col("customer_id").isNull()) &
                                    (F.col("email").isNull()) &
                                    (F.col("phone").isNull()) &
                                    (~F.col("email").contains("@")) &
                                    (~F.col("email").contains(".")) &
                                    (F.col("date_of_birth") > F.current_date())
                                )

    valid_records = df_valid_customers.count()
    invalid_records = df_invalid_customers.count()

    print(f"Valid records: {valid_records}")
    print(f"Invalid records: {invalid_records}")

except Exception as e:
    print(f"Error reading orders data: {str(e)}")
    raise
# here raise once it raise error it makes it fail the pipeline so next cell won't run
    

# COMMAND ----------

# Data enrichment - Calculate customer age and segment
try:
    # Calculate age from date of birth
    df_valid_customers = df_valid_customers.withColumn(
        "age", F.floor(F.datediff(F.current_date(),F.col("date_of_birth"))/365)
    )
    
    # Create age segments
    df_valid_customers = df_valid_customers.withColumn(
        "age_segment",
        F.when(F.col("age") < 25, "Gen Z")
         .when(F.col("age") <40, "Millennial")
         .when(F.col("age") < 55, "Gen X")
         .otherwise("Boomer")
    )

    # Calculate days since registration
    df_valid_customers = df_valid_customers.withColumn(
        "days_since_registration",
        F.datediff(F.current_date(),F.col("registration_date"))
    )

    # Create customer lifecycle stage
    df_valid_customers = df_valid_customers.withColumn(
        "lifecycel_stage",
        F.when(F.col("days_since_registration") < 30,"New")
         .when(F.col("days_since_registration") < 365, "Active")
         .otherwise("Established")
    )

    print("Data enrichment completed")

except Exception as e:
    print(F"Error in data enrichment: {str(e)}")
    raise

# COMMAND ----------

# Write valid data to staging table
try:
    # Create or overwrite staging table
    df_valid_customers.write.format("delta").mode("overwrite").saveAsTable(stage_table)
    print(f"Successfully loaded {valid_records} valid orders to staging table")

    # Write invalid data to error table for investigation
    if invalid_records>0:
        df_invalid_customers.withColumn("error_reason", F.lit("Data quality validation failed"))\
                        .withColumn("error_timestamp", F.current_timestamp())\
                        .write.format("delta").mode("append").saveAsTable(error_table)
        print(f"Logged {invalid_records} invalid orders to error table")

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
    "task": "customers_stage_load",
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

# DBTITLE 1,calculate age
from pyspark.sql import functions as F
from pyspark.sql import *
from datetime import *

spark.range(1).select(
    F.floor(F.datediff(F.current_date(), F.lit(date(2000, 12, 2)))/365).alias("days_passed")
).show()

# COMMAND ----------

