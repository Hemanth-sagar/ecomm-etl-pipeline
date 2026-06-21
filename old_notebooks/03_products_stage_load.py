# Databricks notebook source
# MAGIC %md
# MAGIC # Products Data Stage Load
# MAGIC This notebook processes products data from source files and loads it into the staging table.

# COMMAND ----------

# Configuration
source_dir = "/Volumes/accenture/hemanth/incremental_load/products_data/source/"
archive_dir = "/Volumes/accenture/hemanth/incremental_load/products_data/archive/"
stage_table = "`accenture`.hemanth.products_stage"
error_table = "`accenture`.hemanth.products_errors"

print(f"Processing products data from: {source_dir}")
print(f"Staging table: {stage_table}")

# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import json

# Define schema for products data
products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("subcategory", StringType(), False),
    StructField("brand", StringType(), False),
    StructField("price", DecimalType(10,2), False),
    StructField("currency", StringType(), False),
    StructField("stock_quantity", IntegerType(), False),
    StructField("weight_kg", DecimalType(8,2), False),
    StructField("dimensions_cm", StringType(), False),
    StructField("color", StringType(), False),
    StructField("material", StringType(), False),
    StructField("description", StringType(), False),
    StructField("launch_date", DateType(), False),
    StructField("discontinued", BooleanType(), False),
    StructField("created_timestamp", TimestampType(), False)
])

print("Schema defined for products data")


# COMMAND ----------

# Read and validate products data
try:
    # Read CSV files with Schema validation
    df_products = spark.read.schema(products_schema).csv(source_dir, header=True,
                                                      dateFormat="yyyy-MM-dd", timestampFormat="yyyy-MM-dd HH:mm:ss")
    
    # Add processing metadata
    df_products = df_products.withColumn("processed_timestamp", F.current_timestamp())\
                        .withColumn("batch_id",F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))\
                        .withColumn("source_system", F.lit("ecommerce_orders"))

    # Data quality checks - Simplified validation
    total_records = df_products.count()
    null_product_ids = df_products.filter(F.col("product_id").isNull()).count()
    invalid_prices = df_products.filter(F.col("price") <= 0).count()
    negative_stock = df_products.filter(F.col("stock_quantity") < 0).count()
    future_launch_dates = df_products.filter(F.col("launch_date") > F.current_date()).count()
    
    print(f"Total records processed: {total_records}")
    print(f"Records with null product_id: {null_product_ids}")
    print(f"Records with invalid prices: {invalid_prices}")
    print(f"Records with negative stock: {negative_stock}")
    print(f"Records with future launch dates: {future_launch_dates}")

    # Filter out valid records - Fixed boolean logic
    df_valid_products = df_products.filter(
        (F.col("product_id").isNotNull()) & 
        (F.col("price") > 0) & 
        (F.col("stock_quantity") >= 0) & 
        (F.col("launch_date") <= F.current_date())
    )
    
    # Capture invalid records for error handling - Fixed boolean logic
    df_invalid_products = df_products.filter(
        (F.col("product_id").isNull()) | 
        (F.col("price") <= 0) | 
        (F.col("stock_quantity") < 0) | 
        (F.col("launch_date") > F.current_date())
    )
    valid_records = df_valid_products.count()
    invalid_records = df_invalid_products.count()

    print(f"Valid records: {valid_records}")
    print(f"Invalid records: {invalid_records}")

except Exception as e:
    print(f"Error reading orders data: {str(e)}")
    raise
# here raise once it raise error it makes it fail the pipeline so next cell won't run
    

# COMMAND ----------

# Data enrichment - Product categorization and pricing analysis
try:
    # Create price segments
    df_valid_products = df_valid_products.withColumn(
        "price_segment",
        F.when(F.col("price") < 50, "Budget")
         .when(F.col("price") < 150, "Mid-range")
         .when(F.col("price") < 300, "Premium")
         .otherwise("Luxury")
    )
    
    # Create stock status
    df_valid_products = df_valid_products.withColumn(
        "stock_status",
        F.when(F.col("stock_quantity") == 0, "Out of Stock")
         .when(F.col("stock_quantity") < 10, "Low Stock")
         .when(F.col("stock_quantity") < 50, "Medium Stock")
         .otherwise("High Stock")
    )


    # Calculate days since launch
    df_valid_products = df_valid_products.withColumn(
        "days_since_launch",
        F.datediff(F.current_date(), F.col("launch_date"))
    )

    # Create product lifecycle stage
    df_valid_products = df_valid_products.withColumn(
        "lifecycle_stage",
        F.when(F.col("days_since_launch") <30,"New")
         .when(F.col("days_since_launch") < 365,"Growth")
         .when(F.col("discontinued") == True,"Discontinued")
         .otherwise("Mature")
    )

    # Parse dimensions and calculate volume(step 2)
    df_valid_products = df_valid_products.withColumn(
        "dimensions_array",
        F.split(F.col("dimensions_cm"), "x")
    )

    # Calculate volume (assuming dimensions are in format "LxWxH")
    df_valid_products = df_valid_products.withColumn(
        "volume_cm3",
        F.when(F.size(F.col("dimensions_array"))==3,
               F.col("dimensions_array")[0].cast("double") *
               F.col("dimensions_array")[1].cast("double") *
               F.col("dimensions_array")[2].cast("double"))
        .otherwise(F.lit(0))
    )

    # Calculate density (weight/volume)
    df_valid_products = df_valid_products.withColumn(
        "density_kg_cm3",
        F.when(F.col("volume_cm3")>0, F.col("weight_kg")/F.col("volume_cm3"))
        .otherwise(F.lit(0))
    )

    print("Data enrichment completed")

except Exception as e:
    print(F"Error in data enrichment: {str(e)}")
    raise

# COMMAND ----------

# Write valid data to staging table
try:
    # Create or overwrite staging table
    df_valid_products.write.format("delta").mode("overwrite").saveAsTable(stage_table)
    print(f"Successfully loaded {valid_records} valid products to staging table")

    # Write invalid data to error table for investigation
    if invalid_records>0:
        df_invalid_products.withColumn("error_reason", F.lit("Data quality validation failed"))\
                        .withColumn("error_timestamp", F.current_timestamp())\
                        .write.format("delta").mode("append").saveAsTable(error_table)
        print(f"Logged {invalid_records} invalid products to error table")

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
    "task": "products_stage_load",
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

# DBTITLE 1,product dimensions
from pyspark.sql import functions as F
from pyspark.sql import *
df=spark.createDataFrame([("a","b"),(1,2)])

df = df.withColumn("dim",
    F.split(F.lit("12x3x5"),"x")
)


# COMMAND ----------

