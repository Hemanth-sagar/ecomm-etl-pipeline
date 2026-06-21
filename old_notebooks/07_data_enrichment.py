# Databricks notebook source
# MAGIC %md
# MAGIC # Data Enrichment and Business Intelligence
# MAGIC This notebook enriches the validated data with additional business metrics and prepares it for analytics.
# MAGIC

# COMMAND ----------

# Configuration
orders_stage = "`accenture`.hemanth.orders_stage"
customers_stage = "`accenture`.hemanth.customers_stage"
products_stage = "`accenture`.hemanth.products_stage"
inventory_stage = "`accenture`.hemanth.inventory_stage"
shipping_stage = "`accenture`.hemanth.shipping_stage"
enriched_orders_table = "`accenture`.hemanth.enriched_orders"
customer_analytics_table = "`accenture`.hemanth.customer_analytics"
product_analytics_table = "`accenture`.hemanth.product_analytics"

print("Starting data enrichment process...")

# COMMAND ----------

# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
import json

# Read all staging tables
try:
    df_orders = spark.read.table(orders_stage)
    df_customers = spark.read.table(customers_stage)
    df_products = spark.read.table(products_stage)
    df_inventory = spark.read.table(inventory_stage)
    df_shipping = spark.read.table(shipping_stage)

    print("Successfully loaded all staging table for enrichment")

except Exception as e:
    print(f"Error loading stage tables: {str(e)}")
    raise

# COMMAND ----------

# Create enriched orders dataset with all related information
try:
    # Rename ALL conflicting columns to avoid ambiguity
    df_customers_renamed = df_customers.withColumnRenamed("created_timestamp", "customer_created_timestamp") \
                                      .withColumnRenamed("batch_id", "customer_batch_id") \
                                      .withColumnRenamed("processed_timestamp", "customer_processed_timestamp") \
                                      .withColumnRenamed("source_system", "customer_source_system") \
                                      .withColumnRenamed("lifecycle_stage", "customer_lifecycle_stage")
                                    
    df_products_renamed = df_products.withColumnRenamed("created_timestamp", "product_created_timestamp") \
                                    .withColumnRenamed("batch_id", "product_batch_id") \
                                    .withColumnRenamed("processed_timestamp", "product_processed_timestamp") \
                                    .withColumnRenamed("source_system", "product_source_system") \
                                    .withColumnRenamed("currency", "product_currency") \
                                    .withColumnRenamed("weight_kg", "product_weight_kg") \
                                    .withColumnRenamed("lifecycle_stage", "product_lifecycle_stage") \
                                    .withColumnRenamed("stock_quantity", "product_stock_quantity") \
                                    .withColumnRenamed("stock_status", "product_stock_status") \
                                    .withColumnRenamed("price_segment", "product_price_segment")

    df_inventory_renamed = df_inventory.withColumnRenamed("created_timestamp", "inventory_created_timestamp") \
                                      .withColumnRenamed("batch_id", "inventory_batch_id") \
                                      .withColumnRenamed("processed_timestamp", "inventory_processed_timestamp") \
                                      .withColumnRenamed("source_system", "inventory_source_system") \
                                      .withColumnRenamed("stock_status", "inventory_stock_status")
    
    df_shipping_renamed = df_shipping.withColumnRenamed("created_timestamp", "shipping_created_timestamp") \
                                    .withColumnRenamed("batch_id", "shipping_batch_id") \
                                    .withColumnRenamed("processed_timestamp", "shipping_processed_timestamp") \
                                    .withColumnRenamed("source_system", "shipping_source_system") \
                                    .withColumnRenamed("currency", "shipping_currency") \
                                    .withColumnRenamed("package_weight", "shipping_package_weight")
    
    # Join orders with customers, products, inventory, and shipping
    df_enriched_orders = df_orders \
        .join(df_customers_renamed, "customer_id", "left") \
        .join(df_products_renamed, "product_id", "left") \
        .join(df_inventory_renamed, "product_id", "left") \
        .join(df_shipping_renamed, "order_id", "left")

    # Add business metrics
    df_enriched_orders = df_enriched_orders.withColumn(
        "order_profit_margin",
        F.col("order_amount") * 0.3  # Assuming 30% profit margin
    )

    # Add customer lifetime value estimation
    df_enriched_orders = df_enriched_orders.withColumn(
        "estimated_clv",
        F.col("order_amount") * F.when(F.col("customer_tier") == "premium", 10)
                                 .when(F.col("customer_tier") == "gold", 7)
                                 .when(F.col("customer_tier") == "silver", 5)
                                 .otherwise(3)
    )    

    # Add seasonal indicators
    df_enriched_orders = df_enriched_orders.withColumn(
        "season",
        F.when(F.month(F.col("order_date")).isin([12, 1, 2]), "winter")
         .when(F.month(F.col("order_date")).isin([3, 4, 5]), "Spring")
         .when(F.month(F.col("order_date")).isin([6, 7, 8]), "Summer")
         .otherwise("Fall")
    )

    # Add day of week and time of day indicators
    df_enriched_orders = df_enriched_orders.withColumn(
        "day_of_week", F.dayofweek(F.col("order_date"))
    ).withColumn(
        "is_weekend",F.when(F.dayofweek(F.col("order_date")).isin([1,7]), True)
                      .otherwise(False)
    ).withColumn(
        "time_of_day",
        F.when(F.hour(F.col("created_timestamp")) < 6, "Early Morning")
         .when(F.hour(F.col("created_timestamp")) < 12, "Morning")
         .when(F.hour(F.col("created_timestamp")) < 18, "Afternoon")
         .otherwise("Evening")
    )
    
    print("Enriched orders dataset created successfully")
    
except Exception as e:
    print(f"Error creating enriched orders: {str(e)}")
    raise

# COMMAND ----------

# Create customer analytics dataset
try:
    # Calculate customer metrics
    df_customer_analytics = df_enriched_orders.groupBy("customer_id") \
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_spent"),
            F.avg("order_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("product_id").alias("unique_products_purchased"),
            F.countDistinct("category").alias("unique_categories_purchased"),
            F.sum("order_profit_margin").alias("total_profit_generated"),
            F.avg("estimated_clv").alias("avg_estimated_clv")
        )
    
    # Join with customer details
    df_customer_analytics = df_customer_analytics.join(df_customers_renamed, "customer_id", "left")
    
    # Calculate additional metrics
    df_customer_analytics = df_customer_analytics.withColumn(
        "days_since_first_order",
        F.datediff(F.current_date(), F.col("first_order_date"))
    ).withColumn(
        "days_since_last_order",
        F.datediff(F.current_date(), F.col("last_order_date"))
    ).withColumn(
        "order_frequency_days",
        F.col("days_since_first_order") / F.col("total_orders")
    )
    # Create customer segments
    df_customer_analytics = df_customer_analytics.withColumn(
        "customer_segment",
        F.when((F.col("total_spent") >= 1000) & (F.col("total_orders") >= 5), "VIP")
         .when((F.col("total_spent") >= 500) & (F.col("total_orders") >= 3), "High Value")
         .when((F.col("total_spent") >= 200) & (F.col("total_orders") >= 2), "Medium Value")
         .otherwise("Low Value")
    )
    
    # Create customer lifecycle stage
    df_customer_analytics = df_customer_analytics.withColumn(
        "lifecycle_stage",
        F.when(F.col("days_since_last_order") <= 30, "Active")
         .when(F.col("days_since_last_order") <= 90, "At Risk")
         .when(F.col("days_since_last_order") <= 180, "Inactive")
         .otherwise("Lost")
    )
    
    print("Customer analytics dataset created successfully")
    
except Exception as e:
    print(f"Error creating customer analytics: {str(e)}")
    raise

# COMMAND ----------

# Create product analytics dataset
try:
    # Calculate product metrics
    df_product_analytics = df_enriched_orders.groupBy("product_id") \
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("order_profit_margin").alias("total_profit"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date")
        )
    
    # Join with product details
    df_product_analytics = df_product_analytics.join(df_products_renamed, "product_id", "left")
    
    # Calculate additional metrics
    df_product_analytics = df_product_analytics.withColumn(
        "days_since_first_order",
        F.datediff(F.current_date(), F.col("first_order_date"))
    ).withColumn(
        "days_since_last_order",
        F.datediff(F.current_date(), F.col("last_order_date"))
    ).withColumn(
        "order_frequency_days",
        F.col("days_since_first_order") / F.col("total_orders")
    ).withColumn(
        "revenue_per_customer",
        F.col("total_revenue") / F.col("unique_customers")
    )

    # Create product performance categories
    df_product_analytics = df_product_analytics.withColumn(
        "performance_category",
        F.when((F.col("total_revenue") >= 5000) & (F.col("total_orders") >= 20), "Star")
         .when((F.col("total_revenue") >= 2000) & (F.col("total_orders") >= 10), "High Performer")
         .when((F.col("total_revenue") >= 500) & (F.col("total_orders") >= 5), "Medium Performer")
         .otherwise("Low Performer")
    )
    
    # Create product lifecycle stage
    df_product_analytics = df_product_analytics.withColumn(
        "product_lifecycle",
        F.when(F.col("days_since_last_order") <= 30, "Active")
         .when(F.col("days_since_last_order") <= 90, "Declining")
         .when(F.col("discontinued") == True, "Discontinued")
         .otherwise("Stagnant")
    )
    
    print("Product analytics dataset created successfully")
    
except Exception as e:
    print(f"Error creating product analytics: {str(e)}")
    raise

# COMMAND ----------

# Write enriched datasets to tables
try:
    # Write enriched orders
    df_enriched_orders.write.format("delta").mode("overwrite").saveAsTable(enriched_orders_table)
    print(f"Successfully wrote enriched orders to {enriched_orders_table}")
    
    # Write customer analytics
    df_customer_analytics.write.format("delta").mode("overwrite").saveAsTable(customer_analytics_table)
    print(f"Successfully wrote customer analytics to {customer_analytics_table}")
    
    # Write product analytics
    df_product_analytics.write.format("delta").mode("overwrite").saveAsTable(product_analytics_table)
    print(f"Successfully wrote product analytics to {product_analytics_table}")

    # Log enrichment statistics
    from pyspark.sql.types import StructType, StructField, LongType, StringType

    enrichment_summary = {
        "archived_files": None,
        "invalid_records": None,
        "status": "SUCCESS",
        "task": "data_enrichment",
        "timestamp": datetime.now().isoformat(),
        "total_records": None,
        "valid_records": None
    }

    print("Enrichment Summary:")
    print(json.dumps(enrichment_summary, indent=2))

    processing_log_schema = StructType([
        StructField("archived_files", LongType(), True),
        StructField("invalid_records", LongType(), True),
        StructField("status", StringType(), True),
        StructField("task", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("total_records", LongType(), True),
        StructField("valid_records", LongType(), True)
    ])

    summary_df = spark.createDataFrame([enrichment_summary], schema=processing_log_schema)
    summary_df.write.format("delta").mode("append").saveAsTable("`accenture`.hemanth.process_log")

    dbutils.jobs.taskValues.set("enrichment_status", "SUCCESS")
    
except Exception as e:
    print(f"Error writing enriched datasets: {str(e)}")
    raise

# COMMAND ----------

