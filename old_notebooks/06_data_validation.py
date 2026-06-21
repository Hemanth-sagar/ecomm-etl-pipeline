# Databricks notebook source
# MAGIC %md
# MAGIC # Data Validation and Cross-Reference Checks
# MAGIC This notebook performs comprehensive data validation across all staging tables and applies business rules.
# MAGIC

# COMMAND ----------

# Configuration
orders_stage = "`accenture`.hemanth.orders_stage"
customers_stage = "`accenture`.hemanth.customers_stage"
products_stage = "`accenture`.hemanth.products_stage"
inventory_stage = "`accenture`.hemanth.inventory_stage"
shipping_stage = "`accenture`.hemanth.shipping_stage"
validation_results_table = "`accenture`.hemanth.validation_results"

print("Starting comprehensive data validation process...")

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

    print("Successfully loaded all staging tables")
    print(f"Orders: {df_orders.count()} records")
    print(f"Customers: {df_customers.count()} records")
    print(f"Products: {df_products.count()} records")
    print(f"Inventory: {df_inventory.count()} records")
    print(f"Shipping: {df_shipping.count()} records")
    
except Exception as e:
    print("Error loading staging tables: {str(e)}")
    raise

# COMMAND ----------

# Cross-reference validation: Orders vs Customers
try:
    # Check for orphaned orders (orders without valid customers)
    orphaned_orders = df_orders.join(df_customers, "customer_id","left_anti")
    orphaned_orders_count = orphaned_orders.count()

    # Check for orphaned customers (customers without valid orders)
    orphaned_customers = df_customers.join(df_orders, "customer_id","left_anti")
    orphaned_customers_count = orphaned_customers.count()

    print(f"Orphaned orders (no of valid customer): {orphaned_orders_count}")
    print(f"Orphaned customers (no of valid orders): {orphaned_customers_count}")

    # Validate order amounts are reasonable
    unreasonable_orders = df_orders.filter(
        (F.col("order_amount") < 1) | (F.col("order_amount") > 10000)
    )
    unreasonable_orders_count = unreasonable_orders.count()

    print(f"Orders with unreasonable amounts: {unreasonable_orders_count}")

except Exception as e:
    print(f"Error in orders-customers validation: {str(e)}")
    raise

# COMMAND ----------

# Cross-reference validation: Orders vs Products
try:
    # Check for orphaned orders (orders without valid products)
    orphaned_orders_products = df_orders.join(df_products, "product_id", "left_anti")
    orphaned_orders_products_count = orphaned_orders_products.count()
    
    # Check for orphaned products (products without any orders)
    orphaned_products = df_products.join(df_orders, "product_id", "left_anti")
    orphaned_products_count = orphaned_products.count()

    print(f"Orders with invalid products: {orphaned_orders_products_count}")
    print(f"Products without orders: {orphaned_products_count}")

    # Validate order amount against product prices
    orders_with_products = df_orders.join(df_products, "product_id","inner")
    price_mismatch = orders_with_products.filter(
        F.abs(F.col("order_amount") - F.col("price")) > 0.01
    )
    price_mismatch_count = price_mismatch.count()

    print(f"Orders with price mismatches: {price_mismatch_count}")

except Exception as e:
    print(f"Error in orders-products validation: {str(e)}")
    raise

# COMMAND ----------

# Cross-reference validation: Orders vs Shipping
try:
    # Check for orders without shipping information
    orders_without_shipping = df_orders.join(df_shipping, "order_id", "left_anti")
    orders_without_shipping_count = orders_without_shipping.count()
    
    # Check for shipping without orders
    shipping_without_orders = df_shipping.join(df_orders, "order_id", "left_anti")
    shipping_without_orders_count = shipping_without_orders.count()
    
    print(f"Orders without shipping: {orders_without_shipping_count}")
    print(f"Shipping without orders: {shipping_without_orders_count}")

    # Validate shipping costs are reasonable
    unreasonable_shipping = df_shipping.filter(
        (F.col("shipping_cost") < 0) | (F.col("shipping_cost") > 100)
    )
    unreasonable_shipping_count = unreasonable_shipping.count()

    print(f"Shipping with unreasonable costs: {unreasonable_shipping_count}")
    
except Exception as e:
    print(f"Error in orders-shipping validation: {str(e)}")
    raise

# COMMAND ----------

# Cross-reference validation: Products vs Inventory
try:
    # Check for products without inventory
    products_without_inventory = df_products.join(df_inventory, "product_id", "left_anti")
    products_without_inventory_count = products_without_inventory.count()
    
    # Check for inventory without products
    inventory_without_products = df_inventory.join(df_products, "product_id", "left_anti")
    inventory_without_products_count = inventory_without_products.count()
    
    print(f"Products without inventory: {products_without_inventory_count}")
    print(f"Inventory without products: {inventory_without_products_count}")

    # Validate stock quantities are consistent
    products_with_inventory = df_products.join(df_inventory, "product_id", "inner")
    stock_mismatch = products_with_inventory.filter(
        F.col("products_stage.stock_quantity") != F.col("inventory_stage.stock_quantity")
    )
    stock_mismatch_count = stock_mismatch.count()
    # check here why products_stage.stock_quantity not df_products.stock_quantity

    print(f"Stock quantity mismatches: {stock_mismatch_count}")
    
except Exception as e:
    print(f"Error in products-inventory validation: {str(e)}")
    raise


# COMMAND ----------

# Business Rules Validation
try:
    # Rule 1: Premium customers should have higher order values
    # This is the business rule which has been communicated by data analysts team or business analysts team
    premium_customers_orders = df_orders.join(df_customers,"customer_id","inner")\
                                        .filter(F.col("customer_tier") == "premium")

    low_value_premium_orders = premium_customers_orders.filter(F.col("order_amount") < 100)
    low_value_premium_count = low_value_premium_orders.count()

    # Rule 2: Orders should be processed within business hours (8 AM - 6 PM)
    orders_outside_hours = df_orders.filter(
        (F.hour(F.col("created_timestamp")) < 8) | 
        (F.hour(F.col("created_timestamp")) > 18)
    )
    orders_outside_hours_count = orders_outside_hours.count()

    # Rule 3: Discontinued products should not have new orders
    discontinued_orders = df_orders.join(df_products, "product_id", "inner")\
                                .filter(F.col("discontinued")==True)
    discontinued_orders_count=discontinued_orders.count()

    print(f"Premium customers with low-value orders: {low_value_premium_count}")
    print(f"Orders outside business hours: {orders_outside_hours_count}")
    print(f"Orders for discontinued products: {discontinued_orders_count}")
    
except Exception as e:
    print(f"Error in business rules validation: {str(e)}")
    raise


# COMMAND ----------

# Compile validation results
try:
    validation_results = [
        {
            "validation_type": "orphaned_orders",
            "count": orphaned_orders_count,
            "severity": "HIGH" if orphaned_orders_count > 0 else "NONE",
            "description": "Orders without valid customers"
        },
        {
            "validation_type": "orphaned_customers",
            "count": orphaned_customers_count,
            "severity": "MEDIUM" if orphaned_customers_count > 0 else "NONE",
            "description": "Customers without any orders"
        },
        {
            "validation_type": "unreasonable_orders",
            "count": unreasonable_orders_count,
            "severity": "HIGH" if unreasonable_orders_count > 0 else "NONE",
            "description": "Orders with unreasonable amounts"
        },
        {
            "validation_type": "orphaned_orders_products",
            "count": orphaned_orders_products_count,
            "severity": "HIGH" if orphaned_orders_products_count > 0 else "NONE",
            "description": "Orders with invalid products"
        },
        {
            "validation_type": "price_mismatch",
            "count": price_mismatch_count,
            "severity": "MEDIUM" if price_mismatch_count > 0 else "NONE",
            "description": "Orders with price mismatches"
        },
        {
            "validation_type": "orders_without_shipping",
            "count": orders_without_shipping_count,
            "severity": "HIGH" if orders_without_shipping_count > 0 else "NONE",
            "description": "Orders without shipping information"
        },
        {
            "validation_type": "unreasonable_shipping",
            "count": unreasonable_shipping_count,
            "severity": "MEDIUM" if unreasonable_shipping_count > 0 else "NONE",
            "description": "Shipping with unreasonable costs"
        },
        {
            "validation_type": "products_without_inventory",
            "count": products_without_inventory_count,
            "severity": "MEDIUM" if products_without_inventory_count > 0 else "NONE",
            "description": "Products without inventory"
        },
        {
            "validation_type": "low_value_premium_orders",
            "count": low_value_premium_count,
            "severity": "LOW" if low_value_premium_count > 0 else "NONE",
            "description": "Premium customers with low-value orders"
        },
        {
            "validation_type": "orders_outside_hours",
            "count": orders_outside_hours_count,
            "severity": "LOW" if orders_outside_hours_count > 0 else "NONE",
            "description": "Orders outside business hours"
        },
        {
            "validation_type": "discontinued_orders",
            "count": discontinued_orders_count,
            "severity": "HIGH" if discontinued_orders_count > 0 else "NONE",
            "description": "Orders for discontinued products"
        }
    ]
    
    # Create DataFrame from validation results
    df_validation_results = spark.createDataFrame(validation_results)
    df_validation_results = df_validation_results.withColumn("validation_timestamp", F.current_timestamp()) \
                                                .withColumn("batch_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
    
    # Write validation results to table
    df_validation_results.write.format("delta").mode("append").saveAsTable(validation_results_table)
    
    # Calculate overall validation score
    high_severity_issues = sum(1 for result in validation_results if result["severity"] == "HIGH")
    medium_severity_issues = sum(1 for result in validation_results if result["severity"] == "MEDIUM")
    low_severity_issues = sum(1 for result in validation_results if result["severity"] == "LOW")
    
    overall_status = "PASS" if high_severity_issues == 0 else "FAIL"
    
    print(f"Validation Summary:")
    print(f"High severity issues: {high_severity_issues}")
    print(f"Medium severity issues: {medium_severity_issues}")
    print(f"Low severity issues: {low_severity_issues}")
    print(f"Overall status: {overall_status}")
    
except Exception as e:
    print(f"Error compiling validation results: {str(e)}")
    raise


# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, StringType

# Log validation summary
validation_summary = {
    "archived_files": None,
    "invalid_records": None,
    "status": None,
    "task": "data_validation",
    "timestamp": datetime.now().isoformat(),
    "total_records": None,
    "valid_records": None
}

print("Validation Summary:")
print(json.dumps(validation_summary, indent=2))

# Explicit schema for processing_log table
processing_log_schema = StructType([
    StructField("archived_files", LongType(), True),
    StructField("invalid_records", LongType(), True),
    StructField("status", StringType(), True),
    StructField("task", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("total_records", LongType(), True),
    StructField("valid_records", LongType(), True)
])

summary_df = spark.createDataFrame([validation_summary], schema=processing_log_schema)
summary_df.write.format("delta").mode("append").saveAsTable("`accenture`.hemanth.process_log")

# Set validation status for downstream tasks
dbutils.jobs.taskValues.set("validation_status", overall_status)
dbutils.jobs.taskValues.set("high_severity_count", high_severity_issues)


# COMMAND ----------

