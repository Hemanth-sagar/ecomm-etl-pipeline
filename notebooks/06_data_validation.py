# Databricks notebook source
# ============================================================
# 06_data_validation.py
# Cross-reference & business-rules validation across all stage tables
# ============================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/<your-repo>/ecomm-etl-pipeline")

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType
from datetime import datetime
import json

from configs.pipeline_config import TABLES, BUSINESS_RULES
from utils.pipeline_utils import log_summary

# COMMAND ----------
# ---- Load all staging tables --------------------------------
try:
    df_orders    = spark.read.table(TABLES["orders_stage"])
    df_customers = spark.read.table(TABLES["customers_stage"])
    df_products  = spark.read.table(TABLES["products_stage"])
    df_inventory = spark.read.table(TABLES["inventory_stage"])
    df_shipping  = spark.read.table(TABLES["shipping_stage"])

    # Cache tables used in multiple joins
    df_orders.cache(); df_customers.cache(); df_products.cache()

    print("Staging tables loaded.")
    print(f"  Orders: {df_orders.count()} | Customers: {df_customers.count()} | "
          f"Products: {df_products.count()} | Inventory: {df_inventory.count()} | "
          f"Shipping: {df_shipping.count()}")

except Exception as e:
    print(f"Error loading staging tables: {e}"); raise

# COMMAND ----------
# ---- Cross-reference: Orders <-> Customers ------------------
try:
    orphaned_orders_count    = df_orders.join(df_customers, "customer_id", "left_anti").count()
    orphaned_customers_count = df_customers.join(df_orders,  "customer_id", "left_anti").count()
    unreasonable_orders_count = df_orders.filter(
        (F.col("order_amount") < BUSINESS_RULES["min_order_amount"]) |
        (F.col("order_amount") > BUSINESS_RULES["max_order_amount"])
    ).count()
    print(f"Orphaned orders (no customer): {orphaned_orders_count}")
    print(f"Orphaned customers (no order): {orphaned_customers_count}")
    print(f"Orders with unreasonable amounts: {unreasonable_orders_count}")
except Exception as e:
    print(f"Error in orders-customers validation: {e}"); raise

# COMMAND ----------
# ---- Cross-reference: Orders <-> Products -------------------
try:
    orphaned_orders_products_count = df_orders.join(df_products, "product_id", "left_anti").count()
    orphaned_products_count        = df_products.join(df_orders,  "product_id", "left_anti").count()
    price_mismatch_count = (df_orders.join(df_products, "product_id", "inner")
                            .filter(F.abs(F.col("order_amount") - F.col("price")) > 0.01)
                            .count())
    print(f"Orders with invalid product: {orphaned_orders_products_count}")
    print(f"Products with no orders    : {orphaned_products_count}")
    print(f"Price mismatches           : {price_mismatch_count}")
except Exception as e:
    print(f"Error in orders-products validation: {e}"); raise

# COMMAND ----------
# ---- Cross-reference: Orders <-> Shipping -------------------
try:
    orders_without_shipping_count  = df_orders.join(df_shipping,  "order_id", "left_anti").count()
    shipping_without_orders_count  = df_shipping.join(df_orders, "order_id", "left_anti").count()
    unreasonable_shipping_count    = df_shipping.filter(
        (F.col("shipping_cost") < 0) |
        (F.col("shipping_cost") > BUSINESS_RULES["max_shipping_cost"])
    ).count()
    print(f"Orders without shipping        : {orders_without_shipping_count}")
    print(f"Shipping without orders        : {shipping_without_orders_count}")
    print(f"Unreasonable shipping costs    : {unreasonable_shipping_count}")
except Exception as e:
    print(f"Error in orders-shipping validation: {e}"); raise

# COMMAND ----------
# ---- Cross-reference: Products <-> Inventory ----------------
try:
    products_without_inventory_count = df_products.join(df_inventory, "product_id", "left_anti").count()
    inventory_without_products_count = df_inventory.join(df_products, "product_id", "left_anti").count()
    print(f"Products without inventory : {products_without_inventory_count}")
    print(f"Inventory without products : {inventory_without_products_count}")
except Exception as e:
    print(f"Error in products-inventory validation: {e}"); raise

# COMMAND ----------
# ---- Business Rules -----------------------------------------
try:
    # Rule 1: Premium customers expected to have order_amount >= threshold
    low_value_premium_count = (df_orders.join(df_customers, "customer_id", "inner")
                               .filter(
                                   (F.col("customer_tier") == "premium") &
                                   (F.col("order_amount") < BUSINESS_RULES["premium_min_order"])
                               ).count())

    # Rule 2: Orders should be placed within business hours
    orders_outside_hours_count = df_orders.filter(
        (F.hour("created_timestamp") < BUSINESS_RULES["business_hours_start"]) |
        (F.hour("created_timestamp") > BUSINESS_RULES["business_hours_end"])
    ).count()

    # Rule 3: No orders for discontinued products
    discontinued_orders_count = (df_orders.join(df_products, "product_id", "inner")
                                 .filter(F.col("discontinued") == True)
                                 .count())

    print(f"Premium customers with low-value orders : {low_value_premium_count}")
    print(f"Orders outside business hours           : {orders_outside_hours_count}")
    print(f"Orders for discontinued products        : {discontinued_orders_count}")
except Exception as e:
    print(f"Error in business rules validation: {e}"); raise

# COMMAND ----------
# ---- Compile & persist validation results -------------------
try:
    results = [
        {"validation_type": "orphaned_orders",             "count": orphaned_orders_count,            "severity": "HIGH"   if orphaned_orders_count > 0            else "NONE", "description": "Orders without valid customers"},
        {"validation_type": "orphaned_customers",          "count": orphaned_customers_count,         "severity": "MEDIUM" if orphaned_customers_count > 0           else "NONE", "description": "Customers without any orders"},
        {"validation_type": "unreasonable_order_amounts",  "count": unreasonable_orders_count,        "severity": "HIGH"   if unreasonable_orders_count > 0           else "NONE", "description": "Orders with amounts outside expected range"},
        {"validation_type": "orders_invalid_product",      "count": orphaned_orders_products_count,   "severity": "HIGH"   if orphaned_orders_products_count > 0      else "NONE", "description": "Orders with no matching product"},
        {"validation_type": "price_mismatch",              "count": price_mismatch_count,             "severity": "MEDIUM" if price_mismatch_count > 0                else "NONE", "description": "Order amount differs from product price"},
        {"validation_type": "orders_without_shipping",     "count": orders_without_shipping_count,    "severity": "HIGH"   if orders_without_shipping_count > 0       else "NONE", "description": "Orders with no shipping record"},
        {"validation_type": "unreasonable_shipping_cost",  "count": unreasonable_shipping_count,      "severity": "MEDIUM" if unreasonable_shipping_count > 0         else "NONE", "description": "Shipping cost outside expected range"},
        {"validation_type": "products_without_inventory",  "count": products_without_inventory_count, "severity": "MEDIUM" if products_without_inventory_count > 0    else "NONE", "description": "Products with no inventory record"},
        {"validation_type": "low_value_premium_orders",    "count": low_value_premium_count,          "severity": "LOW"    if low_value_premium_count > 0             else "NONE", "description": "Premium customers with low-value orders"},
        {"validation_type": "orders_outside_hours",        "count": orders_outside_hours_count,       "severity": "LOW"    if orders_outside_hours_count > 0          else "NONE", "description": "Orders placed outside business hours"},
        {"validation_type": "discontinued_product_orders", "count": discontinued_orders_count,        "severity": "HIGH"   if discontinued_orders_count > 0           else "NONE", "description": "Orders placed for discontinued products"},
    ]

    results_schema = StructType([
        StructField("validation_type", StringType(), True),
        StructField("count",           LongType(),   True),
        StructField("severity",        StringType(), True),
        StructField("description",     StringType(), True),
    ])

    df_results = spark.createDataFrame(results, schema=results_schema)
    df_results = df_results.withColumn("validated_at", F.current_timestamp())
    df_results.write.format("delta").mode("overwrite").saveAsTable(TABLES["validation_results"])

    high_severity_issues = sum(1 for r in results if r["severity"] == "HIGH")
    overall_status = "FAILED" if high_severity_issues > 0 else "PASSED"

    print(f"\nValidation complete. Status: {overall_status} | HIGH severity issues: {high_severity_issues}")
    display(df_results)

except Exception as e:
    print(f"Error compiling validation results: {e}"); raise

# COMMAND ----------
# ---- Pass status to downstream tasks ------------------------
dbutils.jobs.taskValues.set("validation_status", overall_status)
dbutils.jobs.taskValues.set("high_severity_count", high_severity_issues)

log_summary(spark, "data_validation",
            total=sum(r["count"] for r in results),
            valid=0, invalid=high_severity_issues,
            archived=0,
            process_log_table=TABLES["process_log"])
