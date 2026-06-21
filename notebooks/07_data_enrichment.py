# Databricks notebook source
# ============================================================
# 07_data_enrichment.py
# Joins all stage tables → enriched_orders, customer_analytics, product_analytics
# ============================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/<your-repo>/ecomm-etl-pipeline")

from pyspark.sql import functions as F
from datetime import datetime
import json

from configs.pipeline_config import TABLES
from utils.pipeline_utils import log_summary

# COMMAND ----------
# ---- Load stage tables --------------------------------------
try:
    df_orders    = spark.read.table(TABLES["orders_stage"])
    df_customers = spark.read.table(TABLES["customers_stage"])
    df_products  = spark.read.table(TABLES["products_stage"])
    df_inventory = spark.read.table(TABLES["inventory_stage"])
    df_shipping  = spark.read.table(TABLES["shipping_stage"])
    print("All staging tables loaded for enrichment.")
except Exception as e:
    print(f"Error loading staging tables: {e}"); raise

# COMMAND ----------
# ---- Rename conflicting columns before joins ----------------
try:
    # Customers
    df_customers_r = (df_customers
        .withColumnRenamed("created_timestamp",  "customer_created_timestamp")
        .withColumnRenamed("batch_id",           "customer_batch_id")
        .withColumnRenamed("processed_timestamp","customer_processed_timestamp")
        .withColumnRenamed("source_system",      "customer_source_system")
        .withColumnRenamed("lifecycle_stage",    "customer_lifecycle_stage"))

    # Products
    df_products_r = (df_products
        .withColumnRenamed("created_timestamp",  "product_created_timestamp")
        .withColumnRenamed("batch_id",           "product_batch_id")
        .withColumnRenamed("processed_timestamp","product_processed_timestamp")
        .withColumnRenamed("source_system",      "product_source_system")
        .withColumnRenamed("currency",           "product_currency")
        .withColumnRenamed("weight_kg",          "product_weight_kg")
        .withColumnRenamed("lifecycle_stage",    "product_lifecycle_stage")
        .withColumnRenamed("stock_quantity",     "product_stock_quantity")
        .withColumnRenamed("stock_status",       "product_stock_status")
        .withColumnRenamed("price_segment",      "product_price_segment"))

    # Inventory
    df_inventory_r = (df_inventory
        .withColumnRenamed("created_timestamp",  "inventory_created_timestamp")
        .withColumnRenamed("batch_id",           "inventory_batch_id")
        .withColumnRenamed("processed_timestamp","inventory_processed_timestamp")
        .withColumnRenamed("source_system",      "inventory_source_system")
        .withColumnRenamed("stock_status",       "inventory_stock_status")
        .withColumnRenamed("stock_quantity",     "inventory_stock_quantity"))

    # Shipping
    df_shipping_r = (df_shipping
        .withColumnRenamed("created_timestamp",  "shipping_created_timestamp")
        .withColumnRenamed("batch_id",           "shipping_batch_id")
        .withColumnRenamed("processed_timestamp","shipping_processed_timestamp")
        .withColumnRenamed("source_system",      "shipping_source_system")
        .withColumnRenamed("currency",           "shipping_currency"))

    print("Column renaming complete.")
except Exception as e:
    print(f"Error renaming columns: {e}"); raise

# COMMAND ----------
# ---- Build enriched orders ----------------------------------
try:
    df_enriched = (df_orders
        .join(df_customers_r, "customer_id", "left")
        .join(df_products_r,  "product_id",  "left")
        .join(df_inventory_r.select("product_id","inventory_stock_status","inventory_stock_quantity","reorder_level"),
              "product_id", "left")
        .join(df_shipping_r.select("order_id","carrier","shipping_status","delivery_performance",
                                   "delivery_days","estimated_delivery_days","cost_per_kg",
                                   "shipping_cost","shipping_currency"),
              "order_id", "left"))

    # Order-level enrichment
    df_enriched = (df_enriched
        .withColumn("order_profit_margin",
            F.col("order_amount") - F.col("price"))
        .withColumn("estimated_clv",
            F.col("order_amount") * F.lit(12))           # simplified 12-month CLV proxy
        .withColumn("season",
            F.when(F.month("order_date").isin(12, 1, 2), "Winter")
             .when(F.month("order_date").isin(3, 4, 5),  "Spring")
             .when(F.month("order_date").isin(6, 7, 8),  "Summer")
             .otherwise("Autumn"))
        .withColumn("day_of_week", F.dayofweek("order_date"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
        .withColumn("time_of_day",
            F.when(F.hour("created_timestamp") < 12, "Morning")
             .when(F.hour("created_timestamp") < 17, "Afternoon")
             .when(F.hour("created_timestamp") < 21, "Evening")
             .otherwise("Night"))
    )
    print(f"Enriched orders: {df_enriched.count()} records")
except Exception as e:
    print(f"Error creating enriched orders: {e}"); raise

# COMMAND ----------
# ---- Customer analytics -------------------------------------
try:
    df_customer_analytics = (df_enriched
        .groupBy("customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_spent"),
            F.avg("order_amount").alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("product_id").alias("unique_products_purchased"),
            F.countDistinct("category").alias("unique_categories_purchased"),
            F.sum("order_profit_margin").alias("total_profit_generated"),
            F.avg("estimated_clv").alias("avg_estimated_clv"),
        )
        .join(df_customers_r, "customer_id", "left")
        .withColumn("days_since_first_order", F.datediff(F.current_date(), F.col("first_order_date")))
        .withColumn("days_since_last_order",  F.datediff(F.current_date(), F.col("last_order_date")))
        .withColumn("order_frequency_days",
            F.col("days_since_first_order") / F.col("total_orders"))
        .withColumn("customer_segment",
            F.when((F.col("total_spent") >= 1000) & (F.col("total_orders") >= 5), "VIP")
             .when((F.col("total_spent") >= 500)  & (F.col("total_orders") >= 3), "High Value")
             .when((F.col("total_spent") >= 200)  & (F.col("total_orders") >= 2), "Medium Value")
             .otherwise("Low Value"))
        .withColumn("lifecycle_stage",
            F.when(F.col("days_since_last_order") <= 30,  "Active")
             .when(F.col("days_since_last_order") <= 90,  "At Risk")
             .when(F.col("days_since_last_order") <= 180, "Inactive")
             .otherwise("Lost"))
    )
    print(f"Customer analytics: {df_customer_analytics.count()} records")
except Exception as e:
    print(f"Error creating customer analytics: {e}"); raise

# COMMAND ----------
# ---- Product analytics --------------------------------------
try:
    df_product_analytics = (df_enriched
        .groupBy("product_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum("order_profit_margin").alias("total_profit"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
        )
        .join(df_products_r, "product_id", "left")
        .withColumn("days_since_first_order", F.datediff(F.current_date(), F.col("first_order_date")))
        .withColumn("days_since_last_order",  F.datediff(F.current_date(), F.col("last_order_date")))
        .withColumn("revenue_per_customer", F.col("total_revenue") / F.col("unique_customers"))
        .withColumn("performance_category",
            F.when((F.col("total_revenue") >= 5000) & (F.col("total_orders") >= 20), "Star")
             .when((F.col("total_revenue") >= 2000) & (F.col("total_orders") >= 10), "High Performer")
             .when((F.col("total_revenue") >= 500)  & (F.col("total_orders") >= 5),  "Medium Performer")
             .otherwise("Low Performer"))
        .withColumn("product_lifecycle",
            F.when(F.col("days_since_last_order") <= 30,    "Active")
             .when(F.col("days_since_last_order") <= 90,    "Declining")
             .when(F.col("discontinued") == True,           "Discontinued")
             .otherwise("Stagnant"))
    )
    print(f"Product analytics: {df_product_analytics.count()} records")
except Exception as e:
    print(f"Error creating product analytics: {e}"); raise

# COMMAND ----------
# ---- Write enriched tables ----------------------------------
try:
    df_enriched.write.format("delta").mode("overwrite").saveAsTable(TABLES["enriched_orders"])
    df_customer_analytics.write.format("delta").mode("overwrite").saveAsTable(TABLES["customer_analytics"])
    df_product_analytics.write.format("delta").mode("overwrite").saveAsTable(TABLES["product_analytics"])
    print("All enriched tables written successfully.")
except Exception as e:
    print(f"Error writing enriched tables: {e}"); raise

# COMMAND ----------
dbutils.jobs.taskValues.set("enrichment_status", "SUCCESS")

log_summary(spark, "data_enrichment",
            total=df_enriched.count(), valid=df_enriched.count(),
            invalid=0, archived=0,
            process_log_table=TABLES["process_log"])
