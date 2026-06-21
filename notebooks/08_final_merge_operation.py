# Databricks notebook source
# ============================================================
# 08_final_merge_operation.py
# SCD2 merge into target tables + analytics summary generation
# ============================================================

# COMMAND ----------
import sys
sys.path.append("/Workspace/Repos/<your-repo>/ecomm-etl-pipeline")

from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from datetime import datetime
import json

from configs.pipeline_config import TABLES
from utils.pipeline_utils import scd2_merge, log_summary

# COMMAND ----------
# ---- Load enriched tables -----------------------------------
try:
    df_enriched_orders     = spark.read.table(TABLES["enriched_orders"])
    df_customer_analytics  = spark.read.table(TABLES["customer_analytics"])
    df_product_analytics   = spark.read.table(TABLES["product_analytics"])

    print(f"Enriched orders   : {df_enriched_orders.count()}")
    print(f"Customer analytics: {df_customer_analytics.count()}")
    print(f"Product analytics : {df_product_analytics.count()}")
except Exception as e:
    print(f"Error loading enriched tables: {e}"); raise

# COMMAND ----------
# ---- SCD2 Merge: Orders -------------------------------------
try:
    df_orders_merge = df_enriched_orders.select(
        "order_id", "customer_id", "product_id", "order_date", "order_amount",
        "currency", "payment_method", "shipping_address", "order_status",
        "created_timestamp", "processed_timestamp", "batch_id", "source_system",
        "order_profit_margin", "estimated_clv", "season", "day_of_week",
        "is_weekend", "time_of_day"
    )
    scd2_merge(spark, df_orders_merge, TABLES["orders_target"], id_col="order_id")
    print("Orders SCD2 merge complete.")
except Exception as e:
    print(f"Error merging orders: {e}"); raise

# COMMAND ----------
# ---- SCD2 Merge: Customers ----------------------------------
try:
    df_customers_merge = df_customer_analytics.select(
        "customer_id", "first_name", "last_name", "email", "phone",
        "date_of_birth", "registration_date", "address", "city", "state",
        "zip_code", "country", "customer_tier", "last_login",
        "customer_created_timestamp", "age", "age_segment",
        "days_since_registration", "lifecycle_stage",
        "total_orders", "total_spent", "avg_order_value", "customer_segment"
    )
    scd2_merge(spark, df_customers_merge, TABLES["customers_target"], id_col="customer_id")
    print("Customers SCD2 merge complete.")
except Exception as e:
    print(f"Error merging customers: {e}"); raise

# COMMAND ----------
# ---- SCD2 Merge: Products -----------------------------------
try:
    df_products_merge = df_product_analytics.select(
        "product_id", "product_name", "category", "subcategory", "brand",
        "price", "product_currency", "product_stock_quantity", "product_weight_kg",
        "dimensions_cm", "color", "material", "description", "launch_date",
        "discontinued", "product_created_timestamp", "product_price_segment",
        "product_stock_status", "days_since_launch", "product_lifecycle_stage",
        "volume_cm3", "density_kg_cm3",
        "total_orders", "total_revenue", "unique_customers",
        "performance_category", "product_lifecycle"
    )
    scd2_merge(spark, df_products_merge, TABLES["products_target"], id_col="product_id")
    print("Products SCD2 merge complete.")
except Exception as e:
    print(f"Error merging products: {e}"); raise

# COMMAND ----------
# Disable strict ANSI so NULL arithmetic doesn't raise errors
spark.conf.set("spark.sql.ansi.enabled", "false")

# COMMAND ----------
# ---- Analytics Summary Dashboard ----------------------------
try:
    analytics_summary = (df_enriched_orders
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_amount").alias("total_revenue"),
            F.avg("order_amount").alias("avg_order_value"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.countDistinct("product_id").alias("unique_products"),
            F.sum("order_profit_margin").alias("total_profit"),
            F.avg("estimated_clv").alias("avg_estimated_clv"),
        )
        .withColumn("profit_margin_pct",
            F.col("total_profit") / F.col("total_revenue") * 100)
        .withColumn("revenue_per_customer",
            F.col("total_revenue") / F.col("unique_customers"))
        .withColumn("orders_per_customer",
            F.col("total_orders") / F.col("unique_customers"))
        .withColumn("report_date",      F.current_date())
        .withColumn("report_timestamp", F.current_timestamp())
    )

    seasonal_analysis = (df_enriched_orders.groupBy("season")
        .agg(F.count("order_id").alias("orders_count"),
             F.sum("order_amount").alias("seasonal_revenue"),
             F.avg("order_amount").alias("avg_seasonal_order_value")))

    segment_analysis = (df_customer_analytics.groupBy("customer_segment")
        .agg(F.count("customer_id").alias("customers_count"),
             F.sum("total_spent").alias("segment_revenue"),
             F.avg("total_spent").alias("avg_segment_value")))

    category_analysis = (df_product_analytics.groupBy("category")
        .agg(F.count("product_id").alias("products_count"),
             F.sum("total_revenue").alias("category_revenue"),
             F.avg("total_revenue").alias("avg_category_revenue")))

    # Write summaries
    analytics_summary.write.format("delta").mode("overwrite").saveAsTable(TABLES["analytics_summary"])
    seasonal_analysis.write.format("delta").mode("overwrite").saveAsTable(TABLES["seasonal_analysis"])
    segment_analysis.write.format("delta").mode("overwrite").saveAsTable(TABLES["segment_analysis"])
    category_analysis.write.format("delta").mode("overwrite").saveAsTable(TABLES["category_analysis"])

    final_stats = analytics_summary.collect()[0]
    print("\n=== Final Analytics Summary ===")
    print(f"Total Orders     : {final_stats['total_orders']}")
    print(f"Total Revenue    : ${float(final_stats['total_revenue'] or 0):,.2f}")
    print(f"Avg Order Value  : ${float(final_stats['avg_order_value'] or 0):,.2f}")
    print(f"Unique Customers : {final_stats['unique_customers']}")
    print(f"Unique Products  : {final_stats['unique_products']}")
    print(f"Total Profit     : ${float(final_stats['total_profit'] or 0):,.2f}")
    print(f"Profit Margin    : {float(final_stats['profit_margin_pct'] or 0):.2f}%")
    print(f"Revenue/Customer : ${float(final_stats['revenue_per_customer'] or 0):,.2f}")
    print(f"Orders/Customer  : {float(final_stats['orders_per_customer'] or 0):.2f}")

except Exception as e:
    print(f"Error creating analytics summary: {e}"); raise

# COMMAND ----------
# ---- Pass final values downstream ---------------------------
dbutils.jobs.taskValues.set("final_merge_status", "SUCCESS")
dbutils.jobs.taskValues.set("total_revenue", float(final_stats["total_revenue"] or 0))
dbutils.jobs.taskValues.set("total_orders",  int(final_stats["total_orders"]  or 0))

log_summary(spark, "final_merge_operation",
            total=int(final_stats["total_orders"] or 0),
            valid=int(final_stats["total_orders"] or 0),
            invalid=0, archived=0,
            process_log_table=TABLES["process_log"])

print("\n Event-driven pipeline completed successfully!")
