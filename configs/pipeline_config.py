# ============================================================
# Pipeline Configuration
# Ecommerce Incremental Data Load - Databricks ETL Pipeline
# ============================================================
# How to use:
#   from configs.pipeline_config import CATALOG, SCHEMA, get_table, get_paths
# ============================================================

CATALOG = "accenture"
SCHEMA  = "hemanth"

# --------------- Volume / Storage Paths ---------------------
BASE_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/incremental_load"

PATHS = {
    "orders":    {"source": f"{BASE_VOLUME}/orders_data/source/",    "archive": f"{BASE_VOLUME}/orders_data/archive/"},
    "customers": {"source": f"{BASE_VOLUME}/customers_data/source/", "archive": f"{BASE_VOLUME}/customers_data/archive/"},
    "products":  {"source": f"{BASE_VOLUME}/products_data/source/",  "archive": f"{BASE_VOLUME}/products_data/archive/"},
    "inventory": {"source": f"{BASE_VOLUME}/inventory_data/source/", "archive": f"{BASE_VOLUME}/inventory_data/archive/"},
    "shipping":  {"source": f"{BASE_VOLUME}/shipping_file/source/",  "archive": f"{BASE_VOLUME}/shipping_file/archive/"},
}

# --------------- Table Names --------------------------------
def _tbl(name):
    return f"`{CATALOG}`.{SCHEMA}.{name}"

TABLES = {
    # Staging
    "orders_stage":     _tbl("orders_stage"),
    "customers_stage":  _tbl("customers_stage"),
    "products_stage":   _tbl("products_stage"),
    "inventory_stage":  _tbl("inventory_stage"),
    "shipping_stage":   _tbl("shipping_stage"),
    # Error
    "orders_errors":    _tbl("orders_errors"),
    "customers_errors": _tbl("customers_errors"),
    "products_errors":  _tbl("products_errors"),
    "inventory_errors": _tbl("inventory_errors"),
    "shipping_errors":  _tbl("shipping_errors"),
    # Enriched / Analytics
    "enriched_orders":       _tbl("enriched_orders"),
    "customer_analytics":    _tbl("customer_analytics"),
    "product_analytics":     _tbl("product_analytics"),
    # Validation
    "validation_results":    _tbl("validation_results"),
    # Target (SCD2)
    "orders_target":    _tbl("orders_target"),
    "customers_target": _tbl("customers_target"),
    "products_target":  _tbl("products_target"),
    "inventory_target": _tbl("inventory_target"),
    "shipping_target":  _tbl("shipping_target"),
    # Summary / Analytics
    "analytics_summary":  _tbl("analytics_summary"),
    "seasonal_analysis":  _tbl("seasonal_analysis"),
    "segment_analysis":   _tbl("segment_analysis"),
    "category_analysis":  _tbl("category_analysis"),
    # Monitoring
    "process_log": _tbl("process_log"),
}

# --------------- Read Options --------------------------------
CSV_READ_OPTIONS = {
    "header":          True,
    "dateFormat":      "yyyy-MM-dd",
    "timestampFormat": "yyyy-MM-dd HH:mm:ss",
}

# --------------- Business Rules ------------------------------
BUSINESS_RULES = {
    "max_order_amount":       10000,
    "min_order_amount":       1,
    "max_shipping_cost":      100,
    "premium_min_order":      100,       # premium customers expected order value
    "business_hours_start":   8,
    "business_hours_end":     18,
    "audit_overdue_days":     90,
    "audit_due_soon_days":    60,
}
