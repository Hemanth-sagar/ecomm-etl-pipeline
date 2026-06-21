# ============================================================
# Pipeline Utilities
# Shared helper functions for the ETL pipeline
# ============================================================

import json
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType


# --------------- Logging ------------------------------------

PROCESS_LOG_SCHEMA = StructType([
    StructField("archived_files",  LongType(),  True),
    StructField("invalid_records", LongType(),  True),
    StructField("status",          StringType(), True),
    StructField("task",            StringType(), True),
    StructField("timestamp",       StringType(), True),
    StructField("total_records",   LongType(),  True),
    StructField("valid_records",   LongType(),  True),
])


def log_summary(spark: SparkSession, task: str, total: int, valid: int,
                invalid: int, archived: int, process_log_table: str) -> None:
    """Write a processing summary row to the process_log Delta table."""
    status = "SUCCESS" if invalid == 0 else "SUCCESS_WITH_WARNINGS"
    summary = {
        "task":            task,
        "timestamp":       datetime.now().isoformat(),
        "total_records":   total,
        "valid_records":   valid,
        "invalid_records": invalid,
        "archived_files":  archived,
        "status":          status,
    }
    print(f"\nProcessing Summary for [{task}]:")
    print(json.dumps(summary, indent=2))

    df = spark.createDataFrame([summary], schema=PROCESS_LOG_SCHEMA)
    df.write.format("delta").mode("append").saveAsTable(process_log_table)


# --------------- File Archiving -----------------------------

def archive_csv_files(source_dir: str, archive_dir: str, dbutils) -> int:
    """Move all CSV files from source_dir to archive_dir. Returns count archived."""
    files = dbutils.fs.ls(source_dir)
    archived_count = 0
    for f in files:
        if f.name.endswith(".csv"):
            dbutils.fs.mv(f.path, archive_dir + f.name)
            archived_count += 1
            print(f"  Archived: {f.name}")
    print(f"Total archived: {archived_count} file(s)")
    return archived_count


# --------------- Data Quality Helpers -----------------------

def write_valid_and_errors(df_valid: DataFrame, df_invalid: DataFrame,
                           stage_table: str, error_table: str,
                           entity: str, invalid_count: int) -> None:
    """Write valid records to stage table; log invalid records to error table."""
    df_valid.write.format("delta").mode("overwrite").saveAsTable(stage_table)
    print(f"Loaded {df_valid.count()} valid {entity} records -> {stage_table}")

    if invalid_count > 0:
        (df_invalid
         .withColumn("error_reason",    F.lit("Data quality validation failed"))
         .withColumn("error_timestamp", F.current_timestamp())
         .write.format("delta").mode("append").saveAsTable(error_table))
        print(f"Logged {invalid_count} invalid {entity} records -> {error_table}")


# --------------- SCD2 Merge Helper --------------------------

def scd2_merge(spark: SparkSession, df_new: DataFrame, target_table: str,
               id_col: str) -> None:
    """
    Lightweight SCD2 helper using DeltaTable API.
    - Expires existing current records whose ID appears in df_new.
    - Appends all rows from df_new as new current records.
    """
    from delta.tables import DeltaTable

    df_new = (df_new
              .withColumn("effective_date", F.current_date())
              .withColumn("expiry_date",    F.lit(None).cast("date"))
              .withColumn("is_current",     F.lit(True)))

    if spark.catalog.tableExists(target_table):
        # Collect IDs to expire  — OK for moderate volumes;
        # for large datasets prefer a MERGE statement instead.
        ids = [row[id_col] for row in df_new.select(id_col).distinct().collect()]
        target = DeltaTable.forName(spark, target_table)
        target.update(
            condition=F.col(id_col).isin(ids) & F.col("is_current"),
            set={"expiry_date": F.current_date(), "is_current": F.lit(False)},
        )
        df_new.write.format("delta").mode("append").saveAsTable(target_table)
    else:
        df_new.write.format("delta").saveAsTable(target_table)

    print(f"SCD2 merge complete for {target_table}")

