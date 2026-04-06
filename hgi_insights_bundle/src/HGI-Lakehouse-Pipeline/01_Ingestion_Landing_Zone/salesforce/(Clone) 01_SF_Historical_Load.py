# Databricks notebook source
# =============================================================================
# Notebook  : 01_SF_Historical_Load  [UPDATED for Lakehouse Connect]
# Location  : /HGI-Lakehouse-Pipeline/01_Ingestion/01_SF_Historical_Load
#
# WHAT CHANGED vs previous version:
#   ✅ REMOVED watermark table write
#      Why: Salesforce Lakehouse Connect (LC) manages CDC automatically.
#           LC keeps salesforce.* catalog tables in sync with Salesforce
#           using SF Change Data Capture APIs. There is no "last seen timestamp"
#           to track on the SF side — the catalog table IS always current.
#           Watermark was only needed if YOU were polling SF for changes.
#           Since LC does the polling, you just snapshot what's there.
#
#   ✅ KEPT landing zone write
#      Why: Even though LC provides a live mirror, writing to S3 parquet is
#           still needed so the Bronze Autoloader has files to pick up.
#           It also provides an audit trail and replay capability.
#
#   ✅ ADDED pipeline metrics (PipelineMetrics.init + .save)
#      Writes to ingestion_metadata.pipeline_metrics_stats Delta table.
#      Concurrent-safe (MERGE on UUID run_id). Non-blocking (<100ms overhead).
#
# HOW THIS FITS THE FULL FLOW:
#   SF Org → [LC auto-sync] → salesforce.{cust}.{Object} (Unity Catalog)
#     → [THIS NOTEBOOK — one-time snapshot] → S3 landing-zone/.../historical/
#       → [Bronze Autoloader] → bronze.{cust}.crm_{object}
#         → [Silver CDF] → hgi.silver.{object}
#
# WHEN TO RUN: Once per object, per customer. One-time bootstrapping.
# =============================================================================

import sys
import os
from datetime import datetime
from pyspark.sql.functions import col, date_format, max as spark_max

# COMMAND ----------

# CELL 1 — Widgets
dbutils.widgets.text("customer_id",      "")
dbutils.widgets.text("object_name",      "")
dbutils.widgets.text("source_system",    "salesforce")
dbutils.widgets.text("sf_catalog_table", "")
dbutils.widgets.text("parent_run_id",    "manual")

customer_id       = dbutils.widgets.get("customer_id").strip()
object_name       = dbutils.widgets.get("object_name").strip()
source_system     = dbutils.widgets.get("source_system").strip()
sf_catalog_table  = dbutils.widgets.get("sf_catalog_table").strip()
parent_run_id     = dbutils.widgets.get("parent_run_id").strip()
load_type         = "historical"

if not customer_id or not object_name or not sf_catalog_table:
    raise Exception("❌ Missing: customer_id, object_name, sf_catalog_table")

# COMMAND ----------

# DBTITLE 1,Load pipeline config
# Config: single source of truth (replaces DataLakeConfig import)
%run ../../config/pipeline_config

# COMMAND ----------

# DBTITLE 1,Load audit logger
# Load audit logger for error-only logging
%run ../../utils/audit_logger

# COMMAND ----------

# CELL 2 — Metrics init (config loaded via %run in previous cell)
project_root = "/Workspace/Users/ayush.gunjal@hginsights.com/HGI-Lakehouse-Pipeline"
if os.path.abspath(project_root) not in sys.path:
    sys.path.append(os.path.abspath(project_root))

from utils.pipeline_metrics import PipelineMetrics

# ── Init metrics (writes RUNNING row immediately) ───────────────────────────
pm = PipelineMetrics(
    spark         = spark,
    parent_run_id = parent_run_id,
    job_name      = "SF_Historical_Load",
    task_key      = "sf_historical",
    source_system = source_system,
    load_type     = load_type,
    customer_id   = customer_id,
    object_name   = object_name,
)
pm.init()   # <100ms — writes RUNNING status immediately

# Ensure audit tables exist for error logging
initialize_audit_tables()

# COMMAND ----------

# CELL 3 — Spark performance config (Serverless-safe)
def safe_spark_conf(configs: dict):
    for k, v in configs.items():
        try:
            spark.conf.set(k, v)
        except Exception:
            pass

safe_spark_conf({
    "spark.sql.shuffle.partitions":                          "32",
    "spark.sql.adaptive.enabled":                            "true",
    "spark.sql.adaptive.coalescePartitions.enabled":         "true",
    "spark.sql.adaptive.coalescePartitions.minPartitionNum": "1",
    "spark.sql.adaptive.skewJoin.enabled":                   "true",
    "spark.hadoop.fs.s3a.connection.maximum":                "200",
    "spark.hadoop.fs.s3a.fast.upload":                       "true",
    "spark.hadoop.fs.s3a.multipart.size":                    "134217728",
})

num_cores = 4  # Serverless manages parallelism; default to 4
print("Spark config applied (unsupported configs silently skipped on Serverless)")

# COMMAND ----------

# CELL 4 — S3 target path (using landing_path() from pipeline_config)
now = datetime.now()
historical_s3_path = f"{landing_path(source_system, customer_id, object_name, load_type)}{now.strftime('%Y/%m/%d/%H%M%S')}/"

print(f"🚀 SF Historical Load (Lakehouse Connect snapshot)")
print(f"   Customer   : {customer_id}")
print(f"   Object     : {object_name}")
print(f"   Source     : {sf_catalog_table}")
print(f"   S3 Target  : {historical_s3_path}")
print(f"   run_id     : {pm.run_id}")
print()
print(f"   Version watermark will be recorded after write completes.")

# COMMAND ----------

# CELL 5 — Read → transform → write (single pipeline pass)
total_rows = 0
t_start = datetime.now()  # Track duration even on failure

try:
    df_raw    = spark.table(sf_catalog_table)

    # Coalesce avoids shuffle; target ~128MB output files
    target_partitions = max(num_cores * 4, 32)

    (
        df_raw
        .withColumn(
            "SystemModstamp",
            date_format(col("SystemModstamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        .coalesce(target_partitions)
        .write
        .mode("append")
        .format("parquet")
        .option("compression",       "snappy")
        .option("maxRecordsPerFile", "3000000")
        .save(historical_s3_path)
    )

    elapsed    = (datetime.now() - t_start).total_seconds()
    total_rows = spark.read.parquet(historical_s3_path).count()

    print(f"✅ Written {total_rows:,} rows in {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"   Throughput: {total_rows/elapsed:,.0f} records/sec")

    # ── Save SUCCESS metrics ──
    pm.save(status="SUCCESS", rows_processed=total_rows)

except Exception as e:
    elapsed_ms = int((datetime.now() - t_start).total_seconds() * 1000)
    print(f"❌ Pipeline Failed: {e}")
    # ✅ Error-only audit logging — log to bronze.logs.audit_logs
    log_audit(
        job_name       = "SF_Historical_Load",
        customer_id    = customer_id,
        status         = "failure",
        layer          = "bronze",
        alert_type     = "FAILURE",
        error_type     = type(e).__name__,
        error_reason   = str(e)[:500],
        duration_ms    = elapsed_ms,
    )
    pm.save(status="FAILED", error_reason=str(e))
    raise

# COMMAND ----------

# DBTITLE 1,Record version watermark
# CELL 5b — Record Delta version watermark for incremental load
# Without this, the incremental notebook starts from version 0,
# CDF fails (logs vacuumed beyond 168hr retention), and the fallback
# reads ALL rows — causing duplicates in the landing zone.

from datetime import datetime as _dt

try:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ingestion_metadata.sf_version_watermarks (
            source_system            STRING NOT NULL,
            customer_id              STRING NOT NULL,
            object_name              STRING NOT NULL,
            last_processed_version   BIGINT,
            last_processed_at        TIMESTAMP
        ) USING DELTA
    """)

    # Get the current version of the source table AFTER the snapshot was taken
    hist = spark.sql(f"DESCRIBE HISTORY {sf_catalog_table} LIMIT 1").collect()
    current_version = hist[0]["version"] if hist else 0

    spark.sql(f"""
        MERGE INTO ingestion_metadata.sf_version_watermarks AS tgt
        USING (
            SELECT
                '{source_system}'      AS source_system,
                '{customer_id}'        AS customer_id,
                '{object_name}'        AS object_name,
                {current_version}      AS last_processed_version,
                TIMESTAMP('{_dt.now()}') AS last_processed_at
        ) AS src
        ON  tgt.source_system = src.source_system
        AND tgt.customer_id   = src.customer_id
        AND tgt.object_name   = src.object_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"✅ Version watermark set to {current_version} for {source_system}/{customer_id}/{object_name}")
    print(f"   Next incremental will start from version {current_version + 1}")

except Exception as e:
    print(f"❌ Version watermark update failed: {e}")
    # ✅ Error-only audit logging
    log_audit(
        job_name       = "SF_Historical_Load",
        customer_id    = customer_id,
        status         = "failure",
        layer          = "bronze",
        alert_type     = "FAILURE",
        error_type     = type(e).__name__,
        error_reason   = f"Watermark update failed: {str(e)[:450]}",
    )
    raise

# COMMAND ----------

# CELL 6 — Summary
print(f"\n{'='*55}")
print(f"  SF Historical Snapshot COMPLETE")
print(f"  Object   : {object_name}")
print(f"  Records  : {total_rows:,}")
print(f"  S3 Path  : {historical_s3_path}")
print(f"  run_id   : {pm.run_id}")
print(f"{'='*55}")
print()
print("⚡ Next step: Bronze Autoloader will pick up files from the S3 path above.")
print("   No further action needed — LC handles all future incremental changes.")


