# Databricks notebook source
# =============================================================================
# Notebook  : 02_SF_Incremental_Load  [UPDATED for Lakehouse Connect]
# Location  : /HGI-Lakehouse-Pipeline/01_Ingestion/02_SF_Incremental_Load
#
# KEY ARCHITECTURAL CHANGE — Why watermark is no longer needed:
#
#   Salesforce Lakehouse Connect (LC) keeps the salesforce.* Unity Catalog
#   tables in sync using Salesforce Change Data Capture (CDC) APIs.
#   This means:
#
#   BEFORE (old approach):
#     - You polled Salesforce by filtering SystemModstamp > last_watermark
#     - You stored the watermark yourself in ingestion_metadata.watermarks
#     - Problem: you were doing CDC yourself on top of LC which already does CDC
#     - Double CDC = unnecessary complexity + potential gaps if watermark drifts
#
#   NOW (LC-aware approach):
#     - LC has already applied all inserts/updates to the salesforce.* tables
#     - The salesforce.* tables are Delta tables — they have DESCRIBE HISTORY
#     - Incremental = "what Delta versions have not been sent to S3 yet?"
#     - We track the LAST PROCESSED DELTA VERSION (not a timestamp)
#     - This is more reliable: version-based = exactly-once, no timestamp drift
#
# HOW VERSION-BASED WATERMARK WORKS:
#   1. Read current Delta version of the LC catalog table
#   2. Look up last_processed_version from ingestion_metadata.sf_version_watermarks
#   3. Read CDF (Change Data Feed) between (last_version, current_version]
#   4. Write changed rows to S3 landing zone
#   5. Update last_processed_version = current_version
#
#   First run (no watermark): reads from version 0 (full history via CDF)
#
# CONCURRENCY NOTE:
#   Multiple objects run in parallel (Account, Contact, Opportunity, etc.)
#   Version watermarks are keyed on (source_system, customer_id, object_name)
#   → MERGE is concurrent-safe for different objects.
#
# PIPELINE METRICS:
#   PipelineMetrics.init() called at start → writes RUNNING row
#   PipelineMetrics.save() called at end → updates with final stats
# =============================================================================

import sys
import os
from datetime import datetime
from pyspark.sql.functions import col, date_format
from pyspark.sql import functions as F

# COMMAND ----------

# CELL 1 — Widgets
dbutils.widgets.text("customer_id",      "")
dbutils.widgets.text("object_name",      "")
dbutils.widgets.text("source_system",    "salesforce")
dbutils.widgets.text("sf_catalog_table", "")
dbutils.widgets.text("parent_run_id",    "")

customer_id       = dbutils.widgets.get("customer_id").strip()
object_name       = dbutils.widgets.get("object_name").strip()
source_system     = dbutils.widgets.get("source_system").strip()
sf_catalog_table  = dbutils.widgets.get("sf_catalog_table").strip()
parent_run_id     = dbutils.widgets.get("parent_run_id").strip()
load_type         = "incremental"

if not all([customer_id, object_name, sf_catalog_table]):
    raise Exception("❌ Missing required parameters.")

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

pm = PipelineMetrics(
    spark         = spark,
    parent_run_id = parent_run_id,
    job_name      = "SF_Incremental_Load",
    task_key      = "run_job_B_sf_ingestion",
    source_system = source_system,
    load_type     = load_type,
    customer_id   = customer_id,
    object_name   = object_name,
)
pm.init()   # writes RUNNING immediately

# Ensure audit tables exist for error logging
initialize_audit_tables()

# COMMAND ----------

# CELL 3 — Spark config (Serverless-safe)
# SF incremental batches are small — lightweight config
def safe_spark_conf(configs: dict):
    for k, v in configs.items():
        try:
            spark.conf.set(k, v)
        except Exception:
            pass   # Serverless manages this internally

safe_spark_conf({
    "spark.sql.shuffle.partitions":                    "8",
    "spark.sql.adaptive.enabled":                      "true",
    "spark.sql.adaptive.coalescePartitions.enabled":   "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "67108864",
    "spark.hadoop.fs.s3a.fast.upload":                 "true",
    "spark.hadoop.fs.s3a.multipart.size":              "67108864",
})

print("✅ Spark config applied (unsupported configs silently skipped on Serverless)")

# COMMAND ----------

# CELL 4 — Version watermark table (replaces timestamp-based watermarks)
#
# This is a SEPARATE table from ingestion_metadata.watermarks.
# It tracks the Delta table version, not a timestamp.
# Key: (source_system, customer_id, object_name) — unique per object per tenant.

spark.sql("""
    CREATE TABLE IF NOT EXISTS ingestion_metadata.sf_version_watermarks (
        source_system            STRING NOT NULL,
        customer_id              STRING NOT NULL,
        object_name              STRING NOT NULL,
        last_processed_version   BIGINT,
        last_processed_at        TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
""")

# Look up last processed version for this object
wm_rows = spark.sql(f"""
    SELECT last_processed_version
    FROM   ingestion_metadata.sf_version_watermarks
    WHERE  source_system = '{source_system}'
      AND  customer_id   = '{customer_id}'
      AND  object_name   = '{object_name}'
""").collect()

last_version = wm_rows[0]["last_processed_version"] if wm_rows else None
print(f"📍 Last processed Delta version: {last_version if last_version is not None else 'NONE (first run)'}")

# COMMAND ----------

# CELL 5 — Get current Delta version of the LC catalog table
try:
    hist = spark.sql(f"DESCRIBE HISTORY {sf_catalog_table} LIMIT 1").collect()
    current_version = hist[0]["version"] if hist else 0
    print(f"📍 Current Delta version of {sf_catalog_table}: {current_version}")
except Exception as e:
    print(f"❌ Could not read table history: {e}")
    # ✅ Error-only audit logging
    log_audit(
        job_name       = "SF_Incremental_Load",
        customer_id    = customer_id,
        status         = "failure",
        layer          = "bronze",
        alert_type     = "FAILURE",
        error_type     = type(e).__name__,
        error_reason   = f"Could not read table history: {str(e)[:450]}",
    )
    pm.save(status="FAILED", error_reason=f"Could not read table history: {e}")
    raise

# Nothing new?
if last_version is not None and current_version <= last_version:
    print(f"✅ No new versions since {last_version}. Nothing to process.")
    pm.save(status="SUCCESS", rows_processed=0)
    dbutils.notebook.exit("0")

# COMMAND ----------

# CELL 6 — Read changed rows via Delta CDF
#
# CDF reads exactly the rows that LC added/updated since last_version.
# This is more precise than SystemModstamp filtering:
#   - No timestamp drift
#   - No re-reading unchanged rows
#   - Handles deletes if needed (change_type = 'delete')
#
# starting_version: if first run, start from 0 (full history)
#                   otherwise start from last_version + 1

starting_version = 0 if last_version is None else last_version + 1

print(f"Reading CDF from version {starting_version} to {current_version}...")

try:
    df_changes = (
        spark.read
        .format("delta")
        .option("readChangeFeed",  "true")
        .option("startingVersion", starting_version)
        .option("endingVersion",   current_version)
        .table(sf_catalog_table)
    )

    # Keep only inserts and updates (latest image) — skip pre-images and deletes
    # for the landing zone (bronze will handle deletes via its own merge logic)
    df_incremental = df_changes.filter(
        F.col("_change_type").isin("insert", "update_postimage")
    ).drop("_change_type", "_commit_version", "_commit_timestamp")

    record_count = df_incremental.count()
    dbutils.jobs.taskValues.set(key=object_name, value=record_count)
    print(f"📊 Changed records (via CDF): {record_count:,}")

except Exception as e:
    # CDF might not be enabled on the LC table — fall back to SystemModstamp
    print(f"⚠️  CDF read failed ({e}). Falling back to SystemModstamp filter.")

    if last_version is None:
        # First run — read everything
        df_incremental = spark.table(sf_catalog_table)
    else:
        # Get the commit timestamp for the last processed version
        last_commit_rows = spark.sql(f"""
            SELECT timestamp FROM (DESCRIBE HISTORY {sf_catalog_table})
            WHERE version = {last_version}
        """).collect()

        if not last_commit_rows:
            df_incremental = spark.table(sf_catalog_table)
        else:
            ts = last_commit_rows[0]["timestamp"]
            df_incremental = spark.sql(f"""
                SELECT * FROM {sf_catalog_table}
                WHERE SystemModstamp > TIMESTAMP('{ts}')
            """)

    record_count = df_incremental.count()
    print(f"📊 Fallback changed records: {record_count:,}")

if record_count == 0:
    dbutils.jobs.taskValues.set(key=object_name, value=0)
    print("✅ No changed records found. Exiting.")
    pm.save(status="SUCCESS", rows_processed=0)
    dbutils.notebook.exit("0")

# COMMAND ----------

# CELL 7 — Write to S3 landing zone (using landing_path() from pipeline_config)
now = datetime.now()
incremental_path = f"{landing_path(source_system, customer_id, object_name, load_type)}{now.strftime('%Y/%m/%d/%H%M%S')}/"

print(f"Writing {record_count:,} records to: {incremental_path}")

try:
    # For incremental, data is small — coalesce to avoid tiny files
    # ~10k rows per file for SF objects; max 8 files
    output_partitions = max(1, min(8, record_count // 10_000))

    (
        df_incremental
        .withColumn(
            "SystemModstamp",
            date_format(col("SystemModstamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        )
        .coalesce(output_partitions)
        .write
        .mode("append")
        .format("parquet")
        .option("compression", "snappy")
        .save(incremental_path)
    )
    print(f"✅ Write complete ({output_partitions} file(s))")

except Exception as e:
    print(f"❌ S3 write failed: {e}")
    # ✅ Error-only audit logging
    log_audit(
        job_name       = "SF_Incremental_Load",
        customer_id    = customer_id,
        status         = "failure",
        layer          = "bronze",
        alert_type     = "FAILURE",
        error_type     = type(e).__name__,
        error_reason   = f"S3 write failed: {str(e)[:450]}",
    )
    pm.save(status="FAILED", error_reason=f"S3 write failed: {e}")
    raise

# COMMAND ----------

# CELL 8 — Update version watermark (Delta MERGE — concurrent-safe)
try:
    now_ts = datetime.now()

    spark.sql(f"""
        MERGE INTO ingestion_metadata.sf_version_watermarks AS tgt
        USING (
            SELECT
                '{source_system}'      AS source_system,
                '{customer_id}'        AS customer_id,
                '{object_name}'        AS object_name,
                {current_version}      AS last_processed_version,
                TIMESTAMP('{now_ts}')  AS last_processed_at
        ) AS src
        ON  tgt.source_system = src.source_system
        AND tgt.customer_id   = src.customer_id
        AND tgt.object_name   = src.object_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"✅ Version watermark updated to {current_version}")

    # ── Save metrics ──
    pm.save(status="SUCCESS", rows_processed=record_count)

    print(f"\n{'='*55}")
    print(f"  SF Incremental Load COMPLETE")
    print(f"  Object   : {object_name}  |  Customer: {customer_id}")
    print(f"  Records  : {record_count:,}")
    print(f"  Versions : {starting_version} → {current_version}")
    print(f"  S3 Path  : {incremental_path}")
    print(f"  run_id   : {pm.run_id}")
    print(f"{'='*55}")

except Exception as e:
    print(f"❌ Watermark update / metrics failed: {e}")
    # ✅ Error-only audit logging
    log_audit(
        job_name       = "SF_Incremental_Load",
        customer_id    = customer_id,
        status         = "failure",
        layer          = "bronze",
        alert_type     = "FAILURE",
        error_type     = type(e).__name__,
        error_reason   = f"Watermark/metrics failed: {str(e)[:450]}",
    )
    pm.save(status="FAILED", error_reason=str(e))
    raise
