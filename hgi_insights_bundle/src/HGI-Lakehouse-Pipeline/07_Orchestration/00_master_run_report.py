# Databricks notebook source
# ── LINE 1: Add widget for parent_run_id (top of notebook) ──
dbutils.widgets.text("parent_run_id", "")
parent_run_id = dbutils.widgets.get("parent_run_id").strip()

# COMMAND ----------

# =============================================================================
# Notebook  : 00_master_run_report
# Location  : /HGI-Lakehouse-Pipeline/06_Orchestration/00_master_run_report
# Purpose   : Always-runs final task in the master job.
#             Reads all pipeline_metrics_stats rows for this parent_run_id
#             and prints a comprehensive end-to-end pipeline report.
#
# TOTAL JOB TIME:
#   = max(end_time) - min(start_time) across all rows with this parent_run_id
#   This correctly captures the parallel execution time (not sum of all tasks).
#
# INPUT:
#   parent_run_id widget — the master job's run_id, passed via YAML:
#     "{{job.parameters.parent_run_id}}" or computed from context
# =============================================================================

import sys
import os
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("customer_id",   "cust_001")
dbutils.widgets.text("parent_run_id", "")

customer_id   = dbutils.widgets.get("customer_id").strip()
parent_run_id = dbutils.widgets.get("parent_run_id").strip()

# Auto-detect parent_run_id if not passed
if not parent_run_id:
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        parent_run_id = str(ctx.currentRunId().get())
    except Exception:
        parent_run_id = "unknown"

print(f"=== Master Run Report ===")
print(f"  customer_id   : {customer_id}")
print(f"  parent_run_id : {parent_run_id}")
print(f"  Generated at  : {datetime.now()}")
print("=" * 65)

# COMMAND ----------

source_system = "salesforce"
object_name   = "report"
load_type     = "incremental"

import sys, os
project_root = "/Workspace/Users/ayush.gunjal@hginsights.com/HGI-Lakehouse-Pipeline"
if os.path.abspath(project_root) not in sys.path:
    sys.path.append(os.path.abspath(project_root))

# ── LINE 2: Import and initialize metrics (after config imports) ──
from utils.pipeline_metrics import PipelineMetrics
pm = PipelineMetrics(
    spark          = spark,
    parent_run_id  = parent_run_id,
    job_name       = "00_master_run_report",
    task_key       = "master_run_report",
    source_system  = source_system,
    load_type      = load_type,
    customer_id    = customer_id,
    object_name    = object_name,
)
pm.init()   # ← writes RUNNING row immediately (<100ms)

# COMMAND ----------

# ── Pull all metrics for this pipeline run ────────────────────────────────────
from pyspark.sql import functions as F

METRICS_TABLE = "ingestion_metadata.pipeline_metrics_stats"

try:
    df_metrics = spark.sql(f"""
        SELECT *
        FROM {METRICS_TABLE}
        WHERE parent_run_id = '{parent_run_id}'
        ORDER BY start_time ASC
    """)
    rows = df_metrics.collect()
except Exception as e:
    print(f"⚠️  Could not read metrics table: {e}")
    rows = []

if not rows:
    print("ℹ️  No metrics rows found for this parent_run_id.")
    print(f"   (parent_run_id={parent_run_id})")
    dbutils.notebook.exit("No metrics")

# COMMAND ----------

# ── Summary table ─────────────────────────────────────────────────────────────
print(f"\n{'─'*65}")
print(f"{'TASK':<30} {'STATUS':<10} {'ROWS':>12} {'DURATION':>10} {'COST':>10}")
print(f"{'─'*65}")

total_rows = 0
total_cost = 0.0
all_failed = []

for r in rows:
    status    = r["status"] or "UNKNOWN"
    rows_proc = r["rows_processed"] or 0
    dur_secs  = r["duration_seconds"] or 0.0
    cost      = r["est_cost_usd"] or 0.0
    task      = r["task_key"] or r["job_name"] or "?"

    total_rows += rows_proc
    total_cost += cost
    if status == "FAILED":
        all_failed.append(task)

    dur_str  = f"{dur_secs:.1f}s" if dur_secs < 120 else f"{dur_secs/60:.1f}m"
    cost_str = f"${cost:.4f}"
    mark     = "✅" if status == "SUCCESS" else ("❌" if status == "FAILED" else "🔄")

    print(f"{mark} {task:<28} {status:<10} {rows_proc:>12,} {dur_str:>10} {cost_str:>10}")

# COMMAND ----------

# ── Total wall-clock time (parallel execution aware) ──────────────────────────
agg = spark.sql(f"""
    SELECT
        MIN(start_time)                                  AS pipeline_start,
        MAX(end_time)                                    AS pipeline_end,
        (unix_timestamp(MAX(end_time))
         - unix_timestamp(MIN(start_time)))              AS total_wall_seconds,
        SUM(duration_seconds)                            AS sum_task_seconds,
        SUM(COALESCE(rows_processed, 0))                 AS total_rows,
        SUM(COALESCE(est_cost_usd, 0))                   AS total_cost,
        COUNT(*)                                         AS task_count,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count
    FROM {METRICS_TABLE}
    WHERE parent_run_id = '{parent_run_id}'
""").first()

wall_secs   = agg["total_wall_seconds"] or 0.0
sum_secs    = agg["sum_task_seconds"]   or 0.0
t_rows      = agg["total_rows"]         or 0
t_cost      = agg["total_cost"]         or 0.0
task_count  = agg["task_count"]         or 0
failed_cnt  = agg["failed_count"]       or 0
p_start     = agg["pipeline_start"]
p_end       = agg["pipeline_end"]

# COMMAND ----------

print(f"\n{'─'*65}")
print(f"  PIPELINE SUMMARY  (parent_run_id: {parent_run_id})")
print(f"{'─'*65}")
print(f"  Start time          : {p_start}")
print(f"  End time            : {p_end}")
print(f"  ⏱  Wall-clock time  : {wall_secs:.1f}s ({wall_secs/60:.1f} min)")
print(f"     (parallel tasks — wall < sum of individual tasks)")
print(f"  ∑  Sum of task time : {sum_secs:.1f}s ({sum_secs/60:.1f} min)")
print(f"  📊 Total rows moved : {t_rows:,}")
print(f"  💰 Total est. cost  : ${t_cost:.4f}")
print(f"  ✅ Tasks succeeded  : {task_count - failed_cnt}/{task_count}")
if all_failed:
    print(f"  ❌ Failed tasks    : {', '.join(all_failed)}")
print(f"{'─'*65}")

# COMMAND ----------

# ── Per-task detail with error reasons ───────────────────────────────────────
if all_failed:
    print(f"\n{'─'*65}")
    print("  FAILURE DETAILS")
    print(f"{'─'*65}")
    for r in rows:
        if r["status"] == "FAILED" and r["error_reason"]:
            print(f"  ❌ {r['task_key']}")
            print(f"     {r['error_reason'][:300]}")

# COMMAND ----------

# ── Write total pipeline summary row ─────────────────────────────────────────
# One extra row summarizing the full end-to-end run
import uuid

summary_run_id = str(uuid.uuid4())
overall_status = "FAILED" if all_failed else "SUCCESS"
error_summary  = f"Failed tasks: {', '.join(all_failed)}" if all_failed else None

spark.sql(f"""
    MERGE INTO {METRICS_TABLE} AS tgt
    USING (
        SELECT
            '{summary_run_id}'       AS run_id,
            '{parent_run_id}'        AS parent_run_id,
            'PIPELINE_TOTAL'         AS job_name,
            'master_run_report'      AS task_key,
            'all'                    AS source_system,
            'all'                    AS load_type,
            '{customer_id}'          AS customer_id,
            'all'                    AS object_name,
            '{overall_status}'       AS status,
            TIMESTAMP('{p_start}')   AS start_time,
            TIMESTAMP('{p_end}')     AS end_time,
            {wall_secs}              AS duration_seconds,
            {t_rows}                 AS rows_processed,
            'summary'                AS compute_type,
            'summary'                AS instance_type,
            NULL                     AS dbus_consumed,
            {t_cost}                 AS est_cost_usd,
            {'NULL' if not error_summary else f"'{error_summary}'"}  AS error_reason
    ) AS src
    ON tgt.run_id = src.run_id
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"\n✅ Pipeline summary row written (run_id={summary_run_id})")
print(f"   Query all runs: SELECT * FROM {METRICS_TABLE}")
print(f"   This run only:  SELECT * FROM {METRICS_TABLE} WHERE parent_run_id='{parent_run_id}'")

overall = "✅ PIPELINE SUCCESS" if not all_failed else "❌ PIPELINE FAILED"
print(f"\n{overall}")

# COMMAND ----------

try:
    total_rows = spark.table(METRICS_TABLE).filter(f"parent_run_id = '{parent_run_id}'").count()
    pm.save(status="SUCCESS", rows_processed=total_rows)
except Exception as e:
    pm.save(status="FAILED", error_reason=str(e))
    raise
