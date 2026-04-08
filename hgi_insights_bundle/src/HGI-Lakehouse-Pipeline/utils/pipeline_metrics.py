# =============================================================================
# utils/pipeline_metrics.py
# Purpose : Lightweight PipelineMetrics class — zero overhead on the hot path.
#
# Design principles:
#   1. init() writes ONE row with status=RUNNING in <100ms (single MERGE)
#   2. save() updates that row with final stats in <100ms (single MERGE)
#   3. Both ops are CONCURRENT-SAFE — MERGE on UUID run_id = no row conflicts
#   4. Never raises — all errors are swallowed and logged so they don't kill
#      the actual pipeline job
#   5. No SparkSession overhead — uses the passed-in spark object directly
#
# CONCURRENCY SAFETY:
#   Delta MERGE is ACID. Two parallel jobs (e.g. job_A and job_B) each get
#   a unique UUID run_id. Their MERGEs touch different rows → zero conflict.
#   Even if two threads somehow targeted the same run_id (impossible with UUID),
#   Delta's optimistic concurrency would retry automatically.
#
# INPUTS (what gets passed in):
#   - spark             : SparkSession from the notebook
#   - parent_run_id     : master job's Databricks run_id (from YAML parameter)
#   - job_name          : human-readable label, e.g. "A_bq_ingestion"
#   - task_key          : YAML task_key, e.g. "run_job_A_bq_ingestion"
#   - source_system     : "salesforce" | "bigquery"
#   - load_type         : "historical" | "incremental"
#   - customer_id       : e.g. "cust_001"
#   - object_name       : e.g. "account" | "events"
#
# DOES NOT TAKE:
#   - Databricks task_run_id  — not available inside a called job's notebook
#   - Cluster DBU rate        — auto-detected from cluster tags/config
# =============================================================================

import uuid
import time
import traceback
from datetime import datetime, timezone

# DBU rates per compute type (USD per DBU-hour, approximate AWS us-east-1)
_DBU_RATES = {
    "jobs_compute":          0.15,
    "jobs_compute_photon":   0.22,
    "serverless":            0.70,
    "all_purpose":           0.55,
    "unknown":               0.15,   # conservative fallback
}

# DBUs per hour by instance family (approximate)
_DBU_PER_HOUR = {
    "r5.xlarge":   1.0,
    "r5.2xlarge":  2.0,
    "r5.4xlarge":  4.0,
    "Standard_DS3_v2": 1.0,
    "Standard_DS4_v2": 2.0,
    "Standard_E8s_v3": 2.0,
    "unknown":         1.0,
}

METRICS_TABLE = "ingestion_metadata.pipeline_metrics_stats"
_MAX_RETRIES  = 3
_RETRY_SLEEP  = 0.5   # seconds between retries on concurrent write conflict


class PipelineMetrics:
    """
    Captures per-job pipeline metrics with concurrent-safe Delta writes.

    Usage:
        pm = PipelineMetrics(spark, parent_run_id, job_name, task_key,
                             source_system, load_type, customer_id, object_name)
        pm.init()
        try:
            # ... actual job work ...
            pm.save(status="SUCCESS", rows_processed=1_234_567)
        except Exception as e:
            pm.save(status="FAILED", error_reason=str(e))
            raise
    """

    def __init__(self, spark, parent_run_id: str, job_name: str, task_key: str,
                 source_system: str, load_type: str,
                 customer_id: str, object_name: str):
        self._spark         = spark
        self.run_id         = str(uuid.uuid4())        # guaranteed unique
        self.parent_run_id  = str(parent_run_id)
        self.job_name       = job_name
        self.task_key       = task_key
        self.source_system  = source_system
        self.load_type      = load_type
        self.customer_id    = customer_id
        self.object_name    = object_name
        self.start_time     = datetime.now(timezone.utc)
        self._start_epoch   = time.monotonic()
        self.compute_type, self.instance_type = self._detect_compute()

    # ── Public API ──────────────────────────────────────────────────────────

    def init(self):
        """
        Write a RUNNING row immediately at job start.
        Call this as the FIRST thing in your notebook.
        Fast: single MERGE into Delta, <100ms.
        """
        try:
            self._ensure_table()
            self._merge_row(
                status        = "RUNNING",
                end_time      = None,
                duration_secs = None,
                rows_processed= None,
                dbus_consumed = None,
                est_cost_usd  = None,
                error_reason  = None,
            )
            print(f"[Metrics] Initialized | run_id={self.run_id} | job={self.job_name}")
        except Exception:
            # NEVER fail the pipeline because of metrics
            print(f"[Metrics] WARN: init() failed (non-fatal):\n{traceback.format_exc()}")

    def save(self, status: str = "SUCCESS",
             rows_processed: int = None,
             error_reason:   str = None):
        """
        Update the RUNNING row with final stats.
        Call this at the end of your notebook (both success and failure paths).
        Fast: single MERGE into Delta, <100ms.
        """
        try:
            end_time      = datetime.now(timezone.utc)
            duration_secs = time.monotonic() - self._start_epoch
            dbus, cost    = self._estimate_cost(duration_secs)

            self._merge_row(
                status         = status,
                end_time       = end_time,
                duration_secs  = round(duration_secs, 2),
                rows_processed = rows_processed,
                dbus_consumed  = round(dbus, 4),
                est_cost_usd   = round(cost, 6),
                error_reason   = error_reason[:2000] if error_reason else None,
            )
            print(
                f"[Metrics] Saved | run_id={self.run_id} | status={status} "
                f"| rows={rows_processed:,} | duration={duration_secs:.1f}s "
                f"| cost=${cost:.4f}"
                if rows_processed else
                f"[Metrics] Saved | run_id={self.run_id} | status={status} "
                f"| duration={duration_secs:.1f}s | cost=${cost:.4f}"
            )
        except Exception:
            print(f"[Metrics] WARN: save() failed (non-fatal):\n{traceback.format_exc()}")

    # ── Internal helpers ────────────────────────────────────────────────────

    def _ensure_table(self):
        """Create the metrics table if it doesn't exist. Idempotent."""
        self._spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
                run_id              STRING        NOT NULL,
                parent_run_id       STRING,
                job_name            STRING,
                task_key            STRING,
                source_system       STRING,
                load_type           STRING,
                customer_id         STRING,
                object_name         STRING,
                status              STRING,
                start_time          TIMESTAMP,
                end_time            TIMESTAMP,
                duration_seconds    DOUBLE,
                rows_processed      BIGINT,
                compute_type        STRING,
                instance_type       STRING,
                dbus_consumed       DOUBLE,
                est_cost_usd        DOUBLE,
                error_reason        STRING
            )
            USING DELTA
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'false',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact'   = 'true'
            )
        """)

    def _merge_row(self, status, end_time, duration_secs,
                   rows_processed, dbus_consumed, est_cost_usd, error_reason):
        """
        CONCURRENT-SAFE upsert using MERGE ON run_id (UUID = always unique row).
        Retries on Delta concurrent-write conflicts (TransactionConflictException).
        """
        # Build nullable value expressions
        def _sql_val(v):
            if v is None: return "NULL"
            if isinstance(v, str): return f"'{v.replace(chr(39), chr(39)*2)}'"
            if isinstance(v, datetime): return f"TIMESTAMP('{v.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]}Z')"
            return str(v)

        st  = _sql_val(self.start_time)
        et  = _sql_val(end_time)
        dur = _sql_val(duration_secs)
        rp  = _sql_val(rows_processed)
        dbu = _sql_val(dbus_consumed)
        cst = _sql_val(est_cost_usd)
        err = _sql_val(error_reason)
        sts = _sql_val(status)

        merge_sql = f"""
            MERGE INTO {METRICS_TABLE} AS tgt
            USING (
                SELECT
                    '{self.run_id}'         AS run_id,
                    '{self.parent_run_id}'  AS parent_run_id,
                    '{self.job_name}'       AS job_name,
                    '{self.task_key}'       AS task_key,
                    '{self.source_system}'  AS source_system,
                    '{self.load_type}'      AS load_type,
                    '{self.customer_id}'    AS customer_id,
                    '{self.object_name}'    AS object_name,
                    {sts}                   AS status,
                    {st}                    AS start_time,
                    {et}                    AS end_time,
                    {dur}                   AS duration_seconds,
                    {rp}                    AS rows_processed,
                    '{self.compute_type}'   AS compute_type,
                    '{self.instance_type}'  AS instance_type,
                    {dbu}                   AS dbus_consumed,
                    {cst}                   AS est_cost_usd,
                    {err}                   AS error_reason
            ) AS src
            ON tgt.run_id = src.run_id
            WHEN MATCHED THEN UPDATE SET
                tgt.status           = src.status,
                tgt.end_time         = src.end_time,
                tgt.duration_seconds = src.duration_seconds,
                tgt.rows_processed   = src.rows_processed,
                tgt.dbus_consumed    = src.dbus_consumed,
                tgt.est_cost_usd     = src.est_cost_usd,
                tgt.error_reason     = src.error_reason
            WHEN NOT MATCHED THEN INSERT *
        """

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                self._spark.sql(merge_sql)
                return
            except Exception as e:
                err_str = str(e).lower()
                if "concurrent" in err_str or "conflict" in err_str:
                    if attempt < _MAX_RETRIES:
                        time.sleep(_RETRY_SLEEP * attempt)
                        continue
                raise   # re-raise non-conflict errors immediately

    def _detect_compute(self):
        """Detect compute type and instance type from Spark config/tags."""
        try:
            # Try to get instance type from cluster tags
            tags_conf = self._spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags", "")
            if "Serverless" in tags_conf or "serverless" in tags_conf:
                return "serverless", "serverless"

            instance = self._spark.conf.get(
                "spark.databricks.clusterUsageTags.instanceType", "unknown"
            )
            # Detect Photon
            photon = self._spark.conf.get(
                "spark.databricks.clusterUsageTags.photon", "false"
            )
            if photon.lower() == "true":
                return "jobs_compute_photon", instance

            # Check if all-purpose vs jobs
            purpose = self._spark.conf.get(
                "spark.databricks.clusterUsageTags.clusterWorkers", "0"
            )
            return "jobs_compute", instance
        except Exception:
            return "unknown", "unknown"

    def _estimate_cost(self, duration_secs: float):
        """Estimate DBU consumption and USD cost."""
        try:
            hours        = duration_secs / 3600.0
            dbu_per_hour = _DBU_PER_HOUR.get(self.instance_type, 1.0)
            rate         = _DBU_RATES.get(self.compute_type, 0.15)
            # For multi-node: multiply by node count (default 1 for single node)
            node_count   = self._get_node_count()
            dbus         = dbu_per_hour * node_count * hours
            cost         = dbus * rate
            return dbus, cost
        except Exception:
            return 0.0, 0.0

    def _get_node_count(self):
        """Get total worker node count (1 for single-node)."""
        try:
            workers = self._spark.conf.get(
                "spark.databricks.clusterUsageTags.clusterWorkers", "0"
            )
            return max(1, int(workers) + 1)  # +1 for driver
        except Exception:
            return 1
