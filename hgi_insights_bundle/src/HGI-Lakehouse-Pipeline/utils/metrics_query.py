from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import when, lit, col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json
import os
import time


class MetricsQuery:
    """
    Utility class to query pipeline observability metrics.
    Combines system tables (lakeflow + billing) and saves
    results to pipeline_metrics_stats.

    Usage:
        import sys
        sys.path.append("/Workspace/Users/pratibha.j.kumari@v4c.ai/hgi-databricks-ingestion-poc/hgi_insights_bundle/src/Main-historical-incremental")
        from utils.metrics_query import MetricsQuery

        mq = MetricsQuery(spark)
        mq.save_stats(days=30, rows_processed=df.count())
    """

    # ── Catalog config ─────────────────────────────────────────────────
    METRICS_CATALOG  = "bronze"
    METRICS_SCHEMA   = "pipeline_observability"
    STATS_TABLE      = "pipeline_metrics_stats"
    FULL_STATS_TABLE = f"{METRICS_CATALOG}.{METRICS_SCHEMA}.{STATS_TABLE}"

    # ──────────────────────────────────────────────────────────────────
    def __init__(self, spark: SparkSession):
        self.spark   = spark
        self._run_id = self._get_current_run_id()
        self._ensure_stats_table_exists()

    # ── Capture Current Databricks Run ID ─────────────────────────────
    def _get_current_run_id(self) -> str:
        """Captures current Databricks run ID from environment."""
        try:
            from pyspark.dbutils import DBUtils
            dbutils      = DBUtils(self.spark)
            context_json = json.loads(
                dbutils.notebook.entry_point
                .getDbutils().notebook().getContext().toJson()
            )
            return str(context_json.get("currentRunId", "UNKNOWN"))
        except Exception:
            return os.environ.get("DATABRICKS_RUN_ID", "UNKNOWN")

    # ── Table Bootstrap ────────────────────────────────────────────────
    def _ensure_stats_table_exists(self):
        """
        Creates pipeline_metrics_stats table if it doesn't exist.
        Same catalog + schema, new table name.
        """
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {self.FULL_STATS_TABLE} (
                run_id              STRING,
                job_id              STRING,
                workspace_id        STRING,
                job_name            STRING,
                trigger_type        STRING,
                run_type            STRING,
                source_system       STRING,
                layer_from          STRING,
                layer_to            STRING,
                pipeline_flow       STRING,
                load_type           STRING,
                start_time          TIMESTAMP,
                end_time            TIMESTAMP,
                run_date            DATE,
                duration_seconds    DOUBLE,
                status              STRING,
                error_reason        STRING,
                rows_processed      LONG,
                dbus_consumed       DOUBLE,
                estimated_cost_usd  DOUBLE
            )
            USING DELTA
            PARTITIONED BY (run_date)
            COMMENT 'Pipeline metrics stats combining system tables and custom metrics'
        """)
        print(f"[MetricsQuery] ✅ Stats table ready : {self.FULL_STATS_TABLE}")

    # ── Core Query Builder ─────────────────────────────────────────────
    def _build_full_stats_query(self, days: int = 30, job_name_filter: str = None) -> str:
        """
        Builds the full SQL query combining:
          - system.lakeflow  (job runs + metadata)
          - system.billing   (cost + DBUs)
        rows_processed is applied separately in save_stats()
        """
        job_name_clause = ""
        if job_name_filter:
            job_name_clause = f"AND LOWER(j.name) LIKE '%{job_name_filter.lower()}%'"

        return f"""
            SELECT
                -- ── IDs ──────────────────────────────────────────────
                r.run_id,
                r.job_id,
                r.workspace_id,

                -- ── Job Info ─────────────────────────────────────────
                j.name                                              AS job_name,
                r.trigger_type,
                r.run_type,

                -- ── source_system ─────────────────────────────────────
                CASE
                    WHEN LOWER(j.name) LIKE '%hgi_crm%'    THEN 'salesforce'
                    WHEN LOWER(j.name) LIKE '%hgi_events%' THEN 'bigquery'
                    ELSE 'multi_source'
                END                                                 AS source_system,

                -- ── layer_from ────────────────────────────────────────
                CASE
                    WHEN LOWER(j.name) LIKE '%landingzone_to_bronze%' THEN 'landing'
                    WHEN LOWER(j.name) LIKE '%bronze_to_silver%'      THEN 'bronze'
                    WHEN LOWER(j.name) LIKE '%silver_to_gold%'        THEN 'silver'
                    WHEN LOWER(j.name) LIKE '%load_pipeline%'         THEN 'source'
                    ELSE 'unknown'
                END                                                 AS layer_from,

                -- ── layer_to ──────────────────────────────────────────
                CASE
                    WHEN LOWER(j.name) LIKE '%landingzone_to_bronze%' THEN 'bronze'
                    WHEN LOWER(j.name) LIKE '%bronze_to_silver%'      THEN 'silver'
                    WHEN LOWER(j.name) LIKE '%silver_to_gold%'        THEN 'gold'
                    WHEN LOWER(j.name) LIKE '%load_pipeline%'         THEN 'landing'
                    ELSE 'unknown'
                END                                                 AS layer_to,

                -- ── pipeline_flow ─────────────────────────────────────
                CASE
                    WHEN LOWER(j.name) LIKE '%landingzone_to_bronze%' THEN 'landing→bronze'
                    WHEN LOWER(j.name) LIKE '%bronze_to_silver%'      THEN 'bronze→silver'
                    WHEN LOWER(j.name) LIKE '%silver_to_gold%'        THEN 'silver→gold'
                    WHEN LOWER(j.name) LIKE '%load_pipeline%'         THEN 'source→landing'
                    ELSE 'unknown'
                END                                                 AS pipeline_flow,

                -- ── load_type ─────────────────────────────────────────
                CASE
                    WHEN LOWER(j.name) LIKE '%historical%'  THEN 'historical'
                    WHEN LOWER(j.name) LIKE '%incremental%' THEN 'incremental'
                    ELSE 'full'
                END                                                 AS load_type,

                -- ── Timestamps ────────────────────────────────────────
                r.period_start_time                                 AS start_time,
                r.period_end_time                                   AS end_time,
                TO_DATE(r.period_start_time)                        AS run_date,

                -- ── Performance ───────────────────────────────────────
                ROUND(
                    unix_timestamp(r.period_end_time) -
                    unix_timestamp(r.period_start_time), 2
                )                                                   AS duration_seconds,

                -- ── Status ────────────────────────────────────────────
                r.result_state                                      AS status,
                r.termination_code                                  AS error_reason,

                -- ── rows_processed placeholder ────────────────────────
                CAST(NULL AS LONG)                                  AS rows_processed,

                -- ── Cost ──────────────────────────────────────────────
                ROUND(SUM(b.usage_quantity), 4)                     AS dbus_consumed,
                ROUND(SUM(b.usage_quantity * p.pricing.default), 4) AS estimated_cost_usd

            FROM system.lakeflow.job_run_timeline                   r

            JOIN system.lakeflow.jobs                               j
                ON  r.job_id        = j.job_id
                AND r.workspace_id  = j.workspace_id

            LEFT JOIN system.billing.usage                          b
                ON  b.usage_metadata.job_id     = r.job_id
                AND b.usage_metadata.job_run_id = r.run_id

            LEFT JOIN system.billing.list_prices                    p
                ON  b.sku_name          = p.sku_name
                AND b.usage_start_time >= p.price_start_time
                AND (p.price_end_time IS NULL
                     OR b.usage_start_time < p.price_end_time)

            WHERE r.period_start_time >= CURRENT_DATE - INTERVAL {days} DAYS
              AND r.result_state IS NOT NULL
              {job_name_clause}

            GROUP BY
                r.run_id, r.job_id, r.workspace_id,
                j.name, r.trigger_type, r.run_type,
                source_system, layer_from, layer_to,
                pipeline_flow, load_type,
                r.period_start_time, r.period_end_time, run_date,
                r.result_state, r.termination_code

            ORDER BY r.period_start_time DESC
        """

    # ── Public Methods ─────────────────────────────────────────────────
    def get_full_pipeline_stats(self, days: int = 30, job_name_filter: str = None) -> DataFrame:
        """
        Returns full pipeline stats as a Spark DataFrame.
        Does NOT save to table.

        Example:
            df = mq.get_full_pipeline_stats(days=30)
            df.display()

            df = mq.get_full_pipeline_stats(days=30, job_name_filter="hgi_crm")
            df.display()
        """
        query = self._build_full_stats_query(days=days, job_name_filter=job_name_filter)
        print(f"[MetricsQuery] 🔍 Querying last {days} days"
              + (f" | filter: '{job_name_filter}'" if job_name_filter else ""))
        return self.spark.sql(query)

    def save_stats(self, days: int = 30, rows_processed: int = None):
        """
        Runs the full stats query and UPSERTS results into
        bronze.pipeline_observability.pipeline_metrics_stats

        rows_processed is applied to the current run only.

        Example:
            mq.save_stats(days=30)
            mq.save_stats(days=30, rows_processed=df.count())
        """
        df = self.get_full_pipeline_stats(days=days)

        # Apply rows_processed to current run only
        if rows_processed is not None and self._run_id != "UNKNOWN":
            df = df.withColumn(
                "rows_processed",
                when(
                    col("run_id") == self._run_id,
                    lit(rows_processed)
                ).otherwise(col("rows_processed"))
            )

        # ── Deduplicate by run_id before MERGE ────────────────────────
        # System tables can return duplicate run_ids due to multiple billing
        # records or task runs. Keep the record with most complete data
        # (highest dbus_consumed, or latest timestamp if tied).
        window_spec = Window.partitionBy("run_id").orderBy(
            col("dbus_consumed").desc_nulls_last(),
            col("end_time").desc_nulls_last()
        )
        df = (
            df.withColumn("_row_num", row_number().over(window_spec))
            .filter(col("_row_num") == 1)
            .drop("_row_num")
        )

        # ── MERGE with retry logic for concurrent write conflicts ─────
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                (
                    DeltaTable.forName(self.spark, self.FULL_STATS_TABLE)
                    .alias("tgt")
                    .merge(df.alias("src"), "tgt.run_id = src.run_id AND tgt.run_date = src.run_date")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                break
            except Exception as e:
                if "ConcurrentAppend" in str(e) and attempt < max_retries:
                    print(f"[MetricsQuery] ⚠️ Concurrent write conflict, retrying ({attempt}/{max_retries})...")
                    time.sleep(10 * attempt)
                else:
                    raise

        print(f"[MetricsQuery] ✅ Stats saved → {self.FULL_STATS_TABLE}")

    def get_failure_summary(self, days: int = 30) -> DataFrame:
        """
        Returns only FAILED runs.

        Example:
            mq.get_failure_summary(days=7).display()
        """
        return (
            self.get_full_pipeline_stats(days=days)
            .filter("status = 'FAILED'")
            .select(
                "job_name", "run_id", "run_date",
                "pipeline_flow", "error_reason",
                "duration_seconds", "rows_processed"
            )
        )

    def get_cost_summary(self, days: int = 30) -> DataFrame:
        """
        Returns cost summary grouped by job.

        Example:
            mq.get_cost_summary(days=30).display()
        """
        return self.spark.sql(f"""
            SELECT
                job_name,
                pipeline_flow,
                source_system,
                COUNT(*)                            AS total_runs,
                ROUND(SUM(dbus_consumed), 4)        AS total_dbus,
                ROUND(SUM(estimated_cost_usd), 4)   AS total_cost_usd,
                ROUND(AVG(estimated_cost_usd), 4)   AS avg_cost_per_run_usd
            FROM {self.FULL_STATS_TABLE}
            WHERE run_date >= CURRENT_DATE - INTERVAL {days} DAYS
            GROUP BY job_name, pipeline_flow, source_system
            ORDER BY total_cost_usd DESC
        """)

    def get_duration_trends(self, days: int = 30) -> DataFrame:
        """
        Returns P50, P90, P95 duration trends per job.

        Example:
            mq.get_duration_trends(days=30).display()
        """
        return self.spark.sql(f"""
            SELECT
                job_name,
                pipeline_flow,
                source_system,
                COUNT(*)                                        AS total_runs,
                ROUND(AVG(duration_seconds), 2)                AS avg_sec,
                ROUND(PERCENTILE(duration_seconds, 0.50), 2)   AS p50_sec,
                ROUND(PERCENTILE(duration_seconds, 0.90), 2)   AS p90_sec,
                ROUND(PERCENTILE(duration_seconds, 0.95), 2)   AS p95_sec,
                ROUND(MAX(duration_seconds), 2)                AS max_sec
            FROM {self.FULL_STATS_TABLE}
            WHERE run_date >= CURRENT_DATE - INTERVAL {days} DAYS
              AND status    = 'SUCCEEDED'
            GROUP BY job_name, pipeline_flow, source_system
            ORDER BY p95_sec DESC
        """)