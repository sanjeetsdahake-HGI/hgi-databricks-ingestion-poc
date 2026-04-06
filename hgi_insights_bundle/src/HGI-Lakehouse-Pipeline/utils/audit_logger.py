#MODULE 2 — utilities/audit_logger.py

# ===================================================================================
# MODULE   : utilities/audit_logger.py
# PURPOSE  : Centralized audit logging + email alerting for all pipeline notebooks
# USAGE    : %run ./utilities/audit_logger.py
#            initialize_audit_tables()
#            log_audit(job_name="...", customer_id="...", status="success", ...)
#
# FUNCTIONS:
#   initialize_audit_tables() — creates audit_logs tables in bronze, silver, gold
#   log_audit()               — writes audit record + sends email
#   send_email()              — standalone email sender
#
# WRITES TO:
#   bronze.logs.audit_logs    — for bronze layer jobs
#   hgi.silver.audit_logs     — for silver layer jobs
#   hgi.gold.audit_logs       — for gold layer jobs
#
# ALERT TYPES:
#   CRITICAL : FAILURE, DQ_CHECK_FAILED, COVERAGE_LOW, CDC_DISABLED
#   WARNING  : PARTIAL_SUCCESS, PERFORMANCE_SLOW, NULL_VALUES_DETECTED
#   INFO     : SUCCESS, SUCCESS_WITH_WARNINGS, SCHEMA_CHANGE_DETECTED
#
# EMAIL:
#   Disabled by default — set EMAIL_ENABLED = True and configure secrets to enable.
#   Uses Gmail SMTP — replace credentials below for POC
#   Move to Databricks Secrets for production
#
# PRODUCTION TODO (uncomment when running through Databricks Jobs):
#   execution_id  — unique Databricks job run ID
#   user_name     — who triggered the job
#   notebook_path — full notebook path
# ===================================================================================

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pyspark.sql import Row, SparkSession

# ===================================================================================
# EMAIL CONFIGURATION
# Email is disabled for now — audit logs are written to Delta only.
# To enable email notifications, set EMAIL_ENABLED = True and ensure
# the email_secrets scope exists with sender_email and sender_pass keys.
# ===================================================================================
EMAIL_ENABLED = False  # ← Set to True when email secrets are configured

SMTP_SERVER  = "smtp.gmail.com"
SMTP_PORT    = 587
SENDER_EMAIL = None
SENDER_PASS  = None

if EMAIL_ENABLED:
    try:
        SENDER_EMAIL = dbutils.secrets.get(scope="email_secrets", key="sender_email")
        SENDER_PASS  = dbutils.secrets.get(scope="email_secrets", key="sender_pass")
    except Exception as e:
        print(f"  ⚠️  Email secrets not found — email notifications disabled: {str(e)}")
        EMAIL_ENABLED = False


def initialize_audit_tables():
    """
    Creates audit_logs tables in bronze, silver, and gold catalogs if they don't exist.
    Call this once at the START of your notebook.
    """
    spark = SparkSession.getActiveSession()

    audit_schema = """
        job_name        STRING    COMMENT 'Name of the notebook or job',
        customer_id     STRING    COMMENT 'Tenant or customer identifier',
        status          STRING    COMMENT 'success or failure',
        alert_type      STRING    COMMENT 'CRITICAL WARNING or INFO alert category',
        error_type      STRING    COMMENT 'Python exception class name if failure',
        error_reason    STRING    COMMENT 'Full error message if failure',
        layer           STRING    COMMENT 'bronze silver or gold',
        rows_processed  LONG      COMMENT 'Number of rows written or processed',
        duration_ms     LONG      COMMENT 'Job duration in milliseconds',
        timestamp       TIMESTAMP COMMENT 'When the job ran',
        run_date        DATE      COMMENT 'Date of the run',
        email_notified  STRING    COMMENT 'Email address that was notified'
    """

    # Bronze
    try:
        spark.sql("CREATE SCHEMA IF NOT EXISTS bronze.logs")
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze.logs.audit_logs ({audit_schema})
            USING DELTA
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """)
        print("  ✅ bronze.logs.audit_logs ready")
    except Exception as e:
        print(f"  ⚠️  bronze.logs.audit_logs: {str(e)}")

    # Silver
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS hgi.silver.audit_logs ({audit_schema})
            USING DELTA
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """)
        print("  ✅ hgi.silver.audit_logs ready")
    except Exception as e:
        print(f"  ⚠️  hgi.silver.audit_logs: {str(e)}")

    # Gold
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS hgi.gold.audit_logs ({audit_schema})
            USING DELTA
            TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
        """)
        print("  ✅ hgi.gold.audit_logs ready")
    except Exception as e:
        print(f"  ⚠️  hgi.gold.audit_logs: {str(e)}")


def get_alert_category(alert_type):
    """Returns CRITICAL / WARNING / INFO based on alert_type."""
    critical = ["FAILURE", "DQ_CHECK_FAILED", "COVERAGE_LOW", "CDC_DISABLED"]
    warning  = ["PARTIAL_SUCCESS", "PERFORMANCE_SLOW", "NULL_VALUES_DETECTED"]
    info     = ["SUCCESS", "SUCCESS_WITH_WARNINGS", "SCHEMA_CHANGE_DETECTED"]

    if alert_type in critical: return "CRITICAL"
    if alert_type in warning:  return "WARNING"
    if alert_type in info:     return "INFO"
    return "INFO"


def send_email(to_email, subject, body):
    """
    Sends email notification using Gmail SMTP.
    No-op if EMAIL_ENABLED is False or credentials are missing.

    Args:
        to_email : str — recipient email
        subject  : str — email subject
        body     : str — email body
    """
    if not EMAIL_ENABLED or not SENDER_EMAIL or not SENDER_PASS:
        print(f"  ℹ️  Email skipped (disabled) — would notify {to_email}")
        return

    try:
        msg = MIMEMultipart()
        msg['From']    = SENDER_EMAIL
        msg['To']      = to_email
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASS)
            server.sendmail(SENDER_EMAIL, to_email, msg.as_string())

        print(f"  ✅ Email sent to {to_email}")
    except Exception as e:
        print(f"  ⚠️  Email failed: {str(e)}")


def log_audit(
    job_name,
    customer_id,
    status,
    layer,
    alert_type      = "SUCCESS",
    error_type      = None,
    error_reason    = None,
    rows_processed  = None,
    duration_ms     = None,
    email_on_failure = None,

    # ===========================================================================
    # PRODUCTION TODO — Uncomment when running through Databricks Jobs/Pipelines:
    # execution_id  = None,  # Unique Databricks job run ID (auto-injected by Jobs)
    # user_name     = None,  # Who triggered the job
    # notebook_path = None,  # Full notebook path
    # ===========================================================================
):
    """
    Logs job execution to the appropriate audit_logs table and sends email.

    Args:
        job_name        : str — notebook or job name
        customer_id     : str — tenant identifier
        status          : str — success or failure
        layer           : str — bronze, silver, or gold
        alert_type      : str — SUCCESS, FAILURE, DQ_CHECK_FAILED, etc.
        error_type      : str — exception class name (optional)
        error_reason    : str — error message (optional)
        rows_processed  : int — rows written or processed (optional)
        duration_ms     : int — job duration in milliseconds (optional)
        email_on_failure: str — email address to notify (optional)
    """
    spark = SparkSession.getActiveSession()

    alert_category = get_alert_category(alert_type)
    now            = datetime.now()
    timestamp_str  = now.strftime("%Y-%m-%d %H:%M:%S")

    # Target audit table per layer
    table_map = {
        "bronze": "bronze.logs.audit_logs",
        "silver": "hgi.silver.audit_logs",
        "gold":   "hgi.gold.audit_logs"
    }
    target_table = table_map.get(layer.lower(), "hgi.gold.audit_logs")

    # Write audit record
    try:
        audit_row = spark.createDataFrame([Row(
            job_name       = job_name,
            customer_id    = customer_id,
            status         = status,
            alert_type     = f"{alert_category}:{alert_type}",
            error_type     = error_type     or "N/A",
            error_reason   = error_reason   or "N/A",
            layer          = layer,
            rows_processed = rows_processed or 0,
            duration_ms    = duration_ms    or 0,
            timestamp      = now,
            run_date       = now.date(),
            email_notified = email_on_failure or "N/A"
        )])

        audit_row.write.format("delta").mode("append").saveAsTable(target_table)

        print(f"\n  ✅ Audit log written to {target_table}")
        print(f"     job_name   : {job_name}")
        print(f"     status     : {status.upper()}")
        print(f"     alert_type : {alert_category}:{alert_type}")
        print(f"     layer      : {layer}")
        print(f"     duration   : {duration_ms or 0} ms")
        print(f"     rows       : {rows_processed or 0}")

    except Exception as e:
        print(f"\n  ⚠️  Audit log write failed: {str(e)}")

    # Send email (no-op if EMAIL_ENABLED is False)
    if email_on_failure:
        icon    = "✅" if status == "success" else "❌"
        subject = f"{icon} [{alert_category}] {alert_type} — {job_name} | {customer_id}"
        body    = f"""
Pipeline Notification
=====================
Job Name    : {job_name}
Customer    : {customer_id}
Status      : {status.upper()}
Alert Type  : {alert_category} — {alert_type}
Layer       : {layer}
Timestamp   : {timestamp_str}
Duration    : {duration_ms or 0} ms
Rows        : {rows_processed or 0}

Error Type  : {error_type or 'N/A'}
Error Reason: {error_reason or 'N/A'}

Review logs : {target_table}
        """
        send_email(email_on_failure, subject, body)


print("✅ audit_logger.py loaded — functions: initialize_audit_tables(), log_audit(), send_email()")