# =============================================================================
# Notebook  : pipeline_config
# Location  : /HGI-Lakehouse-Pipeline/pipeline_config
# Purpose   : Single source of truth for ALL pipeline notebooks (bronze + silver).
#
# USAGE IN EVERY NOTEBOOK:
#   Cell 1:  %run ./pipeline_config      ← loads all variables into scope
#   Cell 2:  %run ./_bronze_common       ← (bronze notebooks only)
#        or  %run ./_silver_cdf_common   ← (silver CDF notebooks only)
#        or  %run ./_map_common          ← (map notebooks only)
#
# TO CHANGE ENVIRONMENTS:
#   Edit section 1 (S3 paths) and section 2 (catalog names) only.
#   Everything else derives from those two sections.
# =============================================================================

# =============================================================================
# 1. S3 / Storage Paths
# =============================================================================
bucket_path     = "s3://hgi-databricks-data-lakehouse-dev"
landing_root    = f"{bucket_path}/landing-zone"

# =============================================================================
# 2. Unity Catalog Names
# =============================================================================
bronze_catalog  = "bronze"           # bronze.{customer_id}.{table}
silver_catalog  = "hgi"              # hgi.silver.{table}
silver_schema   = "silver"
meta_catalog    = "ingestion_metadata"   # watermarks table
sv              = f"{silver_catalog}.{silver_schema}"    # shorthand used everywhere

# =============================================================================
# 3. Bronze Table Names
#    Key change: BQ events → events_raw  (NOT crm_events)
#    Stored as:  bronze.{customer_id}.{table_name}
# =============================================================================
BRONZE_TABLES = {
    "account":        "crm_accounts",
    "contact":        "crm_contacts",
    "opportunity":    "crm_opportunities",
    "task":           "crm_tasks",
    "campaign":       "crm_campaigns",
    "campaignmember": "crm_campaign_members",
    "user":           "crm_users",
    "events":         "events_raw",       # BQ events — explicitly named events_raw
}

# =============================================================================
# 4. Silver Table Names
#    Stored as:  hgi.silver.{table_name}
#    No "unified_" prefix
# =============================================================================
SILVER_TABLES = {
    "account":        "accounts",
    "contact":        "contacts",
    "opportunity":    "opportunities",
    "task":           "tasks",
    "campaign":       "campaigns",
    "campaignmember": "campaign_members",
    "user":           "users",
    "events":         "events",
}

# Map layer output tables (written by 02b map notebooks)
MAP_TABLES = {
    "contacts_all":             f"{sv}.contacts_all",
    "accounts_all":             f"{sv}.accounts_all",
    "crm_events":               f"{sv}.crm_events",
    "mapped_events":            f"{sv}.mapped_events",
    "contacts_to_accounts":     f"{sv}.contacts_to_accounts",
    "accounts_attributes":      f"{sv}.accounts_attributes",
    "contacts_attributes":      f"{sv}.contacts_attributes",
    "email_events_mapped":      f"{sv}.email_events_mapped",
    "domain_events_mapped":     f"{sv}.domain_events_mapped",
    "mk_account_events_mapped": f"{sv}.mk_account_events_mapped",
    "email_audience":           f"{sv}.email_audience",
    "email_model_conversion":   f"{sv}.email_model_conversion",
}

# =============================================================================
# 5. Object Casing for source_system_object column
#    IMPORTANT: capitalize() breaks CampaignMember → Campaignmember
#    Always use this dict instead
# =============================================================================
OBJECT_CASING = {
    "account":        "Account",
    "contact":        "Contact",
    "opportunity":    "Opportunity",
    "task":           "Task",
    "campaign":       "Campaign",
    "campaignmember": "CampaignMember",   # NOT "Campaignmember"
    "user":           "User",
    "event":          "Event",
    "events":         "Event",
}

# =============================================================================
# 6. Auto Loader Defaults (widgets override these per notebook)
# =============================================================================
DEFAULT_MAX_FILES_SF_HIST    = 500     # SF objects historical load
DEFAULT_MAX_FILES_SF_INCR    = 50      # SF objects incremental load
DEFAULT_MAX_FILES_BQ_HIST    = 1000    # BQ events historical (1B rows)
DEFAULT_MAX_FILES_BQ_INCR    = 100     # BQ events incremental
DEFAULT_MAX_FILES_SERVERLESS = 10      # Safe default for Serverless 2X-Small testing

# =============================================================================
# 7. CDF Streaming Defaults
# =============================================================================
CDF_MAX_FILES_PER_TRIGGER = "200"
CDF_STARTING_VERSION      = "0"    # "0" = replay all history; set to specific version to skip

# =============================================================================
# 8. Watermark Settings
# =============================================================================
WATERMARK_LOOKBACK_MINUTES = 1     # subtract N minutes before storing watermark

# =============================================================================
# 9. Free Email Domains  (contacts_to_accounts Phase 3 matching)
# =============================================================================
FREE_EMAIL_DOMAINS = frozenset({
    "gmail.com", "googlemail.com",
    "yahoo.com", "yahoo.co.uk", "yahoo.co.in", "yahoo.fr", "yahoo.de",
    "yahoo.es", "yahoo.it", "yahoo.com.au",
    "hotmail.com", "hotmail.co.uk", "hotmail.fr", "hotmail.de",
    "outlook.com", "outlook.fr", "outlook.de", "live.com", "msn.com",
    "aol.com", "aim.com",
    "icloud.com", "me.com", "mac.com",
    "protonmail.com", "protonmail.ch", "pm.me",
    "zoho.com", "yandex.com", "yandex.ru",
    "mail.com", "inbox.com",
    "gmx.com", "gmx.net", "gmx.de",
    "web.de", "t-online.de", "freenet.de",
    "rediffmail.com",
    "163.com", "126.com", "qq.com", "sina.com",
    "naver.com", "hanmail.net", "daum.net",
    "rocketmail.com", "ymail.com",
    "fastmail.com", "fastmail.fm",
    "tutanota.com", "tutamail.com",
    "mailfence.com",
})

# =============================================================================
# 10. Delta Table Properties
# =============================================================================
DELTA_TBLPROPS_BRONZE = """
    TBLPROPERTIES (
        'delta.enableDeletionVectors'          = 'true',
        'delta.autoOptimize.optimizeWrite'     = 'true',
        'delta.autoOptimize.autoCompact'       = 'true',
        'delta.checkpointInterval'             = '10',
        'delta.dataSkippingNumIndexedCols'     = '6'
    )
"""

DELTA_TBLPROPS_SILVER = """
    TBLPROPERTIES (
        'delta.enableDeletionVectors'          = 'true',
        'delta.autoOptimize.optimizeWrite'     = 'true',
        'delta.autoOptimize.autoCompact'       = 'true',
        'delta.checkpointInterval'             = '10',
        'delta.dataSkippingNumIndexedCols'     = '6'
    )
"""

DELTA_TBLPROPS_MAP = """
    TBLPROPERTIES (
        'delta.enableDeletionVectors'          = 'true',
        'delta.autoOptimize.optimizeWrite'     = 'true',
        'delta.autoOptimize.autoCompact'       = 'true'
    )
"""

# =============================================================================
# 11. Spark Configurations
#     Used via safe_spark_conf() which silently skips configs unsupported
#     on Serverless (spark.databricks.*, spark.hadoop.fs.s3a.*, shuffle.partitions)
# =============================================================================
SPARK_CONF_BRONZE = {
    # AQE — works on both Serverless and Job Cluster
    "spark.sql.adaptive.enabled":                              "true",
    "spark.sql.adaptive.coalescePartitions.enabled":           "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes":         "134217728",
    "spark.sql.adaptive.skewJoin.enabled":                     "true",
    # Delta MERGE — Job Cluster only; silently skipped on Serverless
    "spark.databricks.delta.merge.enableLowShuffle":           "true",
    "spark.databricks.delta.merge.repartitionBeforeWrite":     "true",
    "spark.databricks.delta.optimizeWrite.enabled":            "true",
    "spark.databricks.delta.autoCompact.enabled":              "true",
    # Shuffle — skipped on Serverless (it manages this automatically)
    "spark.sql.shuffle.partitions":                            "400",
    # S3 write — skipped on Serverless (uses Databricks managed connector)
    "spark.hadoop.fs.s3a.fast.upload":                         "true",
    "spark.hadoop.fs.s3a.multipart.size":                      "134217728",
    "spark.hadoop.fs.s3a.threads.max":                         "64",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
    # Parquet — works on both
    "spark.sql.parquet.mergeSchema":                           "false",
    "spark.sql.parquet.filterPushdown":                        "true",
    "spark.sql.streaming.metricsEnabled":                      "true",
}

SPARK_CONF_SILVER_CDF = {
    **SPARK_CONF_BRONZE,
    "spark.databricks.delta.readChangeFeed.maxFilesPerTrigger": "200",
}

SPARK_CONF_MAP = {
    "spark.sql.adaptive.enabled":                              "true",
    "spark.sql.adaptive.coalescePartitions.enabled":           "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes":         "134217728",
    "spark.sql.adaptive.skewJoin.enabled":                     "true",
    "spark.sql.shuffle.partitions":                            "800",
    "spark.sql.autoBroadcastJoinThreshold":                    "52428800",   # 50 MB broadcast
    "spark.databricks.delta.merge.enableLowShuffle":           "true",
    "spark.databricks.delta.optimizeWrite.enabled":            "true",
    "spark.databricks.delta.autoCompact.enabled":              "true",
    "spark.sql.parquet.mergeSchema":                           "false",
}

# =============================================================================
# 12. Data Quality Thresholds  (from client spec PDFs)
# =============================================================================
DQ_THRESHOLDS = {
    "c2a_linkage_pct":               80.0,   # % contacts linked to an account
    "event_contact_resolution_pct":  70.0,   # % events with resolved contactid
    "meta_event_nonnull_pct":        100.0,  # % events with non-null meta_event
    "person_enrichment_pct":         60.0,   # % emails enriched (Augment)
    "company_enrichment_pct":        70.0,   # % domains enriched (Augment)
    "compute_company_coverage_pct":  50.0,   # % contacts with company data (Compute)
}

# =============================================================================
# 13. Path Helper Functions
#     Used throughout all notebooks — never hardcode paths, use these
# =============================================================================

def landing_path(source_sys: str, cust_id: str, obj_name: str, load_type: str) -> str:
    """Returns S3 path for the landing zone parquet files."""
    return f"{landing_root}/{source_sys}/{cust_id}/{obj_name}/{load_type}/"

def checkpoint_path(layer: str, source_sys: str, cust_id: str, obj_name: str) -> str:
    """Returns S3 checkpoint path for Auto Loader or CDF streams."""
    # NEW FORMAT: s3://.../layers/{layer}/checkpoint/{source_sys}/{cust_id}/{obj_name}/
    return f"{bucket_path}/layers/{layer}/checkpoint/{source_sys}/{cust_id}/{obj_name}/"

def bronze_table(cust_id: str, obj_name: str) -> str:
    """Returns fully qualified bronze table name."""
    tbl = BRONZE_TABLES.get(obj_name, f"crm_{obj_name}s")
    return f"{bronze_catalog}.{cust_id}.{tbl}"

def silver_table(obj_name: str) -> str:
    """Returns fully qualified silver table name."""
    tbl = SILVER_TABLES.get(obj_name, obj_name)
    return f"{sv}.{tbl}"

def tenant_id_from_customer(cust_id: str) -> int:
    """Extracts integer tenant ID from customer_id string (e.g., 'cust_001' → 1)."""
    try:
        return int(cust_id.split("_")[1])
    except (IndexError, ValueError):
        raise ValueError(f"customer_id must be 'cust_NNN', got: {cust_id}")

# =============================================================================
# 14. Bronze DDL Column Definitions per Object
#     Centralised here so object notebooks just call create_bronze_table()
# =============================================================================
BRONZE_DDL_COLUMNS = {
    "account": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, created_date TIMESTAMP,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        status STRING, record_hash STRING,
        name STRING, domain STRING,
        a_isdeleted STRING, a_website STRING,
        a_industry STRING, a_numberofemployees STRING, a_billingcountry STRING
    """,
    "contact": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, created_date TIMESTAMP,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        status STRING, record_hash STRING,
        email STRING, domain STRING,
        a_accountid STRING, a_firstname STRING, a_lastname STRING,
        a_isdeleted STRING, a_isconverted STRING, a_leadsource STRING,
        a_country STRING, a_title STRING, a_mailingcountry STRING
    """,
    "opportunity": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, created_date TIMESTAMP,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        status STRING, record_hash STRING,
        a_accountid STRING, a_amount STRING, a_stagename STRING,
        a_iswon STRING, a_isclosed STRING, a_isdeleted STRING,
        a_probability STRING, a_closedate STRING, a_type STRING
    """,
    "task": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, created_date TIMESTAMP,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        status STRING, record_hash STRING,
        contact_source_system_object STRING,
        contact_source_key_value STRING,
        event_timestamp TIMESTAMP,
        a_subject STRING, a_isdeleted STRING,
        a_status STRING, a_type STRING
    """,
    "campaign": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, created_date TIMESTAMP,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        status STRING, record_hash STRING,
        a_name STRING, a_type STRING,
        a_isactive STRING, a_status STRING,
        a_startdate STRING, a_enddate STRING
    """,
    "campaignmember": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, created_date TIMESTAMP,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        status STRING, record_hash STRING,
        campaign_source_key_value STRING,
        contact_source_key_value STRING,
        a_status STRING, a_hasresponded STRING
    """,
    "user": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, created_date TIMESTAMP,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        status STRING, record_hash STRING,
        email STRING, domain STRING,
        a_firstname STRING, a_lastname STRING,
        a_username STRING, a_isactive STRING,
        a_profileid STRING, a_userroleid STRING
    """,
    "events": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, created_date TIMESTAMP,
        created_at TIMESTAMP, updated_at TIMESTAMP,
        status STRING, record_hash STRING
    """,
}

# =============================================================================
# 15. Silver DDL Column Definitions per Object
# =============================================================================
SILVER_DDL_COLUMNS = {
    "accounts": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, status STRING,
        domain STRING, name STRING,
        custom_metadata MAP<STRING, STRING>
    """,
    "contacts": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, status STRING,
        email STRING, domain STRING, a_accountid STRING,
        custom_metadata MAP<STRING, STRING>
    """,
    "opportunities": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, status STRING,
        a_accountid STRING, a_amount STRING, a_stagename STRING,
        custom_metadata MAP<STRING, STRING>
    """,
    "tasks": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, status STRING,
        contact_source_system_object STRING,
        contact_source_key_value STRING,
        a_subject STRING, event_timestamp TIMESTAMP,
        custom_metadata MAP<STRING, STRING>
    """,
    "campaigns": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, status STRING,
        custom_metadata MAP<STRING, STRING>
    """,
    "campaign_members": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, status STRING,
        campaign_source_key_value STRING,
        contact_source_key_value STRING,
        custom_metadata MAP<STRING, STRING>
    """,
    "users": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, status STRING,
        email STRING, domain STRING,
        custom_metadata MAP<STRING, STRING>
    """,
    "events": """
        tenant BIGINT, id STRING NOT NULL,
        source_system STRING, source_system_object STRING,
        source_key_name STRING, source_key_value STRING,
        data_timestamp TIMESTAMP, status STRING,
        event STRING, event_text STRING,
        event_timestamp TIMESTAMP, domain STRING,
        custom_metadata MAP<STRING, STRING>
    """,
}

print("pipeline_config loaded.")
print(f"  S3             : {bucket_path}")
print(f"  bronze_catalog : {bronze_catalog}")
print(f"  silver         : {sv}")
print(f"  meta_catalog   : {meta_catalog}")
print(f"  BQ events table: {BRONZE_TABLES['events']}   ← events_raw")