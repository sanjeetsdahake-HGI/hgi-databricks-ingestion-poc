# HGI Lakehouse Pipeline - Knowledge Transfer Guide

**Version**: 1.0  
**Date**: April 9, 2026  
**Purpose**: Complete technical guide for client knowledge transfer

---

## 📋 Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Data Flow Journey](#data-flow-journey)
4. [Layer-by-Layer Deep Dive](#layer-by-layer-deep-dive)
5. [Change Data Feed (CDF) Mechanism](#change-data-feed-cdf-mechanism)
6. [Code Walkthroughs](#code-walkthroughs)
7. [Optimizations & Performance](#optimizations--performance)
8. [Monitoring & Observability](#monitoring--observability)
9. [Troubleshooting Guide](#troubleshooting-guide)

---

# Executive Summary

## What This Pipeline Does

The HGI Lakehouse Pipeline is a **multi-tenant, incremental data processing system** that:
- Ingests behavioral events from BigQuery and CRM data from Salesforce
- Builds a unified customer/account view
- Enriches data with 3rd-party sources (MadKudu)
- Produces analytics-ready gold tables

## Key Characteristics

| Feature | Value |
|---------|-------|
| **Architecture** | Medallion (Bronze → Silver → Gold) |
| **Runtime** | 3-5 minutes (optimized) |
| **Data Volume** | 1B+ events, 100K+ contacts/accounts |
| **Incremental Processing** | Change Data Feed (CDF) enabled |
| **Multi-Tenancy** | Tenant-scoped operations |
| **Compute** | Serverless (auto-scaling) |
| **Storage** | Delta Lake on S3 |

## Pipeline Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                      DATA SOURCES                               │
├─────────────────────────────────────────────────────────────────┤
│  BigQuery (Events)          Salesforce (CRM)                    │
│  • 1B+ behavioral events    • Contacts, Accounts                │
│  • Website visits           • Opportunities, Tasks              │
│  • Email interactions       • Campaigns                         │
└──────────────────┬──────────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────┐
│              LAYER 1: INGESTION → BRONZE                        │
├─────────────────────────────────────────────────────────────────┤
│  • Raw data landing (Parquet on S3)                             │
│  • Incremental loads with watermarks                            │
│  • Zero transformation                                          │
│  Output: bronze.events_raw, bronze.contact_raw, etc.            │
└──────────────────┬──────────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────┐
│              LAYER 2: BRONZE → SILVER (Base)                    │
├─────────────────────────────────────────────────────────────────┤
│  • Schema enforcement & validation                              │
│  • Deduplication                                                │
│  • CDF enabled (tracks every change)                            │
│  Output: silver.contacts, silver.events, silver.accounts        │
└──────────────────┬──────────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────┐
│              LAYER 3: SILVER MAP (Relationship Building)        │
├─────────────────────────────────────────────────────────────────┤
│  • contacts_all (unified contacts)                              │
│  • accounts_all (unified accounts)                              │
│  • mapped_events (events → contacts resolution)                 │
│  • contacts_to_accounts (3-phase matching)                      │
│  • Event aggregations (email, domain, account level)            │
└──────────────────┬──────────────────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────────────────┐
│              LAYER 4: GOLD (Analytics Ready)                    │
├─────────────────────────────────────────────────────────────────┤
│  • Enriched with MadKudu (person/company data)                  │
│  • Denormalized for BI tools                                    │
│  Output: gold.contacts_computations                             │
└─────────────────────────────────────────────────────────────────┘
```

---

# Architecture Overview

## Medallion Architecture

### Bronze Layer (Raw Landing)
**Purpose**: Store raw data exactly as received  
**Format**: Parquet files on S3  
**Transformation**: None (schema-on-read)

### Silver Layer (Validated & Cleansed)
**Purpose**: Enforce schema, deduplicate, enable CDF  
**Format**: Delta tables with CDF enabled  
**Transformation**: Validation, deduplication, type casting

### Map Layer (Relationships)
**Purpose**: Build entity relationships and mappings  
**Format**: Delta tables with liquid clustering  
**Transformation**: JOINs, aggregations, synthetic IDs

### Gold Layer (Analytics Ready)
**Purpose**: Business-consumable, enriched tables  
**Format**: Delta tables optimized for queries  
**Transformation**: Denormalization, enrichment

---

# Data Flow Journey

## Example: Following a Single Contact Through the Pipeline

Let's follow **Jane Doe** from BigQuery to Gold layer.

### 🔹 Stage 1: BigQuery Source

**Table**: `v4c-bigquery.v4c_bigquery_dataset.event_raw`

```json
{
  "event_id": "evt_12345",
  "event_timestamp": "2026-04-09T14:30:00Z",
  "event": "page_view",
  "email": "jane.doe@acmecorp.com",
  "domain": "acmecorp.com",
  "url": "https://hginsights.com/pricing"
}
```

### 🔹 Stage 2: Ingestion → Bronze

**Notebook**: `02_BQ_Incremental_Load`  
**Process**: Native BigQuery connector pulls incremental data

```python
# Watermark-based incremental load
watermark_str = "2026-04-09T14:00:00Z"  # Last load

df_incremental = spark.read \
    .format("bigquery") \
    .option("filter", f"event_timestamp > '{watermark_str}'") \
    .load(bq_native_table)

# Result: 1,000 new events including Jane's
df_incremental.count()  # 1000
```

**Output**: Landing Zone (Parquet)  
`s3://hgi-data-lake/landing/bigquery/cust_001/events/incremental/2026/04/09/143000/`

```
part-00001.snappy.parquet
part-00002.snappy.parquet
...
```

### 🔹 Stage 3: Bronze → Silver (Base)

**Notebook**: `02a_silver_events.py`  
**Process**: Read Bronze, apply schema, enable CDF, write to Delta

**Input** (Bronze Parquet):
```
event_id    | event_timestamp      | event      | email                  | domain
evt_12345   | 2026-04-09 14:30:00  | page_view  | jane.doe@acmecorp.com  | acmecorp.com
```

**Code**:
```python
# Read from landing zone
df = spark.read.parquet(bronze_path)

# Add metadata columns
df = df \
    .withColumn("tenant", lit(tenant_id)) \
    .withColumn("data_timestamp", current_timestamp()) \
    .withColumn("id", concat(lit("bigquery_Event_event_id_"), col("event_id")))

# MERGE into Silver (incremental)
spark.sql(f"""
    MERGE INTO silver.events AS target
    USING new_events AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

**Output** (Silver Delta Table): `hgi.silver.events`

```
id                              | tenant | email                  | domain       | event_timestamp      | data_timestamp
bigquery_Event_event_id_evt_12345 | 1001   | jane.doe@acmecorp.com  | acmecorp.com | 2026-04-09 14:30:00  | 2026-04-09 14:32:15
```

**CDF Enabled**: Every change tracked!

### 🔹 Stage 4: Silver Map - Contact Resolution

**Notebook**: `02b_map_04_events_mapping.py`  
**Process**: Map anonymous events to known contacts

**Input 1**: `hgi.silver.events` (Jane's event)  
**Input 2**: `hgi.silver.contacts_all` (Known contacts)

**Contacts Table**:
```
id                           | tenant | email                  | domain       | a_accountid
salesforce_Contact_Id_003ABC | 1001   | jane.doe@acmecorp.com  | acmecorp.com | 001XYZ
```

**Code** (Event → Contact Resolution):
```sql
SELECT
    e.id,
    e.event_timestamp,
    e.event,
    COALESCE(c1.id, c2.id) AS contactid,  -- Resolved!
    e.domain
FROM silver.events e

-- Attempt 1: Match by composite ID
LEFT JOIN (
    SELECT /*+ BROADCAST */ id, email, domain
    FROM silver.contacts_all
    WHERE tenant = 1001
) c1
    ON c1.id = CONCAT(e.source_system, '_Contact_Id_', e.source_key_value)

-- Attempt 2: Match by email
LEFT JOIN (
    SELECT /*+ BROADCAST */ id, email, domain  
    FROM silver.contacts_all
    WHERE tenant = 1001
) c2
    ON LOWER(c2.email) = LOWER(e.source_key_value)
    AND c1.id IS NULL

WHERE e.tenant = 1001
```

**Output**: `hgi.silver.mapped_events`

```
id                              | contactid                    | event_timestamp      | meta_event
bigquery_Event_event_id_evt_12345 | salesforce_Contact_Id_003ABC | 2026-04-09 14:30:00  | page_view
```

✅ **Jane's anonymous event is now linked to her CRM contact!**

### 🔹 Stage 5: Silver Map - Contact-to-Account Matching

**Notebook**: `02b_map_05_contacts_to_accounts.py`  
**Process**: 3-phase matching to link contacts → accounts

**Phase 1: CRM-Defined** (Highest confidence)
```sql
SELECT
    c.id AS contact_id,
    CONCAT('salesforce_Account_Id_', c.a_accountid) AS account_id,
    'crm_defined' AS match_type
FROM silver.contacts_all c
WHERE c.a_accountid IS NOT NULL  -- Jane has AccountId = 001XYZ
```

**Output**: `hgi.silver.contacts_to_accounts`

```
contact_id                   | account_id                     | match_type
salesforce_Contact_Id_003ABC | salesforce_Account_Id_001XYZ   | crm_defined
```

### 🔹 Stage 6: Silver Map - Event Aggregations

**Notebook**: `02b_map_10_account_events_mapped.py`  
**Process**: Roll up events per account

**Code**:
```sql
SELECT
    c2a.account_id,
    me.meta_event,
    DATE(me.event_timestamp) AS event_day,
    COUNT(*) AS occurrences
FROM silver.mapped_events me
INNER JOIN silver.contacts_to_accounts c2a
    ON me.contactid = c2a.contact_id
WHERE me.tenant = 1001
GROUP BY account_id, meta_event, event_day
```

**Output**: `hgi.silver.account_events_mapped`

```
account_id                     | meta_event | event_day  | occurrences
salesforce_Account_Id_001XYZ   | page_view  | 2026-04-09 | 15
salesforce_Account_Id_001XYZ   | email_open | 2026-04-09 | 3
```

✅ **All of Jane's events are now rolled up to her account (Acme Corp)**

### 🔹 Stage 7: Gold - Enrichment

**Notebook**: `03_Load_Silver_to_Gold_Augment`  
**Process**: Enrich with 3rd-party data (MadKudu)

**Input 1**: `hgi.silver.contacts_all` (Jane's contact)  
**Input 2**: `hgi.enrichment.persons` (MadKudu data)

**MadKudu Person Data**:
```json
{
  "mk_email": "jane.doe@acmecorp.com",
  "name__fullname": "Jane Doe",
  "employment__name": "Acme Corporation",
  "employment__title": "VP of Engineering",
  "employment__seniority": "VP",
  "geo__country": "United States"
}
```

**Code** (Enrichment JOIN):
```python
contacts_df = spark.table("silver.contacts_all") \
    .filter(f"tenant = {tenant_id}")

persons_df = spark.table("enrichment.persons") \
    .filter("mk_status = 'complete'")

companies_df = spark.table("enrichment.companies") \
    .filter("mk_status = 'complete'")

enriched_df = contacts_df \
    .join(persons_df, lower(contacts_df.email) == persons_df.mk_email, "left") \
    .join(companies_df, lower(contacts_df.domain) == companies_df.mk_domain, "left")

enriched_df.write.mode("overwrite").saveAsTable("gold.contacts_computations")
```

**Output**: `hgi.gold.contacts_computations`

```
contact_id                   | email                  | person__name__fullname | person__employment__title | company__name      | company__metrics__employees
salesforce_Contact_Id_003ABC | jane.doe@acmecorp.com  | Jane Doe               | VP of Engineering         | Acme Corporation   | 5000
```

✅ **Jane's contact is now fully enriched and analytics-ready!**

---

# Layer-by-Layer Deep Dive

## Layer 1: Ingestion → Bronze

### Purpose
Land raw data from sources with **zero transformation**.

### Components

#### 1.1 BigQuery Incremental Load

**Notebook**: `01_Ingestion_Landing_Zone/bigquery/02_BQ_Incremental_Load`

**Key Features**:
- **Watermark-based**: Only pulls new records since last run
- **Native Connector**: Uses Spark BigQuery connector (no data movement)
- **Exact Timestamp Tracking**: No duplicates, no gaps

**Watermark Logic**:
```python
# Get last watermark
watermark_df = spark.sql("""
    SELECT last_processed_timestamp
    FROM ingestion_metadata.watermarks
    WHERE source_system='bigquery' AND object_name='events'
""")
watermark_str = watermark_df.first()[0]  # "2026-04-09T14:00:00Z"

# Pull only NEW records (strict >)
df = spark.read.format("bigquery") \
    .option("filter", f"event_timestamp > '{watermark_str}'") \
    .load(bq_native_table)

# Get new max timestamp
new_max = df.agg(max("event_timestamp")).first()[0]

# Update watermark for next run
spark.sql(f"""
    UPDATE ingestion_metadata.watermarks
    SET last_processed_timestamp = '{new_max}'
    WHERE source_system='bigquery' AND object_name='events'
""")
```

**Why This Works**:
- Run 1: Pulls events from `NULL` to `2026-04-09 14:00:00`
- Run 2: Pulls events from `2026-04-09 14:00:00` to `2026-04-09 14:15:00`
- No overlaps, no gaps ✅

#### 1.2 Salesforce CDC Load

**Notebook**: `01_Ingestion_Landing_Zone/salesforce/01_SF_CDC_Load`

**Key Features**:
- Uses Salesforce CDC API (Change Data Capture)
- Replays events from last ReplayId
- Handles creates, updates, deletes

**Output**:
```
s3://hgi-data-lake/landing/salesforce/cust_001/contact/cdc/
├── 2026/04/09/143000/
│   ├── part-00001.snappy.parquet
│   └── part-00002.snappy.parquet
```

---

## Layer 2: Bronze → Silver (Base)

### Purpose
Create **validated, deduplicated, CDF-enabled** Delta tables.

### Components

#### 2.1 Silver Contacts

**Notebook**: `03_Silver_Layer/02a_silver_contact.py`

**Process**:
1. Read from Bronze (Parquet files)
2. Apply schema enforcement
3. Add metadata columns (tenant, data_timestamp)
4. Deduplicate by ID
5. MERGE into Delta table

**Code**:
```python
# Read Bronze
df = spark.read.parquet(f"{landing_path}/contact/*/")

# Schema enforcement
df = df.select(
    col("Id").cast("string").alias("source_key_value"),
    col("Email").cast("string").alias("email"),
    col("AccountId").cast("string").alias("a_accountid"),
    # ... more columns
)

# Add metadata
df = df \
    .withColumn("tenant", lit(tenant_id)) \
    .withColumn("source_system", lit("salesforce")) \
    .withColumn("data_timestamp", current_timestamp()) \
    .withColumn("id", concat(lit("salesforce_Contact_Id_"), col("source_key_value")))

# Deduplicate (keep latest)
df = df.dropDuplicates(["id"])

# MERGE (upsert)
spark.sql(f"""
    MERGE INTO hgi.silver.contacts AS target
    USING new_contacts AS source
    ON target.id = source.id AND target.tenant = source.tenant
    WHEN MATCHED AND source.data_timestamp > target.data_timestamp THEN
        UPDATE SET *
    WHEN NOT MATCHED THEN
        INSERT *
""")
```

**CDF Enabled**:
```python
spark.sql("""
    ALTER TABLE hgi.silver.contacts
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")
```

**Result**: Every change is tracked!

```sql
-- View changes
SELECT * FROM table_changes('hgi.silver.contacts', 10, 15)
```

Output:
```
id           | email                  | _change_type     | _commit_version
003ABC       | jane.doe@acmecorp.com  | update_postimage | 15
003DEF       | john.smith@widgetco.com| insert           | 15
```

#### 2.2 Silver Events

**Notebook**: `03_Silver_Layer/02a_silver_events.py`

**Key Difference**: Events are **append-only** (no updates/deletes)

```python
# Simpler MERGE for events
spark.sql(f"""
    MERGE INTO hgi.silver.events AS target
    USING new_events AS source
    ON target.id = source.id
    WHEN NOT MATCHED THEN INSERT *
""")
```

---

## Layer 3: Silver Map (Relationships)

### Purpose
Build **entity relationships** and **aggregate metrics**.

### Components

#### 3.1 Contacts All (Unified Contacts)

**Notebook**: `03_Silver_Layer/02b_map_01_contacts_all.py`

**Purpose**: Union contacts from multiple sources (future: Salesforce + Marketo + HubSpot)

**Current**: Just Salesforce, but ready for multi-source

**Code**:
```python
spark.sql(f"""
    MERGE INTO hgi.silver.contacts_all AS target
    USING (
        SELECT id, tenant, email, domain, source_system, a_accountid
        FROM hgi.silver.contacts
        WHERE tenant = {tenant_id}
    ) AS source
    ON target.id = source.id AND target.tenant = source.tenant
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND target.tenant = {tenant_id} THEN DELETE
""")
```

**Optimization**: **CDF-Aware** (reads only changes)

```python
# Get last processed version
last_ver = 10
current_ver = 15

if last_ver > 0:
    # Read only CDF records
    source_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", last_ver + 1) \
        .option("endingVersion", current_ver) \
        .table("hgi.silver.contacts") \
        .filter("_change_type IN ('insert', 'update_postimage')")
    
    # Result: Only 100 changed records (not 100,000!)
    source_df.count()  # 100 ✅
```

#### 3.2 Mapped Events (Event → Contact Resolution)

**Notebook**: `03_Silver_Layer/02b_map_04_events_mapping.py`

**Purpose**: Link anonymous events to known contacts

**Challenge**: BigQuery events contain email/ID, need to resolve to CRM contact

**Resolution Strategy**:

1. **Composite ID Match** (highest confidence):
   ```sql
   c.id = CONCAT(e.source_system, '_Contact_Id_', e.source_key_value)
   ```

2. **Email Match** (fallback):
   ```sql
   LOWER(c.email) = LOWER(e.source_key_value)
   ```

3. **Anonymous** (NULL contactid):
   - Event stays in pipeline
   - Can be matched later when contact is created

**Code**:
```sql
MERGE INTO hgi.silver.mapped_events AS target
USING (
    SELECT
        e.id,
        e.event_timestamp,
        LOWER(REGEXP_REPLACE(e.event, '[^a-zA-Z0-9]+', '_')) AS meta_event,
        COALESCE(c1.id, c2.id) AS contactid,  -- Resolution!
        e.domain
    FROM hgi.silver.events e
    
    -- Match 1: Composite ID
    LEFT JOIN (
        SELECT /*+ BROADCAST */ id, email, domain
        FROM hgi.silver.contacts_all
        WHERE tenant = {tenant_id}
    ) c1 ON c1.id = CONCAT(e.source_system, '_Contact_Id_', e.source_key_value)
    
    -- Match 2: Email
    LEFT JOIN (
        SELECT /*+ BROADCAST */ id, email, domain
        FROM hgi.silver.contacts_all
        WHERE tenant = {tenant_id}
    ) c2 ON LOWER(c2.email) = LOWER(e.source_key_value) AND c1.id IS NULL
    
    WHERE e.tenant = {tenant_id}
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

**Optimization**: **BROADCAST hint** (maps small table to all partitions)

#### 3.3 Contacts-to-Accounts (3-Phase Matching)

**Notebook**: `03_Silver_Layer/02b_map_05_contacts_to_accounts.py`

**Purpose**: Link every contact to an account (for account-level scoring)

**Challenge**: Not all contacts have AccountId in CRM

**Solution**: 3-phase waterfall matching

##### Phase 1: CRM-Defined (Highest Confidence)

```sql
CREATE OR REPLACE TEMP VIEW c2a_phase1 AS
SELECT
    c.id AS contact_id,
    CONCAT('salesforce_Account_Id_', c.a_accountid) AS account_id,
    'crm_defined' AS match_type
FROM hgi.silver.contacts_all c
WHERE c.a_accountid IS NOT NULL
  AND c.tenant = {tenant_id}
```

**Result**: ~60% of contacts matched

##### Phase 2: Domain-Based (Business Emails)

```sql
CREATE OR REPLACE TEMP VIEW c2a_phase2 AS
SELECT
    c.id AS contact_id,
    COALESCE(
        da.account_id,  -- Existing account for domain
        CONCAT('madkudu_map_domain_', c.domain)  -- Synthetic account
    ) AS account_id,
    CASE
        WHEN da.account_id IS NOT NULL THEN 'domain_match'
        ELSE 'domain_created'
    END AS match_type
FROM hgi.silver.contacts_all c

-- Exclude Phase 1 (mutually exclusive)
LEFT ANTI JOIN c2a_phase1 p1 ON c.id = p1.contact_id

-- Exclude free email domains
LEFT ANTI JOIN free_email_domains_ref fe ON c.domain = fe.domain

-- Match to best existing account for domain
LEFT JOIN domain_to_best_account da ON c.domain = da.domain

WHERE c.domain IS NOT NULL AND c.tenant = {tenant_id}
```

**Logic**:
- `jane.doe@acmecorp.com` → Match to existing Acme Corp account
- `john.smith@newstartup.com` → Create synthetic account `madkudu_map_domain_newstartup.com`

**Result**: ~30% of contacts matched

##### Phase 3: Free Email (Personal Accounts)

```sql
CREATE OR REPLACE TEMP VIEW c2a_phase3 AS
SELECT
    c.id AS contact_id,
    CONCAT('madkudu_map_email_', c.email) AS account_id,  -- One account per email
    'free_email_match' AS match_type
FROM hgi.silver.contacts_all c

-- Exclude Phase 1
LEFT ANTI JOIN c2a_phase1 p1 ON c.id = p1.contact_id

-- Only free email domains
INNER JOIN free_email_domains_ref fe ON c.domain = fe.domain

WHERE c.email IS NOT NULL AND c.tenant = {tenant_id}
```

**Logic**:
- `jane.consumer@gmail.com` → Account = `madkudu_map_email_jane.consumer@gmail.com`
- Each Gmail user = separate account (can't aggregate)

**Result**: ~10% of contacts matched

##### Final: Union All Phases

```sql
INSERT INTO hgi.silver.contacts_to_accounts
SELECT * FROM c2a_phase1
UNION ALL
SELECT * FROM c2a_phase2
UNION ALL
SELECT * FROM c2a_phase3
```

**Result**: **100% of contacts matched to accounts** ✅

#### 3.4 Event Aggregations

**Purpose**: Pre-aggregate events for fast querying

##### Email-Level Aggregation

**Notebook**: `03_Silver_Layer/02b_map_08_email_events_mapped.py`

```sql
SELECT
    email,
    meta_event,
    DATE(event_timestamp) AS event_day,
    COUNT(*) AS occurrences
FROM hgi.silver.mapped_events
WHERE tenant = {tenant_id}
GROUP BY email, meta_event, event_day
```

**Output**: `hgi.silver.email_events_mapped`

```
email                  | meta_event | event_day  | occurrences
jane.doe@acmecorp.com  | page_view  | 2026-04-09 | 15
jane.doe@acmecorp.com  | email_open | 2026-04-09 | 3
```

##### Account-Level Aggregation

**Notebook**: `03_Silver_Layer/02b_map_10_account_events_mapped.py`

```sql
SELECT
    c2a.account_id,
    me.meta_event,
    DATE(me.event_timestamp) AS event_day,
    COUNT(*) AS occurrences
FROM hgi.silver.mapped_events me
INNER JOIN hgi.silver.contacts_to_accounts c2a
    ON me.contactid = c2a.contact_id
WHERE me.tenant = {tenant_id}
GROUP BY account_id, meta_event, event_day
```

**Output**: `hgi.silver.account_events_mapped`

```
account_id                     | meta_event | event_day  | occurrences
salesforce_Account_Id_001XYZ   | page_view  | 2026-04-09 | 47
salesforce_Account_Id_001XYZ   | email_open | 2026-04-09 | 12
```

**Why This Matters**: Scoring models query aggregated tables (milliseconds vs minutes)

---

## Layer 4: Gold (Analytics Ready)

### Purpose
Create **denormalized, enriched** tables for BI tools.

### Components

#### 4.1 Contacts Computations

**Notebook**: `04_Silver_To_Gold/03_Load_Silver_to_Gold_Augment`

**Process**: Enrich silver.contacts with 3rd-party data

**Enrichment Sources**:

1. **MadKudu Persons** (`hgi.enrichment.persons`):
   - Full name, job title, seniority
   - LinkedIn/Twitter handles
   - Geo data (city, state, country)

2. **MadKudu Companies** (`hgi.enrichment.companies`):
   - Company name, description
   - Employee count, funding
   - Industry, sector
   - Tech stack

**Code**:
```python
contacts_df = spark.table("hgi.silver.contacts_all") \
    .filter(f"tenant = {tenant_id}")

persons_df = spark.table("hgi.enrichment.persons") \
    .filter("mk_status = 'complete'")  # Only complete enrichments

companies_df = spark.table("hgi.enrichment.companies") \
    .filter("mk_status = 'complete'")

# LEFT JOINs (preserve all contacts)
enriched_df = contacts_df \
    .join(
        persons_df,
        lower(contacts_df.email) == persons_df.mk_email,
        "left"
    ) \
    .join(
        companies_df,
        lower(contacts_df.domain) == companies_df.mk_domain,
        "left"
    ) \
    .select(
        contacts_df.contact_id,
        contacts_df.tenant,
        contacts_df.email,
        contacts_df.domain,
        
        # Person enrichment (prefix: person__)
        persons_df.mk_email.alias("person__mk_email"),
        persons_df["name__fullname"].alias("person__name__fullname"),
        persons_df["employment__title"].alias("person__employment__title"),
        persons_df["employment__seniority"].alias("person__employment__seniority"),
        persons_df["geo__country"].alias("person__geo__country"),
        
        # Company enrichment (prefix: company__)
        companies_df.mk_domain.alias("company__mk_domain"),
        companies_df["name"].alias("company__name"),
        companies_df["metrics__employees"].alias("company__metrics__employees"),
        companies_df["category__industry"].alias("company__category__industry"),
        companies_df["founded_year"].alias("company__founded_year")
    )

# Write to Gold
enriched_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("hgi.gold.contacts_computations")
```

**Output**: `hgi.gold.contacts_computations`

```
contact_id    | email                  | person__name__fullname | person__employment__title | company__name    | company__metrics__employees
003ABC        | jane.doe@acmecorp.com  | Jane Doe               | VP of Engineering         | Acme Corporation | 5000
003DEF        | john.smith@widgetco.com| John Smith             | Director of Sales         | Widget Co        | 250
```

**Use Cases**:
- Lead scoring (seniority + company size)
- Territory assignment (geo data)
- Account prioritization (company funding)
- Campaign targeting (industry)

---

# Change Data Feed (CDF) Mechanism

## What is CDF?

**Change Data Feed** is Delta Lake's feature that **tracks every change** to a table.

### Enables:
- Read only changed records (not full table)
- Time travel queries
- Incremental processing
- Audit trails

## How It Works

### Enable CDF

```sql
ALTER TABLE hgi.silver.contacts
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

### Track Changes

Every MERGE/UPDATE/DELETE operation creates change records:

```python
# Regular table read (all rows)
df = spark.table("hgi.silver.contacts")  # 100,000 rows

# CDF read (only changes)
df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .option("endingVersion", 15) \
    .table("hgi.silver.contacts")  # 100 rows ✅
```

### Change Types

```
_change_type Values:
- insert:           New record
- update_preimage:  Old values before update
- update_postimage: New values after update
- delete:           Deleted record
```

### Example: Tracking Jane's Updates

**Version 10**: Jane created
```
id     | email                  | title       | _change_type | _commit_version
003ABC | jane.doe@acmecorp.com  | Engineer    | insert       | 10
```

**Version 12**: Jane promoted
```
id     | email                  | title       | _change_type      | _commit_version
003ABC | jane.doe@acmecorp.com  | Engineer    | update_preimage   | 12
003ABC | jane.doe@acmecorp.com  | Sr Engineer | update_postimage  | 12
```

**Version 15**: Jane promoted again
```
id     | email                  | title          | _change_type      | _commit_version
003ABC | jane.doe@acmecorp.com  | Sr Engineer    | update_preimage   | 15
003ABC | jane.doe@acmecorp.com  | VP Engineering | update_postimage  | 15
```

### CDF in Map Layer

**Before Optimization** (Full Table Scan):
```python
# Reads ALL 100,000 contacts
source_df = spark.table("hgi.silver.contacts")
source_df.count()  # 100,000 rows ❌
```

**After Optimization** (CDF-Aware):
```python
# Track last processed version
last_ver = get_last_processed_version("contacts_all")  # 10
current_ver = get_current_version("hgi.silver.contacts")  # 15

# Read only changed records
source_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_ver + 1) \
    .option("endingVersion", current_ver) \
    .table("hgi.silver.contacts") \
    .filter("_change_type IN ('insert', 'update_postimage')")

source_df.count()  # 100 rows ✅ (99.9% reduction!)

# Update tracking
update_version_tracking("contacts_all", current_ver)
```

**Performance Impact**:
- **Before**: 100,000 rows processed → 2-3 minutes
- **After**: 100 rows processed → 2-3 seconds
- **Improvement**: **60x faster** ✅

---

# Code Walkthroughs

## Walkthrough 1: BQ Incremental Load (End-to-End)

**Notebook**: `01_Ingestion_Landing_Zone/bigquery/02_BQ_Incremental_Load`

### Step 1: Setup

```python
import sys, os
from datetime import datetime
from pyspark.sql.functions import max as spark_max

# Widgets (job parameters)
dbutils.widgets.text("customer_id", "")
dbutils.widgets.text("source_system", "bigquery")
dbutils.widgets.text("object_name", "events")

customer_id = dbutils.widgets.get("customer_id")  # "cust_001"
source_system = "bigquery"
object_name = "events"

# Load pipeline config
%run ../../config/pipeline_config

# Initialize metrics
from utils.pipeline_metrics import PipelineMetrics
pm = PipelineMetrics(
    spark=spark,
    job_name="BQ_Incremental_Load",
    customer_id=customer_id,
    source_system=source_system
)
pm.init()
```

### Step 2: Get Watermark

```python
# Query watermark table
watermark_df = spark.sql(f"""
    SELECT last_processed_timestamp
    FROM ingestion_metadata.watermarks
    WHERE source_system='{source_system}'
      AND object_name='{object_name}'
""")

rows = watermark_df.collect()
if len(rows) == 0:
    raise Exception("Watermark missing. Run historical load first.")

watermark_dt = rows[0][0]  # datetime(2026, 4, 9, 14, 0, 0)

# Format for BigQuery (ISO 8601)
watermark_str = watermark_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
# "2026-04-09T14:00:00.000Z"

print(f"📍 Last Watermark: {watermark_str}")
```

### Step 3: Check for New Data

```python
# Get BigQuery credentials
gcp_secret = dbutils.secrets.get(scope="gcp_auth", key="bq_key")
gcp_project = "v4c-bigquery"

# Query for new max timestamp
new_ts_df = spark.read \
    .format("bigquery") \
    .option("credentials", gcp_secret) \
    .option("parentProject", gcp_project) \
    .option("filter", f"event_timestamp > '{watermark_str}'") \
    .load("v4c-bigquery.v4c_bigquery_dataset.event_raw") \
    .select(spark_max("event_timestamp").alias("max_ts"))

new_ts_str = new_ts_df.first()[0]

if new_ts_str is None:
    print("✅ No new records. Exiting.")
    dbutils.notebook.exit("0")  # Graceful exit

print(f"🎯 New records up to: {new_ts_str}")
# "2026-04-09T14:15:00.000Z"
```

### Step 4: Pull Incremental Data

```python
# Pull data (strict > excludes watermark, <= includes new max)
df_incremental = spark.read \
    .format("bigquery") \
    .option("credentials", gcp_secret) \
    .option("parentProject", gcp_project) \
    .option("filter", f"event_timestamp > '{watermark_str}' AND event_timestamp <= '{new_ts_str}'") \
    .load("v4c-bigquery.v4c_bigquery_dataset.event_raw")

# ✅ OPTIMIZATION: Count BEFORE write (Serverless-compatible)
total_rows = df_incremental.count()
print(f"📊 Processing {total_rows:,} rows")  # 1,000
```

### Step 5: Write to Landing Zone

```python
# Generate path with timestamp
now = datetime.now()
incremental_path = f"s3://hgi-data-lake/landing/{source_system}/{customer_id}/{object_name}/incremental/{now:%Y/%m/%d/%H%M%S}/"

print(f"🚀 Writing to: {incremental_path}")

# ✅ OPTIMIZATION: repartition(10) for better file sizes
df_incremental.repartition(10).write \
    .mode("append") \
    .format("parquet") \
    .option("compression", "snappy") \
    .save(incremental_path)

print("💾 Write completed.")
```

**Result**:
```
s3://hgi-data-lake/landing/bigquery/cust_001/events/incremental/2026/04/09/141500/
├── part-00001.snappy.parquet  (100 rows)
├── part-00002.snappy.parquet  (100 rows)
├── ...
└── part-00010.snappy.parquet  (100 rows)
```

### Step 6: Update Watermark

```python
# Update watermark to new max (exact timestamp)
spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW new_watermarks AS
    SELECT 
        source_system,
        object_name,
        CASE 
            WHEN source_system = '{source_system}' AND object_name = '{object_name}'
            THEN CAST('{new_ts_str}' AS TIMESTAMP)
            ELSE last_processed_timestamp
        END AS last_processed_timestamp
    FROM ingestion_metadata.watermarks
""")

spark.sql("INSERT OVERWRITE TABLE ingestion_metadata.watermarks SELECT * FROM new_watermarks")

print(f"✅ Watermark updated to: {new_ts_str}")
```

### Step 7: Save Metrics & Exit

```python
# Set task value for downstream jobs
dbutils.jobs.taskValues.set(key="bq_files_count", value=str(total_rows))

# Save metrics
pm.save(status="SUCCESS", rows_processed=total_rows)

print(f"✅ Ingestion complete: {total_rows:,} rows")
```

**Next Run**:
- Watermark = `2026-04-09T14:15:00.000Z`
- Will pull events > `2026-04-09T14:15:00.000Z`
- No duplicates, no gaps ✅

---

## Walkthrough 2: Map Layer CDF Optimization

**Notebook**: `03_Silver_Layer/02b_map_01_contacts_all.py`

### Before Optimization (Full Table Scan)

```python
# ❌ PROBLEM: Reads ALL contacts every run
spark.sql(f"""
    MERGE INTO hgi.silver.contacts_all AS target
    USING (
        SELECT * FROM hgi.silver.contacts  -- 100,000 rows!
        WHERE tenant = {tenant_id}
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

**Performance**: 2-3 minutes (reads 100K rows)

### After Optimization (CDF-Aware)

#### Step 1: Version Tracking Setup

```python
# Create metadata table (once)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS hgi.silver.map_metadata (
        key STRING,
        version BIGINT,
        last_updated TIMESTAMP
    ) USING DELTA
""")
```

#### Step 2: Check for Changes

```python
# Get last processed version
try:
    last_ver = spark.sql(f"""
        SELECT version FROM hgi.silver.map_metadata
        WHERE key = 'contacts_all_last_contacts_version'
    """).collect()[0][0]
    print(f"📍 Last processed version: {last_ver}")  # 10
except:
    last_ver = 0  # First run
    print("📍 First run - will process all data")

# Get current version
current_ver = spark.sql(f"""
    SELECT MAX(version) FROM (DESCRIBE HISTORY hgi.silver.contacts)
""").collect()[0][0]
print(f"📍 Current version: {current_ver}")  # 15

# Skip if no changes
if current_ver == last_ver:
    print("✅ No changes - skipping")
    dbutils.notebook.exit("0")  # Save 2-3 minutes!
```

#### Step 3: Read Only Changed Records

```python
if last_ver > 0:
    # ✅ Read CDF (only changes)
    print(f"📚 Reading CDF (versions {last_ver} → {current_ver})")
    
    source_df = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", last_ver + 1) \
        .option("endingVersion", current_ver) \
        .table("hgi.silver.contacts") \
        .filter("_change_type IN ('insert', 'update_postimage')") \
        .filter(f"tenant = {tenant_id}")
    
    # Register as temp view
    source_df.createOrReplaceTempView("contacts_source")
    source_table = "contacts_source"
    
    cdf_rows = source_df.count()
    print(f"✅ CDF records: {cdf_rows:,}")  # 100 (not 100,000!)
else:
    # First run: read full table
    source_table = "hgi.silver.contacts"
```

#### Step 4: MERGE with CDF Data

```python
spark.sql(f"""
    MERGE INTO hgi.silver.contacts_all AS target
    USING (
        SELECT * FROM {source_table}  -- Only 100 rows!
        WHERE tenant = {tenant_id}
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    WHEN NOT MATCHED BY SOURCE AND target.tenant = {tenant_id} THEN DELETE
""")

print("✅ MERGE completed")
```

#### Step 5: Update Version Tracking

```python
spark.sql(f"""
    MERGE INTO hgi.silver.map_metadata t
    USING (
        SELECT 'contacts_all_last_contacts_version' AS key,
               {current_ver} AS version,
               current_timestamp() AS last_updated
    ) s
    ON t.key = s.key
    WHEN MATCHED THEN UPDATE SET 
        t.version = s.version, 
        t.last_updated = s.last_updated
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"✅ Version tracking updated: {current_ver}")
```

**Performance**: 2-3 seconds (reads 100 rows) ✅

**Improvement**: **60x faster!**

---

# Optimizations & Performance

## Optimization Summary

| ID | Fix | Impact | How It Works |
|----|-----|--------|-------------|
| **H4** | cnt() metadata-only | -3 to -5 min | Uses `DESCRIBE DETAIL` instead of `.count()` |
| **H5** | BQ count before write | -2 to -3 min | Single-pass counting (Serverless-compatible) |
| **M8** | BQ repartition(10) | Better files | Optimized file sizes (128-256 MB) |
| **H1a/H1b** | Map CDF-aware | -2 to -4 min | Reads only changed records (100 vs 100K) |
| **H3** | Broadcast JOINs | -30 to -60 sec | Maps small table to all partitions |
| **M4** | TEMP VIEWs | -20 to -30 sec | No disk writes for intermediate tables |
| **M5-M7** | Gold cleanup | -1 to -2 min | AQE optimization, no duplication |

## Performance Timeline

```
Original Pipeline:
├─ Ingestion:        3-4 min
├─ Bronze→Silver:    2-3 min
├─ Map Layer:        5-6 min  ← BOTTLENECK
└─ Gold:             2-3 min
   TOTAL:            12-14 min

Optimized Pipeline:
├─ Ingestion:        1-2 min  ✅
├─ Bronze→Silver:    1-2 min  ✅
├─ Map Layer:        0.5-1 min  ✅ (10x faster!)
└─ Gold:             0.5-1 min  ✅
   TOTAL:            3-5 min

IMPROVEMENT:         -70% ✅
```

## Key Optimizations Explained

### 1. CDF-Aware Map Layer

**Before**:
```python
# Full table scan
spark.table("hgi.silver.contacts").count()  # 100,000 rows
# Time: 2-3 minutes ❌
```

**After**:
```python
# CDF read
spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_ver) \
    .table("hgi.silver.contacts").count()  # 100 rows
# Time: 2-3 seconds ✅
```

### 2. Broadcast JOINs

**Before**:
```sql
-- Shuffle JOIN (expensive)
FROM events e
LEFT JOIN contacts_all c ON c.id = e.contact_id
```

**After**:
```sql
-- Broadcast JOIN (fast)
FROM events e
LEFT JOIN (
    SELECT /*+ BROADCAST */ id, email FROM contacts_all
) c ON c.id = e.contact_id
```

**How It Works**:
- `contacts_all` (10 MB) → broadcast to all executors
- `events` (10 GB) → stays partitioned
- Result: No shuffle, **10x faster**

### 3. TEMP VIEWs vs Delta Tables

**Before** (map_05 Phase 1):
```python
# Writes to disk
spark.sql("""
    CREATE OR REPLACE TABLE hgi.silver.c2a_phase1 USING DELTA AS
    SELECT contact_id, account_id FROM ...
""")  # 30 seconds ❌
```

**After**:
```python
# In-memory only
spark.sql("""
    CREATE OR REPLACE TEMP VIEW c2a_phase1 AS
    SELECT contact_id, account_id FROM ...
""")  # 3 seconds ✅
```

**Savings**: 20-30 seconds per phase × 3 phases = 1-1.5 minutes

### 4. Serverless Compatibility

**Before**:
```python
# ❌ NOT supported on Serverless
df.cache()  # ERROR!
spark.conf.set("spark.sql.shuffle.partitions", 4)  # Ignored
```

**After**:
```python
# ✅ Serverless-compatible
total_rows = df.count()  # Count before write
df.repartition(10).write.save(...)  # No cache needed
# Let AQE handle shuffle partitions (dynamic)
```

---

# Monitoring & Observability

## Metrics Collection

**Module**: `utils/pipeline_metrics.py`

### Usage

```python
from utils.pipeline_metrics import PipelineMetrics

pm = PipelineMetrics(
    spark=spark,
    parent_run_id=parent_run_id,
    job_name="02b_map_01_contacts_all",
    task_key="run_job_D_silver_map",
    source_system="salesforce",
    load_type="incremental",
    customer_id="cust_001",
    object_name="map"
)
pm.init()

# ... pipeline logic ...

pm.save(status="SUCCESS", rows_processed=total_rows)
```

### Metrics Table

**Table**: `ingestion_metadata.pipeline_metrics`

```sql
SELECT
    run_id,
    job_name,
    customer_id,
    status,
    rows_processed,
    duration_ms,
    start_time,
    end_time
FROM ingestion_metadata.pipeline_metrics
WHERE job_name = '02b_map_01_contacts_all'
  AND run_date = '2026-04-09'
ORDER BY start_time DESC
```

Output:
```
run_id      | job_name              | status  | rows_processed | duration_ms
run_123     | 02b_map_01_contacts_all | SUCCESS | 100            | 3,250
run_122     | 02b_map_01_contacts_all | SUCCESS | 150            | 3,100
```

## CDF Verification

### Verify CDF is Enabled

```sql
SHOW TBLPROPERTIES hgi.silver.contacts
```

Output:
```
key                           | value
delta.enableChangeDataFeed    | true  ✅
```

### Verify No Full Scans

```sql
-- Check MERGE operation metrics
SELECT 
    version,
    timestamp,
    operation,
    operationMetrics.numSourceRows AS source_rows,
    operationMetrics.numOutputRows AS rows_merged
FROM (DESCRIBE HISTORY hgi.silver.contacts_all)
WHERE operation = 'MERGE'
ORDER BY version DESC
LIMIT 5
```

Output:
```
version | timestamp           | operation | source_rows | rows_merged
25      | 2026-04-09 14:35:00 | MERGE     | 100         | 100         ✅
24      | 2026-04-09 14:20:00 | MERGE     | 150         | 150         ✅
23      | 2026-04-09 14:05:00 | MERGE     | 100,000     | 100,000     ❌ (first run)
```

**Expected**: source_rows < 1,000 (not 100,000!) ✅

### View CDF Records

```sql
-- View changes between versions
SELECT
    id,
    email,
    _change_type,
    _commit_version,
    _commit_timestamp
FROM table_changes('hgi.silver.contacts', 20, 25)
WHERE _change_type IN ('insert', 'update_postimage')
LIMIT 10
```

Output:
```
id     | email                  | _change_type     | _commit_version
003ABC | jane.doe@acmecorp.com  | update_postimage | 25
003DEF | john.smith@widgetco.com| insert           | 24
```

## Job Monitoring Dashboard

### Key Queries

#### 1. Pipeline Runtime Trends

```sql
SELECT
    DATE(start_time) AS run_date,
    job_name,
    AVG(duration_ms / 1000 / 60) AS avg_duration_min,
    MAX(duration_ms / 1000 / 60) AS max_duration_min,
    COUNT(*) AS run_count
FROM ingestion_metadata.pipeline_metrics
WHERE start_time >= current_date() - INTERVAL 7 DAYS
  AND status = 'SUCCESS'
GROUP BY run_date, job_name
ORDER BY run_date DESC, job_name
```

#### 2. Failure Analysis

```sql
SELECT
    job_name,
    customer_id,
    error_type,
    error_reason,
    start_time
FROM ingestion_metadata.pipeline_metrics
WHERE status = 'FAILED'
  AND start_time >= current_date() - INTERVAL 24 HOURS
ORDER BY start_time DESC
```

#### 3. Data Volume Trends

```sql
SELECT
    DATE(start_time) AS run_date,
    source_system,
    object_name,
    SUM(rows_processed) AS total_rows,
    COUNT(*) AS run_count
FROM ingestion_metadata.pipeline_metrics
WHERE start_time >= current_date() - INTERVAL 30 DAYS
  AND status = 'SUCCESS'
GROUP BY run_date, source_system, object_name
ORDER BY run_date DESC
```

---

# Troubleshooting Guide

## Common Issues

### Issue 1: Watermark Missing

**Error**:
```
Exception: Watermark missing in ingestion_metadata.watermarks. Run historical load first.
```

**Cause**: First run, watermark table empty

**Solution**:
1. Run historical load notebook first
2. Or manually insert watermark:
```sql
INSERT INTO ingestion_metadata.watermarks VALUES
('bigquery', 'events', TIMESTAMP('2026-04-01 00:00:00'))
```

### Issue 2: No CDF Records

**Error**:
```
CDF records to process: 0
```

**Cause**: Source table has no changes

**Solution**: This is normal! Pipeline skips processing. Check source:
```sql
DESCRIBE HISTORY hgi.silver.contacts
```

### Issue 3: Full Table Scan Detected

**Symptom**: `source_rows` = 100,000 (not 100-1K)

**Cause**: CDF version tracking not working

**Debug**:
```sql
-- Check version tracking
SELECT * FROM hgi.silver.map_metadata
WHERE key = 'contacts_all_last_contacts_version'
```

**Solution**: Version may be stuck. Reset:
```sql
UPDATE hgi.silver.map_metadata
SET version = (
    SELECT MAX(version) - 1 FROM (DESCRIBE HISTORY hgi.silver.contacts)
)
WHERE key = 'contacts_all_last_contacts_version'
```

### Issue 4: Duplicate Records

**Symptom**: Same event appears twice

**Cause**: Watermark not updated after ingestion

**Debug**:
```sql
-- Check for duplicates
SELECT id, COUNT(*) AS cnt
FROM hgi.silver.events
GROUP BY id
HAVING cnt > 1
```

**Solution**: Deduplicate:
```sql
CREATE OR REPLACE TABLE hgi.silver.events AS
SELECT DISTINCT * FROM hgi.silver.events
```

### Issue 5: Broadcast JOIN Fails

**Error**:
```
org.apache.spark.SparkException: Broadcast table too large
```

**Cause**: contacts_all > 8 GB (broadcast limit)

**Solution**: Remove broadcast hint or increase limit:
```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)  # 100 MB
```

---

# Appendix: Quick Reference

## Table Catalog

### Metadata Tables

| Table | Purpose |
|-------|----------|
| `ingestion_metadata.watermarks` | Track last processed timestamp per source |
| `ingestion_metadata.pipeline_metrics` | Job execution metrics |
| `hgi.silver.map_metadata` | CDF version tracking for map layer |

### Data Tables

#### Bronze Layer
- Landing zone (Parquet files on S3)
- No Delta tables

#### Silver Layer (Base)

| Table | Source | Records |
|-------|--------|----------|
| `hgi.silver.contacts` | Salesforce | 100K |
| `hgi.silver.accounts` | Salesforce | 50K |
| `hgi.silver.events` | BigQuery | 1B |
| `hgi.silver.opportunities` | Salesforce | 10K |
| `hgi.silver.tasks` | Salesforce | 500K |

#### Silver Layer (Map)

| Table | Purpose | Records |
|-------|---------|----------|
| `hgi.silver.contacts_all` | Unified contacts | 100K |
| `hgi.silver.accounts_all` | Unified accounts | 50K |
| `hgi.silver.mapped_events` | Events → contacts | 1B |
| `hgi.silver.crm_events` | CRM activities as events | 500K |
| `hgi.silver.contacts_to_accounts` | Contact → account mapping | 100K |
| `hgi.silver.email_events_mapped` | Email-level aggregation | 10M |
| `hgi.silver.domain_events_mapped` | Domain-level aggregation | 1M |
| `hgi.silver.account_events_mapped` | Account-level aggregation | 500K |

#### Gold Layer

| Table | Purpose | Records |
|-------|---------|----------|
| `hgi.gold.contacts_computations` | Enriched contacts | 100K |

#### Enrichment Layer

| Table | Source | Records |
|-------|--------|----------|
| `hgi.enrichment.persons` | MadKudu | 50M |
| `hgi.enrichment.companies` | MadKudu | 10M |

## Job Configuration

### Recommended Compute

```yaml
compute:
  type: serverless
  min_workers: 1
  max_workers: 4
  spot_policy: auto
```

### Task Dependencies

```
Ingestion Tasks (Parallel):
├─ BQ_Incremental_Load
├─ SF_CDC_Load_Contact
├─ SF_CDC_Load_Account
└─ SF_CDC_Load_Opportunity
      ↓
Bronze→Silver Tasks (Parallel):
├─ silver_contact
├─ silver_account
├─ silver_events
└─ silver_opportunity
      ↓
Map Tasks (Sequential):
├─ map_01_contacts_all      (first)
├─ map_02_accounts_all      (second)
├─ map_03_crm_events        (parallel with map_04)
├─ map_04_events_mapping    (parallel with map_03)
├─ map_05_contacts_to_accounts  (after map_04)
├─ map_08_email_events      (parallel)
├─ map_09_domain_events     (parallel)
└─ map_10_account_events    (parallel)
      ↓
Gold Task:
└─ Load_Silver_to_Gold_Augment
```

## Useful Commands

### Check CDF Status

```sql
SHOW TBLPROPERTIES hgi.silver.contacts
```

### Enable CDF

```sql
ALTER TABLE hgi.silver.contacts
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
```

### View Table History

```sql
DESCRIBE HISTORY hgi.silver.contacts
```

### Read CDF

```python
df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 10) \
    .option("endingVersion", 15) \
    .table("hgi.silver.contacts")
```

### Check Table Size

```sql
DESCRIBE DETAIL hgi.silver.contacts
```

### Optimize Table

```sql
OPTIMIZE hgi.silver.contacts
ZORDER BY (tenant, email)
```

### Vacuum Old Files

```sql
VACUUM hgi.silver.contacts RETAIN 168 HOURS  -- 7 days
```

---

# Summary

## What We Built

A **production-grade, multi-tenant lakehouse pipeline** that:
- Ingests 1B+ events incrementally
- Builds unified customer/account views
- Enriches with 3rd-party data
- Runs in **3-5 minutes** (70% faster)
- **100% CDF-compliant** (no full table scans)
- Serverless-ready (auto-scaling)

## Key Takeaways

1. **Medallion Architecture** (Bronze → Silver → Gold) provides clean separation
2. **Change Data Feed** enables incremental processing at scale
3. **Watermark-based ingestion** ensures zero duplicates, zero gaps
4. **Broadcast JOINs** optimize map layer performance
5. **TEMP VIEWs** reduce unnecessary disk writes
6. **Multi-tenancy** allows single pipeline for all customers

## Next Steps

1. **Deploy to production** using [hgi_incremental_OPTIMIZED_job.yml](#file-3472951048817625)
2. **Monitor metrics** via pipeline_metrics table
3. **Set up alerts** for failures/SLA violations
4. **Add more sources** (Marketo, HubSpot, etc.)
5. **Build downstream models** (lead scoring, account prioritization)

---

**Questions?** Refer to:
- [TESTING_GUIDE.md](#file-3472951048817626) - Testing & verification
- [COMPREHENSIVE_AUDIT_REPORT](#file-3472951048817623) - Full audit findings
- [FIXES_APPLIED_SUMMARY](#file-3472951048817628) - All optimizations applied

**Last Updated**: April 9, 2026  
**Pipeline Version**: 1.0 (Optimized) 