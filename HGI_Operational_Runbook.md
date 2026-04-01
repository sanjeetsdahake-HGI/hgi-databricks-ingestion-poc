# HGI Insights — Operational Runbook

## Databricks Medallion Architecture Pipeline

### V4C.ai | HG Insights POC

### Last Updated: April 1, 2026

-----

## TABLE OF CONTENTS

1. Pipeline Overview
1. Job Inventory & Functionality
1. Orchestration & Job Execution
1. How to Interact with Data — Layer by Layer
1. Custom Attributes Guide
1. Data Quality Checks & Verification Queries
1. Gold Layer — Business Insight Queries
1. Pipeline Monitoring & Observability
1. Common Issues & Quick Fixes
1. Contact & Support

-----

## 1. PIPELINE OVERVIEW

```
┌─────────────────────────────────────────────────────────────────────┐
│                  HGI DATABRICKS PIPELINE — END TO END               │
└─────────────────────────────────────────────────────────────────────┘

SOURCE SYSTEMS
┌──────────────────┐        ┌──────────────────┐
│   Salesforce     │        │    BigQuery      │
│   7 Objects      │        │    1 Object      │
│   (CRM Data)     │        │    (Events)      │
└────────┬─────────┘        └────────┬─────────┘
         │                           │
         └─────────────┬─────────────┘
                       │ JOBS A & B (4 jobs)
                       ▼
┌─────────────────────────────────────────────┐
│              LANDING ZONE                   │
│              AWS S3                         │
│   s3://hgi-databricks-data-lakehouse-dev/   │
└─────────────────┬───────────────────────────┘
                  │ JOBS C (4 jobs)
                  ▼
┌─────────────────────────────────────────────┐
│              BRONZE LAYER                   │
│         Catalog: bronze                     │
│         Schema:  cust_001                   │
│                                             │
│  crm_accounts      crm_contacts             │
│  crm_events        crm_tasks                │
│  crm_campaigns     crm_opportunities        │
│  crm_users         crm_campaign_members     │
└─────────────────┬───────────────────────────┘
                  │ JOB D (1 job)
                  ▼
┌─────────────────────────────────────────────┐
│              SILVER LAYER                   │
│         Catalog: hgi                        │
│         Schema:  silver                     │
│                                             │
│  contacts          accounts                 │
│  events            tasks                    │
│  campaigns         opportunities            │
│  users             campaign_members         │
│  contacts_all      accounts_all             │
│  unified_events    unified_tasks            │
│  unified_campaigns unified_opportunities    │
│  accounts_attributes contacts_attributes    │
│  email_model_conversion  (19 tables total)  │
└─────────────────┬───────────────────────────┘
                  │ JOB E (1 job)
                  ▼
┌─────────────────────────────────────────────┐
│               GOLD LAYER                    │
│         Catalog: hgi                        │
│         Schema:  gold                       │
│                                             │
│  persons                                    │
│  companies                                  │
│  contacts_computations                      │
│  audit_logs                                 │
│  logs/cdc_validation_log                    │
└─────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────┐
│         OBSERVABILITY LAYER                 │
│  bronze.pipeline_observability              │
│    .pipeline_metrics_stats                  │
│  bronze.logs.audit_logs                     │
│  hgi.silver.audit_logs                      │
│  hgi.gold.audit_logs                        │
└─────────────────────────────────────────────┘
```

**Tech Stack:**

- Platform: Databricks (Serverless Compute)
- Storage: AWS S3 (Landing Zone) + Delta Lake
- Sources: Salesforce (7 objects) + BigQuery (1 object)
- Ingestion: Auto Loader (checkpoint-based)
- Tenants: cust_001, cust_002
- Tenant identifier: `tenant` (bronze/silver), `customer_id` (gold)

-----

## 2. JOB INVENTORY & FUNCTIONALITY

### GROUP A & B — Source → Landing Zone (4 Jobs)

```
┌─────────────────────────────────────────────────────────────────────┐
│              SOURCE → LANDING ZONE JOBS                             │
└─────────────────────────────────────────────────────────────────────┘
```

|Job Name                                              |Type       |Source    |Description                                                                                          |
|------------------------------------------------------|-----------|----------|-----------------------------------------------------------------------------------------------------|
|`A_hgi_bigquery_historical_load_to_landingzone_job`   |Historical |BigQuery  |Full load of all BigQuery event data to S3 landing zone. Run once for initial setup or full refresh. |
|`A_hgi_bigquery_incremental_load_to_landingzone_job`  |Incremental|BigQuery  |Loads only new/changed BigQuery records since last run using watermark. Run daily.                   |
|`B_hgi_salesforce_historical_load_to_landingzone_job` |Historical |Salesforce|Full load of all 7 Salesforce objects to S3 landing zone. Run once for initial setup or full refresh.|
|`B_hgi_salesforce_incremental_load_to_landingzone_job`|Incremental|Salesforce|Loads only new/changed Salesforce records since last run. Run daily.                                 |

**What these jobs do:**

- Extract data from Salesforce and BigQuery
- Write raw files to AWS S3 Landing Zone
- Update watermark table after each successful run
- Support both historical (full load) and incremental (delta load) patterns

-----

### GROUP C — Landing Zone → Bronze (4 Jobs)

```
┌─────────────────────────────────────────────────────────────────────┐
│              LANDING ZONE → BRONZE JOBS                             │
└─────────────────────────────────────────────────────────────────────┘
```

|Job Name                                                          |Type       |Source    |Description                                                                                        |
|------------------------------------------------------------------|-----------|----------|---------------------------------------------------------------------------------------------------|
|`C_hgi_bigquery_Landingzone_To_Bronze_Historical_Ingestion_job`   |Historical |BigQuery  |Full ingestion of BigQuery landing files into bronze Delta tables.                                 |
|`C_hgi_bigquery_Landingzone_To_Bronze_Incremental_Ingestion_job`  |Incremental|BigQuery  |Incremental ingestion using Auto Loader — picks up only new files since last checkpoint.           |
|`C_hgi_salesforce_Landingzone_To_Bronze_Historical_Ingestion_job` |Historical |Salesforce|Full ingestion of all Salesforce landing files into bronze Delta tables.                           |
|`C_hgi_salesforce_Landingzone_To_Bronze_Incremental_Ingestion_job`|Incremental|Salesforce|Incremental ingestion using Auto Loader — picks up only new Salesforce files since last checkpoint.|

**What these jobs do:**

- Read raw files from S3 Landing Zone using Auto Loader
- Apply schema enforcement and basic data typing
- Write to bronze Delta tables in `bronze.cust_001` schema
- Enable Change Data Feed (CDF) on all bronze tables
- Maintain checkpoints for incremental processing
- Update watermark table after each successful run

-----

### JOB D — Bronze → Silver (1 Job)

```
┌─────────────────────────────────────────────────────────────────────┐
│              BRONZE → SILVER JOB                                    │
└─────────────────────────────────────────────────────────────────────┘
```

|Job Name                               |Description                                                                                                                                            |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
|`D_Bronze_to_silver_transformation_job`|Reads changes from bronze via CDF, applies Map logic to transform and unify 8 Salesforce objects + 1 BigQuery object into 19 standardized silver tables|

**What this job does:**

- Reads only changed records from bronze using `readChangeFeed = true`
- Applies client-defined Map logic for field standardization
- Creates unified views (`contacts_all`, `accounts_all`) for downstream consumption
- Stores custom attributes as JSON in `custom_metadata` column
- Writes to 19 silver tables in `hgi.silver` schema
- Enables CDF on all silver tables
- Updates watermark after successful run

-----

### JOB E — Silver → Gold (1 Job)

```
┌─────────────────────────────────────────────────────────────────────┐
│              SILVER → GOLD JOB                                      │
│              (2 Tasks — Chained)                                    │
└─────────────────────────────────────────────────────────────────────┘

Task 1: 03_Load_Silver_to_Gold_Augment
    ↓ (on success)
Task 2: 05_Gold_Augment_Unit_Test
```

|Job Name                             |Tasks                                          |Description                                                                                                                                                            |
|-------------------------------------|-----------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`E_Silver_to_gold_transformation_job`|Task 1: Main enrichment + Task 2: DQ validation|Enriches contacts with demographic data (persons) and accounts with firmographic data (companies). Builds wide feature table for AI/ML scoring. Validates data quality.|

**What this job does:**

**Task 1 — Main Enrichment:**

- Reads contacts and accounts from silver layer
- Applies Priority-based email candidate selection (Priority 1 → 2 → 3)
- Enriches contacts with person_profiles (demographic data) → writes to `hgi.gold.persons`
- Enriches accounts with company_profiles (firmographic data) → writes to `hgi.gold.companies`
- Builds wide feature table (47 columns) → writes to `hgi.gold.contacts_computations`
- Enables CDF on all gold tables
- Saves pipeline metrics via MetricsQuery module
- Writes audit log and sends email alert via audit_logger module

**Task 2 — DQ Validation:**

- Runs 11 automated data quality checks
- Validates enrichment coverage thresholds
- Verifies CDF enabled on gold tables
- Saves test metrics

-----

### ORCHESTRATION JOB

```
┌─────────────────────────────────────────────────────────────────────┐
│              MASTER ORCHESTRATION JOB                               │
└─────────────────────────────────────────────────────────────────────┘

hgi_incremental-orchestration_job_with_pipeline_metrics

Includes ALL jobs in sequence:
A (incremental) → B (incremental) → C (incremental) → D → E
```

|Job Name                                                 |Description                                                                                                                                                               |
|---------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|`hgi_incremental-orchestration_job_with_pipeline_metrics`|Master orchestration job that chains all incremental pipeline jobs in correct sequence. Captures pipeline metrics across all jobs. Triggers on demand — not yet scheduled.|

**What this job does:**

- Runs all incremental jobs in correct dependency order
- Captures pipeline metrics across all layers in `pipeline_metrics_stats`
- Provides single trigger point for full pipeline execution
- Email alerts on any job failure

-----

## 3. ORCHESTRATION & JOB EXECUTION

### How to Run Jobs Manually

**Step 1:** Go to Databricks → **Jobs & Pipelines**
**Step 2:** Find the job by name
**Step 3:** Click **Run now**
**Step 4:** Monitor in **Graph** or **Timeline** view

### Job Execution Order

```
FOR INITIAL SETUP (run once):
────────────────────────────────────────────────
A_hgi_bigquery_historical_load_to_landingzone_job
        ↓
A_hgi_salesforce_historical_load_to_landingzone_job (or B)
        ↓
C_hgi_bigquery_Landingzone_To_Bronze_Historical_Ingestion_job
        ↓
C_hgi_salesforce_Landingzone_To_Bronze_Historical_Ingestion_job
        ↓
D_Bronze_to_silver_transformation_job
        ↓
E_Silver_to_gold_transformation_job

FOR DAILY INCREMENTAL RUNS:
────────────────────────────────────────────────
hgi_incremental-orchestration_job_with_pipeline_metrics
(runs all incremental jobs in sequence automatically)
```

### How to Check if a Job Succeeded or Failed

1. Go to **Jobs & Pipelines** → click job name
1. Click **Runs** tab
1. Green = Success | Red = Failed
1. Click on failed run → click failed task → read error in **Output** tab
1. Check email — failure alerts sent automatically via audit_logger

-----

## 4. HOW TO INTERACT WITH DATA — LAYER BY LAYER

### BRONZE LAYER — `bronze.cust_001`

**List all bronze tables:**

```sql
SHOW TABLES IN bronze.cust_001;
```

**Query data for a specific tenant:**

```sql
-- Get all contacts for tenant cust_001
SELECT * FROM bronze.cust_001.crm_contacts
WHERE tenant = 1
LIMIT 100;
```

**Check record count per table:**

```sql
SELECT 'crm_contacts' AS table_name, COUNT(*) AS record_count FROM bronze.cust_001.crm_contacts
UNION ALL
SELECT 'crm_accounts', COUNT(*) FROM bronze.cust_001.crm_accounts
UNION ALL
SELECT 'crm_events', COUNT(*) FROM bronze.cust_001.crm_events
UNION ALL
SELECT 'crm_tasks', COUNT(*) FROM bronze.cust_001.crm_tasks
UNION ALL
SELECT 'crm_campaigns', COUNT(*) FROM bronze.cust_001.crm_campaigns
UNION ALL
SELECT 'crm_opportunities', COUNT(*) FROM bronze.cust_001.crm_opportunities
UNION ALL
SELECT 'crm_users', COUNT(*) FROM bronze.cust_001.crm_users
UNION ALL
SELECT 'crm_campaign_members', COUNT(*) FROM bronze.cust_001.crm_campaign_members;
```

**Check latest ingested records:**

```sql
SELECT * FROM bronze.cust_001.crm_contacts
ORDER BY data_timestamp DESC
LIMIT 10;
```

**Check CDF is enabled on bronze tables:**

```sql
SHOW TBLPROPERTIES bronze.cust_001.crm_contacts;
-- Look for: delta.enableChangeDataFeed = true
```

**View schema of a bronze table:**

```sql
DESCRIBE TABLE bronze.cust_001.crm_contacts;
```

-----

### SILVER LAYER — `hgi.silver`

**List all silver tables:**

```sql
SHOW TABLES IN hgi.silver;
```

**View schema of key silver tables:**

```sql
DESCRIBE TABLE hgi.silver.contacts;
DESCRIBE TABLE hgi.silver.accounts;
DESCRIBE TABLE hgi.silver.events;
```

**Query contacts for a specific tenant:**

```sql
-- Get all contacts for tenant
SELECT
    id,
    email,
    domain,
    source_system,
    data_timestamp,
    status
FROM hgi.silver.contacts
WHERE tenant = 1
LIMIT 100;
```

**Query accounts for a specific tenant:**

```sql
SELECT
    id,
    domain,
    name,
    source_system,
    data_timestamp,
    status
FROM hgi.silver.accounts
WHERE tenant = 1
LIMIT 100;
```

**Query events for a specific tenant:**

```sql
SELECT
    contact_id,
    domain,
    source_system,
    created_at,
    updated_at
FROM hgi.silver.events
WHERE tenant = 1
LIMIT 100;
```

**Get contacts with custom attributes:**

```sql
-- Get contacts with their custom attributes
SELECT
    id,
    email,
    domain,
    data_timestamp,
    status,
    custom_metadata
FROM hgi.silver.contacts
WHERE tenant = 1
AND custom_metadata IS NOT NULL
LIMIT 50;
```

**Extract specific custom attribute from contacts:**

```sql
-- Extract a specific custom attribute field
SELECT
    id,
    email,
    custom_metadata['a_contactsource']  AS contact_source,
    custom_metadata['a_leadsource']     AS lead_source,
    custom_metadata['a_title']          AS title,
    custom_metadata['a_department']     AS department,
    custom_metadata['a_ownerid']        AS owner_id
FROM hgi.silver.contacts
WHERE tenant = 1
LIMIT 50;
```

**Extract custom attributes from opportunities:**

```sql
SELECT
    id,
    custom_metadata['a_amount__c']          AS amount,
    custom_metadata['a_forecastcategory']   AS forecast_category,
    custom_metadata['a_isclosed']           AS is_closed,
    custom_metadata['a_iswon']              AS is_won,
    custom_metadata['a_probability__c']     AS probability,
    custom_metadata['a_type__c']            AS opportunity_type,
    custom_metadata['a_fiscal']             AS fiscal_period
FROM hgi.silver.opportunities
WHERE tenant = 1
LIMIT 50;
```

**Get contacts with recent activity (last 7 days):**

```sql
SELECT
    id,
    email,
    domain,
    data_timestamp
FROM hgi.silver.contacts
WHERE tenant = 1
AND data_timestamp >= date_add(current_date(), -7)
ORDER BY data_timestamp DESC;
```

**Get contacts via unified view (contacts_all):**

```sql
SELECT
    id,
    email,
    domain,
    source_system,
    tenant
FROM hgi.silver.contacts_all
WHERE email IS NOT NULL
AND tenant = 1
LIMIT 100;
```

-----

### GOLD LAYER — `hgi.gold`

**List all gold tables:**

```sql
SHOW TABLES IN hgi.gold;
```

**View schema of gold tables:**

```sql
DESCRIBE TABLE hgi.gold.persons;
DESCRIBE TABLE hgi.gold.companies;
DESCRIBE TABLE hgi.gold.contacts_computations;
```

-----

## 5. CUSTOM ATTRIBUTES GUIDE

Custom attributes are stored as a JSON map in the `custom_metadata` column across these silver tables:

|Table                        |Key Custom Attributes                                                                                    |
|-----------------------------|---------------------------------------------------------------------------------------------------------|
|`hgi.silver.contacts`        |`a_title`, `a_department`, `a_leadsource`, `a_contactsource`, `a_ownerid`, `a_reportstoid`               |
|`hgi.silver.accounts`        |`a_industry`, `a_annualrevenue`, `a_numberofemployees`, `a_website`, `a_type`, `a_rating`                |
|`hgi.silver.events`          |`a_contactid`, `a_created_date__c`, `a_data_timestamp__c`                                                |
|`hgi.silver.opportunities`   |`a_amount__c`, `a_forecastcategory`, `a_isclosed`, `a_iswon`, `a_probability__c`, `a_type__c`, `a_fiscal`|
|`hgi.silver.tasks`           |Custom task attributes                                                                                   |
|`hgi.silver.campaigns`       |Custom campaign attributes                                                                               |
|`hgi.silver.users`           |Custom user attributes                                                                                   |
|`hgi.silver.campaign_members`|Custom campaign member attributes                                                                        |

**How to list all keys in custom_metadata:**

```sql
SELECT DISTINCT explode(map_keys(custom_metadata)) AS attribute_key
FROM hgi.silver.contacts
WHERE custom_metadata IS NOT NULL
ORDER BY attribute_key;
```

**How to get all custom attributes for a contact:**

```sql
SELECT
    id,
    email,
    explode(custom_metadata) AS (attribute_key, attribute_value)
FROM hgi.silver.contacts
WHERE email = 'example@company.com';
```

-----

## 6. DATA QUALITY CHECKS & VERIFICATION QUERIES

### Bronze Layer DQ

**Check for duplicate records in bronze:**

```sql
SELECT source_key_value, COUNT(*) AS cnt
FROM bronze.cust_001.crm_contacts
GROUP BY source_key_value
HAVING cnt > 1
ORDER BY cnt DESC;
```

**Check for NULL emails in bronze:**

```sql
SELECT COUNT(*) AS null_email_count
FROM bronze.cust_001.crm_contacts
WHERE email IS NULL;
```

**Check CDF enabled on all bronze tables:**

```sql
SHOW TBLPROPERTIES bronze.cust_001.crm_contacts;
SHOW TBLPROPERTIES bronze.cust_001.crm_accounts;
SHOW TBLPROPERTIES bronze.cust_001.crm_events;
```

-----

### Silver Layer DQ

**Check for duplicate contacts in silver:**

```sql
SELECT email, COUNT(*) AS cnt
FROM hgi.silver.contacts
GROUP BY email
HAVING cnt > 1
ORDER BY cnt DESC;
```

**Check NULL emails in silver:**

```sql
SELECT COUNT(*) AS null_email_count
FROM hgi.silver.contacts
WHERE email IS NULL;
```

**Check data freshness — when was silver last updated:**

```sql
SELECT
    MAX(data_timestamp) AS latest_record,
    MIN(data_timestamp) AS oldest_record,
    COUNT(*)            AS total_records
FROM hgi.silver.contacts;
```

**Check CDF enabled on silver tables:**

```sql
SHOW TBLPROPERTIES hgi.silver.contacts;
SHOW TBLPROPERTIES hgi.silver.accounts;
SHOW TBLPROPERTIES hgi.silver.events;
```

-----

### Gold Layer DQ — 11 Automated Checks

**Run all 11 DQ checks manually:**

```sql
-- CHECK 1: No duplicate emails in persons
SELECT COUNT(*) AS duplicate_emails FROM (
    SELECT mk_email, COUNT(*) AS c
    FROM hgi.gold.persons
    GROUP BY mk_email HAVING c > 1
);
-- Expected: 0

-- CHECK 2: Emails are lowercase in persons
SELECT COUNT(*) AS non_lowercase_emails
FROM hgi.gold.persons
WHERE mk_email != lower(mk_email);
-- Expected: 0

-- CHECK 3: Status not null in persons
SELECT COUNT(*) AS null_status
FROM hgi.gold.persons
WHERE mk_status IS NULL;
-- Expected: 0

-- CHECK 4: No duplicate domains in companies
SELECT COUNT(*) AS duplicate_domains FROM (
    SELECT mk_domain, COUNT(*) AS c
    FROM hgi.gold.companies
    GROUP BY mk_domain HAVING c > 1
);
-- Expected: 0

-- CHECK 5: Domains are lowercase in companies
SELECT COUNT(*) AS non_lowercase_domains
FROM hgi.gold.companies
WHERE mk_domain != lower(mk_domain);
-- Expected: 0

-- CHECK 6: Status not null in companies
SELECT COUNT(*) AS null_status
FROM hgi.gold.companies
WHERE mk_status IS NULL;
-- Expected: 0

-- CHECK 7: Persons enrichment coverage >= 60%
SELECT
    COUNT(*) AS total_persons,
    SUM(CASE WHEN mk_status = 'ok' THEN 1 ELSE 0 END) AS ok_persons,
    ROUND(SUM(CASE WHEN mk_status = 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS coverage_pct
FROM hgi.gold.persons;
-- Expected: coverage_pct >= 60.0

-- CHECK 8: Companies enrichment coverage >= 70%
SELECT
    COUNT(*) AS total_companies,
    SUM(CASE WHEN mk_status = 'ok' THEN 1 ELSE 0 END) AS ok_companies,
    ROUND(SUM(CASE WHEN mk_status = 'ok' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS coverage_pct
FROM hgi.gold.companies;
-- Expected: coverage_pct >= 70.0

-- CHECK 9: Contacts with company data >= 50%
SELECT
    total_contacts,
    contacts_with_company,
    ROUND(contacts_with_company * 100.0 / total_contacts, 2) AS coverage_pct
FROM (
    SELECT COUNT(*) AS total_contacts FROM hgi.silver.contacts_all
) t1, (
    SELECT COUNT(*) AS contacts_with_company
    FROM hgi.gold.contacts_computations
    WHERE company__name IS NOT NULL
) t2;
-- Expected: coverage_pct >= 50.0

-- CHECK 10: CDF enabled on gold persons
SHOW TBLPROPERTIES hgi.gold.persons;
-- Expected: delta.enableChangeDataFeed = true

-- CHECK 11: CDF enabled on gold companies and contacts_computations
SHOW TBLPROPERTIES hgi.gold.companies;
SHOW TBLPROPERTIES hgi.gold.contacts_computations;
-- Expected: delta.enableChangeDataFeed = true
```

-----

## 7. GOLD LAYER — BUSINESS INSIGHT QUERIES

### Persons (Contact Demographics)

**Get enriched contacts with full profile:**

```sql
SELECT
    mk_email,
    mk_domain,
    mk_status,
    name__fullname,
    name__givenname,
    name__familyname,
    employment__title,
    employment__seniority,
    employment__name        AS company_name,
    geo__city,
    geo__country,
    geo__state,
    linkedin__handle
FROM hgi.gold.persons
WHERE mk_status = 'ok'
LIMIT 100;
```

**Get enrichment coverage summary:**

```sql
SELECT
    mk_status,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM hgi.gold.persons
GROUP BY mk_status
ORDER BY count DESC;
```

**Get contacts by seniority level:**

```sql
SELECT
    employment__seniority,
    COUNT(*) AS count
FROM hgi.gold.persons
WHERE mk_status = 'ok'
GROUP BY employment__seniority
ORDER BY count DESC;
```

**Get contacts by country:**

```sql
SELECT
    geo__country,
    COUNT(*) AS contact_count
FROM hgi.gold.persons
WHERE mk_status = 'ok'
AND geo__country IS NOT NULL
GROUP BY geo__country
ORDER BY contact_count DESC
LIMIT 20;
```

-----

### Companies (Account Firmographics)

**Get enriched companies with full profile:**

```sql
SELECT
    mk_domain,
    mk_status,
    name,
    category__industry,
    category__sector,
    metrics__employees,
    metrics__employeesrange,
    metrics__raised,
    geo__country,
    geo__city,
    tech,
    founded_year,
    is_personal
FROM hgi.gold.companies
WHERE mk_status = 'ok'
LIMIT 100;
```

**Get companies by industry:**

```sql
SELECT
    category__industry,
    COUNT(*) AS company_count
FROM hgi.gold.companies
WHERE mk_status = 'ok'
AND category__industry IS NOT NULL
GROUP BY category__industry
ORDER BY company_count DESC;
```

**Get companies by employee size:**

```sql
SELECT
    metrics__employeesrange,
    COUNT(*) AS company_count
FROM hgi.gold.companies
WHERE mk_status = 'ok'
AND metrics__employeesrange IS NOT NULL
GROUP BY metrics__employeesrange
ORDER BY company_count DESC;
```

**Get top tech stacks used by companies:**

```sql
SELECT
    tech,
    COUNT(*) AS company_count
FROM hgi.gold.companies
WHERE mk_status = 'ok'
AND tech IS NOT NULL
GROUP BY tech
ORDER BY company_count DESC
LIMIT 20;
```

-----

### Contacts Computations (Wide Feature Table)

**Get full enriched contact with person + company data:**

```sql
SELECT
    contact_id,
    email,
    domain,
    -- Person data
    person__name__fullname,
    person__employment__title,
    person__employment__seniority,
    person__geo__country,
    -- Company data
    company__name,
    company__category__industry,
    company__metrics__employees,
    company__geo__country,
    company__tech
FROM hgi.gold.contacts_computations
WHERE person__mk_status = 'ok'
AND company__mk_status = 'ok'
LIMIT 100;
```

**Get contacts by industry and seniority (for targeting):**

```sql
SELECT
    company__category__industry     AS industry,
    person__employment__seniority   AS seniority,
    COUNT(*)                        AS contact_count
FROM hgi.gold.contacts_computations
WHERE person__mk_status = 'ok'
AND company__mk_status = 'ok'
GROUP BY industry, seniority
ORDER BY contact_count DESC;
```

**Get contacts in specific country with company size filter:**

```sql
SELECT
    contact_id,
    email,
    person__name__fullname,
    person__employment__title,
    company__name,
    company__metrics__employeesrange,
    company__geo__country
FROM hgi.gold.contacts_computations
WHERE company__geo__country = 'United States'
AND company__metrics__employees > 1000
AND person__mk_status = 'ok'
LIMIT 50;
```

**Get enrichment summary per contact:**

```sql
SELECT
    contact_id,
    email,
    person__mk_status   AS person_enrichment,
    company__mk_status  AS company_enrichment,
    CASE
        WHEN person__mk_status = 'ok' AND company__mk_status = 'ok' THEN 'FULLY ENRICHED'
        WHEN person__mk_status = 'ok' THEN 'PERSON ONLY'
        WHEN company__mk_status = 'ok' THEN 'COMPANY ONLY'
        ELSE 'NOT ENRICHED'
    END AS enrichment_status
FROM hgi.gold.contacts_computations
LIMIT 100;
```

**Get enrichment status summary:**

```sql
SELECT
    CASE
        WHEN person__mk_status = 'ok' AND company__mk_status = 'ok' THEN 'FULLY ENRICHED'
        WHEN person__mk_status = 'ok' THEN 'PERSON ONLY'
        WHEN company__mk_status = 'ok' THEN 'COMPANY ONLY'
        ELSE 'NOT ENRICHED'
    END AS enrichment_status,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM hgi.gold.contacts_computations
GROUP BY enrichment_status
ORDER BY count DESC;
```

-----

## 8. PIPELINE MONITORING & OBSERVABILITY

### Pipeline Metrics Stats

**Get latest run stats for all jobs:**

```sql
SELECT
    job_name,
    pipeline_flow,
    status,
    duration_seconds,
    rows_processed,
    dbus_consumed,
    estimated_cost_usd,
    compute_type,
    start_time,
    end_time
FROM bronze.pipeline_observability.pipeline_metrics_stats
ORDER BY start_time DESC
LIMIT 20;
```

**Get latest run for silver to gold job:**

```sql
SELECT
    job_name,
    status,
    rows_processed,
    duration_seconds,
    estimated_cost_usd,
    start_time
FROM bronze.pipeline_observability.pipeline_metrics_stats
WHERE job_name LIKE '%silver_to_gold%'
ORDER BY start_time DESC
LIMIT 5;
```

**Get cost summary per job (last 7 days):**

```sql
SELECT
    job_name,
    pipeline_flow,
    COUNT(*)                            AS total_runs,
    ROUND(SUM(dbus_consumed), 4)        AS total_dbus,
    ROUND(SUM(estimated_cost_usd), 4)   AS total_cost_usd,
    ROUND(AVG(estimated_cost_usd), 4)   AS avg_cost_per_run
FROM bronze.pipeline_observability.pipeline_metrics_stats
WHERE run_date >= date_add(current_date(), -7)
GROUP BY job_name, pipeline_flow
ORDER BY total_cost_usd DESC;
```

**Get failed runs (last 7 days):**

```sql
SELECT
    job_name,
    run_id,
    run_date,
    pipeline_flow,
    error_reason,
    duration_seconds
FROM bronze.pipeline_observability.pipeline_metrics_stats
WHERE status = 'FAILED'
AND run_date >= date_add(current_date(), -7)
ORDER BY run_date DESC;
```

**Get duration trends per job:**

```sql
SELECT
    job_name,
    pipeline_flow,
    COUNT(*)                                        AS total_runs,
    ROUND(AVG(duration_seconds), 2)                AS avg_seconds,
    ROUND(PERCENTILE(duration_seconds, 0.50), 2)   AS p50_seconds,
    ROUND(PERCENTILE(duration_seconds, 0.90), 2)   AS p90_seconds,
    ROUND(MAX(duration_seconds), 2)                AS max_seconds
FROM bronze.pipeline_observability.pipeline_metrics_stats
WHERE status = 'SUCCEEDED'
GROUP BY job_name, pipeline_flow
ORDER BY p90_seconds DESC;
```

-----

### Audit Logs

**Get latest audit logs across all layers:**

```sql
-- Bronze audit logs
SELECT job_name, status, alert_type, rows_processed, duration_ms, timestamp
FROM bronze.logs.audit_logs
ORDER BY timestamp DESC
LIMIT 10;

-- Silver audit logs
SELECT job_name, status, alert_type, rows_processed, duration_ms, timestamp
FROM hgi.silver.audit_logs
ORDER BY timestamp DESC
LIMIT 10;

-- Gold audit logs
SELECT job_name, status, alert_type, rows_processed, duration_ms, timestamp
FROM hgi.gold.audit_logs
ORDER BY timestamp DESC
LIMIT 10;
```

**Get all failures across layers:**

```sql
SELECT 'bronze' AS layer, job_name, status, alert_type, error_reason, timestamp
FROM bronze.logs.audit_logs WHERE status = 'failure'
UNION ALL
SELECT 'silver', job_name, status, alert_type, error_reason, timestamp
FROM hgi.silver.audit_logs WHERE status = 'failure'
UNION ALL
SELECT 'gold', job_name, status, alert_type, error_reason, timestamp
FROM hgi.gold.audit_logs WHERE status = 'failure'
ORDER BY timestamp DESC;
```

-----

### CDC Validation Log

**Check CDF status on gold tables:**

```sql
SELECT
    layer,
    table_name,
    cdf_status,
    validation_timestamp
FROM hgi.gold.logs.cdc_validation_log
ORDER BY validation_timestamp DESC
LIMIT 10;
```

**Check if any gold table has CDF disabled:**

```sql
SELECT
    table_name,
    cdf_status,
    validation_timestamp
FROM hgi.gold.logs.cdc_validation_log
WHERE cdf_status = 'DISABLED'
ORDER BY validation_timestamp DESC;
```

-----

### Watermarks

**Check last processed timestamps:**

```sql
SELECT
    source_system,
    object_name,
    last_processed_timestamp
FROM ingestion_metadata.watermarks
ORDER BY last_processed_timestamp DESC;
```

-----




## 9. CONTACT & SUPPORT
###              V4C Team
-----

*Document: HGI_Operational_Runbook.md*
*Project: HGI POC — Databricks Medallion Architecture*
*Author: Pratibha, Lead Data Engineer, V4C.ai*
*Last Updated: April 1, 2026*