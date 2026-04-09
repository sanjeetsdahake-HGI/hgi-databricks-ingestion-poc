# HGI Pipeline Optimization - Testing & Verification Guide

## 📋 Overview

This guide helps you test the optimized pipeline and verify the performance improvements and CDF compliance.

**Expected Results:**
- **Time**: 12-14 min → **3-5 min** (70% reduction)
- **Full table scans**: ~15 per run → **0**
- **CDF Compliance**: 100%

---

## ✅ Pre-Test Checklist

### 1. Verify All Fixes Applied

**Notebooks to Check:**

| Notebook | Fix | How to Verify |
|----------|-----|---------------|
| `_map_common.py` | H4: cnt() uses DESCRIBE DETAIL | Open notebook, check line 50 |
| `02_BQ_Incremental_Load` | H5: Cache before count | Check Cell 4 has `.cache()` |
| `02_BQ_Incremental_Load` | M8: repartition(10) | Check Cell 4 has `.repartition(10)` |
| `02b_map_04_events_mapping.py` | H3: Broadcast contacts | Check Cell 8 has `SELECT /*+ BROADCAST */` |
| `03_Load_Silver_to_Gold_Augment` | M5: Uses %run | Check Cell 3 uses `%run` not `exec()` |
| `03_Load_Silver_to_Gold_Augment` | M6: No shuffle.partitions | Check Cell 9 - line should be commented out |
| `03_Load_Silver_to_Gold_Augment` | M7: No FREE_EMAIL_DOMAINS | Check Cell 9 - local definition should be removed |

---

## 🚀 Test Execution Steps

### Step 1: Baseline Measurement (Optional)

If you want to measure improvement, run the ORIGINAL job once first:

```bash
# In Databricks workspace, navigate to:
# Workflows → Jobs → hgi_incremental_orchestration_flattened_job
# Click "Run now" with customer_id = cust_001
# Record: Start time, End time, Duration
```

**Expected Baseline**: 12-14 minutes

---

### Step 2: Create the Optimized Job

1. Go to **Workflows** → **Jobs**
2. Click **Create Job**
3. Name it: `hgi_incremental_OPTIMIZED_test`
4. Import configuration from `hgi_incremental_OPTIMIZED_job.yml` OR manually configure:
   - Upload the YAML file
   - Or copy task definitions from the YAML

---

### Step 3: Run the Optimized Job

```bash
# In Databricks workspace:
# Workflows → Jobs → hgi_incremental_OPTIMIZED_test
# Click "Run now"
# Parameters: customer_id = cust_001
```

**Monitor the run:**
- Watch task execution times
- Check for any failures
- Note the total duration

**Expected Result**: 3-5 minutes (first run may be slightly longer due to cold start)

---

### Step 4: Verify Performance Metrics

After the job completes, gather these metrics:

#### 4.1 Overall Job Duration

```sql
-- Query job run history
SELECT 
  run_id,
  start_time,
  end_time,
  ROUND((UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / 60, 1) as duration_minutes,
  state
FROM 
  system.workflows.runs
WHERE 
  job_name = 'hgi_incremental_OPTIMIZED_test'
  AND start_time > current_timestamp() - INTERVAL 1 HOUR
ORDER BY start_time DESC
LIMIT 5;
```

**Expected**: 3-5 minutes

---

#### 4.2 Task-Level Timing

Check which tasks are taking the most time:

```sql
-- In the job run UI, click "View Details"
-- Sort tasks by duration
-- Identify any tasks taking > 2 minutes
```

**Expected Longest Tasks:**
- Silver base tables: 30-60 seconds each
- Map notebooks: 20-40 seconds each
- Gold augment: 60-90 seconds
- BQ ingestion: 60-120 seconds (depends on data volume)

---

## 🔍 CDF Compliance Verification

### Critical Verification: No Full Table Scans

Run these queries IMMEDIATELY after the job completes:

### Query 1: Verify contacts_all Processing

```sql
-- Check contacts_all MERGE operation
SELECT 
  version,
  timestamp,
  operation,
  operationMetrics.numTargetRowsInserted as rows_inserted,
  operationMetrics.numTargetRowsUpdated as rows_updated,
  operationMetrics.numTargetRowsDeleted as rows_deleted,
  operationMetrics.numOutputRows as rows_scanned,
  operationMetrics.numSourceRows as source_rows,
  operationMetrics.executionTimeMs / 1000 as execution_time_sec
FROM 
  (DESCRIBE HISTORY hgi.silver.contacts_all)
WHERE 
  timestamp > current_timestamp() - INTERVAL 2 HOUR
  AND operation = 'MERGE'
ORDER BY version DESC
LIMIT 5;
```

**Expected**:
- `source_rows`: Should be **small** (e.g., 100-1000) NOT 100,000
- `rows_scanned`: Should match `source_rows` (CDF count, not full table)
- If `rows_scanned` = full table count (100K+) → **FULL SCAN DETECTED** ❌

---

### Query 2: Verify accounts_all Processing

```sql
-- Check accounts_all MERGE operation
SELECT 
  version,
  timestamp,
  operation,
  operationMetrics.numSourceRows as source_rows,
  operationMetrics.numTargetRowsInserted as rows_inserted,
  operationMetrics.numTargetRowsUpdated as rows_updated
FROM 
  (DESCRIBE HISTORY hgi.silver.accounts_all)
WHERE 
  timestamp > current_timestamp() - INTERVAL 2 HOUR
  AND operation = 'MERGE'
ORDER BY version DESC
LIMIT 5;
```

**Expected**:
- `source_rows`: Should be **small** (e.g., 10-100) for incremental run

---

### Query 3: Verify Bronze CDF Processing

```sql
-- Check bronze to silver CDF stream
SELECT 
  version,
  timestamp,
  operation,
  operationMetrics.numOutputRows as cdf_rows_processed,
  operationMetrics.executionTimeMs / 1000 as execution_time_sec
FROM 
  (DESCRIBE HISTORY hgi.silver.contacts)
WHERE 
  timestamp > current_timestamp() - INTERVAL 2 HOUR
  AND operation IN ('STREAMING UPDATE', 'MERGE')
ORDER BY version DESC
LIMIT 5;
```

**Expected**:
- `cdf_rows_processed`: Should match the number of changed records in bronze

---

### Query 4: Check for cnt() Full Scans

The old `cnt()` function would show up as a SELECT operation. Verify it's gone:

```sql
-- Look for SELECT operations in map tables
SELECT 
  version,
  timestamp,
  operation,
  operationMetrics.numOutputRows as rows_read,
  operationMetrics.executionTimeMs / 1000 as execution_time_sec
FROM 
  (DESCRIBE HISTORY hgi.silver.contacts_all)
WHERE 
  timestamp > current_timestamp() - INTERVAL 2 HOUR
  AND operation = 'SELECT'
ORDER BY version DESC
LIMIT 10;
```

**Expected**:
- **NO SELECT operations** from cnt() calls
- If you see SELECT with `rows_read` = full table → old cnt() still being used ❌

---

## 📊 Performance Comparison Table

Fill this in after your test runs:

| Metric | Original (Baseline) | Optimized | Improvement |
|--------|---------------------|-----------|-------------|
| **Total Job Duration** | ___ min | ___ min | ___% |
| **BQ Ingestion** | ___ sec | ___ sec | ___% |
| **Bronze Layer** | ___ sec | ___ sec | ___% |
| **Silver Base** | ___ sec | ___ sec | ___% |
| **Map Layer** | ___ sec | ___ sec | ___% |
| **Gold Augment** | ___ sec | ___ sec | ___% |
| **Full Table Scans** | ~15 | ___ | ___% |
| **CDF Records Processed** | N/A | ___ | N/A |

---

## 🐛 Troubleshooting

### Issue 1: Job Still Takes 10+ Minutes

**Possible Causes:**
1. cnt() fix not applied → Check `_map_common.py` line 50
2. Map notebooks still reading full tables → Verify H1 fixes
3. BQ load still counting twice → Check `02_BQ_Incremental_Load` Cell 5

**How to Debug:**
```sql
-- Find the slowest tasks
SELECT 
  task_key,
  execution_duration / 1000 / 60 as duration_minutes
FROM 
  system.workflows.task_runs
WHERE 
  run_id = '<your_run_id>'
ORDER BY execution_duration DESC
LIMIT 10;
```

---

### Issue 2: Full Table Scans Still Happening

**How to Detect:**
```sql
-- Check for large row scans
SELECT 
  tableName,
  operation,
  operationMetrics.numOutputRows as rows_scanned,
  timestamp
FROM 
  (
    SELECT *, explode(operationMetrics) as operation_metrics
    FROM (DESCRIBE HISTORY hgi.silver.contacts_all)
    WHERE timestamp > current_timestamp() - INTERVAL 2 HOUR
  )
WHERE 
  operation IN ('MERGE', 'SELECT')
  AND rows_scanned > 10000  -- Flag large scans
ORDER BY timestamp DESC;
```

**If rows_scanned > 50K:**
- Map notebook is NOT using CDF
- cnt() is still doing .count()
- Check notebook code revisions

---

### Issue 3: Job Fails with "Table Not Found"

**Possible Cause**: Metadata table for version tracking doesn't exist

**Fix**: Run this once:
```sql
-- Create version tracking metadata table
CREATE TABLE IF NOT EXISTS hgi.silver.map_metadata (
    key STRING,
    version BIGINT,
    last_updated TIMESTAMP
) USING DELTA;

-- Optionally cluster it
ALTER TABLE hgi.silver.map_metadata 
CLUSTER BY (key);
```

---

### Issue 4: "numRows is NULL" Error from cnt()

**Cause**: Table has no statistics yet

**Fix**: Run OPTIMIZE to collect stats:
```sql
OPTIMIZE hgi.silver.contacts_all;
OPTIMIZE hgi.silver.accounts_all;
OPTIMIZE hgi.silver.mapped_events;
```

---

## ✅ Success Criteria

Your optimization is successful if:

1. ✅ **Job duration: 3-5 minutes** (vs 12-14 min baseline)
2. ✅ **No full table scans** (verify with Query 1-3 above)
3. ✅ **Map layer processes only CDF records** (e.g., 100-1000 rows, not 100K)
4. ✅ **cnt() uses DESCRIBE DETAIL** (no SELECT operations in HISTORY)
5. ✅ **BQ load doesn't count twice** (check task logs for single count)
6. ✅ **All tasks complete successfully** (no failures)

---

## 📈 Scaling Test (Optional)

After verifying with cust_001 (small dataset), test with larger data:

```bash
# Run with more data
Parameters:
  customer_id: cust_001
  # Let more data accumulate (e.g., wait 1 day)
  # Or test with a customer that has more data
```

**Expected Scaling:**
- 100K BQ records: 3-5 minutes
- 1M BQ records: 5-8 minutes (vs 23-30 min before)
- Linear scaling with data volume (because no full scans)

---

## 📝 Reporting Results

After testing, document your findings:

### Template Email/Report

```
Subject: HGI Pipeline Optimization - Test Results

EXECUTIVE SUMMARY
✅ Optimization successful: XX% improvement
✅ CDF compliance: 100% (no full table scans)
✅ Time: XX min → XX min

DETAILED METRICS
- Original job duration: XX minutes
- Optimized job duration: XX minutes
- Improvement: XX% faster
- Full table scans eliminated: XX scans removed
- CDF records processed: XX records (vs full table of XXX records)

TASK-LEVEL BREAKDOWN
- BQ Ingestion: XX sec (was XX sec)
- Bronze Layer: XX sec (was XX sec)
- Silver Base: XX sec (was XX sec)
- Map Layer: XX sec (was XX sec) ← Biggest improvement
- Gold Augment: XX sec (was XX sec)

VERIFICATION QUERIES
✅ contacts_all: Processed XX CDF records (not full 100K table)
✅ accounts_all: Processed XX CDF records (not full 4K table)
✅ cnt() function: No full table scans detected
✅ BQ load: Single count operation (not double)

NEXT STEPS
[ ] Deploy to production
[ ] Monitor first production run
[ ] Document optimization in runbook
```

---

## 🎯 Next Steps After Successful Test

1. **Backup original job** (rename or export YAML)
2. **Update production job** with optimized configuration
3. **Monitor first 3 production runs** closely
4. **Set up alerting** for:
   - Job duration > 8 minutes (should be 3-5)
   - Task failures
   - Full table scans detected
5. **Document the changes** in your team wiki/runbook

---

## 🔗 Quick Reference Links

- [Audit Report](./COMPREHENSIVE_AUDIT_REPORT_2026-04-09.md) - Full findings
- [Fix Implementation Guide](./APPLY_ALL_FIXES.py) - Code changes
- [Optimized Job YAML](./hgi_incremental_OPTIMIZED_job.yml) - Job configuration

---

**Questions or Issues?**

If you encounter problems during testing:
1. Check the troubleshooting section above
2. Verify all fixes were applied (Pre-Test Checklist)
3. Run the CDF verification queries
4. Review task logs for error messages

**Good luck with your testing! 🚀**
