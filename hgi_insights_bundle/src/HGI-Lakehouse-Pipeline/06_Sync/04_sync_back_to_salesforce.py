# Databricks notebook source
# =============================================================================
# Notebook  : 04_sync_back_to_salesforce
# Location  : /HGI-Lakehouse-Pipeline/06_Sync/04_sync_back_to_salesforce
# Spec Ref  : §3.4 Sync (Back to Tenant)
#
# PURPOSE:
#   Push enriched + scored contact data from hgi.gold.contacts_computations
#   back to the tenant's Salesforce CRM via a SIDECAR custom object.
#
# ARCHITECTURE (updated):
#   Old: Wrote enrichment fields directly onto the standard Contact object.
#        Risk: overwrites CRM data, triggers unintended SF automations.
#   New: Writes to MK_Contact_Enrichment__c (sidecar custom object).
#        - Linked to Contact via Contact__c Lookup field.
#        - Deduplicated via MK_Composite_ID__c External ID field.
#        - Databricks upserts: if sidecar exists for that composite ID→update,
#          otherwise create a new sidecar record attached to the Contact.
#
# HOW IT WORKS (spec §3.4):
#   Step 1  → Authenticate with Salesforce (client_credentials)
#   Step 2  → Create sync log + snapshot infrastructure tables
#   Step 3  → Load contacts_computations (source of truth for what to push)
#   Step 4  → Hash all rows (MD5 of all pushable field values)
#   Step 5  → Compare stage hash vs snapshot hash
#             • hash differs     → record CHANGED  → mark “updated”
#             • no match in snap → record is NEW    → mark “new”
#             • hash same        → NO CHANGE        → skip entirely
#   Step 6  → Build SF payload with sidecar field mapping
#   Step 7  → Stream delta records to Salesforce in batches of 10,000
#             Uses Salesforce REST Composite API (PATCH upsert by ExternalId)
#   Step 8  → Write push logs (success/failure per record) to push_log table
#   Step 9  → Update snapshot with new hashes (so next run only diffs again)
#   Step 10 → Final run report
#
# SIDECAR OBJECT: MK_Contact_Enrichment__c
#   External ID : MK_Composite_ID__c   (upsert key ← id)
#   Contact Link: Contact__c           (lookup ← source_key_value = bare SF Id)
#
# FIELD MAPPING (contacts_computations → Sidecar MK_Contact_Enrichment__c):
#   id                              → MK_Composite_ID__c  (External ID)
#   source_key_value                → Contact__c           (Lookup to Contact)
#   customer_fit_score              → Customer_Fit_Score__c
#   lead_grade                      → Lead_Grade__c
#   company__name                   → Company_Name__c
#   company__category__industry     → Industry__c
#   company__metrics__employees     → Employee_Count__c
#   person__employment__title       → Job_Title__c
#   person__employment__seniority   → Seniority__c
#   person__employment__role        → Job_Role__c
#   is_paying                       → Is_Paying__c
#   mrr                             → MRR__c
#   mk_status                       → Enrichment_Status__c
#
# Serverless: ✅ works (uses requests library, no spark.conf issues)
# Job Compute: ✅ works (same code, higher batch concurrency available)
# =============================================================================

# COMMAND ----------

# Load pipeline_config.py (stored as a workspace file, not a notebook,
# so %run cannot resolve it — use exec() instead)
_config_path = "/Workspace/Users/ayush.gunjal@hginsights.com/HGI-Lakehouse-Pipeline/config/pipeline_config.py"
exec(open(_config_path).read())

# Load audit logger for centralized pipeline logging
_audit_path = "/Workspace/Users/ayush.gunjal@hginsights.com/HGI-Lakehouse-Pipeline/utils/audit_logger.py"
exec(open(_audit_path).read())
initialize_audit_tables()
print("✅ audit_logger loaded")

# COMMAND ----------

# ── Widgets ────────────────────────────────────────────────────────────────
dbutils.widgets.text("customer_id",    "cust_001")
dbutils.widgets.text("batch_size",     "10000")    # records per SF API call
dbutils.widgets.text("dry_run",        "false")    # "true" = compute delta but don't push to SF
dbutils.widgets.text("max_records",    "0")        # 0 = no limit; set to N for testing

customer_id  = dbutils.widgets.get("customer_id").strip().lower()
batch_size   = int(dbutils.widgets.get("batch_size")   or "10000")
dry_run      = dbutils.widgets.get("dry_run").strip().lower() == "true"
max_records  = int(dbutils.widgets.get("max_records")  or "0")

try:
    tenant_id = int(customer_id.split("_")[1])
except Exception:
    raise ValueError(f"customer_id must be 'cust_NNN', got: {customer_id}")

from datetime import datetime
import requests, json, math, time
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

run_ts = datetime.utcnow()

print("=" * 65)
print("  🔄  SYNC BACK TO SALESFORCE")
print("=" * 65)
print(f"  customer_id  : {customer_id}  (tenant={tenant_id})")
print(f"  batch_size   : {batch_size:,}")
print(f"  dry_run      : {dry_run}  ← {'NO data sent to SF' if dry_run else 'LIVE: will push to SF'}")
print(f"  max_records  : {'unlimited' if max_records == 0 else max_records}")
print(f"  Started at   : {run_ts.strftime('%Y-%m-%d %H:%M:%S UTC')}")

# COMMAND ----------

# ── Salesforce credentials ─────────────────────────────────────────────────
# These are stored in Databricks Secrets for security.
SF_CLIENT_ID     = dbutils.secrets.get(scope="salesforce-scope", key="SF_CLIENT_ID")
SF_CLIENT_SECRET = dbutils.secrets.get(scope="salesforce-scope", key="SF_CLIENT_SECRET")
SF_LOGIN_URL     = dbutils.secrets.get(scope="salesforce-scope", key="SF_LOGIN_URL")

# ── Sidecar Custom Object ──────────────────────────────────────────────────
# NEW ARCHITECTURE: We push to a sidecar custom object (MK_Contact_Enrichment__c)
# instead of writing directly to the standard Contact object.
# This avoids overwriting CRM data or triggering unintended SF automations.

# Salesforce custom object to upsert into
SF_OBJECT = "MK_Contact_Enrichment__c"

# External ID field used for upsert — prevents duplicate sidecar records
# Databricks composite ID → MK_Composite_ID__c (must be marked External ID in SF)
SF_EXTERNAL_ID_FIELD = "MK_Composite_ID__c"

# Lookup field that links sidecar record back to the parent SF Contact
# source_key_value (bare SF Contact Id) → Contact__c
SF_CONTACT_LOOKUP_FIELD = "Contact__c"

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: AUTHENTICATE WITH SALESFORCE (Driver-level)
# ═══════════════════════════════════════════════════════════════════════════
# We authenticate once on the driver. The token is then broadcast to all
# executor batches for the actual API calls.

print("\n── Step 1: Salesforce Authentication ──")
try:
    auth_payload = {
        "grant_type":    "client_credentials",
        "client_id":     SF_CLIENT_ID,
        "client_secret": SF_CLIENT_SECRET,
    }
    auth_response = requests.post(SF_LOGIN_URL, data=auth_payload, timeout=30)
    auth_response.raise_for_status()
    sf_token_data    = auth_response.json()
    access_token     = sf_token_data["access_token"]
    instance_url     = sf_token_data["instance_url"]
    driver_headers   = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type":  "application/json",
    }
    print(f"  ✅ Authenticated")
    print(f"  instance_url : {instance_url}")
    print(f"  token prefix : {access_token[:20]}...")
except Exception as e:
    raise RuntimeError(f"Salesforce authentication failed: {e}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: INFRASTRUCTURE — create log + snapshot tables
# ═══════════════════════════════════════════════════════════════════════════
# sync_push_snapshot: stores the last-pushed hash per record
#   so next run only processes what actually changed
# sync_push_log: every push attempt logged here (success/failure/skipped)

print("\n── Step 2: Ensuring log + snapshot tables exist ──")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {meta_catalog}")

# Snapshot table: tracks last-pushed hash per composite key
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {meta_catalog}.sync_push_snapshot (
        id                    STRING NOT NULL,
        source_system         STRING,
        source_system_object  STRING,
        source_key_name       STRING,
        source_key_value      STRING,
        customer_id           STRING,
        row_hash              STRING,
        pushed_at             TIMESTAMP
    )
    USING DELTA
    CLUSTER BY (customer_id, source_key_value)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

# Push log table: every API call outcome recorded here
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {meta_catalog}.sync_push_log (
        push_id               STRING,
        customer_id           STRING,
        sf_id                 STRING,
        composite_id          STRING,
        push_status           STRING,
        change_type           STRING,
        sf_response_code      INTEGER,
        sf_error_message      STRING,
        pushed_at             TIMESTAMP,
        batch_number          INTEGER,
        run_id                STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'   = 'true'
    )
""")

print(f"  ✅ {meta_catalog}.sync_push_snapshot")
print(f"  ✅ {meta_catalog}.sync_push_log")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: LOAD contacts_computations for this tenant
# ═══════════════════════════════════════════════════════════════════════════
# contacts_computations lives in hgi.gold (the gold/compute layer)
# It was built by the Compute stage (§3.1) joining:
#   contacts_all + persons (by email) + companies (by domain)
# We filter to this tenant only.

print("\n── Step 3: Loading contacts_computations ──")

GOLD_TABLE = "hgi.gold.contacts_computations"

try:
    df_compute = spark.table(GOLD_TABLE).filter(F.col("tenant") == tenant_id)

    if max_records > 0:
        df_compute = df_compute.limit(max_records)
        print(f"  ⚠  max_records={max_records} applied (testing mode)")

    total_records = df_compute.count()
    print(f"  Source table  : {GOLD_TABLE}")
    print(f"  Tenant filter : tenant = {tenant_id}")
    print(f"  Total records : {total_records:,}")

    if total_records == 0:
        print("  ⚠  No records found for this tenant. Exiting.")
        dbutils.notebook.exit("no_records")

except Exception as e:
    raise RuntimeError(
        f"Cannot read {GOLD_TABLE}: {e}\n"
        f"Make sure the Compute stage has run and built contacts_computations."
    )

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4: COMPUTE ROW HASHES (MD5 of all field values)
# ═══════════════════════════════════════════════════════════════════════════
# Per spec §3.4: "Hash all rows (MD5)"
# We concatenate all columns that we intend to push into a single string,
# then MD5 hash it. If ANY of those fields changed since last push,
# the hash changes → record goes into the delta batch.
#
# We exclude tracking/metadata columns from the hash
# (we don't want to re-push if only run_timestamp changed).

print("\n── Step 4: Computing row hashes ──")

# These are the columns we actually push to Salesforce
# If they haven't changed → same hash → skip this record
PUSH_COLUMNS = [
    "customer_fit_score",
    "lead_grade",
    "company__name",
    "company__category__industry",
    "company__metrics__employees",
    "person__employment__title",
    "person__employment__seniority",
    "person__employment__role",
    "geo__country",
    "is_paying",
    "mrr",
    "mk_status",
]

# Only hash columns that actually exist in the table
existing_cols = set(df_compute.columns)
hash_cols = [c for c in PUSH_COLUMNS if c in existing_cols]
missing_cols = [c for c in PUSH_COLUMNS if c not in existing_cols]

if missing_cols:
    print(f"  ⚠  These push columns are not in contacts_computations yet: {missing_cols}")
    print(f"     They will be skipped (NULL in SF push). Add them when Compute/Augment builds them.")

# Compute MD5 hash: concat all hashable field values as "col1|col2|col3..."
hash_expr = F.md5(
    F.concat_ws("|", *[F.coalesce(F.col(c).cast(StringType()), F.lit("__null__"))
                       for c in hash_cols])
)

df_hashed = df_compute.withColumn("row_hash", hash_expr)
print(f"  Hash columns  : {len(hash_cols)} fields")
print(f"  Hash sample   : {df_hashed.select('row_hash').first()[0][:16]}...")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 5: COMPARE AGAINST SNAPSHOT — find what changed
# ═══════════════════════════════════════════════════════════════════════════
# Per spec §3.4:
#   INNER JOIN where stage.hash != current.hash  → "updated"
#   LEFT JOIN where current IS NULL               → "new"
#   Hash same                                     → SKIP
#
# The snapshot stores the hash from the LAST SUCCESSFUL push.
# So we are comparing "what we want to push now" vs "what we already pushed".
#
# NOTE: Gold table uses `contact_id` (composite key), snapshot uses `id`.
#   We join on contact_id = id.

print("\n── Step 5: Delta detection (new vs updated vs unchanged) ──")

# Load existing snapshot for this customer
df_snapshot = (
    spark.table(f"{meta_catalog}.sync_push_snapshot")
         .filter(F.col("customer_id") == customer_id)
         .select(F.col("id").alias("snap_id"), F.col("row_hash").alias("snap_hash"))
)

# Join current data against snapshot (contact_id = snap_id)
df_joined = df_hashed.join(df_snapshot,
                           df_hashed["contact_id"] == df_snapshot["snap_id"],
                           how="left")

# Classify each record
df_delta = df_joined.withColumn(
    "change_type",
    F.when(F.col("snap_hash").isNull(),                              F.lit("new"))
     .when(F.col("row_hash") != F.col("snap_hash"),                 F.lit("updated"))
     .otherwise(                                                      F.lit("unchanged"))
).filter(F.col("change_type") != "unchanged")   # only process changed + new

delta_count     = df_delta.count()
new_count       = df_delta.filter(F.col("change_type") == "new").count()
updated_count   = df_delta.filter(F.col("change_type") == "updated").count()
unchanged_count = total_records - delta_count

print(f"  Total records    : {total_records:>10,}")
print(f"  ── Unchanged     : {unchanged_count:>10,}  (hash same → SKIPPED, zero API calls)")
print(f"  ── NEW           : {new_count:>10,}  (never pushed before)")
print(f"  ── UPDATED       : {updated_count:>10,}  (hash changed since last push)")
print(f"  ── Delta to push : {delta_count:>10,}  (new + updated)")

if delta_count == 0:
    print("\n  ✅ No changes detected. Nothing to push.")
    dbutils.notebook.exit("no_changes")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 6: BUILD SF PAYLOAD — map internal fields → SF sidecar field API names
# ═══════════════════════════════════════════════════════════════════════════
# NEW ARCHITECTURE: We push to MK_Contact_Enrichment__c (sidecar custom object)
# instead of writing directly to the standard Contact object.
#
# Key fields:
#   MK_Composite_ID__c — External ID for upsert (from contact_id)
#   Contact__c         — Lookup to parent SF Contact (bare SF Id extracted from contact_id)
#
# Gold table uses `contact_id` format: "salesforce_Contact_Id_003gL00000ZOKyHQAX"
#   → MK_Composite_ID__c = full contact_id (composite key for upsert)
#   → Contact__c = bare SF Id after last "_Id_" (e.g., "003gL00000ZOKyHQAX")

print("\n── Step 6: Building Salesforce sidecar payload ──")

# Extract the bare Salesforce Contact Id from the composite contact_id
# Format: "salesforce_Contact_Id_003gL00000ZOKyHQAX" → "003gL00000ZOKyHQAX"
sf_id_expr = F.regexp_extract(F.col("contact_id"), r"_Id_(.+)$", 1)

# Select and rename columns to their SF sidecar field API names
df_payload = df_delta.select(
    # Delta key (internal) — kept for logging
    F.col("contact_id").alias("_composite_id"),
    sf_id_expr.alias("_sf_id"),                          # bare SF Contact Id
    F.col("change_type").alias("_change_type"),
    F.col("row_hash").alias("_row_hash"),

    # External ID field — this is what SF uses to upsert the sidecar record
    F.col("contact_id").alias(SF_EXTERNAL_ID_FIELD),      # composite ID → MK_Composite_ID__c

    # Lookup field — links sidecar record back to the parent SF Contact
    sf_id_expr.alias(SF_CONTACT_LOOKUP_FIELD),             # bare SF Id → Contact__c

    # Enrichment fields pushed to sidecar object (no MK_ prefix)
    F.coalesce(F.col("customer_fit_score").cast("string"),    F.lit("")).alias("Customer_Fit_Score__c") if "customer_fit_score" in existing_cols else F.lit("").alias("Customer_Fit_Score__c"),
    F.coalesce(F.col("lead_grade"),                           F.lit("")).alias("Lead_Grade__c") if "lead_grade" in existing_cols else F.lit("").alias("Lead_Grade__c"),
    F.coalesce(F.col("company__name"),                        F.lit("")).alias("Company_Name__c"),
    F.coalesce(F.col("company__category__industry"),          F.lit("")).alias("Industry__c"),
    F.coalesce(F.col("company__metrics__employees").cast("string"), F.lit("")).alias("Employee_Count__c"),
    F.coalesce(F.col("person__employment__title"),            F.lit("")).alias("Job_Title__c"),
    F.coalesce(F.col("person__employment__seniority"),        F.lit("")).alias("Seniority__c"),
    F.coalesce(F.col("person__employment__role"),             F.lit("")).alias("Job_Role__c"),
    F.coalesce(F.col("is_paying").cast("string"),             F.lit("false")).alias("Is_Paying__c") if "is_paying" in existing_cols else F.lit("false").alias("Is_Paying__c"),
    F.coalesce(F.col("mrr").cast("string"),                   F.lit("0")).alias("MRR__c") if "mrr" in existing_cols else F.lit("0").alias("MRR__c"),
    F.coalesce(F.col("mk_status"),                            F.lit("not_found")).alias("Enrichment_Status__c") if "mk_status" in existing_cols else F.lit("not_found").alias("Enrichment_Status__c"),
)

print(f"  Target object : {SF_OBJECT}  (sidecar custom object)")
print(f"  External ID   : {SF_EXTERNAL_ID_FIELD}")
print(f"  Contact lookup: {SF_CONTACT_LOOKUP_FIELD}")
print(f"  Payload columns : {len(df_payload.columns) - 4}  SF fields (excluding internal tracking cols)")
print(f"  Records to push : {delta_count:,}")
print(f"  Batches needed  : {math.ceil(delta_count / batch_size):,}  (batch_size={batch_size:,})")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 7: PUSH TO SALESFORCE IN BATCHES OF 10,000
# ═══════════════════════════════════════════════════════════════════════════
# Per spec §3.4: "Stream delta records in batches of 10,000"
# Uses Salesforce REST Composite SObject Collections API:
#   PATCH /services/data/v60.0/composite/sobjects/{SFObject}/{ExternalIdField}
#
# This endpoint:
#   • Creates the record if no match on ExternalIdField
#   • Updates the record if a match is found
#   = exactly what spec calls "upsert"
#
# allOrNone=false means partial success is OK:
#   if 1 record in a batch of 200 fails, the other 199 still succeed.

SF_API_VERSION = "v60.0"
SF_UPSERT_URL = (
    f"{instance_url}/services/data/{SF_API_VERSION}"
    f"/composite/sobjects/{SF_OBJECT}/{SF_EXTERNAL_ID_FIELD}"
)

# Internal columns to EXCLUDE from the SF payload
INTERNAL_COLS = {"_composite_id", "_sf_id", "_change_type", "_row_hash"}

# Collect delta records to driver
# NOTE: For very large tenants (>500k records) consider partitioned push
# using foreachPartition instead of collect. For POC (100k contacts) this is safe.
if dry_run:
    print(f"\n── Step 7: DRY RUN — showing payload preview (no SF calls) ──")
    print(f"  Target object : {SF_OBJECT}")
    print(f"  Upsert URL    : PATCH .../composite/sobjects/{SF_OBJECT}/{SF_EXTERNAL_ID_FIELD}")
    df_payload.select("_composite_id", "_sf_id", "_change_type",
                      SF_CONTACT_LOOKUP_FIELD,
                      "Customer_Fit_Score__c", "Lead_Grade__c",
                      "Industry__c", "Enrichment_Status__c"
                      ).show(10, truncate=False)
    print(f"  Would push {delta_count:,} records in {math.ceil(delta_count/batch_size):,} batches")
    dbutils.notebook.exit("dry_run_complete")

print(f"\n── Step 7: Pushing {delta_count:,} records to Salesforce ──")

delta_rows = df_payload.collect()
push_logs  = []
run_id     = f"sync_{customer_id}_{run_ts.strftime('%Y%m%d_%H%M%S')}"

total_batches   = math.ceil(len(delta_rows) / batch_size)
success_count   = 0
failed_count    = 0
batch_push_size = 200    # SF Composite SObject Collections max = 200 per request

for batch_num in range(total_batches):
    batch_start  = batch_num * batch_size
    batch_end    = min(batch_start + batch_size, len(delta_rows))
    batch_rows   = delta_rows[batch_start:batch_end]

    print(f"\n  Batch {batch_num + 1}/{total_batches}:  records {batch_start:,}–{batch_end:,}")

    # SF Composite API only allows 200 records per request
    # So we chunk each 10,000-record batch into sub-batches of 200
    sub_batches     = math.ceil(len(batch_rows) / batch_push_size)
    batch_success   = 0
    batch_failed    = 0
    pushed_at       = datetime.utcnow()

    for sub_idx in range(sub_batches):
        sub_start  = sub_idx * batch_push_size
        sub_end    = min(sub_start + batch_push_size, len(batch_rows))
        sub_rows   = batch_rows[sub_start:sub_end]

        # Build Salesforce records list
        sf_records = []
        for row in sub_rows:
            row_dict = row.asDict()
            sf_rec = {
                "attributes": {"type": SF_OBJECT},
            }
            for col, val in row_dict.items():
                if col not in INTERNAL_COLS and col != SF_EXTERNAL_ID_FIELD:
                    if val != "":   # Don't send empty strings — let SF keep existing value
                        sf_rec[col] = val
            # The external ID field value
            sf_rec[SF_EXTERNAL_ID_FIELD] = row_dict["_composite_id"]
            sf_records.append(sf_rec)

        payload = {"allOrNone": False, "records": sf_records}

        try:
            response = requests.patch(
                SF_UPSERT_URL,
                headers=driver_headers,
                data=json.dumps(payload),
                timeout=60
            )

            if response.status_code == 200:
                results = response.json()
                for i, (row, result) in enumerate(zip(sub_rows, results)):
                    row_dict = row.asDict()
                    if result.get("success"):
                        batch_success += 1
                        push_logs.append({
                            "push_id":          f"{run_id}_{batch_num}_{sub_idx}_{i}",
                            "customer_id":      customer_id,
                            "sf_id":            row_dict["_sf_id"],
                            "composite_id":     row_dict["_composite_id"],
                            "push_status":      "success",
                            "change_type":      row_dict["_change_type"],
                            "sf_response_code": 200,
                            "sf_error_message": "",
                            "pushed_at":        pushed_at,
                            "batch_number":     batch_num,
                            "run_id":           run_id,
                        })
                    else:
                        batch_failed += 1
                        errors = result.get("errors", [])
                        err_msg = "; ".join(
                            e.get("message", str(e)) for e in errors
                        ) if errors else "unknown_error"
                        push_logs.append({
                            "push_id":          f"{run_id}_{batch_num}_{sub_idx}_{i}",
                            "customer_id":      customer_id,
                            "sf_id":            row_dict["_sf_id"],
                            "composite_id":     row_dict["_composite_id"],
                            "push_status":      "failed",
                            "change_type":      row_dict["_change_type"],
                            "sf_response_code": 200,
                            "sf_error_message": err_msg[:500],
                            "pushed_at":        pushed_at,
                            "batch_number":     batch_num,
                            "run_id":           run_id,
                        })
            else:
                # Entire sub-batch request failed (auth error, rate limit, etc.)
                batch_failed += len(sub_rows)
                err_text = response.text[:300]
                for i, row in enumerate(sub_rows):
                    row_dict = row.asDict()
                    push_logs.append({
                        "push_id":          f"{run_id}_{batch_num}_{sub_idx}_{i}",
                        "customer_id":      customer_id,
                        "sf_id":            row_dict["_sf_id"],
                        "composite_id":     row_dict["_composite_id"],
                        "push_status":      "http_error",
                        "change_type":      row_dict["_change_type"],
                        "sf_response_code": response.status_code,
                        "sf_error_message": err_text,
                        "pushed_at":        pushed_at,
                        "batch_number":     batch_num,
                        "run_id":           run_id,
                    })
                print(f"    ⚠  Sub-batch {sub_idx + 1}: HTTP {response.status_code} — {err_text[:100]}")

        except Exception as e:
            batch_failed += len(sub_rows)
            print(f"    ❌ Sub-batch {sub_idx + 1} exception: {e}")
            for i, row in enumerate(sub_rows):
                row_dict = row.asDict()
                push_logs.append({
                    "push_id":          f"{run_id}_{batch_num}_{sub_idx}_{i}",
                    "customer_id":      customer_id,
                    "sf_id":            row_dict["_sf_id"],
                    "composite_id":     row_dict["_composite_id"],
                    "push_status":      "exception",
                    "change_type":      row_dict["_change_type"],
                    "sf_response_code": -1,
                    "sf_error_message": str(e)[:500],
                    "pushed_at":        pushed_at,
                    "batch_number":     batch_num,
                    "run_id":           run_id,
                })

        # Small sleep between sub-batches to respect SF API rate limits
        # SF allows 100,000 API calls per 24h — this is conservative
        if sub_idx < sub_batches - 1:
            time.sleep(0.1)

    success_count += batch_success
    failed_count  += batch_failed
    print(f"    ✅ {batch_success:,} success  |  ❌ {batch_failed:,} failed")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 8: WRITE PUSH LOGS
# ═══════════════════════════════════════════════════════════════════════════
# Per spec §3.4: "Push logs come back (success/failure per record)"
# We write all log entries, then deduplicate to keep latest per composite_id.

print(f"\n── Step 8: Writing push logs ──")

if push_logs:
    df_logs = spark.createDataFrame(push_logs)
    # Cast INT columns to match existing table schema (Python int → Spark LONG,
    # but the table was created with INTEGER — Delta rejects LONG → INT merge)
    df_logs = df_logs.withColumn("sf_response_code", F.col("sf_response_code").cast("int")) \
                     .withColumn("batch_number", F.col("batch_number").cast("int"))
    df_logs.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        f"{meta_catalog}.sync_push_log"
    )
    print(f"  Wrote {len(push_logs):,} log entries to {meta_catalog}.sync_push_log")

# Per spec §3.4: "Deduplicate push logs — ROW_NUMBER OVER PARTITION BY id ORDER BY pushed_at DESC"
# We keep the latest push attempt per record in a separate "deduplicated" view
# so the reporting query always shows the current state per record
spark.sql(f"""
    CREATE OR REPLACE TABLE {meta_catalog}.sync_push_log_deduped
    USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
    AS
    SELECT * EXCEPT(rn)
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id, composite_id
                   ORDER BY pushed_at DESC
               ) AS rn
        FROM {meta_catalog}.sync_push_log
        WHERE customer_id = '{customer_id}'
    )
    WHERE rn = 1
""")
print(f"  Deduplicated log written to {meta_catalog}.sync_push_log_deduped")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 9: UPDATE SNAPSHOT — only for successfully pushed records
# ═══════════════════════════════════════════════════════════════════════════
# IMPORTANT: We only update the snapshot for SUCCESSFUL pushes.
# Failed records keep their OLD snapshot hash → will be retried next run.
# This is the retry mechanism: failed records are automatically retried
# on the next pipeline run because their hash is still "old".
#
# NOTE: Gold table uses `contact_id` as the primary key.
#   Format: "salesforce_Contact_Id_003gL00000ZOKyHQAX"
#   We derive source_system/object/key from contact_id for the snapshot.

print(f"\n── Step 9: Updating snapshot ──")

# Get set of successfully pushed composite IDs
successful_ids = {
    log["composite_id"]
    for log in push_logs
    if log["push_status"] == "success"
}

print(f"  Successful pushes: {len(successful_ids):,}")
print(f"  Updating snapshot for successful records only...")

if successful_ids:
    # Build snapshot update DataFrame from the successfully pushed records
    # Derive source columns from contact_id format: "{source_system}_{Object}_Id_{sf_id}"
    df_success_delta = df_hashed.filter(F.col("contact_id").isin(successful_ids)).select(
        F.col("contact_id").alias("id"),
        F.lit("salesforce").alias("source_system"),
        F.lit("Contact").alias("source_system_object"),
        F.lit("Id").alias("source_key_name"),
        F.regexp_extract(F.col("contact_id"), r"_Id_(.+)$", 1).alias("source_key_value"),
        F.lit(customer_id).alias("customer_id"),
        F.col("row_hash"),
        F.lit(run_ts).alias("pushed_at"),
    )

    # MERGE into snapshot: update existing rows, insert new ones
    df_success_delta.createOrReplaceTempView("sync_snapshot_update")

    spark.sql(f"""
        MERGE INTO {meta_catalog}.sync_push_snapshot AS target
        USING sync_snapshot_update AS source
        ON  target.id          = source.id
        AND target.customer_id = source.customer_id
        WHEN MATCHED THEN
            UPDATE SET
                target.row_hash   = source.row_hash,
                target.pushed_at  = source.pushed_at
        WHEN NOT MATCHED THEN
            INSERT (id, source_system, source_system_object, source_key_name,
                    source_key_value, customer_id, row_hash, pushed_at)
            VALUES (source.id, source.source_system, source.source_system_object,
                    source.source_key_name, source.source_key_value,
                    source.customer_id, source.row_hash, source.pushed_at)
    """)
    print(f"  ✅ Snapshot updated for {len(successful_ids):,} records")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════════
# STEP 10: FINAL RUN REPORT + AUDIT LOG
# ═══════════════════════════════════════════════════════════════════════════

run_end    = datetime.utcnow()
duration_s = (run_end - run_ts).total_seconds()

print(f"\n{'='*65}")
print(f"  🏁  SYNC BACK COMPLETE  (Sidecar: {SF_OBJECT})")
print(f"{'='*65}")
print(f"  Customer         : {customer_id}  (tenant={tenant_id})")
print(f"  Run ID           : {run_id}")
print(f"  Duration         : {duration_s:.1f}s")
print(f"{'─'*65}")
print(f"  Total records    : {total_records:>10,}")
print(f"  Unchanged (skip) : {unchanged_count:>10,}  (0 API calls)")
print(f"  Delta detected   : {delta_count:>10,}  (new + updated)")
print(f"  ── NEW           : {new_count:>10,}")
print(f"  ── UPDATED       : {updated_count:>10,}")
print(f"{'─'*65}")
print(f"  ✅ SF success    : {success_count:>10,}")
print(f"  ❌ SF failed     : {failed_count:>10,}  (will retry next run)")
print(f"  Success rate     : {round(100*success_count/max(delta_count,1),1):>9.1f}%")
print(f"{'─'*65}")

if failed_count > 0:
    print(f"\n  Most common failure reasons:")
    spark.sql(f"""
        SELECT sf_error_message, COUNT(*) AS count
        FROM {meta_catalog}.sync_push_log
        WHERE run_id = '{run_id}' AND push_status != 'success'
        GROUP BY sf_error_message
        ORDER BY count DESC
        LIMIT 5
    """).show(truncate=False)

print(f"\n  SF sidecar object : {SF_OBJECT}")
print(f"  External ID field : {SF_EXTERNAL_ID_FIELD}")
print(f"  Contact lookup    : {SF_CONTACT_LOOKUP_FIELD}")

print(f"\n  Field mapping applied:")
print(f"  {'contacts_computations col':<35} {'→'} {'Sidecar field (MK_Contact_Enrichment__c)'}")
print(f"  {'─'*35} {'─'*45}")
for src, dst in [
    ("id",                          "MK_Composite_ID__c  (External ID)"),
    ("source_key_value",            "Contact__c  (Lookup to Contact)"),
    ("customer_fit_score",          "Customer_Fit_Score__c"),
    ("lead_grade",                  "Lead_Grade__c"),
    ("company__name",               "Company_Name__c"),
    ("company__category__industry", "Industry__c"),
    ("company__metrics__employees", "Employee_Count__c"),
    ("person__employment__title",   "Job_Title__c"),
    ("person__employment__seniority", "Seniority__c"),
    ("person__employment__role",    "Job_Role__c"),
    ("is_paying",                   "Is_Paying__c"),
    ("mrr",                         "MRR__c"),
    ("mk_status",                   "Enrichment_Status__c"),
]:
    if src in ("id", "source_key_value"):
        status = "✅"
    else:
        status = "✅" if src in existing_cols else "⚠  (missing in table)"
    print(f"  {src:<35} → {dst}  {status}")

print(f"\n  Run report: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
print(f"{'='*65}")

# ═══════════════════════════════════════════════════════════════════════════
# AUDIT LOG — write to centralized audit_logs table
# ═══════════════════════════════════════════════════════════════════════════
try:
    sync_status = "success" if failed_count == 0 else "partial_success" if success_count > 0 else "failure"
    alert_type  = "SUCCESS" if failed_count == 0 else "FAILURE"

    log_audit(
        job_name        = "sync_back_to_salesforce",
        customer_id     = customer_id,
        status          = sync_status,
        layer           = "gold",
        alert_type      = alert_type,
        error_type      = None if failed_count == 0 else "SF_PUSH_FAILURES",
        error_reason    = None if failed_count == 0 else f"{failed_count} records failed to sync to Salesforce",
        rows_processed  = success_count,
        duration_ms     = int(duration_s * 1000),
    )
    print(f"\n  ✅ Audit log written (layer=gold, rows_processed={success_count:,})")
except Exception as e:
    print(f"\n  ⚠️  Audit log failed: {e}")

# ═══════════════════════════════════════════════════════════════════════════
# NOTEBOOK EXIT — return summary to orchestrator
# ═══════════════════════════════════════════════════════════════════════════
exit_json = json.dumps({
    "status":          sync_status,
    "customer_id":     customer_id,
    "total_records":   total_records,
    "delta_detected":  delta_count,
    "new_records":     new_count,
    "updated_records": updated_count,
    "sf_success":      success_count,
    "sf_failed":       failed_count,
    "unchanged":       unchanged_count,
    "duration_s":      round(duration_s, 1),
    "run_id":          run_id,
})
dbutils.notebook.exit(exit_json)

# COMMAND ----------

# DBTITLE 1,Post-Run Verification
# MAGIC %md
# MAGIC ---
# MAGIC ## Post-Run Verification
# MAGIC Run the cells below **after** the `Salesforce_Reverse_ETL_Sync` job completes to verify success on both sides.

# COMMAND ----------

# DBTITLE 1,Verify: Databricks Push Log
# MAGIC %sql
# MAGIC -- DATABRICKS SIDE: Push log summary for the latest sync run
# MAGIC SELECT
# MAGIC   run_id,
# MAGIC   push_status,
# MAGIC   COUNT(*) AS record_count,
# MAGIC   MIN(pushed_at) AS started,
# MAGIC   MAX(pushed_at) AS ended
# MAGIC FROM ingestion_metadata.sync_push_log
# MAGIC WHERE customer_id = 'cust_001'
# MAGIC GROUP BY run_id, push_status
# MAGIC ORDER BY started DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Verify: Failure Root Causes
# MAGIC %sql
# MAGIC -- DATABRICKS SIDE: Check for any failures with error details
# MAGIC SELECT
# MAGIC   sf_error_message,
# MAGIC   COUNT(*) AS occurrences,
# MAGIC   COLLECT_SET(composite_id) AS sample_ids
# MAGIC FROM ingestion_metadata.sync_push_log
# MAGIC WHERE customer_id = 'cust_001'
# MAGIC   AND push_status != 'success'
# MAGIC GROUP BY sf_error_message
# MAGIC ORDER BY occurrences DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Verify: Salesforce Sidecar Records
# SALESFORCE SIDE: Query sidecar records to confirm data landed in SF
import requests, json

try:
    SF_CLIENT_ID     = dbutils.secrets.get(scope="salesforce-scope", key="SF_CLIENT_ID")
    SF_CLIENT_SECRET = dbutils.secrets.get(scope="salesforce-scope", key="SF_CLIENT_SECRET")
    SF_LOGIN_URL     = dbutils.secrets.get(scope="salesforce-scope", key="SF_LOGIN_URL")

    auth = requests.post(SF_LOGIN_URL, data={
        "grant_type": "client_credentials",
        "client_id": SF_CLIENT_ID,
        "client_secret": SF_CLIENT_SECRET,
    }, timeout=30).json()

    headers = {"Authorization": f"Bearer {auth['access_token']}", "Content-Type": "application/json"}
    base_url = auth["instance_url"]

    # Count sidecar records
    soql = "SELECT COUNT(Id) total FROM MK_Contact_Enrichment__c"
    resp = requests.get(f"{base_url}/services/data/v60.0/query", headers=headers, params={"q": soql}, timeout=30).json()
    total = resp["records"][0]["total"] if resp.get("records") else 0
    print(f"\u2705 Salesforce MK_Contact_Enrichment__c: {total:,} sidecar records")

    # Sample records
    soql_sample = (
        "SELECT MK_Composite_ID__c, Contact__c, Company_Name__c, Industry__c, "
        "Enrichment_Status__c, CreatedDate FROM MK_Contact_Enrichment__c "
        "ORDER BY CreatedDate DESC LIMIT 5"
    )
    sample = requests.get(f"{base_url}/services/data/v60.0/query", headers=headers, params={"q": soql_sample}, timeout=30).json()
    print(f"\n  Latest sidecar records in Salesforce:")
    for r in sample.get("records", []):
        print(f"    Contact__c={r.get('Contact__c','')} | {r.get('Company_Name__c','')} | {r.get('Industry__c','')} | {r.get('Enrichment_Status__c','')}")

except Exception as e:
    print(f"\u274c SF verification failed: {e}")
