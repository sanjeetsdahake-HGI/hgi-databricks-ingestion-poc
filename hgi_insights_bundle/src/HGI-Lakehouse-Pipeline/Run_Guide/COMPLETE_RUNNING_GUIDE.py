# =============================================================================
# HGI LAKEHOUSE PIPELINE — COMPLETE RUNNING GUIDE
# How to run everything from Landing Zone → Bronze → Silver → Map
# For both Serverless (your free account) and Job Compute (client account)
# =============================================================================

# =============================================================================
# PART 1: UNDERSTANDING THE FOLDER STRUCTURE
# =============================================================================

# When you upload notebooks to Databricks, they go into Workspace folders.
# Think of it like folders on your computer but inside Databricks.

# The full folder path in Databricks will be:
# /Workspace/Users/ayushgunjal2003@gmail.com/HGI-Lakehouse-Pipeline/
#   ├── config/
#   │   └── pipeline_config          ← THE MOST IMPORTANT FILE — run this first always
#   │
#   ├── 02_Bronze_Layer/
#   │   ├── _bronze_common           ← shared logic for all bronze notebooks
#   │   ├── bronze_account
#   │   ├── bronze_contact
#   │   ├── bronze_opportunity
#   │   ├── bronze_task
#   │   ├── bronze_campaign
#   │   ├── bronze_campaignmember
#   │   ├── bronze_user
#   │   └── bronze_events            ← BQ events → goes into events_raw table
#   │
#   ├── 03_Silver_Layer/
#   │   ├── _silver_cdf_common       ← shared logic for 02a silver notebooks
#   │   ├── _map_common              ← shared logic for 02b map notebooks
#   │   ├── 02a_silver_account       ← bronze → silver (CDF streaming)
#   │   ├── 02a_silver_contact
#   │   ├── 02a_silver_opportunity
#   │   ├── 02a_silver_task
#   │   ├── 02a_silver_campaign
#   │   ├── 02a_silver_campaignmember
#   │   ├── 02a_silver_user
#   │   ├── 02a_silver_events
#   │   ├── 02b_map_01_contacts_all      ← map layer starts here
#   │   ├── 02b_map_02_accounts_all
#   │   ├── 02b_map_03_crm_events
#   │   ├── 02b_map_04_events_mapping
#   │   ├── 02b_map_05_contacts_to_accounts
#   │   ├── 02b_map_06_accounts_attributes
#   │   ├── 02b_map_07_contacts_attributes
#   │   ├── 02b_map_08_email_events_mapped
#   │   ├── 02b_map_09_domain_events_mapped
#   │   ├── 02b_map_10_account_events_mapped
#   │   └── 02b_map_11_audiences_conversion
#   │
#   ├── 05_Tests/
#   │   ├── TEST_bronze_layer
#   │   └── TEST_silver_layer
#   │
#   └── jobs/
#       └── master_jobs.yml          ← the YAML file for DAB deployment


# =============================================================================
# PART 2: UPLOADING NOTEBOOKS TO DATABRICKS (do this ONCE)
# =============================================================================

# Step 1: Go to your Databricks workspace (app.databricks.com)
# Step 2: Click "Workspace" in the left sidebar
# Step 3: Navigate to Users → your-email → click the "..." → "Create folder"
#         Name it: HGI-Lakehouse-Pipeline
# Step 4: Inside that folder, create sub-folders:
#         config, 02_Bronze_Layer, 03_Silver_Layer, 05_Tests, jobs
# Step 5: Upload files into each folder:
#         Click the folder → click "+" → "Import" → select the .py file
#         When you import a .py file, Databricks turns it into a notebook automatically
#
# IMPORTANT NAMING IN DATABRICKS:
#   The .py filename becomes the notebook name.
#   So "bronze_account.py" becomes a notebook called "bronze_account"
#   The %run command uses this notebook name (without .py)


# =============================================================================
# PART 3: THE %run RULE — THE MOST IMPORTANT THING TO UNDERSTAND
# =============================================================================

# Every notebook has these lines at the top:
#   # %run ../config/pipeline_config
#   # %run ./_bronze_common          (or _silver_cdf_common or _map_common)
#
# These lines have # in front because in a .py file they are comments.
# In Databricks, you MUST:
#   1. Remove the # (uncomment the line)
#   2. Put each %run in its OWN SEPARATE CELL
#
# WHY ITS OWN CELL?
#   %run is a Databricks "magic command" — it MUST be the only thing in a cell.
#   If you put Python code above or below %run in the same cell, it will ERROR.
#
# CORRECT in Databricks (three separate cells):
#   Cell 1: %run ../config/pipeline_config
#   Cell 2: %run ./_bronze_common
#   Cell 3: (the rest of your Python code)
#
# WRONG (all in one cell — will fail):
#   %run ../config/pipeline_config
#   %run ./_bronze_common
#   customer_id = "cust_001"
#
# The %run path is RELATIVE to where the current notebook is:
#   - Bronze notebooks are in 02_Bronze_Layer/
#     So %run ../config/pipeline_config means "go up one folder, into config"
#   - Map notebooks are in 03_Silver_Layer/
#     So %run ../config/pipeline_config means same thing
#   - _map_common is in the SAME folder as the map notebooks
#     So %run ./_map_common means "in this same folder"


# =============================================================================
# PART 4: RUNNING ON SERVERLESS (YOUR FREE ACCOUNT) — STEP BY STEP
# =============================================================================

# Serverless = Databricks manages the compute for you. Free on your account.
# 2X-Small = the smallest size, ~8GB RAM. Good for testing with small data.
#
# BEFORE RUNNING ANYTHING — ONE-TIME SETUP:
# ─────────────────────────────────────────
# 1. Upload all notebooks (see Part 2 above)
# 2. Open EACH notebook and fix the %run lines:
#    - Find the lines that say  # %run ../config/pipeline_config
#    - Remove the # so it becomes  %run ../config/pipeline_config
#    - Split into separate cells if needed (right-click → "Add cell above")
# 3. No YAML file needed for Serverless interactive testing
#
# RUNNING ORDER (follow this EXACTLY — each step depends on the previous):
# ─────────────────────────────────────────────────────────────────────────
#
# STEP 0 — Run Watermark setup (ONE TIME ONLY — before anything else)
#   Open: 00_Watermark_Table_Setup.sql
#   This creates the ingestion_metadata.watermarks table
#   Run it once, then never again unless you reset everything
#
# STEP 1 — Land data into S3 (Ingestion layer)
#   These notebooks read from Salesforce/BQ and write parquet to S3
#   Run them ONCE for historical load:
#     01_SF_Historical_Load           ← for each SF object
#     01_BQ_Historical_Load_NativeConnector_NoGCS  ← for BQ events
#   Widget settings for Serverless testing:
#     customer_id = cust_001
#     load_type   = historical
#
# STEP 2 — Bronze layer (Landing Zone → Delta)
#   Open bronze_account in Databricks
#   Compute selector (top right) → choose "Serverless"
#   Set widgets:
#     customer_id           = cust_001
#     load_type             = historical
#     max_files_per_trigger = 10    ← IMPORTANT: use 10 on Serverless (not 500)
#                                     10 files × 128MB = ~1.28GB fits in 8GB RAM
#   Click "Run All" (top toolbar) or Shift+Enter through each cell
#   Watch the output at bottom of each cell
#   Expected output:
#     pipeline_config loaded.
#     _bronze_common loaded.
#     === Bronze: SALESFORCE Account ===
#     Auto Loader: s3://... → bronze.cust_001.crm_accounts
#       Batch 0: MERGE complete → bronze.cust_001.crm_accounts
#     Stream complete: account
#     Running OPTIMIZE + VACUUM ...
#     Bronze load complete: bronze.cust_001.crm_accounts
#
#   Repeat for each object:
#     bronze_contact          (max_files = 10)
#     bronze_opportunity      (max_files = 10)
#     bronze_task             (max_files = 10)
#     bronze_campaign         (max_files = 10)
#     bronze_campaignmember   (max_files = 10)
#     bronze_user             (max_files = 10)
#     bronze_events           (max_files = 10)  ← BQ events → events_raw table
#
# STEP 3 — Verify Bronze (run tests)
#   Open TEST_bronze_layer
#   Set widgets: customer_id = cust_001
#   Run All
#   All tests should show ✅ PASS
#
# STEP 4 — Silver CDF layer (Bronze → Silver base tables)
#   Open 02a_silver_account
#   Set widgets:
#     customer_id      = cust_001
#     starting_version = 0    ← first run: replay from beginning
#   Run All
#   Expected output:
#     === 02a Silver CDF: SF Account ===
#     CDF enabled: bronze.cust_001.crm_accounts
#     Silver table ready: hgi.silver.accounts
#     Batch 0 merged → hgi.silver.accounts
#     Silver CDF complete
#
#   Repeat for each silver notebook:
#     02a_silver_contact
#     02a_silver_opportunity
#     02a_silver_task
#     02a_silver_campaign
#     02a_silver_campaignmember
#     02a_silver_user
#     02a_silver_events      ← reads from events_raw
#
# STEP 5 — Silver Map layer (02b notebooks)
#   IMPORTANT: Map notebooks MUST run IN ORDER. Each one depends on the previous.
#   Do not skip steps or run out of order.
#
#   Run each in order, clicking "Run All" for each:
#     02b_map_01_contacts_all         ← FIRST (all others depend on this)
#     02b_map_02_accounts_all         ← SECOND
#     02b_map_03_crm_events           ← reads from silver.tasks
#     02b_map_04_events_mapping       ← reads from silver.events + crm_events
#     02b_map_05_contacts_to_accounts ← 3-phase linking (most complex)
#     02b_map_06_accounts_attributes  ← one row per account
#     02b_map_07_contacts_attributes  ← one row per contact
#     02b_map_08_email_events_mapped  ← email × day aggregation
#     02b_map_09_domain_events_mapped ← domain × day aggregation
#     02b_map_10_account_events_mapped ← account rollup
#     02b_map_11_audiences_conversion  ← LAST (POC stubs)
#
# STEP 6 — Verify Silver (run tests)
#   Open TEST_silver_layer
#   Set widgets: customer_id = cust_001
#   Run All
#   Watch for [SPEC GATE] tests — these must all pass


# =============================================================================
# PART 5: RUNNING ON JOB COMPUTE (CLIENT ACCOUNT) — STEP BY STEP
# =============================================================================

# Job Compute = you define a cluster configuration, Databricks spins it up,
# runs your notebooks, then shuts it down. You pay per minute of cluster time.
# On r6id.2xlarge this is ~$0.50/hour per worker (much cheaper on Spot)
#
# APPROACH A: Run notebooks manually in the UI with a Job Cluster
# ───────────────────────────────────────────────────────────────
# 1. Upload notebooks to client workspace (same as Part 2)
# 2. Fix the %run lines in each notebook (same as before)
# 3. Go to Workflows → Create Job
# 4. Add a task:
#      Task name: bronze_account_historical
#      Type: Notebook
#      Source: Workspace
#      Path: /Workspace/.../02_Bronze_Layer/bronze_account
#      Cluster: New job cluster
#        Runtime: 15.4 LTS
#        Node type: r6id.2xlarge
#        Workers: 4-12 (autoscale)
#      Parameters:
#        customer_id:           cust_001
#        load_type:             historical
#        max_files_per_trigger: 500      ← much higher than Serverless
# 5. Click "Run now"
#
# Production settings vs Serverless testing:
#   max_files_per_trigger: 500  (SF historical), 1000 (BQ historical)
#   max_files_per_trigger: 50   (SF incremental), 100 (BQ incremental)
#
# APPROACH B: Use DAB (Databricks Asset Bundle) with master_jobs.yml
# ───────────────────────────────────────────────────────────────────
# This is the professional way — deploy jobs as code.
# Think of it like Terraform but for Databricks.
#
# One-time installation on your laptop:
#   Mac:     brew install databricks/tap/databricks
#   Windows: winget install Databricks.DatabricksCLI
#   Verify:  databricks --version
#
# Connect CLI to client workspace:
#   databricks configure
#   → Enter: Databricks host: https://adb-XXXXX.azuredatabricks.net
#   → Enter: Token: (get from Databricks UI → top-right avatar →
#             User Settings → Access Tokens → Generate New Token)
#
# Create databricks.yml in your project root folder (one level above the others):
#   ─────────────────────────────────────
#   bundle:
#     name: hgi-lakehouse
#   workspace:
#     host: https://adb-XXXXX.azuredatabricks.net
#   include:
#     - jobs/master_jobs.yml
#   ─────────────────────────────────────
#
# Deploy (uploads notebooks + creates job definitions):
#   cd /path/to/HGI-Lakehouse-Pipeline
#   databricks bundle deploy --target dev
#   → You'll see: "Creating job: Bronze_Historical ... Done"
#
# Run historical jobs (do this once per new customer):
#   databricks bundle run Bronze_Historical --target dev
#   databricks bundle run Silver_CDF --target dev
#   databricks bundle run Silver_Map --target dev
#
# Run tests:
#   databricks bundle run TEST_bronze --target dev
#   databricks bundle run TEST_silver --target dev
#
# Check if job finished:
#   databricks jobs list
#   databricks runs get --run-id <run_id>
#
# Incremental jobs (scheduled automatically — no need to run manually):
#   Bronze_Incremental runs every 15 min (set in master_jobs.yml trigger section)
#   Silver_CDF runs after bronze completes


# =============================================================================
# PART 6: SERVERLESS vs JOB COMPUTE — KEY DIFFERENCES
# =============================================================================

# SERVERLESS:
#   ✅ Free on your account
#   ✅ No cluster config needed — Databricks manages it
#   ✅ Starts instantly (no cluster spin-up wait)
#   ❌ Only 8GB RAM on 2X-Small
#   ❌ max_files_per_trigger MUST be 10 (not 500)
#   ❌ Some spark.conf settings silently ignored (safe_spark_conf handles this)
#   ❌ Cannot run Jobs from YAML on Serverless (need UI or CLI override)
#   Use for: testing logic, debugging, small data samples
#
# JOB COMPUTE:
#   ✅ Full RAM (64-128GB on r6id.2xlarge/4xlarge)
#   ✅ max_files_per_trigger can be 500-1000
#   ✅ All spark.conf settings apply (MERGE optimization, shuffle partitions)
#   ✅ Can be scheduled via master_jobs.yml
#   ✅ Autoscales from 4 to 12 workers
#   ❌ Costs ~$15-25 for a full historical load
#   ❌ Cluster takes 3-5 min to spin up
#   Use for: production runs, full historical loads, scheduled incremental

# HOW THE SAME CODE WORKS ON BOTH:
#   The notebooks use safe_spark_conf() which silently skips configs that
#   Serverless doesn't support (spark.databricks.*, spark.hadoop.fs.s3a.*,
#   spark.sql.shuffle.partitions). On Job Cluster, all configs apply.
#   You do NOT need separate notebooks for Serverless vs Job Cluster.
#   The only thing you change is the widget value for max_files_per_trigger.


# =============================================================================
# PART 7: RUNNING ORDER CHEAT SHEET
# =============================================================================

# NEW CUSTOMER SETUP (run each step in order, wait for completion):
#
# ┌─────────────────────────────────────────────────────────────────┐
# │  STEP 0 (one-time): 00_Watermark_Table_Setup.sql               │
# ├─────────────────────────────────────────────────────────────────┤
# │  STEP 1: Landing Zone (ingestion to S3)                        │
# │    01_SF_Historical_Load   (for each SF object)                │
# │    01_BQ_Historical_Load_NativeConnector_NoGCS                 │
# ├─────────────────────────────────────────────────────────────────┤
# │  STEP 2: Bronze (all 8 objects — can run in PARALLEL)          │
# │    bronze_account, bronze_contact, bronze_opportunity,         │
# │    bronze_task, bronze_campaign, bronze_campaignmember,        │
# │    bronze_user, bronze_events                                  │
# ├─────────────────────────────────────────────────────────────────┤
# │  STEP 3: TEST_bronze_layer (verify before continuing)          │
# ├─────────────────────────────────────────────────────────────────┤
# │  STEP 4: Silver CDF (all 8 objects — can run in PARALLEL)      │
# │    02a_silver_account, 02a_silver_contact,                     │
# │    02a_silver_opportunity, 02a_silver_task,                    │
# │    02a_silver_campaign, 02a_silver_campaignmember,             │
# │    02a_silver_user, 02a_silver_events                          │
# ├─────────────────────────────────────────────────────────────────┤
# │  STEP 5: Silver Map (MUST run IN SEQUENCE 01 → 11)             │
# │    02b_map_01 → 02b_map_02 → 02b_map_03 → 02b_map_04          │
# │    → 02b_map_05 → 02b_map_06 → 02b_map_07 → 02b_map_08        │
# │    → 02b_map_09 → 02b_map_10 → 02b_map_11                     │
# ├─────────────────────────────────────────────────────────────────┤
# │  STEP 6: TEST_silver_layer (verify all spec DQ gates)          │
# ├─────────────────────────────────────────────────────────────────┤
# │  SCHEDULE: Bronze_Incremental + Silver_CDF every 15 min        │
# │            Silver_Map every 15 min (offset by 5 min)           │
# └─────────────────────────────────────────────────────────────────┘


# =============================================================================
# PART 8: COMMON ERRORS AND HOW TO FIX THEM
# =============================================================================

# ERROR: "%run: notebook not found"
# FIX: The path is wrong. Check that:
#      - The notebook exists in that exact Workspace folder
#      - The path is relative: ../config/pipeline_config (not absolute)
#      - You used the correct subfolder names (02_Bronze_Layer not bronze_layer)

# ERROR: "NameError: name 'sv' is not defined"
# FIX: pipeline_config didn't run first.
#      Make sure %run ../config/pipeline_config is the FIRST cell
#      and the # was removed.

# ERROR: "cell magic %run cannot be used in a Python script"
# FIX: The %run line must be alone in its own cell.
#      In Databricks: click on the cell → use the "+" button above or below
#      to create a new cell → move the %run line there.

# ERROR: "IllegalArgumentException: spark.conf not supported"
# FIX: You set a spark.conf outside of safe_spark_conf().
#      In _map_common and _bronze_common, ALL spark.conf calls go through
#      safe_spark_conf({}) which silently ignores Serverless-incompatible ones.

# ERROR: "Table not found: hgi.silver.contacts"
# FIX: You ran a map notebook before the silver CDF notebook.
#      Run all 02a_silver_* notebooks BEFORE any 02b_map_* notebooks.

# ERROR: "Table not found: hgi.silver.contacts_all"
# FIX: You ran map_03 or higher before map_01.
#      map_01 (contacts_all) MUST run first. It's the dependency for everything.

# ERROR: "checkpointLocation already exists" or weird streaming state
# FIX: Delete the checkpoint folder in S3 and restart.
#      The checkpoint S3 path is:
#      s3://hgi-databricks-data-lakehouse-dev/layers/checkpoints/bronze/salesforce/cust_001/account/
#      Delete that folder and re-run the notebook with starting_version=0

# ERROR: OOM (Out of Memory) on Serverless
# FIX: Lower max_files_per_trigger to 5 or even 1.
#      Serverless 2X-Small has ~8GB. Each parquet file is ~128MB.
#      So max 5-10 files is safe.

# ERROR: "contacts_to_accounts linkage only 50% (below 80% gate)"
# FIX: This means most contacts don't have AccountId in Salesforce AND
#      don't have company email domains.
#      Check your source data quality. If the data is correct, the threshold
#      may need adjusting for this specific tenant.
