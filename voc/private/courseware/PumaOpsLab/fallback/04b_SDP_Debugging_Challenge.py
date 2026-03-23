# Databricks notebook source
# MAGIC %md
# MAGIC # 🔧 Block 4b: Challenge — Debug a Broken SDP Pipeline
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC A Spark Declarative Pipeline has been deployed that ingests PUMA inventory data
# MAGIC through a bronze → silver → gold medallion architecture. **But it's broken.**
# MAGIC
# MAGIC Your task: use the Event Log, Pipeline UI, and system tables to find and fix the issues.
# MAGIC

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Pipeline
# MAGIC
# MAGIC ```
# MAGIC raw JSON files (Volume)
# MAGIC       │
# MAGIC       ▼
# MAGIC ┌─────────────────┐
# MAGIC │  bronze_inventory │  ← Streaming table (Auto Loader)
# MAGIC │  (ingest raw)     │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌─────────────────┐
# MAGIC │  silver_inventory │  ← Streaming table (clean & validate)
# MAGIC │  (deduplicate)    │
# MAGIC └────────┬─────────┘
# MAGIC          │
# MAGIC          ▼
# MAGIC ┌─────────────────┐
# MAGIC │  gold_inventory   │  ← Materialized view (aggregate by warehouse)
# MAGIC │  (summary)        │
# MAGIC └──────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Pipeline name**: `puma_ops_inventory_pipeline`
# MAGIC
# MAGIC **What's broken**: The pipeline update fails. Your job is to figure out why.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check Pipeline Status in the UI
# MAGIC
# MAGIC 1. Navigate to **Workflows → Delta Live Tables** (or Pipelines)
# MAGIC 2. Find `puma_ops_inventory_pipeline`
# MAGIC 3. Look at the latest run — which table is marked as failed?
# MAGIC 4. Click on the failed table — what error message do you see?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Query the Event Log
# MAGIC
# MAGIC Use the pipeline ID to query the event log for error details.

# COMMAND ----------

PIPELINE_ID = SDP_PIPELINE_ID or "<your-pipeline-id>"

# COMMAND ----------

# Find all errors
spark.sql(f"""
SELECT
    timestamp,
    level,
    origin.flow_name AS table_name,
    message,
    details
FROM event_log('{PIPELINE_ID}')
WHERE level = 'ERROR'
ORDER BY timestamp DESC
LIMIT 10
""").display()

# COMMAND ----------

# Check flow progress — did any tables succeed before the failure?
spark.sql(f"""
SELECT
    timestamp,
    origin.flow_name AS table_name,
    details:flow_progress.status AS status,
    details:flow_progress.metrics.num_output_rows AS rows_written
FROM event_log('{PIPELINE_ID}')
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check Data Quality Expectations
# MAGIC
# MAGIC The silver layer has expectations defined. Are rows being dropped?

# COMMAND ----------

spark.sql(f"""
SELECT
    timestamp,
    origin.flow_name AS table_name,
    details:flow_progress.data_quality.expectations AS expectations
FROM event_log('{PIPELINE_ID}')
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC
LIMIT 10
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Investigate the Root Cause
# MAGIC
# MAGIC Based on the error messages you found, investigate further.
# MAGIC
# MAGIC ### Possible issues to look for:
# MAGIC - **Bad source path**: Does the Volume path for raw data exist?
# MAGIC - **Schema mismatch**: Did the raw JSON schema change?
# MAGIC - **Missing column**: Is a required column missing in the silver transform?
# MAGIC - **Data quality**: Are all rows being dropped by an expectation?

# COMMAND ----------

# Check if the source data exists
volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data/inventory_raw"
try:
    files = dbutils.fs.ls(volume_path)
    print(f"✅ Source path exists with {len(files)} items:")
    for f in files[:5]:
        print(f"   {f.name} ({f.size} bytes)")
except Exception as e:
    print(f"❌ Source path issue: {e}")

# COMMAND ----------

# Check raw data schema
try:
    df_raw = spark.read.json(f"{volume_path}/batch_01")
    print("Raw data schema:")
    df_raw.printSchema()
    print(f"Row count: {df_raw.count()}")
except Exception as e:
    print(f"❌ Cannot read raw data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Fix and Re-run
# MAGIC
# MAGIC Once you've identified the issue:
# MAGIC 1. Fix the pipeline source code (notebook in the `pipelines/` folder)
# MAGIC 2. Re-run the pipeline from the UI
# MAGIC 3. Verify success by checking the event log again
# MAGIC
# MAGIC ### Verification query:

# COMMAND ----------

spark.sql(f"""
SELECT
    timestamp,
    event_type,
    level,
    message,
    details:update_progress.state AS update_state
FROM event_log('{PIPELINE_ID}')
WHERE event_type = 'update_progress'
ORDER BY timestamp DESC
LIMIT 5
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next**: `05_SQL_Warehouse_Performance` — Deep-dive into SQL warehouse performance monitoring.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("04b_SDP_Debugging_Challenge")
