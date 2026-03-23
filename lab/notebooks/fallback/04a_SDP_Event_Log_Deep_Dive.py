# Databricks notebook source
# MAGIC %md
# MAGIC # 🔬 Block 4a: Spark Declarative Pipelines — Event Log & Monitoring
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Understand the SDP (Spark Declarative Pipelines) monitoring surfaces
# MAGIC 2. Query the **Event Log** using the `event_log()` table-valued function
# MAGIC 3. Parse structured event details (row counts, errors, data quality)
# MAGIC 4. Explore pipeline lineage and expectations
# MAGIC 5. Monitor and debug **Auto Loader** ingestion issues
# MAGIC

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## SDP Monitoring Surfaces
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │                    Pipeline Run                            │
# MAGIC │                                                            │
# MAGIC │  ┌─────────┐    ┌─────────┐    ┌─────────┐               │
# MAGIC │  │ Bronze   │ →  │ Silver  │ →  │ Gold     │              │
# MAGIC │  │(stream)  │    │(stream) │    │(mat.view)│              │
# MAGIC │  └────┬────┘    └────┬────┘    └────┬────┘               │
# MAGIC │       │              │              │                      │
# MAGIC │       └──────────────┴──────────────┘                      │
# MAGIC │                      │                                     │
# MAGIC │              Event Log (Delta)                             │
# MAGIC │           ┌──────────────────┐                             │
# MAGIC │           │ flow_progress    │ ← Row counts, throughput   │
# MAGIC │           │ update_progress  │ ← Pipeline start/stop      │
# MAGIC │           │ user_action      │ ← Manual actions           │
# MAGIC │           │ ERROR / WARN     │ ← Failures, DQ violations │
# MAGIC │           └──────────────────┘                             │
# MAGIC └────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Where to find monitoring data:
# MAGIC
# MAGIC | Surface | What It Shows | Access |
# MAGIC |---------|--------------|--------|
# MAGIC | **Pipeline UI** | DAG, run history, data quality, lineage | Databricks UI → Pipelines |
# MAGIC | **Event Log** | All pipeline events as structured Delta table | `event_log('<pipeline_id>')` TVF |
# MAGIC | **System Tables** | Pipeline runs as job runs | `system.lakeflow.job_run_timeline` |
# MAGIC | **Table Lineage** | Which tables feed which | `system.access.table_lineage` |
# MAGIC | **Published Event Log** | Event log exposed in UC | Pipeline Settings → Advanced |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Querying the Event Log
# MAGIC
# MAGIC The `event_log()` table-valued function returns all events for a pipeline.
# MAGIC
# MAGIC **Usage**: `SELECT * FROM event_log('<pipeline_id>')`
# MAGIC
# MAGIC > ⚠️ Replace `<PIPELINE_ID>` below with your actual pipeline ID.
# MAGIC > You can find it in the Pipeline UI URL or from `system.lakeflow.jobs`.

# COMMAND ----------

# Set your pipeline ID here (instructor will provide this)
PIPELINE_ID = SDP_PIPELINE_ID or "<your-pipeline-id>"

# COMMAND ----------

# MAGIC %md
# MAGIC ### All events overview

# COMMAND ----------

spark.sql(f"""
SELECT
    timestamp,
    event_type,
    level,
    message,
    origin.pipeline_name,
    origin.flow_name
FROM event_log('{PIPELINE_ID}')
ORDER BY timestamp DESC
LIMIT 50
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Event types breakdown
# MAGIC What kinds of events does the pipeline generate?

# COMMAND ----------

spark.sql(f"""
SELECT
    event_type,
    level,
    COUNT(*) AS event_count,
    MIN(timestamp) AS first_event,
    MAX(timestamp) AS last_event
FROM event_log('{PIPELINE_ID}')
GROUP BY event_type, level
ORDER BY event_count DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Row Counts & Throughput
# MAGIC The `flow_progress` events contain row-level metrics for each table in the pipeline.

# COMMAND ----------

spark.sql(f"""
SELECT
    timestamp,
    origin.flow_name AS table_name,
    details:flow_progress.metrics.num_output_rows AS rows_written,
    details:flow_progress.status AS flow_status,
    details:flow_progress.metrics.num_upserted_rows AS rows_upserted,
    details:flow_progress.metrics.num_deleted_rows AS rows_deleted
FROM event_log('{PIPELINE_ID}')
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC
LIMIT 30
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Data Quality — Expectations
# MAGIC If your pipeline defines expectations (data quality rules), violations appear in the event log.

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
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parse individual expectation results

# COMMAND ----------

spark.sql(f"""
SELECT
    timestamp,
    origin.flow_name AS table_name,
    exp.name AS expectation_name,
    exp.dataset AS dataset,
    exp.passed_records,
    exp.failed_records
FROM event_log('{PIPELINE_ID}'),
LATERAL VIEW explode(
    from_json(
        details:flow_progress.data_quality.expectations,
        'ARRAY<STRUCT<name:STRING,dataset:STRING,passed_records:LONG,failed_records:LONG>>'
    )
) AS exp
WHERE event_type = 'flow_progress'
  AND details:flow_progress.data_quality IS NOT NULL
ORDER BY timestamp DESC
LIMIT 30
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Error Events
# MAGIC Filter for ERROR-level events to find pipeline failures.

# COMMAND ----------

spark.sql(f"""
SELECT
    timestamp,
    level,
    origin.flow_name AS table_name,
    message,
    details:error.message AS error_detail,
    details:error.exceptions AS exceptions
FROM event_log('{PIPELINE_ID}')
WHERE level IN ('ERROR', 'WARN')
ORDER BY timestamp DESC
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Pipeline Update Lifecycle
# MAGIC Track when updates start, complete, or fail.

# COMMAND ----------

spark.sql(f"""
SELECT
    timestamp,
    event_type,
    message,
    details:update_progress.state AS update_state,
    details:create_update.cause AS trigger_cause
FROM event_log('{PIPELINE_ID}')
WHERE event_type IN ('update_progress', 'create_update')
ORDER BY timestamp DESC
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Pipeline Runs via System Tables
# MAGIC SDP pipelines also appear in `system.lakeflow.*` because they run as jobs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find pipeline job runs
# MAGIC SELECT
# MAGIC     jrt.job_id,
# MAGIC     j.name AS job_name,
# MAGIC     jrt.run_id,
# MAGIC     jrt.result_state,
# MAGIC     jrt.compute_type,
# MAGIC     jrt.period.startTime AS started_at,
# MAGIC     jrt.period.endTime AS ended_at,
# MAGIC     TIMESTAMPDIFF(SECOND, jrt.period.startTime, jrt.period.endTime) AS duration_sec
# MAGIC FROM system.lakeflow.job_run_timeline jrt
# MAGIC LEFT JOIN (
# MAGIC     SELECT job_id, name, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
# MAGIC     FROM system.lakeflow.jobs
# MAGIC ) j ON jrt.job_id = j.job_id AND j.rn = 1
# MAGIC WHERE j.name LIKE '%pipeline%' OR j.name LIKE '%sdp%' OR j.name LIKE '%puma_ops%'
# MAGIC ORDER BY jrt.period.startTime DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publishing the Event Log
# MAGIC
# MAGIC By default, the event log is stored internally. To make it queryable by others:
# MAGIC
# MAGIC 1. Open your pipeline in the UI
# MAGIC 2. Click **Settings → Advanced**
# MAGIC 3. Set **Event log catalog** and **Event log schema**
# MAGIC 4. Give the published table a name
# MAGIC
# MAGIC After publishing, anyone with access can query it as a regular Delta table.
# MAGIC
# MAGIC > ⚠️ **Important**: Never delete the event log table or its parent catalog/schema.
# MAGIC > This can cause pipeline failures in future runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7: Auto Loader Monitoring & Debugging
# MAGIC
# MAGIC Auto Loader (`read_files` / `cloudFiles`) is the primary ingestion mechanism for streaming tables
# MAGIC in SDP pipelines. When the bronze layer uses Auto Loader, its state and metrics surface through
# MAGIC several channels.
# MAGIC
# MAGIC ### Where Auto Loader state appears
# MAGIC
# MAGIC | Surface | What You See |
# MAGIC |---------|-------------|
# MAGIC | **Pipeline UI** | Backlog indicator on streaming table nodes |
# MAGIC | **Event log (`flow_progress`)** | `numFilesOutstanding`, `numBytesOutstanding` in source metrics |
# MAGIC | **`cloud_files_state()` SQL function** | File-level metadata: discovered, processed, pending |
# MAGIC | **Spark UI → Streaming tab** | Input rate, processing rate, batch duration |
# MAGIC | **StreamingQueryListener** | Programmatic access to the same backlog metrics |
# MAGIC
# MAGIC ### Inspecting discovered files with `cloud_files_state()`
# MAGIC
# MAGIC The `cloud_files_state()` SQL function (available in DBR 11.3+) lets you inspect which files
# MAGIC Auto Loader has discovered and processed for a given stream:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM cloud_files_state('<checkpoint_location>');
# MAGIC ```
# MAGIC
# MAGIC This returns file-level metadata showing what has been discovered, processed, or is still pending.
# MAGIC Useful for diagnosing: "Why was my file not ingested?" or "How large is the backlog?"
# MAGIC
# MAGIC ### Backlog metrics
# MAGIC
# MAGIC Auto Loader reports these metrics at every batch:
# MAGIC - **`numFilesOutstanding`** — Files discovered but not yet processed
# MAGIC - **`numBytesOutstanding`** — Total bytes in the backlog
# MAGIC
# MAGIC A growing backlog means the source is producing files faster than the pipeline can consume them.
# MAGIC Remediation: increase compute, reduce batch complexity, or use rate limiting.
# MAGIC
# MAGIC ### Common Auto Loader issues
# MAGIC
# MAGIC | Issue | Symptoms | Resolution |
# MAGIC |-------|----------|-----------|
# MAGIC | **Bad source path** | Pipeline fails immediately at bronze | Verify the Volume path exists and has correct permissions |
# MAGIC | **Schema inference failure** | Errors about unexpected types or corrupt records | Add/fix `schemaHints`, check for corrupt files in source |
# MAGIC | **Schema evolution** | New columns in source data silently dropped or cause failures | Use `cloudFiles.schemaEvolutionMode` = `addNewColumns` or add a `_rescued_data` column |
# MAGIC | **Checkpoint corruption** | Stream fails to start after restart | Reset the checkpoint (requires full refresh) |
# MAGIC | **File notification queue overflow** | Files missed after extended downtime (> 7 days) | Set `cloudFiles.backfillInterval` for periodic directory listing catch-up |
# MAGIC | **Duplicate ingestion** | Same files processed twice | Check `cloudFiles.maxFileAge` — if set too aggressively, expired files may be re-discovered |
# MAGIC
# MAGIC ### Cost control for Auto Loader
# MAGIC
# MAGIC - **`Trigger.AvailableNow`**: Run Auto Loader as a batch job instead of continuously.
# MAGIC   Processes all files that arrived before the query start, then stops. Schedule via Lakeflow Jobs.
# MAGIC - **`cloudFiles.maxFilesPerTrigger`**: Limit files processed per micro-batch (hard limit).
# MAGIC - **`cloudFiles.maxBytesPerTrigger`**: Limit bytes per micro-batch (soft limit).
# MAGIC - Use **file events** mode (default in SDP) instead of directory listing to avoid `LIST` API costs.
# MAGIC
# MAGIC ### File retention and cleanup
# MAGIC
# MAGIC - **`cloudFiles.maxFileAge`**: Expire file tracking state for long-running streams.
# MAGIC   Minimum: 14 days. Recommended: 90+ days. Setting this too low causes missed files or duplicates.
# MAGIC - **`cloudFiles.cleanSource`**: Automatically `MOVE` or `DELETE` source files after processing
# MAGIC   to reduce directory listing costs and storage usage.
# MAGIC - **`cloudFiles.backfillInterval`**: Trigger periodic directory listing backfills (e.g., daily)
# MAGIC   to catch any files missed by the notification system.
# MAGIC
# MAGIC > **⚠️ When using file events**: Run your Auto Loader streams at least once every 7 days
# MAGIC > to keep file discovery incremental. Extended downtime triggers a full directory listing.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🏋️ Exercises
# MAGIC
# MAGIC ### Exercise 1
# MAGIC **Find all ERROR events from the most recent pipeline run. What was the root cause?**

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2
# MAGIC **Extract row counts per table per pipeline run. Which table processes the most rows?**

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3
# MAGIC **Find data quality expectation violations. How many rows failed each expectation?**

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4
# MAGIC **Calculate pipeline run duration from `update_progress` events (RUNNING → COMPLETED).**

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next**: `04b_SDP_Debugging_Challenge` — Fix a broken SDP pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("04a_SDP_Event_Log_Deep_Dive")
