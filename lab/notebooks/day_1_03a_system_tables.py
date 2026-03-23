# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Day 1 — 03a: System Tables & Pre-built Monitoring Dashboards
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. **Trigger your challenge jobs** (they run in the background while you learn)
# MAGIC 2. Understand which system tables exist and what they contain
# MAGIC 3. Import and explore pre-built monitoring dashboards
# MAGIC 4. Write operational queries against system tables

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 Step 0: Create & Trigger Your Challenge Jobs
# MAGIC
# MAGIC Before we dive into system tables, let's kick off your **personal challenge jobs** in the background.
# MAGIC They will run (and mostly fail!) while you work through the next ~30 minutes.
# MAGIC By the time you reach the debugging notebook, they'll be done and you'll have real failure data to investigate.
# MAGIC
# MAGIC **Run the cell below** — it creates your jobs and triggers them all.

# COMMAND ----------

# MAGIC %run ./_setup_jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🗺️ System Tables Overview
# MAGIC
# MAGIC System tables live in the **`system`** catalog and are read-only. They provide operational data across your entire account.
# MAGIC
# MAGIC | Schema | Key Tables | What It Tells You |
# MAGIC |--------|-----------|-------------------|
# MAGIC | `system.access` | `audit`, `table_lineage`, `column_lineage` | Who did what, data flow between tables |
# MAGIC | `system.ai` | `token_usage`, `served_entities` | AI feature usage, token consumption |
# MAGIC | `system.ai_gateway` | `request_logs` | AI Gateway request/response logging |
# MAGIC | `system.billing` | `usage`, `list_prices` | DBU consumption, cost attribution |
# MAGIC | `system.compute` | `clusters`, `warehouses`, `node_types` | Cluster lifecycle, warehouse activity |
# MAGIC | `system.information_schema` | `catalogs`, `schemata`, `tables`, `columns` | Unity Catalog metadata |
# MAGIC | `system.lakeflow` | `jobs`, `job_run_timeline`, `job_task_run_timeline` | Job configs, run history, task-level timing |
# MAGIC | `system.mlflow` | `experiments`, `model_versions` | MLflow experiment and model registry tracking |
# MAGIC | `system.query` | `history` | Every SQL query: duration, rows, user, warehouse |
# MAGIC | `system.serving` | `served_entities`, `endpoint_usage` | Model serving endpoints and traffic |
# MAGIC | `system.storage` | `predictive_optimization_operations_history` | Storage optimization events |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Explore Available System Schemas
# MAGIC Let's first check which system schemas are enabled in this workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT schema_name, catalog_name
# MAGIC FROM system.information_schema.schemata
# MAGIC WHERE catalog_name = 'system'
# MAGIC ORDER BY schema_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Lakeflow Jobs — Run History & Failures
# MAGIC The `system.lakeflow` schema gives you a complete picture of all jobs across the workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Stale jobs — ran at least once but not in the last 30 days
# MAGIC This is a common operational query: find jobs that were created and ran at some point,
# MAGIC but have gone silent. These are candidates for cleanup or investigation.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH latest_jobs AS (
# MAGIC   SELECT
# MAGIC     workspace_id,
# MAGIC     job_id,
# MAGIC     name,
# MAGIC     creator_id,
# MAGIC     create_time,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
# MAGIC   FROM system.lakeflow.jobs
# MAGIC   QUALIFY rn = 1
# MAGIC ),
# MAGIC jobs_last_run AS (
# MAGIC   SELECT
# MAGIC     job_id,
# MAGIC     MAX(period_start_time) AS last_run
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   GROUP BY job_id
# MAGIC ),
# MAGIC recent_runs AS (
# MAGIC   SELECT DISTINCT job_id
# MAGIC   FROM system.lakeflow.job_run_timeline
# MAGIC   WHERE period_start_time >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
# MAGIC )
# MAGIC SELECT
# MAGIC   j.job_id,
# MAGIC   j.name,
# MAGIC   j.creator_id,
# MAGIC   j.create_time,
# MAGIC   lr.last_run
# MAGIC FROM latest_jobs j
# MAGIC INNER JOIN jobs_last_run lr ON j.job_id = lr.job_id
# MAGIC LEFT JOIN recent_runs r ON j.job_id = r.job_id
# MAGIC WHERE r.job_id IS NULL
# MAGIC ORDER BY lr.last_run DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Daily job count by run outcome (last 7 days)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily job count across all workspaces for the last 7 days,
# MAGIC -- distributed by the outcome of the job run.
# MAGIC SELECT
# MAGIC   workspace_id,
# MAGIC   COUNT(DISTINCT run_id) as job_count,
# MAGIC   result_state,
# MAGIC   to_date(period_start_time) as date
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE
# MAGIC   period_start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
# MAGIC   AND result_state IS NOT NULL
# MAGIC GROUP BY ALL

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Historical runtime for your jobs
# MAGIC The query below lists **your** jobs (filtered by your prefix) and their run counts.
# MAGIC Pick one and paste the name into the second query to see its run-by-run history.
# MAGIC
# MAGIC > **Note**: `run_name` is only populated for `SUBMIT_RUN` (one-time runs via `runs/submit`).
# MAGIC > For regular jobs triggered via `run-now`, the name lives on the job definition in `system.lakeflow.jobs`,
# MAGIC > so we join to get it.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List your workshop jobs — replace 'labuser%' with your prefix for exact match
# MAGIC SELECT j.name AS job_name, COUNT(*) AS run_count
# MAGIC FROM system.lakeflow.job_run_timeline jrt
# MAGIC JOIN (
# MAGIC   SELECT job_id, name,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
# MAGIC   FROM system.lakeflow.jobs
# MAGIC ) j ON jrt.job_id = j.job_id AND j.rn = 1
# MAGIC WHERE jrt.period_start_time > CURRENT_TIMESTAMP() - INTERVAL 60 DAYS
# MAGIC   AND j.name LIKE 'labuser%'
# MAGIC GROUP BY j.name
# MAGIC ORDER BY run_count DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC Pick a job name from the results above and paste it into the query below (replace `YOUR_JOB_NAME`).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Historical runtime per run — replace YOUR_JOB_NAME with a name from above
# MAGIC SELECT
# MAGIC   jrt.workspace_id,
# MAGIC   j.name AS job_name,
# MAGIC   jrt.run_id,
# MAGIC   jrt.result_state,
# MAGIC   jrt.period_start_time AS started_at,
# MAGIC   SUM(period_end_time - period_start_time) AS run_time
# MAGIC FROM system.lakeflow.job_run_timeline jrt
# MAGIC JOIN (
# MAGIC   SELECT job_id, name,
# MAGIC          ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
# MAGIC   FROM system.lakeflow.jobs
# MAGIC ) j ON jrt.job_id = j.job_id AND j.rn = 1
# MAGIC WHERE
# MAGIC   jrt.period_start_time > CURRENT_TIMESTAMP() - INTERVAL 60 DAYS
# MAGIC   AND j.name = 'YOUR_JOB_NAME'
# MAGIC GROUP BY ALL
# MAGIC ORDER BY started_at DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Billing & Cost Attribution
# MAGIC Understand **who** and **what** is consuming DBUs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This query joins with the `billing.usage` system table to calculate a cost per job run.
# MAGIC with jobs_usage AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     usage_metadata.job_id,
# MAGIC     usage_metadata.job_run_id as run_id,
# MAGIC     identity_metadata.run_as as run_as
# MAGIC   FROM system.billing.usage
# MAGIC   WHERE billing_origin_product="JOBS"
# MAGIC ),
# MAGIC jobs_usage_with_usd AS (
# MAGIC   SELECT
# MAGIC     jobs_usage.*,
# MAGIC     usage_quantity * pricing.default as usage_usd
# MAGIC   FROM jobs_usage
# MAGIC     LEFT JOIN system.billing.list_prices pricing ON
# MAGIC       jobs_usage.sku_name = pricing.sku_name
# MAGIC       AND pricing.price_start_time <= jobs_usage.usage_start_time
# MAGIC       AND (pricing.price_end_time >= jobs_usage.usage_start_time OR pricing.price_end_time IS NULL)
# MAGIC       AND pricing.currency_code="USD"
# MAGIC ),
# MAGIC jobs_usage_aggregated AS (
# MAGIC   SELECT
# MAGIC     workspace_id,
# MAGIC     job_id,
# MAGIC     run_id,
# MAGIC     FIRST(run_as, TRUE) as run_as,
# MAGIC     sku_name,
# MAGIC     SUM(usage_usd) as usage_usd,
# MAGIC     SUM(usage_quantity) as usage_quantity
# MAGIC   FROM jobs_usage_with_usd
# MAGIC   GROUP BY ALL
# MAGIC )
# MAGIC SELECT
# MAGIC   t1.*,
# MAGIC   MIN(period_start_time) as run_start_time,
# MAGIC   MAX(period_end_time) as run_end_time,
# MAGIC   FIRST(result_state, TRUE) as result_state
# MAGIC FROM jobs_usage_aggregated t1
# MAGIC   LEFT JOIN system.lakeflow.job_run_timeline t2 USING (workspace_id, job_id, run_id)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY usage_usd DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Query Performance (SQL Warehouse)
# MAGIC The `system.query.history` table records every SQL statement executed on a warehouse.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 slowest queries in the last 7 days
# MAGIC SELECT
# MAGIC     statement_id,
# MAGIC     executed_by,
# MAGIC     compute.warehouse_id,
# MAGIC     total_duration_ms,
# MAGIC     execution_duration_ms,
# MAGIC     compilation_duration_ms,
# MAGIC     waiting_at_capacity_duration_ms,
# MAGIC     produced_rows,
# MAGIC     SUBSTRING(statement_text, 1, 120) AS query_preview
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_date() - 7
# MAGIC   AND execution_status = 'FINISHED'
# MAGIC ORDER BY total_duration_ms DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query performance trends: p50, p95 by day
# MAGIC SELECT
# MAGIC     DATE(start_time) AS query_date,
# MAGIC     COUNT(*) AS total_queries,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.50) AS p50_ms,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.95) AS p95_ms,
# MAGIC     SUM(CASE WHEN execution_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_queries
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_date() - 14
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY query_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Audit & Access Logs
# MAGIC Track **who accessed what** across the workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Failed access attempts (security monitoring)
# MAGIC SELECT
# MAGIC     event_time,
# MAGIC     user_identity.email AS user_email,
# MAGIC     source_ip_address,
# MAGIC     action_name,
# MAGIC     request_params['full_name_arg'] AS resource,
# MAGIC     response.error_message
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= current_date() - 7
# MAGIC   AND response.status_code != 200
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 25

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Lineage — Data Flow Between Tables
# MAGIC Unity Catalog automatically captures table-level and column-level lineage.
# MAGIC
# MAGIC > **💡 Tip**: You can also explore lineage **visually** in **Catalog Explorer** — navigate to any table and click the **Lineage** tab to see upstream sources and downstream consumers as an interactive graph.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What tables feed into our gold table?
# MAGIC SELECT DISTINCT
# MAGIC     source_table_full_name,
# MAGIC     source_type,
# MAGIC     target_table_full_name,
# MAGIC     target_type,
# MAGIC     MAX(event_time) AS last_updated
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name LIKE 'puma_ops_lab.workshop.%'
# MAGIC GROUP BY source_table_full_name, source_type, target_table_full_name, target_type
# MAGIC ORDER BY last_updated DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Compute — Cluster Inventory
# MAGIC See all clusters and their configurations.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     cluster_id,
# MAGIC     cluster_name,
# MAGIC     owned_by,
# MAGIC     cluster_source,
# MAGIC     dbr_version,
# MAGIC     worker_node_type,
# MAGIC     driver_node_type,
# MAGIC     change_time
# MAGIC FROM system.compute.clusters
# MAGIC WHERE change_time >= current_date() - 30
# MAGIC   AND delete_time IS NULL
# MAGIC ORDER BY change_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🏋️ Exercises
# MAGIC
# MAGIC Now it's your turn. Write queries to answer the following questions.
# MAGIC
# MAGIC ### Exercise 1
# MAGIC **Find jobs that haven’t run in the last 30 days.**

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2
# MAGIC **Calculate cost per job run for the last 30 days.**

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3
# MAGIC **Identify failed or retried job runs in the last 30 days.**

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📊 Pre-built Monitoring Dashboards
# MAGIC
# MAGIC You don't have to build all these queries yourself — Databricks provides **pre-built AI/BI dashboard templates** that visualize system tables out of the box.
# MAGIC
# MAGIC ### LakeFlow System Tables Dashboard
# MAGIC
# MAGIC Gives you a complete view of **job and pipeline health** across the workspace: run timelines, failure rates, cost per job, task-level breakdown, and pipeline event logs.
# MAGIC
# MAGIC 👉 **[Open the LakeFlow System Tables Dashboard](https://dbc-6f63f358-af2e.cloud.databricks.com/dashboardsv3/01f1236d355b10aaab3cd8472f974424/published?o=7474653647574759)**
# MAGIC
# MAGIC > 💡 This dashboard is already imported into the workspace. Explore it, then continue to the next notebook.
# MAGIC
# MAGIC ---
# MAGIC **Next**: [day_1_03b_genie — Ask questions about your operations in natural language]($./day_1_03b_genie)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_03a_system_tables")
