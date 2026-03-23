# Databricks notebook source
# MAGIC %md
# MAGIC # 🚨 Block 7: SQL Alerts — Proactive Monitoring
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Understand how SQL Alerts work
# MAGIC 2. Create 3 operational alerts
# MAGIC 3. Configure schedules and notifications
# MAGIC 4. View alert history
# MAGIC

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## How SQL Alerts Work
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
# MAGIC │  SQL Query    │ →  │  Schedule     │ →  │  Condition    │ →  │ Notification  │
# MAGIC │  (any query)  │    │  (e.g. 5min)  │    │  (threshold)  │    │  (email/Slack)│
# MAGIC └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
# MAGIC
# MAGIC Status:  TRIGGERED  |  OK  |  ERROR
# MAGIC ```
# MAGIC
# MAGIC ### Key concepts:
# MAGIC - An alert is a **saved query** + **schedule** + **condition** + **notification**
# MAGIC - The query runs on a SQL warehouse on schedule
# MAGIC - If the condition is met, status changes to TRIGGERED and notification fires
# MAGIC - Alert history tracks every evaluation
# MAGIC - Currently **Public Preview** — no parameterized queries supported yet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alert 1: Job Failure Alert
# MAGIC
# MAGIC **Goal**: Get notified when any job fails in the last hour.
# MAGIC
# MAGIC ### Step 1: Create the query
# MAGIC Go to **SQL → SQL Editor** and create a new query:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Alert Query: Job Failures in Last Hour
# MAGIC SELECT COUNT(*) AS failed_jobs
# MAGIC FROM system.lakeflow.job_run_timeline
# MAGIC WHERE result_state = 'FAILED'
# MAGIC   AND period.startTime >= current_timestamp() - INTERVAL 1 HOUR
# MAGIC ```
# MAGIC
# MAGIC **Save as**: `alert_job_failures`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create the alert
# MAGIC 1. Click **Alerts** in the sidebar → **Create Alert**
# MAGIC 2. Select the query `alert_job_failures`
# MAGIC 3. **Column**: `failed_jobs`
# MAGIC 4. **Condition**: `Value > 0`
# MAGIC 5. **Schedule**: Every 15 minutes
# MAGIC 6. **Notification**: *(add your email or a webhook)*
# MAGIC
# MAGIC ### Test it:
# MAGIC Run the query manually — if there are recent failures, the alert will trigger.

# COMMAND ----------

# Let's verify the query works
display(spark.sql("""
SELECT COUNT(*) AS failed_jobs
FROM system.lakeflow.job_run_timeline
WHERE result_state = 'FAILED'
  AND period.startTime >= current_timestamp() - INTERVAL 1 HOUR
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Alert 2: Data Quality Alert
# MAGIC
# MAGIC **Goal**: Alert when the null percentage for `customer_id` in orders exceeds 5%.
# MAGIC
# MAGIC ### Query:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Alert Query: Data Quality — High Null Rate
# MAGIC SELECT
# MAGIC     MAX(null_percentage) AS max_null_pct
# MAGIC FROM puma_ops_lab.workshop.orders_profile_metrics
# MAGIC WHERE column_name = 'customer_id'
# MAGIC   AND window.end >= current_date() - 3
# MAGIC ```
# MAGIC
# MAGIC **Save as**: `alert_orders_null_customer`
# MAGIC
# MAGIC **Condition**: `max_null_pct > 5.0`
# MAGIC
# MAGIC **Schedule**: Every 1 hour

# COMMAND ----------

# Test the query
display(spark.sql(f"""
SELECT
    MAX(null_percentage) AS max_null_pct
FROM {CATALOG}.{SCHEMA}.orders_profile_metrics
WHERE column_name = 'customer_id'
  AND window.end >= current_date() - 3
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Alert 3: Warehouse Queue Alert
# MAGIC
# MAGIC **Goal**: Alert when average query wait time exceeds 30 seconds in the last 30 minutes.
# MAGIC
# MAGIC ### Query:

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Alert Query: Warehouse Queue Backup
# MAGIC SELECT
# MAGIC     AVG(waiting_at_capacity_duration_ms) AS avg_queue_wait_ms
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 30 MINUTES
# MAGIC   AND waiting_at_capacity_duration_ms > 0
# MAGIC ```
# MAGIC
# MAGIC **Save as**: `alert_warehouse_queue`
# MAGIC
# MAGIC **Condition**: `avg_queue_wait_ms > 30000`
# MAGIC
# MAGIC **Schedule**: Every 15 minutes

# COMMAND ----------

# Test the query
display(spark.sql("""
SELECT
    AVG(waiting_at_capacity_duration_ms) AS avg_queue_wait_ms
FROM system.query.history
WHERE start_time >= current_timestamp() - INTERVAL 30 MINUTES
  AND waiting_at_capacity_duration_ms > 0
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Bonus: SQL Alert as a Job Task
# MAGIC
# MAGIC SQL Alerts can also be used as a **task type within Databricks Jobs**:
# MAGIC
# MAGIC ```
# MAGIC Job: Daily Ops Health Check
# MAGIC ├── Task 1: Run ETL pipeline
# MAGIC ├── Task 2: SQL Alert — check for failures (condition: if failed, notify)
# MAGIC └── Task 3: SQL Alert — check data quality (condition: if DQ issue, notify)
# MAGIC ```
# MAGIC
# MAGIC This lets you orchestrate alerts as part of your data pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Managing Alerts Programmatically
# MAGIC
# MAGIC You can also create and manage alerts via the Databricks CLI or REST API:
# MAGIC
# MAGIC ```bash
# MAGIC # List all alerts
# MAGIC databricks alerts list
# MAGIC
# MAGIC # Get alert details
# MAGIC databricks alerts get <alert-id>
# MAGIC
# MAGIC # Create alert (requires query_id, warehouse_id, condition)
# MAGIC databricks alerts create --json '{
# MAGIC   "display_name": "Job Failures",
# MAGIC   "query_id": "<query-id>",
# MAGIC   "warehouse_id": "<warehouse-id>",
# MAGIC   "condition": {
# MAGIC     "op": "GREATER_THAN",
# MAGIC     "operand": {"column": {"name": "failed_jobs"}},
# MAGIC     "threshold": {"value": {"double_value": 0}}
# MAGIC   }
# MAGIC }'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Alert Summary
# MAGIC
# MAGIC | Alert | Query Source | Condition | Schedule |
# MAGIC |-------|-------------|-----------|----------|
# MAGIC | Job Failures | `system.lakeflow.job_run_timeline` | count > 0 | Every 15 min |
# MAGIC | Data Quality | `orders_profile_metrics` | null_pct > 5% | Every 1 hour |
# MAGIC | Warehouse Queue | `system.query.history` | wait > 30s | Every 15 min |
# MAGIC
# MAGIC ### Notification destinations:
# MAGIC - **Email**: Built-in
# MAGIC - **Slack**: Via webhook notification destination
# MAGIC - **PagerDuty**: Via webhook notification destination
# MAGIC - **MS Teams**: Via webhook notification destination
# MAGIC
# MAGIC Configure notification destinations in **Settings → Notification destinations**.
# MAGIC
# MAGIC ---
# MAGIC **Next**: `08_Performance_Investigation` — Investigate and fix a slow table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("07_SQL_Alerts")
