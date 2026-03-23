# Databricks notebook source
# MAGIC %md
# MAGIC # Admin Only: Snapshot System Tables
# MAGIC **Run this notebook as a workspace admin** to copy system table data into `puma_ops_lab.system_tables`
# MAGIC so that workshop participants can query it without needing system table permissions.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS puma_ops_lab.system_tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow (Jobs)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.lakeflow_jobs AS
# MAGIC SELECT * FROM system.lakeflow.jobs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.lakeflow_job_run_timeline AS
# MAGIC SELECT * FROM system.lakeflow.job_run_timeline

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.lakeflow_job_task_run_timeline AS
# MAGIC SELECT * FROM system.lakeflow.job_task_run_timeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Billing

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.billing_usage AS
# MAGIC SELECT * FROM system.billing.usage

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.billing_list_prices AS
# MAGIC SELECT * FROM system.billing.list_prices

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query History

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.query_history AS
# MAGIC SELECT * FROM system.query.history
# MAGIC WHERE start_time >= current_date() - 30

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.compute_clusters AS
# MAGIC SELECT * FROM system.compute.clusters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access & Audit

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.access_audit AS
# MAGIC SELECT * FROM system.access.audit
# MAGIC WHERE event_date >= current_date() - 7

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.access_table_lineage AS
# MAGIC SELECT * FROM system.access.table_lineage

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE puma_ops_lab.system_tables.access_column_lineage AS
# MAGIC SELECT * FROM system.access.column_lineage

# COMMAND ----------

# MAGIC %md
# MAGIC ## Grant access to all participants

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE SCHEMA ON SCHEMA puma_ops_lab.system_tables TO `account users`;
# MAGIC GRANT SELECT ON SCHEMA puma_ops_lab.system_tables TO `account users`;

# COMMAND ----------

# MAGIC %md
# MAGIC ✅ **Done!** Participants can now query `puma_ops_lab.system_tables.*` instead of `system.*`
