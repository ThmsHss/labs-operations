# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Day 1 — 03a: System Tables — Exercise Solutions
# MAGIC **INSTRUCTOR ONLY — Do not distribute to participants**

# COMMAND ----------

# MAGIC %run ../notebooks/LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1 Solution
# MAGIC **Find jobs that haven't run in the last 30 days.**

# COMMAND ----------

# MAGIC %sql
# MAGIC with latest_jobs AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) as rn
# MAGIC     FROM system.lakeflow.jobs QUALIFY rn=1
# MAGIC ),
# MAGIC latest_not_deleted_jobs AS (
# MAGIC     SELECT
# MAGIC         workspace_id,
# MAGIC         job_id,
# MAGIC         name,
# MAGIC         change_time,
# MAGIC         tags
# MAGIC     FROM latest_jobs WHERE delete_time IS NULL
# MAGIC ),
# MAGIC last_seen_job_timestamp AS (
# MAGIC     SELECT
# MAGIC         workspace_id,
# MAGIC         job_id,
# MAGIC         MAX(period_start_time) as last_executed_at
# MAGIC     FROM system.lakeflow.job_run_timeline
# MAGIC     WHERE
# MAGIC         run_type="JOB_RUN"
# MAGIC     GROUP BY ALL
# MAGIC )
# MAGIC SELECT
# MAGIC     t1.workspace_id,
# MAGIC     t1.job_id,
# MAGIC     t1.name,
# MAGIC     t1.change_time as last_modified_at,
# MAGIC     t2.last_executed_at,
# MAGIC     t1.tags
# MAGIC FROM latest_not_deleted_jobs t1
# MAGIC     LEFT JOIN last_seen_job_timestamp t2
# MAGIC         USING (workspace_id, job_id)
# MAGIC WHERE
# MAGIC     (t2.last_executed_at <= CURRENT_DATE() - INTERVAL 30 DAYS) OR (t2.last_executed_at IS NULL)
# MAGIC ORDER BY last_executed_at ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2 Solution
# MAGIC **Calculate cost per job run for the last 30 days.**

# COMMAND ----------

# MAGIC %sql
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
# MAGIC ### Exercise 3 Solution
# MAGIC **Identify failed or retried job runs in the last 30 days.**

# COMMAND ----------

# MAGIC %sql
# MAGIC with repaired_runs as (
# MAGIC     SELECT
# MAGIC     workspace_id, job_id, run_id, COUNT(*) - 1 as retries_count
# MAGIC     FROM system.lakeflow.job_run_timeline
# MAGIC     WHERE result_state IS NOT NULL
# MAGIC     GROUP BY ALL
# MAGIC     HAVING retries_count > 0
# MAGIC     )
# MAGIC SELECT
# MAGIC     *
# MAGIC FROM repaired_runs
# MAGIC ORDER BY retries_count DESC
# MAGIC LIMIT 10
