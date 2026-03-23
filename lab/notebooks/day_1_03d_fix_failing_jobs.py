# Databricks notebook source
# MAGIC %md
# MAGIC # 🛠️ Day 1 — 03d: Challenge — Fix Failing Jobs
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC You have **4 personal jobs** deployed in this workspace. Each one has an intentional bug.
# MAGIC Your mission: **diagnose and fix each failure** using the tools you've learned.
# MAGIC
# MAGIC | Challenge | Failure Type | Where to Look |
# MAGIC |-----------|-------------|---------------|
# MAGIC | 1 | Schema mismatch | Job output, schema comparison |
# MAGIC | 2 | Missing SELECT → insufficient permissions, then ambiguous column (multi-task) | Task-level output, GRANT, Repair Run (×2) |
# MAGIC | 3 | Bad notebook parameter | Job params, assertion error |
# MAGIC | 4 | Runaway join (runs forever / OOM) | The code itself, Genie |
# MAGIC
# MAGIC 💡 **Hints** are available in the `hints/` folder if you get stuck.

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔑 Your Personal Jobs
# MAGIC Run the cell below to see your personal job names. Search for these in **Jobs & Pipelines**.

# COMMAND ----------

JOB_PREFIX = user_prefix
challenge_jobs = {
    "Challenge 1 — Schema Mismatch":       f"{JOB_PREFIX}_challenge_01_schema",
    "Challenge 2 — Permission Denied":     f"{JOB_PREFIX}_challenge_02_permissions",
    "Challenge 3 — Bad Parameters":        f"{JOB_PREFIX}_challenge_04_params",
    "Challenge 4 — Runaway Job":           f"{JOB_PREFIX}_challenge_03_oom",
}
print(f"Your job prefix: {JOB_PREFIX}")
print(f"Your job notebooks: /Shared/puma_ops_masterclass/{current_user.split('@')[0].replace('.', '_') if '@' in current_user else current_user}/jobs/\n")
for desc, name in challenge_jobs.items():
    print(f"  {desc:40s} → {name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 1: Schema Mismatch
# MAGIC
# MAGIC **Job**: Your `{JOB_PREFIX}_challenge_01_schema` (see above)
# MAGIC
# MAGIC **Scenario**: The ETL job reads from the `orders` table and creates a summary report.
# MAGIC Yesterday, the upstream team renamed a column. Now the job fails.
# MAGIC
# MAGIC > This is one of the most common production failures — upstream schema changes breaking
# MAGIC > downstream jobs. For now, focus on **diagnosing and fixing the immediate error**. We'll
# MAGIC > cover how to **handle schema evolution properly** (Auto Loader schema evolution, schema
# MAGIC > hints, merge schemas) in a later session.
# MAGIC
# MAGIC ### Your task:
# MAGIC 1. Go to **Jobs & Pipelines** and search for your job (your prefix + `challenge_01`)
# MAGIC 2. Look at the latest failed run — what's the error?
# MAGIC 3. Identify which column was renamed
# MAGIC 4. Fix the job notebook and re-run
# MAGIC
# MAGIC ### Tools to use:
# MAGIC - Job run output (error message)
# MAGIC - `system.access.table_lineage` (which tables are involved)
# MAGIC - Compare schemas: run the cell below

# COMMAND ----------

# Compare the original and changed schemas
print("=== orders (original) ===")
spark.table(f"{FQ}.orders").printSchema()
print("\n=== orders_v2 (modified upstream) ===")
spark.table(f"{FQ}.orders_v2").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Investigation space — use this cell for your queries:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Investigate here
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 2: Permission Denied (Multi-Task Job)
# MAGIC
# MAGIC **Job**: Your `{JOB_PREFIX}_challenge_02_permissions`
# MAGIC
# MAGIC **Scenario**: A **multi-task pipeline** extracts orders, enriches them with customer segment
# MAGIC data from a curated schema, and builds a revenue report. The job has **3 tasks chained
# MAGIC together** — but only one of them fails. The other tasks either succeeded or never ran.
# MAGIC
# MAGIC > This challenge introduces two important operational skills:
# MAGIC > 1. **Diagnosing partial failures** in multi-task jobs (which task failed? which succeeded?)
# MAGIC > 2. **Repair Run** — re-running a job from the point of failure instead of re-running everything
# MAGIC >
# MAGIC > **Heads up**: This challenge has **two layers**. After fixing the first error you'll hit a second
# MAGIC > one — that's intentional. Use **Repair Run** each time to re-run only from the failed task.
# MAGIC
# MAGIC ### Your task:
# MAGIC 1. Open the failed run — look at the **task-level DAG** view. Which task failed? Which succeeded?
# MAGIC 2. Click the **failed task** to see the error. You'll see `INSUFFICIENT_PERMISSIONS` — you don't have `SELECT` on the table.
# MAGIC 3. Fix by granting yourself `SELECT` on the `curated` schema, then use **Repair Run** to re-run only from the failed task
# MAGIC 5. After the permission fix, you'll hit a **second error**: `AMBIGUOUS_REFERENCE` on the `region` column.
# MAGIC    Both tables have a `region` column — after the join, Spark doesn't know which one to use.
# MAGIC    Fix the code to qualify the ambiguous column (e.g. `df_staging["region"]`), then **Repair Run** again.
# MAGIC
# MAGIC ### Tools to use:
# MAGIC - Job run task DAG view (see which tasks passed/failed/skipped)
# MAGIC - Task-level output (click the failed task)
# MAGIC - SQL `GRANT` statement
# MAGIC - **Repair Run** button (top-right of the failed run page) — you'll use this **twice**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Investigation space:
# MAGIC Check what schemas exist and what permissions you have:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- What schemas exist in the catalog?
# MAGIC SHOW SCHEMAS IN puma_ops_lab

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The schema exists (SHOW SCHEMAS shows it), but can you read from it?
# MAGIC -- Try this — you'll get INSUFFICIENT_PERMISSIONS:
# MAGIC -- SELECT * FROM puma_ops_lab.curated.customer_segments LIMIT 5
# MAGIC --
# MAGIC -- Fix: GRANT SELECT ON SCHEMA puma_ops_lab.curated TO `account users`
# MAGIC 

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 3: Bad Notebook Parameters
# MAGIC
# MAGIC **Job**: Your `{JOB_PREFIX}_challenge_04_params`
# MAGIC
# MAGIC **Scenario**: A parameterized notebook uses `dbutils.widgets` to accept a `region` filter.
# MAGIC The job passes `region = "APEC"` (typo for "APAC"). The notebook runs but returns 0 rows,
# MAGIC causing downstream assertions to fail.
# MAGIC
# MAGIC ### Your task:
# MAGIC 1. Look at the job configuration — what parameters are being passed?
# MAGIC 2. Run the notebook manually with the correct parameter
# MAGIC 3. Fix the job parameter and re-run
# MAGIC
# MAGIC ### Tools to use:
# MAGIC - Job configuration → Task parameters
# MAGIC - Notebook widget inspection
# MAGIC - Job run output (the assertion error message)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quick check: what are the valid regions?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT region FROM puma_ops_lab.workshop.orders ORDER BY region

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Investigation space:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Investigate here
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 4: Runaway Job
# MAGIC
# MAGIC **Job**: Your `{JOB_PREFIX}_challenge_03_oom`
# MAGIC
# MAGIC **Scenario**: A reporting job runs on a **classic job cluster** and joins orders with customers
# MAGIC to create a revenue breakdown. The job either **runs forever** or **crashes with OOM**.
# MAGIC Something about the join is very wrong.
# MAGIC
# MAGIC > This job runs on **classic compute**, so in theory you have the **Spark UI**. But here's
# MAGIC > the reality: sometimes the Spark UI won't tell you much — stages may show no tasks, the SQL
# MAGIC > tab may not reveal the issue, and you're left staring at a job that's stuck. This is common
# MAGIC > with certain join patterns on small clusters.
# MAGIC >
# MAGIC > **The best debugging tool here is the code itself.** Don't rely on job names or UI metrics alone.
# MAGIC > Open the notebook, read what the join is actually doing, and think about the math.
# MAGIC
# MAGIC ### Your task:
# MAGIC 1. Open the run — is it still running? Did it fail with OOM?
# MAGIC 2. The Spark UI may not give you much here. Instead: **click through to the job notebook code**
# MAGIC 3. Read the code — how are the two DataFrames being joined? What type of join is it?
# MAGIC 4. Do the math: 500K orders × 50K customers = **25 billion rows**. That's why it runs forever / OOMs.
# MAGIC 5. Fix the join: replace `crossJoin` with `.join(..., "customer_id")`, then re-run
# MAGIC
# MAGIC > 💡 **Tip**: Try using the **Genie Space** you built earlier — ask it
# MAGIC > *"Which jobs are currently running for longer than 30 minutes?"* or check system tables
# MAGIC > for the run status. Then open the code to understand *why*.
# MAGIC
# MAGIC ### Tools to use:
# MAGIC - **The job notebook code** — this is your primary tool. Read the actual logic.
# MAGIC - Job run output / error message (OOM if it crashes, or the job just hangs)
# MAGIC - Genie / system tables to identify the stuck job

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔎 Investigation space:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Investigate here
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📋 Challenge Debrief
# MAGIC
# MAGIC After completing the challenges, discuss with your team:
# MAGIC
# MAGIC | Challenge | Compute Type | What debugging surfaces did you use? | What was available vs. not? |
# MAGIC |-----------|-------------|--------------------------------------|---------------------------|
# MAGIC | 1 | Serverless | | |
# MAGIC | 2 | Serverless (multi-task) | | |
# MAGIC | 3 | Serverless | | |
# MAGIC | 4 | Classic Compute | | |
# MAGIC
# MAGIC **Discussion questions:**
# MAGIC - Which debugging surfaces were most useful for each challenge?
# MAGIC - What information was **not available** due to the compute type?
# MAGIC - How would your debugging approach differ if a serverless job ran on classic compute, or vice versa?
# MAGIC
# MAGIC ---
# MAGIC **End of Day 1 — Block 3**: Observability hands-on complete. 🎉

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_03d_fix_failing_jobs")
