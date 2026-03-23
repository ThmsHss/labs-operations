# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1 — 03c: Debugging Surfaces — Where to Look When Jobs Fail
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC **What this notebook is**: A guided **read-through walkthrough**. You will explore the Jobs UI, learn
# MAGIC what debugging surfaces exist, and understand what is available on each compute type. **You do not fix
# MAGIC anything here** — the next notebook (`03d`) is where you roll up your sleeves and fix 4 broken jobs.
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Navigate the **Jobs & Pipelines** UI to find failures and read errors
# MAGIC 2. Understand what debugging surfaces are available for each compute type
# MAGIC 3. Trace a real failed job run end-to-end (your Challenge 1 job)
# MAGIC 4. Learn where **Spark UI**, **Query Profile**, **Logs**, and **Cluster Metrics** fit in
# MAGIC 5. Use **system tables** to find failures programmatically
# MAGIC
# MAGIC > Along the way you will discover what you **can** and **cannot** see depending on the compute type
# MAGIC > (serverless vs. classic vs. SQL warehouse) — a critical distinction for day-to-day operations.

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

JOB_PREFIX = user_prefix
print(f"Your job name prefix: {JOB_PREFIX}")
print(f"Use this prefix to find your jobs in the Jobs & Pipelines sidebar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. Finding Your Failed Jobs
# MAGIC
# MAGIC ### Jobs vs. Pipelines
# MAGIC
# MAGIC The sidebar item **Jobs & Pipelines** shows both:
# MAGIC - **Jobs** orchestrate one or more tasks (notebooks, Python scripts, SQL, dbt, JARs) with DAG dependencies, schedules, retries, and parameterization.
# MAGIC - **Pipelines** (Spark Declarative Pipelines / DLT) are declarative data-flow definitions that manage streaming tables and materialized views with built-in quality expectations and their own event log.
# MAGIC
# MAGIC In this workshop we focus on **Jobs**. You can filter the list by clicking the **Jobs** pill at the top.
# MAGIC
# MAGIC ### Step-by-step: find your Challenge 1 job
# MAGIC
# MAGIC 1. Click **Jobs & Pipelines** in the left sidebar.
# MAGIC 2. In the search box, paste your prefix from the cell above (e.g. `labuser14285657_1773931682`) and add `challenge_01`.
# MAGIC 3. Click the job name to open its detail page.
# MAGIC 4. The **Runs** tab shows active and completed runs.
# MAGIC
# MAGIC > **Tip**: Your prefix comes from the LAB_CONFIG output above. You can also find it in the
# MAGIC > top-right corner of the workspace header (your user name) or in the first cell output of any lab notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ### The Runs Tab — Matrix View and List View
# MAGIC
# MAGIC The **Runs** tab has two parts:
# MAGIC
# MAGIC **Matrix view** (top):
# MAGIC - The **Run total duration** row shows a bar for each run. The **height** of the bar indicates duration; the **color** indicates status.
# MAGIC - Below it, the **Tasks** row shows a cell for each task in that run, also color-coded.
# MAGIC - Color coding: **green** = succeeded, **red** = failed, **pink** = skipped, **yellow** = waiting for retry, **grey** = pending / canceled / timed out.
# MAGIC - **Hover** any bar or cell to see metadata (start time, duration, status, cluster details). **Click** to drill into the run or task detail.
# MAGIC
# MAGIC **List view** (below the matrix):
# MAGIC - A table with columns: Start time, Run ID, Launched (manually / schedule / API), Duration, Status, Error code, Run parameters.
# MAGIC - Click the **Start time** link to open the run detail page.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the Error
# MAGIC
# MAGIC Once you open a failed run:
# MAGIC 1. Click the **failed task** (red cell in the matrix, or the task name in the run detail).
# MAGIC 2. The **Output** section shows the notebook cell output and the full error / stack trace.
# MAGIC 3. At the top of the Output section, a **colored error banner** displays the error class and a suggestion
# MAGIC    (e.g. `[UNRESOLVED_COLUMN.WITH_SUGGESTION] ... Did you mean one of the following?`).
# MAGIC 4. You can click **Diagnose error** to have Genie Code explain the error in plain language.
# MAGIC
# MAGIC On the **right-side panel** (Task run details) you will see:
# MAGIC - **Details**: Job ID, Run ID, Task run ID, status, start/end time, duration.
# MAGIC - **Compute**: Which compute ran this task (Serverless, Job Cluster, SQL Warehouse).
# MAGIC - **Query history** link (for serverless and warehouse tasks) — click to see every SQL statement with timing.
# MAGIC - **Logs** link — click to see available log output.
# MAGIC - **Notebook** link — click to open the source notebook.
# MAGIC
# MAGIC > **Important**: What you see on the right panel depends on the compute type.
# MAGIC > For serverless tasks you get **Query history** and **Logs**. For classic compute tasks you also
# MAGIC > get a link to the **Spark UI**. For SQL warehouse tasks you get **Query history** only.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 2. Debugging Surfaces by Compute Type
# MAGIC
# MAGIC This is the most important table in this notebook. Understanding **what is available where** saves
# MAGIC you from looking for things that don't exist on a given compute type.
# MAGIC
# MAGIC | Capability | Classic Compute | Serverless | SQL Warehouse |
# MAGIC |-----------|----------------|------------|---------------|
# MAGIC | **Task output** (cell output, errors) | Yes | Yes | Yes |
# MAGIC | **Query History** | Yes | Yes | Yes |
# MAGIC | **Internals and Performance Deep Dive** | Spark UI | Query Insights | Query Insights |
# MAGIC | | | | |
# MAGIC | **Spark UI** | Yes (full) | No | No |
# MAGIC | **Application logs** (Python print/logging → stdout/stderr) | Yes (Logs tab + exportable) | Yes (task output only) | No |
# MAGIC | **Spark driver/executor logs** (JVM log4j) | Yes (all levels) | No (JAR tasks: ERROR only) | No |
# MAGIC | **Cluster Log Delivery** (auto-export to Volume) | Yes | No | No |
# MAGIC | **Cluster Event Log** (start/stop, spot loss, autoscaling) | Yes | No | No |
# MAGIC
# MAGIC **Key takeaways:**
# MAGIC - **Spark UI** is only on classic compute. For serverless and SQL warehouses, use **Query Insights** (accessible from Query History).
# MAGIC - **Logs**: On serverless, you get **application-level** stdout/stderr (Python `print()`/`logging` output) in the task output. You do **not** get Spark driver/executor logs (JVM log4j). **Cluster Log Delivery** (automatic export to a Volume) is classic compute only.
# MAGIC - **Cluster event log** is classic compute only — serverless manages infrastructure for you.
# MAGIC
# MAGIC > **The trend**: Serverless compute deliberately abstracts away low-level infrastructure details (Spark stages, executor
# MAGIC > memory, JVM logs, cluster events). This means **fewer debugging surfaces**, but also **fewer things that can go wrong**
# MAGIC > at the infrastructure level. Most serverless failures are logic errors (wrong column name, bad SQL) that show up
# MAGIC > directly in the task output — no need to dig through Spark UI stages or driver logs. The sections below on Spark UI,
# MAGIC > cluster logs, and compute metrics are important context for **classic compute workloads** you may still encounter,
# MAGIC > but for new workloads on serverless, the error banner + query history + system tables cover the vast majority of cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Exercise: Trace Challenge 1 (Serverless Job)
# MAGIC
# MAGIC Your job `{prefix}_challenge_01_schema` ran on **serverless** and **failed**.
# MAGIC Using the steps from section 1 above, find the failed run, read the error, and check Query History.
# MAGIC Then confirm via system tables by running the query below.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your job runs and their status
# MAGIC
# MAGIC > **System table latency**: Data in `system.lakeflow.*` is **not real-time** — it can take
# MAGIC > up to ~2 hours for new runs to appear. If you see no results, your jobs may have completed
# MAGIC > too recently. Check the **Jobs UI** directly (Step 1 above) while waiting for the system tables to catch up.

# COMMAND ----------

display(spark.sql(f"""
SELECT
    jrt.job_id,
    j.name AS job_name,
    jrt.run_id,
    jrt.result_state,
    jrt.termination_code,
    jrt.period_start_time AS started_at,
    jrt.period_end_time AS ended_at,
    TIMESTAMPDIFF(SECOND, jrt.period_start_time, jrt.period_end_time) AS duration_sec
FROM system.lakeflow.job_run_timeline jrt
LEFT JOIN (
    SELECT job_id, name, ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) AS rn
    FROM system.lakeflow.jobs
) j ON jrt.job_id = j.job_id AND j.rn = 1
WHERE jrt.period_start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND jrt.result_state IS NOT NULL
  AND j.name LIKE '{JOB_PREFIX}%'
ORDER BY jrt.period_start_time DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Spark UI — Classic Compute Only
# MAGIC
# MAGIC > As serverless adoption grows, the Spark UI becomes less central to daily debugging. Still, for workloads
# MAGIC > running on classic compute (legacy jobs, GPU clusters, custom init scripts), it remains the most detailed view.
# MAGIC
# MAGIC The **Spark UI** is available **only for Classic Compute**. You access it from the Cluster page
# MAGIC (**Compute → your cluster → Spark UI** tab) or from a classic-compute task run detail.
# MAGIC
# MAGIC In the next notebook, **Challenge 4** has a code bug — a `crossJoin` that should be an `inner join` —
# MAGIC causing a cartesian product. On a small cluster this either **runs forever** or **crashes with OOM**.
# MAGIC
# MAGIC Here's the catch: **the Spark UI won't always help.** On a single-node cluster with certain join
# MAGIC patterns, stages show no tasks, and the SQL tab may not reveal much. This is realistic —
# MAGIC sometimes the best debugging tool is **reading the actual code** and thinking about what the
# MAGIC join is doing mathematically (500K × 50K = 25 billion rows).
# MAGIC
# MAGIC **General Spark UI patterns to know for production debugging:**
# MAGIC - **SQL tab → Physical plan**: `BroadcastNestedLoopJoin` = cartesian product. Healthy joins show `SortMergeJoin` or `BroadcastHashJoin`.
# MAGIC - **Stages tab → Spill (Disk)**: Memory pressure — the executor ran out of memory.
# MAGIC - **Stages tab → Task duration skew**: One task takes much longer than others → data skew.
# MAGIC - **Executors tab → Failed tasks**: OOM or spot preemption on a specific executor.
# MAGIC
# MAGIC > **Reference**: [Debugging with the Spark UI](https://docs.databricks.com/aws/en/compute/troubleshooting/debugging-spark-ui)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. Logs — What's Available on Each Compute Type
# MAGIC
# MAGIC ### Classic Compute — full log access
# MAGIC
# MAGIC On classic compute the driver node produces three log streams:
# MAGIC
# MAGIC | Log | What Goes There | When to Look |
# MAGIC |-----|----------------|-------------|
# MAGIC | **`stdout`** | `print()` output | Quick debugging, progress messages |
# MAGIC | **`stderr`** | Exceptions, stack traces, Python `logging` output | Runtime errors, detailed tracebacks |
# MAGIC | **`log4j`** | Spark internal JVM logs (query plans, shuffle, GC) | Spark internals, performance warnings |
# MAGIC
# MAGIC All three are visible in the **Logs tab** and can be exported via **Cluster Log Delivery**.
# MAGIC
# MAGIC ### Serverless — application logs only
# MAGIC
# MAGIC On serverless you get **application-level output only**:
# MAGIC - **stdout/stderr**: Your Python `print()` and `logging` output appears in the task output.
# MAGIC - **Spark driver/executor logs (log4j)**: **Not available**. The JVM-level logs (query plans, shuffle details, GC) are abstracted away.
# MAGIC - For **JAR tasks** on serverless: only `log4j` ERROR level is captured in task output. WARN/INFO/DEBUG are not visible, and the threshold cannot be changed.
# MAGIC
# MAGIC | | Classic Compute | Serverless |
# MAGIC |-|----------------|------------|
# MAGIC | **View in UI** | Logs tab — full (stdout, stderr, log4j) | Task output — application stdout/stderr only |
# MAGIC | **Cluster Log Delivery** (auto-export to Volume) | ✅ Configurable | ❌ Not available |
# MAGIC | **Persist logs yourself** (Python logging to Delta/Volume) | ✅ | ✅ |
# MAGIC
# MAGIC ### Python Logging on Serverless
# MAGIC
# MAGIC Since serverless doesn't give you Spark driver logs or Cluster Log Delivery, Python's `logging`
# MAGIC module is your primary tool for application-level observability. By default only `WARNING` and
# MAGIC above appear — call `basicConfig()` early to get `INFO`-level output.

# COMMAND ----------

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,
)
logger = logging.getLogger("my_etl")

logger.info("Pipeline started — this appears because we set level=INFO")
logger.warning("Schema drift detected — this always appears (WARNING+)")
logger.debug("Partition details — this will NOT appear unless level=DEBUG")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persisting Logs to a Delta Table
# MAGIC
# MAGIC For durable, queryable logs on serverless, write a custom logging handler that buffers
# MAGIC records and flushes them to a Delta table. This gives you the same queryability as
# MAGIC Cluster Log Delivery — but it works on **any compute type**.

# COMMAND ----------

import logging
from datetime import datetime
from pyspark.sql import Row

class DeltaTableHandler(logging.Handler):
    """Buffers log records and writes them to a Delta table."""

    def __init__(self, spark, table_name, buffer_size=100):
        super().__init__()
        self.spark = spark
        self.table_name = table_name
        self.buffer = []
        self.buffer_size = buffer_size

    def emit(self, record):
        try:
            run_id = self.spark.conf.get("spark.databricks.job.runId")
        except Exception:
            run_id = "interactive"
        self.buffer.append(Row(
            timestamp=datetime.utcnow().isoformat(),
            level=record.levelname,
            logger=record.name,
            message=self.format(record),
            job_run_id=run_id,
        ))
        if len(self.buffer) >= self.buffer_size:
            self.flush()

    def flush(self):
        if self.buffer:
            df = self.spark.createDataFrame(self.buffer)
            df.write.mode("append").saveAsTable(self.table_name)
            self.buffer.clear()

# COMMAND ----------

# MAGIC %md
# MAGIC Try it out — the logs land in a Delta table you can query with SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS puma_ops_lab.workshop.application_logs (
# MAGIC   timestamp STRING,
# MAGIC   level STRING,
# MAGIC   logger STRING,
# MAGIC   message STRING,
# MAGIC   job_run_id STRING
# MAGIC )

# COMMAND ----------

handler = DeltaTableHandler(spark, "puma_ops_lab.workshop.application_logs")
handler.setFormatter(logging.Formatter("%(message)s"))
logger = logging.getLogger("my_etl")
logger.addHandler(handler)
logger.setLevel(logging.INFO)

logger.info("Pipeline started")
logger.info("Reading source table")
logger.warning("Detected 3 null rows in customer_id")
logger.info("Pipeline finished")
handler.flush()

print("✅ Logs written to puma_ops_lab.workshop.application_logs")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM puma_ops_lab.workshop.application_logs ORDER BY timestamp DESC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cluster Log Delivery — Classic Compute Only
# MAGIC
# MAGIC On **classic compute**, you can additionally configure **Cluster Log Delivery** to automatically
# MAGIC export driver logs to a Unity Catalog Volume. Logs are delivered every **5 minutes** and
# MAGIC archived hourly. After the cluster terminates, final logs are flushed within a few minutes.
# MAGIC
# MAGIC When configured, logs appear at a path like:
# MAGIC `/Volumes/puma_ops_lab/workshop/raw_data/cluster_logs/`
# MAGIC
# MAGIC > **Note**: This is **classic compute only**. On serverless, use the Python logging approach above
# MAGIC > to persist logs to Delta tables or Unity Catalog Volumes.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hands-on: Query Cluster Logs from the Volume

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Browse all log files delivered to the volume
# MAGIC SELECT
# MAGIC   _metadata.file_path AS log_file,
# MAGIC   _metadata.file_size AS size_bytes,
# MAGIC   _metadata.file_modification_time AS modified_at
# MAGIC FROM read_files(
# MAGIC   '/Volumes/puma_ops_lab/workshop/raw_data/cluster_logs/',
# MAGIC   format => 'text',
# MAGIC   recursiveFileLookup => true
# MAGIC )
# MAGIC GROUP BY ALL
# MAGIC ORDER BY modified_at DESC
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Search for OOM errors and exceptions across all log files
# MAGIC SELECT
# MAGIC   _metadata.file_path AS log_file,
# MAGIC   value AS log_line
# MAGIC FROM read_files(
# MAGIC   '/Volumes/puma_ops_lab/workshop/raw_data/cluster_logs/',
# MAGIC   format => 'text',
# MAGIC   recursiveFileLookup => true
# MAGIC )
# MAGIC WHERE value LIKE '%OutOfMemory%'
# MAGIC    OR value LIKE '%java.lang.Exception%'
# MAGIC    OR value LIKE '%FATAL%'
# MAGIC    OR value LIKE '%spill%'
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read driver stderr (where Python exceptions and stack traces go)
# MAGIC SELECT value AS log_line
# MAGIC FROM read_files(
# MAGIC   '/Volumes/puma_ops_lab/workshop/raw_data/cluster_logs/',
# MAGIC   format => 'text',
# MAGIC   recursiveFileLookup => true
# MAGIC )
# MAGIC WHERE _metadata.file_path LIKE '%driver/stderr'
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC > **Tip**: In production, you would **not** query raw log files directly. Instead, build a
# MAGIC > separate pipeline (e.g. Auto Loader or a Lakeflow Declarative Pipeline) to clean, parse, and
# MAGIC > centralize these logs into dedicated Delta tables — one for stdout, one for stderr, one for log4j.
# MAGIC > From there, build dashboards and alerts for centralized log search across all your clusters.
# MAGIC > Here we just use `read_files()` to show you what raw data is available in the Volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Cluster Event Log and Compute Metrics — Classic Compute Only
# MAGIC
# MAGIC > Again, serverless eliminates these concerns entirely — no clusters to manage means no cluster events to monitor.
# MAGIC
# MAGIC Two more surfaces that are exclusive to **classic compute** (not available on serverless or SQL warehouses):
# MAGIC
# MAGIC ### Cluster Event Log
# MAGIC Found in **Compute → your cluster → Event Log** tab. Records lifecycle events:
# MAGIC `CREATING`, `RUNNING`, `TERMINATING`, `AUTOSCALING`, `NODES_LOST`, `DID_NOT_GET_INSTANCES`, `INIT_SCRIPTS_FINISHED`.
# MAGIC
# MAGIC Check it when: cluster instability, autoscaling behavior, spot preemption, init script failures.
# MAGIC
# MAGIC ### Compute Metrics
# MAGIC Found in **Compute → your cluster → Metrics** tab. Shows CPU, memory, swap, disk I/O, network per node.
# MAGIC
# MAGIC Check it when: OOM investigation (high memory + swap), slow jobs (CPU at 100%), autoscaling verification.
# MAGIC
# MAGIC > **Tip**: Combine Compute Metrics (hardware-level) with the Spark UI Executors tab (Spark-level)
# MAGIC > for a complete picture of what happened on a classic cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Repair Run
# MAGIC
# MAGIC When a **multi-task job** fails, you can use **Repair Run** to re-run only the failed tasks
# MAGIC and their downstream dependents — skipping tasks that already succeeded.
# MAGIC
# MAGIC 1. From the failed run page, click **Repair run**.
# MAGIC 2. The dialog shows which tasks will be re-run (failed + dependents) and which are skipped (already succeeded).
# MAGIC 3. You can edit parameters or task settings before clicking **Repair run**.
# MAGIC 4. After repair, the matrix view adds a new column for the repair attempt.
# MAGIC
# MAGIC > **Note**: Repair is most useful for multi-task DAG jobs. For single-task jobs like the challenge jobs,
# MAGIC > simply re-running the job achieves the same result.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Query History — Works Across All Compute Types
# MAGIC
# MAGIC For any SQL or Python code executed (on warehouses, serverless, or classic compute), the
# MAGIC **Query History** captures detailed metrics. Access it from the sidebar or from a task's right panel.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     statement_id,
# MAGIC     executed_by,
# MAGIC     execution_status,
# MAGIC     total_duration_ms,
# MAGIC     execution_duration_ms,
# MAGIC     compilation_duration_ms,
# MAGIC     waiting_at_capacity_duration_ms,
# MAGIC     waiting_for_compute_duration_ms,
# MAGIC     produced_rows,
# MAGIC     read_bytes,
# MAGIC     SUBSTRING(statement_text, 1, 100) AS query_snippet
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 2 HOURS
# MAGIC ORDER BY total_duration_ms DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC Now that you know **where to look**, it's time to **fix things**.
# MAGIC
# MAGIC **Next**: [day_1_03d_fix_failing_jobs — Your turn! Fix 4 intentionally broken jobs]($./day_1_03d_fix_failing_jobs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_03c_debugging_surfaces")
