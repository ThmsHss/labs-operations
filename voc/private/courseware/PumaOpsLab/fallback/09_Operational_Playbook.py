# Databricks notebook source
# MAGIC %md
# MAGIC # 📖 Block 9: Operational Playbook
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC This notebook is your **reference guide** — a summary of everything covered in the workshop.
# MAGIC Keep this handy for day-to-day operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Where to Look: Quick Reference
# MAGIC
# MAGIC | Scenario | First Look | Deep Dive | Proactive Alert |
# MAGIC |----------|-----------|-----------|----------------|
# MAGIC | **Job failed** | Jobs UI → Run output | Spark UI / Query Profile, `system.lakeflow.*` | SQL Alert on `job_run_timeline` |
# MAGIC | **Slow query** | Query History UI | Query Profile, `system.query.history` | SQL Alert on duration threshold |
# MAGIC | **Data quality issue** | Lakehouse Monitoring dashboard | Profile/drift metrics tables, SDP expectations | SQL Alert on metric thresholds |
# MAGIC | **Pipeline failed** | Pipeline UI → DAG | `event_log()` TVF → ERROR events | SQL Alert on event log |
# MAGIC | **Cost spike** | Billing dashboard | `system.billing.usage` + `system.lakeflow.jobs` | SQL Alert on daily spend |
# MAGIC | **Security incident** | Audit log | `system.access.audit` (failed access) | SQL Alert on denied access |
# MAGIC | **Who changed what?** | Lineage UI | `system.access.table_lineage`, `column_lineage` | — |
# MAGIC | **Capacity issues** | Warehouse monitoring tab | `system.query.history` (queue waits) | SQL Alert on wait times |
# MAGIC | **Auto Loader stuck/failing** | Pipeline UI → backlog | `event_log()` + `cloud_files_state()` | SQL Alert on backlog size |
# MAGIC | **Ad-hoc questions** | Genie on System Tables | Natural language → SQL | — |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 System Tables Cheat Sheet
# MAGIC
# MAGIC | Table | Best Queries |
# MAGIC |-------|-------------|
# MAGIC | `system.lakeflow.jobs` | Job configs, owners, tags (SCD2 — use latest version) |
# MAGIC | `system.lakeflow.job_run_timeline` | Run status, duration, compute type, failure codes |
# MAGIC | `system.lakeflow.job_task_run_timeline` | Task-level breakdown within a job run |
# MAGIC | `system.billing.usage` | DBU consumption by compute type, job, user |
# MAGIC | `system.billing.list_prices` | Price per DBU per SKU (for dollar calculations) |
# MAGIC | `system.query.history` | Every SQL query: duration breakdown, rows, bytes, user |
# MAGIC | `system.access.audit` | Who did what, when, from where (security) |
# MAGIC | `system.access.table_lineage` | Data flow: which tables feed which |
# MAGIC | `system.access.column_lineage` | Column-level data flow |
# MAGIC | `system.compute.clusters` | Cluster configs, lifecycle events |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 Classic vs. Serverless vs. SQL Warehouse
# MAGIC
# MAGIC | Capability | Classic Compute | Serverless Jobs | SQL Warehouse |
# MAGIC |-----------|----------------|----------------|---------------|
# MAGIC | **Spark UI** | ✅ Full | ✅ Available | ❌ |
# MAGIC | **Query Profile** | ❌ | ❌ | ✅ Full |
# MAGIC | **Driver/Executor Logs** | ✅ Log Delivery to Volumes | ❌ REPL logs only | ❌ |
# MAGIC | **Cluster Event Log** | ✅ Start/stop, resize, spot loss | ❌ | ❌ |
# MAGIC | **Compute Metrics** | ✅ CPU, memory, disk, network | ❌ | ❌ |
# MAGIC | **system.lakeflow** | ✅ | ✅ | ✅ (SQL tasks) |
# MAGIC | **system.query.history** | ⚠️ SQL only | ⚠️ SQL only | ✅ All queries |
# MAGIC | **system.billing** | ✅ | ✅ | ✅ |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔧 SDP / Pipeline Debugging
# MAGIC
# MAGIC | What to Check | How |
# MAGIC |--------------|-----|
# MAGIC | Pipeline status | Pipeline UI → latest update |
# MAGIC | Error details | `event_log('<id>') WHERE level = 'ERROR'` |
# MAGIC | Row counts | `event_log() WHERE event_type = 'flow_progress'` → `num_output_rows` |
# MAGIC | DQ violations | `event_log()` → `flow_progress.data_quality.expectations` |
# MAGIC | Update lifecycle | `event_log() WHERE event_type = 'update_progress'` |
# MAGIC | Published event log | Pipeline Settings → Advanced → Event log catalog/schema |
# MAGIC | Pipeline as job | `system.lakeflow.job_run_timeline` (pipelines run as jobs) |
# MAGIC | Auto Loader backlog | Event log `flow_progress` → `numFilesOutstanding`, `numBytesOutstanding` |
# MAGIC | Auto Loader file state | `SELECT * FROM cloud_files_state('<checkpoint_path>')` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📝 Log Types Quick Reference (Classic Compute)
# MAGIC
# MAGIC | Log File | Contents | When to Check |
# MAGIC |----------|----------|--------------|
# MAGIC | `driver/stdout` | `print()` output, general program output | Quick debugging, progress messages |
# MAGIC | `driver/stderr` | Python `logging` output, exceptions, stack traces | **Runtime errors**, structured log messages |
# MAGIC | `driver/log4j-active.log` | Spark JVM internals: query plans, shuffle, GC, broadcast | **Spark internals**, performance warnings |
# MAGIC | `executor/<id>/stderr` | Executor-side errors, OOM stack traces | Executor crashes, task failures |
# MAGIC | `eventlog/` | Cluster lifecycle: start, stop, resize, spot loss | Cluster instability, autoscaling |
# MAGIC | `init_scripts/` | Init script stdout/stderr per node | Init script failures |
# MAGIC
# MAGIC > **Best practice**: Use Python `logging` module (not `print()`) — gives you levels, timestamps, and module names.
# MAGIC > Configure log4j to use `JsonTemplateLayout` via init script for machine-parseable Spark logs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 Performance Tuning Checklist
# MAGIC
# MAGIC ```
# MAGIC 1. DESCRIBE DETAIL <table>        → Check file count, clustering
# MAGIC 2. DESCRIBE HISTORY <table>        → Check for OPTIMIZE runs
# MAGIC 3. system.query.history            → read_rows vs rows_produced ratio
# MAGIC 4. Query Profile (SQL Warehouse)   → Full scan? Missing filter pushdown?
# MAGIC 5. Spark UI (Classic)              → Spill? Skewed stages?
# MAGIC 6. OPTIMIZE <table>                → Compact files
# MAGIC 7. ALTER TABLE CLUSTER BY (cols)   → Add Liquid Clustering
# MAGIC 8. VACUUM <table>                  → Remove old files
# MAGIC 9. Check data skew                 → GROUP BY key column, look for imbalance
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨 SQL Alerts Patterns
# MAGIC
# MAGIC | Alert | Query | Condition |
# MAGIC |-------|-------|-----------|
# MAGIC | Job failures | `SELECT COUNT(*) FROM system.lakeflow.job_run_timeline WHERE result_state='FAILED' AND period.startTime >= now() - 1h` | count > 0 |
# MAGIC | Data quality | `SELECT MAX(null_percentage) FROM <table>_profile_metrics WHERE column_name='...'` | pct > threshold |
# MAGIC | Warehouse queue | `SELECT AVG(waiting_at_capacity_duration_ms) FROM system.query.history WHERE start_time >= now() - 30m` | avg > 30000 |
# MAGIC | Cost spike | `SELECT SUM(usage_quantity) FROM system.billing.usage WHERE usage_date = current_date()` | dbus > daily_budget |
# MAGIC | Unauthorized access | `SELECT COUNT(*) FROM system.access.audit WHERE response.status_code != '200' AND event_date = current_date()` | count > 0 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Monitoring Dashboards
# MAGIC
# MAGIC | Dashboard | Source | Key Metrics |
# MAGIC |-----------|--------|------------|
# MAGIC | **Lakeflow Jobs & Pipelines** | Pre-built (GitHub) | Runs, failures, duration, cost |
# MAGIC | **Billing & Cost** | Pre-built / custom | DBU by SKU, daily trend, top consumers |
# MAGIC | **DBSQL Query Performance** | Custom | p50/p95 latency, queue waits, failure rate |
# MAGIC | **Lakehouse Monitoring** | Auto-generated | Null rates, distributions, drift |
# MAGIC | **Audit & Access** | Custom | Access patterns, denied requests, lineage |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧞 Genie for Operations
# MAGIC
# MAGIC Connect system tables to a **Genie Space** for natural language debugging:
# MAGIC
# MAGIC | Question | System Table Used |
# MAGIC |----------|------------------|
# MAGIC | "Which jobs failed today?" | `system.lakeflow.job_run_timeline` |
# MAGIC | "What's our DBU spend this week?" | `system.billing.usage` |
# MAGIC | "Who ran the slowest query yesterday?" | `system.query.history` |
# MAGIC | "What feeds the gold table?" | `system.access.table_lineage` |
# MAGIC | "Any permission errors recently?" | `system.access.audit` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📐 Useful Queries — Copy & Paste
# MAGIC
# MAGIC These are the most useful queries from the workshop. Copy them to your workspace.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 most expensive jobs (last 30 days)
# MAGIC SELECT
# MAGIC     u.usage_metadata.job_id,
# MAGIC     j.name AS job_name,
# MAGIC     SUM(u.usage_quantity) AS total_dbus
# MAGIC FROM system.billing.usage u
# MAGIC LEFT JOIN (
# MAGIC     SELECT job_id, name, ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) AS rn
# MAGIC     FROM system.lakeflow.jobs
# MAGIC ) j ON u.usage_metadata.job_id = j.job_id AND j.rn = 1
# MAGIC WHERE u.usage_date >= current_date() - 30
# MAGIC   AND u.usage_metadata.job_id IS NOT NULL
# MAGIC GROUP BY u.usage_metadata.job_id, j.name
# MAGIC ORDER BY total_dbus DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query performance trends (p50, p95 by day)
# MAGIC SELECT
# MAGIC     DATE(start_time) AS query_date,
# MAGIC     COUNT(*) AS total_queries,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.50) AS p50_ms,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.95) AS p95_ms,
# MAGIC     AVG(waiting_at_capacity_duration_ms) AS avg_queue_ms,
# MAGIC     SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_date() - 14
# MAGIC GROUP BY DATE(start_time)
# MAGIC ORDER BY query_date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full upstream lineage (recursive)
# MAGIC WITH RECURSIVE lineage AS (
# MAGIC     SELECT source_table_full_name, target_table_full_name, 1 AS depth
# MAGIC     FROM system.access.table_lineage
# MAGIC     WHERE target_table_full_name = 'puma_ops_lab.workshop.gold_order_summary'
# MAGIC     UNION ALL
# MAGIC     SELECT t.source_table_full_name, t.target_table_full_name, l.depth + 1
# MAGIC     FROM system.access.table_lineage t
# MAGIC     JOIN lineage l ON t.target_table_full_name = l.source_table_full_name
# MAGIC     WHERE l.depth < 5
# MAGIC )
# MAGIC SELECT DISTINCT source_table_full_name, target_table_full_name, depth
# MAGIC FROM lineage
# MAGIC ORDER BY depth, source_table_full_name

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🎓 Workshop Complete!
# MAGIC
# MAGIC ### What you learned:
# MAGIC 1. ✅ **System Tables** — Query operational data across the entire account
# MAGIC 2. ✅ **Pre-built Dashboards** — Import and customize monitoring dashboards
# MAGIC 3. ✅ **Genie on System Tables** — Natural language debugging
# MAGIC 4. ✅ **Job Tracing** — End-to-end debugging flow for classic and serverless
# MAGIC 5. ✅ **SDP/Pipeline Debugging** — Event Log, expectations, error diagnosis
# MAGIC 6. ✅ **SQL Warehouse Performance** — Query History, Query Profile, queue analysis
# MAGIC 7. ✅ **Lakehouse Monitoring** — Automated data quality profiling and drift detection
# MAGIC 8. ✅ **SQL Alerts** — Proactive operational monitoring
# MAGIC 9. ✅ **Performance Tuning** — OPTIMIZE, clustering, VACUUM, skew detection
# MAGIC
# MAGIC ### Resources:
# MAGIC - [System Tables Documentation](https://docs.databricks.com/en/admin/system-tables/index.html)
# MAGIC - [Lakehouse Monitoring](https://docs.databricks.com/en/lakehouse-monitoring/index.html)
# MAGIC - [Query History](https://docs.databricks.com/en/sql/user/queries/query-history.html)
# MAGIC - [SDP Event Log](https://docs.databricks.com/aws/en/dlt/monitor-event-logs)
# MAGIC - [SQL Alerts](https://docs.databricks.com/en/sql/user/alerts/index.html)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("09_Operational_Playbook")
