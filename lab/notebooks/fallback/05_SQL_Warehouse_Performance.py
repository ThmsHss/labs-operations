# Databricks notebook source
# MAGIC %md
# MAGIC # ⚡ Block 5: SQL Warehouse — Query History, Query Profile & Performance
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Navigate Query History in the UI
# MAGIC 2. Read a Query Profile to identify bottlenecks
# MAGIC 3. Analyze warehouse performance via `system.query.history`
# MAGIC 4. Monitor warehouse scaling and queue times
# MAGIC

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Query History UI
# MAGIC
# MAGIC Navigate to: **SQL → Query History** (sidebar)
# MAGIC
# MAGIC ### What you see:
# MAGIC - Every SQL statement executed on any warehouse
# MAGIC - Filters: user, warehouse, status, time range
# MAGIC - Per-query: duration, rows, status, statement text
# MAGIC
# MAGIC ### Try this:
# MAGIC 1. Filter to the last 2 hours
# MAGIC 2. Sort by **Duration** (descending) to find slow queries
# MAGIC 3. Click on a query to see:
# MAGIC    - **Duration breakdown**: compilation, execution, result fetch
# MAGIC    - **I/O metrics**: rows read, rows produced, bytes scanned
# MAGIC    - **Query Profile** link

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Generate Some Queries to Analyze
# MAGIC
# MAGIC Let's run a mix of fast and slow queries so we have data to analyze.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fast query: simple aggregation
# MAGIC SELECT channel, COUNT(*) AS order_count, SUM(total_amount) AS revenue
# MAGIC FROM puma_ops_lab.workshop.orders
# MAGIC GROUP BY channel
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Medium query: join + group by
# MAGIC SELECT
# MAGIC     c.loyalty_tier,
# MAGIC     c.region,
# MAGIC     COUNT(DISTINCT o.order_id) AS orders,
# MAGIC     SUM(o.total_amount) AS total_revenue,
# MAGIC     AVG(o.total_amount) AS avg_order_value
# MAGIC FROM puma_ops_lab.workshop.orders o
# MAGIC JOIN puma_ops_lab.workshop.customers c ON o.customer_id = c.customer_id
# MAGIC WHERE o.order_timestamp >= '2024-06-01'
# MAGIC GROUP BY c.loyalty_tier, c.region
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Slow query: un-optimized full scan of the gold table with complex aggregation
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC     customer_region,
# MAGIC     DATE_TRUNC('month', order_timestamp) AS month,
# MAGIC     COUNT(*) AS orders,
# MAGIC     SUM(total_amount) AS revenue,
# MAGIC     SUM(profit) AS profit,
# MAGIC     AVG(unit_price) AS avg_price,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM puma_ops_lab.workshop.gold_order_summary
# MAGIC GROUP BY product_category, customer_region, DATE_TRUNC('month', order_timestamp)
# MAGIC ORDER BY revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Reading a Query Profile
# MAGIC
# MAGIC After running the queries above:
# MAGIC 1. Go to **SQL → Query History**
# MAGIC 2. Find the slowest query
# MAGIC 3. Click it → **Query Profile**
# MAGIC
# MAGIC ### What to look for in the Query Profile:
# MAGIC
# MAGIC | Operator | What It Means | Concern When |
# MAGIC |----------|--------------|-------------|
# MAGIC | **Scan** | Reading data from table | Large `rows_read` vs. `rows_produced` = missing filters |
# MAGIC | **Filter** | Predicate pushdown | Should appear close to scan, not after shuffle |
# MAGIC | **HashAggregate** | GROUP BY processing | High memory if many groups |
# MAGIC | **SortMergeJoin / BroadcastJoin** | Join strategy | SMJ = expensive for large tables; BHJ = efficient for small dim tables |
# MAGIC | **Exchange (Shuffle)** | Data redistribution | High shuffle = potential bottleneck |
# MAGIC | **Spill** | Writing temp data to disk | Memory pressure — increase warehouse size |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Programmatic Query Analysis
# MAGIC
# MAGIC Use `system.query.history` for programmatic analysis.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Duration breakdown for our recent queries
# MAGIC SELECT
# MAGIC     statement_id,
# MAGIC     total_duration_ms,
# MAGIC     execution_duration_ms,
# MAGIC     compilation_duration_ms,
# MAGIC     waiting_at_capacity_duration_ms,
# MAGIC     waiting_for_compute_duration_ms,
# MAGIC     result_fetch_duration_ms,
# MAGIC     rows_produced,
# MAGIC     read_bytes,
# MAGIC     SUBSTRING(statement_text, 1, 80) AS query_snippet
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 30 MINUTES
# MAGIC   AND executed_by = current_user()
# MAGIC ORDER BY total_duration_ms DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the Duration Breakdown
# MAGIC
# MAGIC ```
# MAGIC total_duration_ms = compilation + waiting_for_compute + waiting_at_capacity + execution + result_fetch
# MAGIC
# MAGIC ┌──────────────────────────────────────────────────────────────────┐
# MAGIC │ compilation │ wait_compute │ wait_capacity │ execution │ fetch │
# MAGIC └──────────────────────────────────────────────────────────────────┘
# MAGIC
# MAGIC compilation          = Query planning and optimization
# MAGIC waiting_for_compute  = Waiting for warehouse to start (cold start)
# MAGIC waiting_at_capacity  = Queued because all clusters are busy
# MAGIC execution            = Actual data processing
# MAGIC result_fetch         = Returning results to client
# MAGIC ```
# MAGIC
# MAGIC - High `waiting_at_capacity` → **Scale up the warehouse** (add more clusters)
# MAGIC - High `waiting_for_compute` → **Warehouse cold start** (consider always-on or pre-warming)
# MAGIC - High `execution` → **Optimize the query** or table (OPTIMIZE, better filters)
# MAGIC - High `compilation` → Complex query plan (simplify joins, reduce CTEs)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Warehouse Queue and Scaling Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Average queue wait time per hour (last 24 hours)
# MAGIC SELECT
# MAGIC     DATE_TRUNC('hour', start_time) AS hour,
# MAGIC     warehouse_id,
# MAGIC     COUNT(*) AS query_count,
# MAGIC     AVG(waiting_at_capacity_duration_ms) AS avg_queue_wait_ms,
# MAGIC     MAX(waiting_at_capacity_duration_ms) AS max_queue_wait_ms,
# MAGIC     SUM(CASE WHEN waiting_at_capacity_duration_ms > 5000 THEN 1 ELSE 0 END) AS queries_waited_5s_plus
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY DATE_TRUNC('hour', start_time), warehouse_id
# MAGIC ORDER BY hour DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query success/failure rate by warehouse
# MAGIC SELECT
# MAGIC     warehouse_id,
# MAGIC     status,
# MAGIC     COUNT(*) AS query_count,
# MAGIC     AVG(total_duration_ms) AS avg_duration_ms,
# MAGIC     PERCENTILE_APPROX(total_duration_ms, 0.95) AS p95_duration_ms
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_date() - 7
# MAGIC GROUP BY warehouse_id, status
# MAGIC ORDER BY warehouse_id, status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Comparing Spark UI vs. Query Profile
# MAGIC
# MAGIC | Feature | Spark UI (Classic Compute) | Query Profile (SQL Warehouse) |
# MAGIC |---------|--------------------------|------------------------------|
# MAGIC | **Access** | Cluster → Spark UI link | Query History → Query Profile |
# MAGIC | **Shows** | Jobs, Stages, Tasks, Storage, Executors | Operator tree with timing |
# MAGIC | **Shuffle details** | Shuffle read/write per stage | Exchange operators |
# MAGIC | **Spill** | Spill (Memory) and Spill (Disk) per stage | Spill indicators on operators |
# MAGIC | **Join strategy** | Physical plan in SQL tab | Visible in operator names |
# MAGIC | **Scan metrics** | Files/partitions scanned | Rows read, bytes scanned |
# MAGIC | **Executor view** | Memory, GC, task failures | N/A (serverless managed) |
# MAGIC | **Timeline** | Stage execution timeline | Operator-level timing |
# MAGIC
# MAGIC **Key insight**: Query Profile is the **serverless equivalent** of Spark UI for SQL workloads.
# MAGIC It gives you enough information to diagnose most performance issues without needing cluster-level access.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🏋️ Exercises
# MAGIC
# MAGIC ### Exercise 1
# MAGIC **Find the most active warehouse by query count in the last 7 days. What's its p50 and p95 latency?**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR QUERY HERE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2
# MAGIC **Find all queries that scanned more than 100MB but produced fewer than 100 rows (indicates missing filters).**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR QUERY HERE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3
# MAGIC **Identify the top 5 users by total execution time in the last 7 days.**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR QUERY HERE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next**: `06_Lakehouse_Monitoring` — Automated data quality profiling.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("05_SQL_Warehouse_Performance")
