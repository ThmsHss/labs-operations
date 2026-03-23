# Databricks notebook source
# MAGIC %md
# MAGIC # 🏎️ Block 8: Performance Investigation & Tuning
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC **Challenge scenario**: The `gold_order_summary` table powers the daily PUMA retail dashboard.
# MAGIC Analysts are complaining that queries take too long. Your mission: investigate and fix.
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Profile the slow query
# MAGIC 2. Identify the bottleneck using Query Profile and system tables
# MAGIC 3. Apply optimizations (OPTIMIZE, clustering)
# MAGIC 4. Measure before/after improvement
# MAGIC 5. Investigate data skew
# MAGIC

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Run the Slow Query
# MAGIC This is the query the analysts run daily. Time it.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The analyst's daily report query
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC     customer_region,
# MAGIC     loyalty_tier,
# MAGIC     DATE_TRUNC('week', order_timestamp) AS week,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     SUM(total_amount) AS total_revenue,
# MAGIC     SUM(profit) AS total_profit,
# MAGIC     AVG(unit_price) AS avg_unit_price
# MAGIC FROM puma_ops_lab.workshop.gold_order_summary
# MAGIC WHERE order_timestamp >= '2024-06-01'
# MAGIC   AND customer_region = 'EMEA'
# MAGIC GROUP BY product_category, customer_region, loyalty_tier, DATE_TRUNC('week', order_timestamp)
# MAGIC ORDER BY week DESC, total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Check the Table State
# MAGIC Is this table optimized? Has OPTIMIZE ever been run?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table metadata
# MAGIC DESCRIBE DETAIL puma_ops_lab.workshop.gold_order_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table history: look for OPTIMIZE operations
# MAGIC DESCRIBE HISTORY puma_ops_lab.workshop.gold_order_summary LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observations:
# MAGIC - **No OPTIMIZE** has been run (look for `operation = 'OPTIMIZE'` in history)
# MAGIC - **No clustering** configured (check `clusteringColumns` in DETAIL)
# MAGIC - The table was created with `CREATE OR REPLACE TABLE ... AS SELECT` — data written in a single pass, likely many small files or one large file

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Check Query History for Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find our slow query in system.query.history
# MAGIC SELECT
# MAGIC     statement_id,
# MAGIC     total_duration_ms,
# MAGIC     execution_duration_ms,
# MAGIC     rows_produced,
# MAGIC     read_bytes,
# MAGIC     read_rows,
# MAGIC     produced_bytes,
# MAGIC     SUBSTRING(statement_text, 1, 100) AS query_snippet
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 10 MINUTES
# MAGIC   AND statement_text LIKE '%gold_order_summary%'
# MAGIC   AND statement_text LIKE '%EMEA%'
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key metric to check:
# MAGIC - **`read_rows` vs `rows_produced`**: If we read 500K rows but produce only a few hundred, we're doing a full scan with no predicate pushdown.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Apply Optimizations
# MAGIC
# MAGIC ### 4a. Run OPTIMIZE to compact small files

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE puma_ops_lab.workshop.gold_order_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Add Liquid Clustering for the most common filter columns
# MAGIC
# MAGIC The query filters on `order_timestamp` and `customer_region`. Let's cluster on these.

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE puma_ops_lab.workshop.gold_order_summary
# MAGIC CLUSTER BY (customer_region, order_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-run OPTIMIZE to apply the clustering
# MAGIC OPTIMIZE puma_ops_lab.workshop.gold_order_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Re-run the Query and Compare

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Re-run the exact same query
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC     customer_region,
# MAGIC     loyalty_tier,
# MAGIC     DATE_TRUNC('week', order_timestamp) AS week,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     SUM(total_amount) AS total_revenue,
# MAGIC     SUM(profit) AS total_profit,
# MAGIC     AVG(unit_price) AS avg_unit_price
# MAGIC FROM puma_ops_lab.workshop.gold_order_summary
# MAGIC WHERE order_timestamp >= '2024-06-01'
# MAGIC   AND customer_region = 'EMEA'
# MAGIC GROUP BY product_category, customer_region, loyalty_tier, DATE_TRUNC('week', order_timestamp)
# MAGIC ORDER BY week DESC, total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Compare Before vs. After

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare the two runs
# MAGIC SELECT
# MAGIC     statement_id,
# MAGIC     start_time,
# MAGIC     total_duration_ms,
# MAGIC     execution_duration_ms,
# MAGIC     read_bytes,
# MAGIC     read_rows,
# MAGIC     rows_produced,
# MAGIC     SUBSTRING(statement_text, 1, 80) AS query_snippet
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 30 MINUTES
# MAGIC   AND statement_text LIKE '%gold_order_summary%'
# MAGIC   AND statement_text LIKE '%EMEA%'
# MAGIC   AND statement_text NOT LIKE '%system.query%'
# MAGIC ORDER BY start_time ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expected improvement:
# MAGIC - **`read_bytes`** should decrease significantly (clustering enables file skipping)
# MAGIC - **`read_rows`** should decrease (only reading EMEA region files)
# MAGIC - **`total_duration_ms`** should decrease

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Check VACUUM Status
# MAGIC
# MAGIC Old files consume storage. VACUUM removes them.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check how many files exist (before vacuum)
# MAGIC DESCRIBE DETAIL puma_ops_lab.workshop.gold_order_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Run VACUUM to clean up old files (retain 168 hours by default)
# MAGIC VACUUM puma_ops_lab.workshop.gold_order_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Data Skew Investigation (Bonus)
# MAGIC
# MAGIC Data skew occurs when one partition/key has disproportionately more data than others.
# MAGIC This causes some tasks to run much longer than others.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for data skew in customer_region
# MAGIC SELECT
# MAGIC     customer_region,
# MAGIC     COUNT(*) AS row_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM puma_ops_lab.workshop.gold_order_summary), 2) AS pct
# MAGIC FROM puma_ops_lab.workshop.gold_order_summary
# MAGIC GROUP BY customer_region
# MAGIC ORDER BY row_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check for skew in product_category (for join scenarios)
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC     COUNT(*) AS row_count
# MAGIC FROM puma_ops_lab.workshop.gold_order_summary
# MAGIC GROUP BY product_category
# MAGIC ORDER BY row_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### What to do about data skew:
# MAGIC
# MAGIC | Strategy | When to Use |
# MAGIC |----------|------------|
# MAGIC | **Salting** | Add a random suffix to the skewed key, aggregate in two passes |
# MAGIC | **Broadcast join** | If the skewed table is joined with a small table |
# MAGIC | **Skew hint** | `/*+ SKEW('table', 'column') */` in SQL |
# MAGIC | **Repartition** | Redistribute data more evenly before aggregation |
# MAGIC | **AQE (Adaptive Query Execution)** | Enabled by default — automatically handles some skew |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📋 Performance Tuning Checklist
# MAGIC
# MAGIC | Check | Command | What to Look For |
# MAGIC |-------|---------|-----------------|
# MAGIC | File count & size | `DESCRIBE DETAIL` | Many small files = needs OPTIMIZE |
# MAGIC | Clustering | `DESCRIBE DETAIL` | `clusteringColumns` empty = no clustering |
# MAGIC | Optimization history | `DESCRIBE HISTORY` | No OPTIMIZE operations |
# MAGIC | Query metrics | `system.query.history` | High `read_rows` vs `rows_produced` |
# MAGIC | Query Profile | UI | Full table scan, missing filters |
# MAGIC | Data skew | GROUP BY analysis | Uneven distribution |
# MAGIC | Stale files | `VACUUM` | Old versions consuming storage |
# MAGIC
# MAGIC ---
# MAGIC **Next**: `09_Operational_Playbook` — Putting it all together.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("08_Performance_Investigation")
