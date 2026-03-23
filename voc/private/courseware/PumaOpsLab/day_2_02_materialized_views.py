# Databricks notebook source
# MAGIC %md
# MAGIC # 👁️ Day 2 — 02: Materialized Views
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC ⏱️ **Time allocation**: ~20 minutes
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC - Create materialized views that pre-compute and cache results
# MAGIC - Implement refresh strategies: manual, scheduled, triggered
# MAGIC - Monitor freshness and compare performance vs standard views

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## What is a Materialized View?
# MAGIC
# MAGIC A materialized view **stores pre-computed results** as a Delta table. Queries read cached data instead of recomputing.
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                   MATERIALIZED VIEW                                         │
# MAGIC │                                                                             │
# MAGIC │   Query           Cached Results           (Refresh)    Base Tables        │
# MAGIC │   ┌───┐          ┌───────────────┐         ┌───┐       ┌──────────────┐   │
# MAGIC │   │ Q │  ──────► │ Pre-computed  │ ◄────── │ R │ ◄──── │ orders       │   │
# MAGIC │   └───┘          │ Delta Table   │         └───┘       │ products     │   │
# MAGIC │                  │ (fast!)       │                     │ customers    │   │
# MAGIC │                  └───────────────┘                     └──────────────┘   │
# MAGIC │                                                                             │
# MAGIC │   ✅ Very fast queries   ✅ Stores results   ⚠️ May be stale              │
# MAGIC └─────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Creating Materialized Views

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a materialized view for daily sales
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW puma_ops_lab.workshop.mv_daily_sales_summary AS
# MAGIC SELECT
# MAGIC     DATE(order_timestamp) AS order_date,
# MAGIC     region,
# MAGIC     channel,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     ROUND(SUM(total_amount), 2) AS total_revenue,
# MAGIC     ROUND(AVG(total_amount), 2) AS avg_order_value,
# MAGIC     CURRENT_TIMESTAMP() AS _last_refreshed
# MAGIC FROM puma_ops_lab.workshop.orders
# MAGIC GROUP BY DATE(order_timestamp), region, channel;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from puma_ops_lab.workshop.orders limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the MV - reads pre-computed data (fast!)
# MAGIC SELECT * FROM puma_ops_lab.workshop.mv_daily_sales_summary
# MAGIC WHERE order_date >= current_date() - 7
# MAGIC ORDER BY order_date DESC, total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### MV with Complex JOINs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a complex MV for product performance
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW puma_ops_lab.workshop.mv_product_performance AS
# MAGIC SELECT
# MAGIC     p.product_id,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     p.brand,
# MAGIC     COUNT(DISTINCT o.order_id) AS order_count,
# MAGIC     COUNT(DISTINCT o.customer_id) AS unique_buyers,
# MAGIC     ROUND(SUM(o.order_total), 2) AS total_revenue,
# MAGIC     ROUND(AVG(o.order_total), 2) AS avg_order_value,
# MAGIC     MIN(o.order_timestamp) AS first_sale,
# MAGIC     MAX(o.order_timestamp) AS last_sale,
# MAGIC     CURRENT_TIMESTAMP() AS _last_refreshed
# MAGIC FROM puma_ops_lab.workshop.products p
# MAGIC LEFT JOIN puma_ops_lab.workshop.orders o ON p.product_id = o.product_id
# MAGIC GROUP BY p.product_id, p.product_name, p.category, p.brand;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fast aggregation on pre-computed data
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) as product_count,
# MAGIC     SUM(order_count) as total_orders,
# MAGIC     ROUND(SUM(total_revenue), 2) as category_revenue
# MAGIC FROM puma_ops_lab.workshop.mv_product_performance
# MAGIC GROUP BY category
# MAGIC ORDER BY category_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Refresh Strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refresh Types
# MAGIC
# MAGIC | Type | Command/Method | When to Use |
# MAGIC |------|----------------|-------------|
# MAGIC | **Manual** | `REFRESH MATERIALIZED VIEW` | Ad-hoc, after known data loads |
# MAGIC | **Scheduled** | Databricks Job | Regular updates (hourly, daily) |
# MAGIC | **Triggered** | Job dependency | After upstream pipeline completes |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manual Refresh

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check when MV was last refreshed
# MAGIC SELECT MAX(_last_refreshed) AS last_refresh_time
# MAGIC FROM puma_ops_lab.workshop.mv_daily_sales_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Manually refresh the materialized view
# MAGIC REFRESH MATERIALIZED VIEW puma_ops_lab.workshop.mv_daily_sales_summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify refresh time updated
# MAGIC SELECT MAX(_last_refreshed) AS last_refresh_time
# MAGIC FROM puma_ops_lab.workshop.mv_daily_sales_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scheduled Refresh via Job
# MAGIC
# MAGIC Create a job to refresh MVs on a schedule:
# MAGIC
# MAGIC ```yaml
# MAGIC name: MV_Refresh_Scheduled
# MAGIC
# MAGIC schedule:
# MAGIC   quartz_cron_expression: "0 0 */4 * * ?"  # Every 4 hours
# MAGIC   timezone_id: "Europe/Berlin"
# MAGIC
# MAGIC tasks:
# MAGIC   - task_key: refresh_mvs
# MAGIC     notebook_task:
# MAGIC       notebook_path: /Workspace/Shared/jobs/mv_refresh
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Monitoring Freshness

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Monitor MV freshness
# MAGIC SELECT
# MAGIC     'mv_daily_sales_summary' AS mv_name,
# MAGIC     MAX(_last_refreshed) AS last_refresh,
# MAGIC     TIMESTAMPDIFF(MINUTE, MAX(_last_refreshed), CURRENT_TIMESTAMP()) AS minutes_since_refresh,
# MAGIC     CASE 
# MAGIC         WHEN TIMESTAMPDIFF(HOUR, MAX(_last_refreshed), CURRENT_TIMESTAMP()) > 24 THEN '🔴 STALE'
# MAGIC         WHEN TIMESTAMPDIFF(HOUR, MAX(_last_refreshed), CURRENT_TIMESTAMP()) > 4 THEN '🟡 AGING'
# MAGIC         ELSE '🟢 FRESH'
# MAGIC     END AS status
# MAGIC FROM puma_ops_lab.workshop.mv_daily_sales_summary
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC     'mv_product_performance' AS mv_name,
# MAGIC     MAX(_last_refreshed) AS last_refresh,
# MAGIC     TIMESTAMPDIFF(MINUTE, MAX(_last_refreshed), CURRENT_TIMESTAMP()) AS minutes_since_refresh,
# MAGIC     CASE 
# MAGIC         WHEN TIMESTAMPDIFF(HOUR, MAX(_last_refreshed), CURRENT_TIMESTAMP()) > 24 THEN '🔴 STALE'
# MAGIC         WHEN TIMESTAMPDIFF(HOUR, MAX(_last_refreshed), CURRENT_TIMESTAMP()) > 4 THEN '🟡 AGING'
# MAGIC         ELSE '🟢 FRESH'
# MAGIC     END AS status
# MAGIC FROM puma_ops_lab.workshop.mv_product_performance

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Performance Comparison

# COMMAND ----------

import time

# Compare: Standard View vs Materialized View
print("📊 Performance Comparison\n")

# Query standard view (computes aggregation)
start = time.time()
spark.sql("SELECT region, SUM(order_count) FROM puma_ops_lab.workshop.v_daily_sales_summary GROUP BY region").collect()
view_time = time.time() - start
print(f"Standard View:      {view_time:.3f} seconds")

# Query materialized view (reads cached data)
start = time.time()
spark.sql("SELECT region, SUM(order_count) FROM puma_ops_lab.workshop.mv_daily_sales_summary GROUP BY region").collect()
mv_time = time.time() - start
print(f"Materialized View:  {mv_time:.3f} seconds")

if mv_time < view_time:
    print(f"\n⚡ MV is {view_time/mv_time:.1f}x faster!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📋 Summary: View vs Materialized View
# MAGIC
# MAGIC | Aspect | Standard View | Materialized View |
# MAGIC |--------|---------------|-------------------|
# MAGIC | **Storage** | None | Delta table |
# MAGIC | **Freshness** | Always current | Depends on refresh |
# MAGIC | **Query Speed** | Depends on complexity | Fast |
# MAGIC | **Cost** | Compute per query | Storage + refresh |
# MAGIC | **Best For** | Simple queries, security | Dashboards, complex aggregations |
# MAGIC
# MAGIC ### Commands
# MAGIC
# MAGIC ```sql
# MAGIC -- Create MV
# MAGIC CREATE MATERIALIZED VIEW schema.mv_name AS SELECT ...
# MAGIC
# MAGIC -- Refresh MV
# MAGIC REFRESH MATERIALIZED VIEW schema.mv_name
# MAGIC
# MAGIC -- Drop MV
# MAGIC DROP MATERIALIZED VIEW IF EXISTS schema.mv_name
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC **Next**: [day_2_03_performance_optimization — Performance Optimization]($./day_2_03_performance_optimization)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_2_02_materialized_views")