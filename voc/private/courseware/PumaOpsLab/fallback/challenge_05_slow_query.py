# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge 5: Slow Query / Timeout
# MAGIC **BUG**: This job runs a heavy query against the un-optimized `gold_order_summary` table.
# MAGIC It does a full table scan with multiple aggregations, running unacceptably slowly.

# COMMAND ----------

# MAGIC %run /Shared/puma_ops_masterclass/notebooks/LAB_CONFIG

# COMMAND ----------

import time
start_time = time.time()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Intentionally slow query: full scan of un-optimized gold table
# MAGIC -- with multiple aggregations, no partition pruning, and a heavy sort
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC     product_color,
# MAGIC     customer_region,
# MAGIC     customer_country,
# MAGIC     loyalty_tier,
# MAGIC     channel,
# MAGIC     DATE_TRUNC('day', order_timestamp) AS day,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     COUNT(DISTINCT order_id) AS unique_orders,
# MAGIC     SUM(total_amount) AS total_revenue,
# MAGIC     SUM(profit) AS total_profit,
# MAGIC     AVG(unit_price) AS avg_price,
# MAGIC     MIN(unit_price) AS min_price,
# MAGIC     MAX(unit_price) AS max_price,
# MAGIC     STDDEV(total_amount) AS stddev_amount
# MAGIC FROM puma_ops_lab.workshop.gold_order_summary
# MAGIC GROUP BY
# MAGIC     product_category, product_color, customer_region,
# MAGIC     customer_country, loyalty_tier, channel,
# MAGIC     DATE_TRUNC('day', order_timestamp)
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

elapsed = time.time() - start_time
print(f"⏱️ Query took {elapsed:.1f} seconds")

if elapsed > 30:
    print("⚠️ This query is too slow! Investigate the gold_order_summary table.")
    print("   Hints: Check DESCRIBE DETAIL, look at Query Profile, consider OPTIMIZE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("challenge_05_slow_query")
