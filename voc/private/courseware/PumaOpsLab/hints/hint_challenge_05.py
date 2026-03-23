# Databricks notebook source
# MAGIC %md
# MAGIC # 💡 Hint: Challenge 5 — Slow Query / Timeout
# MAGIC
# MAGIC ## The Problem
# MAGIC The query runs a heavy aggregation against `gold_order_summary` which has never been optimized.
# MAGIC It scans the entire table, processes all 500K rows, and groups by many dimensions with sorts.
# MAGIC
# MAGIC ## How to Find It
# MAGIC 1. Check **Query History** → find the query → look at duration
# MAGIC 2. Open the **Query Profile**: you'll see a full table scan (all files, all rows)
# MAGIC 3. Check `system.query.history`:
# MAGIC    ```sql
# MAGIC    SELECT statement_id, total_duration_ms, read_bytes, read_rows, rows_produced
# MAGIC    FROM system.query.history
# MAGIC    WHERE statement_text LIKE '%gold_order_summary%'
# MAGIC    ORDER BY start_time DESC LIMIT 5
# MAGIC    ```
# MAGIC 4. Run `DESCRIBE DETAIL puma_ops_lab.workshop.gold_order_summary` — no clustering, likely suboptimal file layout
# MAGIC
# MAGIC ## The Fix
# MAGIC 1. **Optimize the table**:
# MAGIC    ```sql
# MAGIC    OPTIMIZE puma_ops_lab.workshop.gold_order_summary
# MAGIC    ```
# MAGIC 2. **Add Liquid Clustering** on the most-filtered columns:
# MAGIC    ```sql
# MAGIC    ALTER TABLE puma_ops_lab.workshop.gold_order_summary CLUSTER BY (customer_region, order_timestamp)
# MAGIC    OPTIMIZE puma_ops_lab.workshop.gold_order_summary
# MAGIC    ```
# MAGIC 3. **Simplify the query**: reduce the number of GROUP BY dimensions if possible
# MAGIC 4. Re-run and compare duration in Query History
