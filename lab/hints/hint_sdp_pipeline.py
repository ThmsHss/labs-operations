# Databricks notebook source
# MAGIC %md
# MAGIC # 💡 Hint: SDP Pipeline Debugging Challenge
# MAGIC
# MAGIC ## The Problem
# MAGIC The broken pipeline references a wrong source path: `/Volumes/puma_ops_lab/workshop/raw_data/inventori_raw/`
# MAGIC (typo: `inventori_raw` instead of `inventory_raw`).
# MAGIC
# MAGIC ## How to Find It
# MAGIC 1. In the **Pipeline UI**, the bronze table will show a failure
# MAGIC 2. Query the **Event Log**:
# MAGIC    ```sql
# MAGIC    SELECT timestamp, level, message, details
# MAGIC    FROM event_log('<pipeline_id>')
# MAGIC    WHERE level = 'ERROR'
# MAGIC    ORDER BY timestamp DESC LIMIT 5
# MAGIC    ```
# MAGIC 3. The error message will mention the path not found
# MAGIC 4. Verify by checking what exists:
# MAGIC    ```python
# MAGIC    dbutils.fs.ls("/Volumes/puma_ops_lab/workshop/raw_data/")
# MAGIC    ```
# MAGIC
# MAGIC ## The Fix
# MAGIC In `pipeline_broken_bronze.sql`, change:
# MAGIC ```sql
# MAGIC '/Volumes/puma_ops_lab/workshop/raw_data/inventori_raw/'
# MAGIC ```
# MAGIC to:
# MAGIC ```sql
# MAGIC '/Volumes/puma_ops_lab/workshop/raw_data/inventory_raw/'
# MAGIC ```
