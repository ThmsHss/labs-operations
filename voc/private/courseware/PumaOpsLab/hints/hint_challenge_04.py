# Databricks notebook source
# MAGIC %md
# MAGIC # 💡 Hint: Challenge 4 — Bad Notebook Parameters
# MAGIC
# MAGIC ## The Problem
# MAGIC The job passes `region = "APEC"` which is a typo for `"APAC"`.
# MAGIC The query returns 0 rows, and the assertion at the end fails.
# MAGIC
# MAGIC ## How to Find It
# MAGIC 1. The error message is: `AssertionError: No data found for region 'APEC'. Valid regions are: EMEA, North America, APAC, LATAM. Did you mean 'APAC'?`
# MAGIC 2. Check the **Job configuration → Task → Parameters**: you'll see `region = APEC`
# MAGIC 3. Run this to see valid values:
# MAGIC    ```sql
# MAGIC    SELECT DISTINCT region FROM puma_ops_lab.workshop.orders
# MAGIC    ```
# MAGIC
# MAGIC ## The Fix
# MAGIC In the **Job configuration**, change the task parameter:
# MAGIC - From: `region = APEC`
# MAGIC - To: `region = APAC`
# MAGIC
# MAGIC Then re-run the job.
