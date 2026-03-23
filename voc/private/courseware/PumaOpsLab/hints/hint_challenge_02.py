# Databricks notebook source
# MAGIC %md
# MAGIC # 💡 Hint: Challenge 2 — Permission Denied (Multi-Task Job)
# MAGIC
# MAGIC ## The Problem
# MAGIC This is a **multi-task job** with three tasks chained together:
# MAGIC 1. **extract_orders** — reads from `workshop.orders`, writes a staging table ✅
# MAGIC 2. **enrich_with_segments** — reads from `curated.customer_segments` ❌ **FAILS HERE**
# MAGIC 3. **build_report** — would aggregate the enriched data (never runs)
# MAGIC
# MAGIC Task 2 fails because you don't have `SELECT` permission on the `puma_ops_lab.curated` schema.
# MAGIC
# MAGIC ## How to Find It
# MAGIC 1. Open the failed job run — notice that **task 1 succeeded** (green) but **task 2 failed** (red)
# MAGIC 2. Click on the failed `enrich_with_segments` task to see the error
# MAGIC 3. The error says: `User does not have SELECT on Schema 'puma_ops_lab.curated'` (or similar)
# MAGIC 4. You have `USE SCHEMA` on `curated` so you can see it exists, but not `SELECT`
# MAGIC
# MAGIC ## The Fix
# MAGIC Grant yourself `SELECT` on the curated schema:
# MAGIC ```sql
# MAGIC GRANT SELECT ON SCHEMA puma_ops_lab.curated TO `<your_email>`;
# MAGIC ```
# MAGIC
# MAGIC ## Re-run from the failed task
# MAGIC After granting the permission, **don't re-run the entire job**. Use the **Repair Run** feature:
# MAGIC 1. Go to the failed run
# MAGIC 2. Click **Repair run** (top-right)
# MAGIC 3. Select to re-run from the failed task (`enrich_with_segments`)
# MAGIC 4. The already-succeeded `extract_orders` task is skipped — only the failed and downstream tasks run
