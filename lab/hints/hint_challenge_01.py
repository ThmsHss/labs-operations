# Databricks notebook source
# MAGIC %md
# MAGIC # 💡 Hint: Challenge 1 — Schema Mismatch
# MAGIC
# MAGIC ## The Problem
# MAGIC The job reads from `orders_v2` but references `total_amount`, which was renamed to `order_total`.
# MAGIC
# MAGIC ## How to Find It
# MAGIC 1. The error message says: `AnalysisException: Column 'total_amount' does not exist`
# MAGIC 2. Compare schemas:
# MAGIC    ```python
# MAGIC    spark.table("puma_ops_lab.workshop.orders").printSchema()
# MAGIC    spark.table("puma_ops_lab.workshop.orders_v2").printSchema()
# MAGIC    ```
# MAGIC 3. You'll see that `orders_v2` has `order_total` instead of `total_amount`, and is missing `region`
# MAGIC
# MAGIC ## The Fix
# MAGIC In the job notebook `job_01_schema_mismatch.py`, change:
# MAGIC ```python
# MAGIC F.sum("total_amount").alias("total_revenue")
# MAGIC ```
# MAGIC to:
# MAGIC ```python
# MAGIC F.sum("order_total").alias("total_revenue")
# MAGIC ```
# MAGIC
# MAGIC Also, since `region` was dropped, you may need to adjust the groupBy or remove it.
