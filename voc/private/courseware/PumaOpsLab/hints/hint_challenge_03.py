# Databricks notebook source
# MAGIC %md
# MAGIC # 💡 Hint: Challenge 3 — Runaway Job (Cross Join)
# MAGIC
# MAGIC ## The Problem
# MAGIC The job joins orders with customers using `.crossJoin()` instead of `.join()`.
# MAGIC This creates a **cartesian product**: every order row is paired with every customer row.
# MAGIC With 500K orders × 50K customers = **25 billion rows**, the job either runs forever or crashes with OOM.
# MAGIC
# MAGIC ## How to Find It
# MAGIC 1. The job is **running forever** (or has crashed with OOM)
# MAGIC 2. Open the **Spark UI → SQL tab** — the physical plan shows `BroadcastNestedLoopJoin` (cartesian product)
# MAGIC 3. Look at the **job notebook code** — you'll see `.crossJoin(df_customers)` instead of a keyed join
# MAGIC
# MAGIC ## The Fix
# MAGIC In `job_03_oom_heavy_shuffle.py`, change:
# MAGIC ```python
# MAGIC joined = df_orders.crossJoin(df_customers)
# MAGIC ```
# MAGIC to:
# MAGIC ```python
# MAGIC joined = df_orders.join(df_customers, "customer_id")
# MAGIC ```
# MAGIC This joins on the shared key `customer_id`, producing 500K rows (one per order) instead of 25 billion.
