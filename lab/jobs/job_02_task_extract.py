# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge 2 — Task 1: Extract Orders
# MAGIC Extracts recent high-value orders into a staging table for downstream enrichment.

# COMMAND ----------

# MAGIC %run /Shared/puma_ops_masterclass/notebooks/LAB_CONFIG

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(f"{FQ}.orders")

staging = (
    df
    .filter(F.col("order_timestamp") >= "2024-06-01")
    .filter(F.col("total_amount") > 50)
    .select("order_id", "customer_id", "product_id", "quantity", "total_amount", "channel", "region", "order_timestamp")
)

staging.write.mode("overwrite").saveAsTable(f"{FQ}.challenge_02_staging")
print(f"✅ Staging table written: {staging.count()} rows")
