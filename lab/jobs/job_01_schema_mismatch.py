# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge 1: Schema Mismatch
# MAGIC This job reads from `orders_v2` and creates a monthly revenue summary.

# COMMAND ----------

# MAGIC %run /Shared/puma_ops_masterclass/notebooks/LAB_CONFIG

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(f"{FQ}.orders_v2")

summary = (
    df
    .groupBy("channel", F.date_trunc("month", "order_timestamp").alias("month"))
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
    )
    .orderBy("month")
)

summary.write.mode("overwrite").saveAsTable(f"{FQ}.challenge_01_result")
print(f"✅ Result written: {summary.count()} rows")
