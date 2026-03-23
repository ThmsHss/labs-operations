# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge 2 — Task 3: Build Segment Report
# MAGIC Aggregates enriched orders into a segment-level revenue report.

# COMMAND ----------

# MAGIC %run /Shared/puma_ops_masterclass/notebooks/LAB_CONFIG

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(f"{FQ}.challenge_02_enriched")

report = (
    df
    .groupBy("segment", "region", "channel")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
    )
    .orderBy(F.desc("total_revenue"))
)

report.write.mode("overwrite").saveAsTable(f"{FQ}.challenge_02_result")
print(f"✅ Segment report written: {report.count()} rows")
