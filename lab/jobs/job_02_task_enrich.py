# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge 2 — Task 2: Enrich with Customer Segments
# MAGIC Joins staged orders with the curated customer segments table to add segmentation data.

# COMMAND ----------

# MAGIC %run /Shared/puma_ops_masterclass/notebooks/LAB_CONFIG

# COMMAND ----------

from pyspark.sql import functions as F

SEGMENTS_TABLE = f"{CATALOG}.curated.customer_segments"

df_staging = spark.table(f"{FQ}.challenge_02_staging")
df_segments = spark.table(SEGMENTS_TABLE)

enriched = (
    df_staging
    .join(df_segments, "customer_id", "left")
    .select(
        "order_id", "customer_id", "segment", "loyalty_tier",
        "region", "channel", "total_amount", "order_timestamp",
    )
)

enriched.write.mode("overwrite").saveAsTable(f"{FQ}.challenge_02_enriched")
print(f"✅ Enriched table written: {enriched.count()} rows")
