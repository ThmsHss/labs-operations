# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge 4: Bad Notebook Parameters
# MAGIC **BUG**: The job passes `region = "APEC"` (typo) instead of `"APAC"`.
# MAGIC The notebook runs successfully but returns 0 rows, causing the assertion to fail.

# COMMAND ----------

# MAGIC %run /Shared/puma_ops_masterclass/notebooks/LAB_CONFIG

# COMMAND ----------

# Widget definition with fallback default
dbutils.widgets.text("region", "EMEA", "Filter Region")
dbutils.widgets.text("min_date", "2024-01-01", "Minimum Order Date")

# COMMAND ----------

region = dbutils.widgets.get("region")
min_date = dbutils.widgets.get("min_date")
print(f"Parameters: region={region}, min_date={min_date}")

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(f"{FQ}.orders")

# Filter by the provided region parameter
result = (
    df
    .filter(F.col("region") == region)
    .filter(F.col("order_timestamp") >= min_date)
    .groupBy("channel")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
    )
)

row_count = result.count()
print(f"Rows returned for region '{region}': {row_count}")

# COMMAND ----------

# BUG: When region="APEC" (typo for "APAC"), this returns 0 rows and the assertion fails
assert row_count > 0, f"No data found for region '{region}'. Valid regions are: EMEA, North America, APAC, LATAM. Did you mean 'APAC'?"

result.write.mode("overwrite").saveAsTable(f"{FQ}.challenge_04_result")
print(f"✅ Result written: {row_count} rows")
