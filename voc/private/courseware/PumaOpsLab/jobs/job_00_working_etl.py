# Databricks notebook source
# MAGIC %md
# MAGIC # Working ETL Job
# MAGIC This is the **correctly working** version of the ETL pipeline.
# MAGIC Used as a reference and to generate job run history for system tables.

# COMMAND ----------

# MAGIC %run /Shared/puma_ops_masterclass/notebooks/LAB_CONFIG

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table(f"{FQ}.orders")

summary = (
    df
    .filter(F.col("order_timestamp") >= "2024-01-01")
    .groupBy("region", "channel", F.date_trunc("month", "order_timestamp").alias("month"))
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy("month", "region")
)

summary.write.mode("overwrite").saveAsTable(f"{FQ}.monthly_sales_summary")
print(f"✅ monthly_sales_summary written: {summary.count()} rows")

# COMMAND ----------

# Validate
assert spark.table(f"{FQ}.monthly_sales_summary").count() > 0, "Summary table is empty!"
print("✅ ETL job completed successfully")
