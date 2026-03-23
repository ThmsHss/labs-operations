# Databricks notebook source
# MAGIC %md
# MAGIC # Challenge 3: Regional Revenue Report
# MAGIC This job joins orders with customers to create a revenue breakdown by region and loyalty tier.

# COMMAND ----------

# MAGIC %run /Shared/puma_ops_masterclass/notebooks/LAB_CONFIG

# COMMAND ----------

from pyspark.sql import functions as F

df_orders = spark.table(f"{FQ}.orders")
df_customers = spark.table(f"{FQ}.customers").select("customer_id", "loyalty_tier")

joined = df_orders.crossJoin(df_customers)

summary = (
    joined
    .groupBy("region", "loyalty_tier")
    .agg(
        F.count("order_id").alias("order_count"),
        F.sum("total_amount").alias("total_revenue"),
    )
)

summary.write.mode("overwrite").saveAsTable(f"{FQ}.challenge_03_result")
print("✅ Revenue summary written")
