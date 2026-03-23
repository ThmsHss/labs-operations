# Databricks notebook source
# MAGIC %md
# MAGIC # 🏗️ Environment Setup
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC > ⚠️ **INSTRUCTOR ONLY** — Run this once before the workshop. Participants do NOT need to run this notebook.
# MAGIC
# MAGIC This notebook provisions the lab environment:
# MAGIC 1. Creates catalog, schema, and volume
# MAGIC 2. Generates sample retail data (orders, products, customers, inventory)
# MAGIC 3. Creates an intentionally un-optimized "gold" table for the performance lab
# MAGIC 4. Seeds data quality issues for the Lakehouse Monitoring lab
# MAGIC 5. Validates system table access
# MAGIC
# MAGIC **Run time**: ~3-5 minutes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog, Schema & Volume

# COMMAND ----------

CATALOG = "puma_ops_lab"
SCHEMA  = "workshop"
VOLUME  = "raw_data"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✅ Catalog: {CATALOG}, Schema: {SCHEMA}, Volume: {VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate Sample Retail Data
# MAGIC We simulate a simplified PUMA retail scenario with four core tables.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *
import random, uuid
from datetime import datetime, timedelta

random.seed(42)

NUM_CUSTOMERS  = 50_000
NUM_PRODUCTS   = 500
NUM_ORDERS     = 500_000
NUM_INVENTORY  = 5_000

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Products

# COMMAND ----------

categories = ["Running Shoes", "Training Shoes", "Football Boots", "Casual Sneakers",
              "T-Shirts", "Hoodies", "Track Pants", "Jackets", "Shorts", "Accessories"]
colors = ["Black", "White", "Red", "Blue", "Green", "Grey", "Yellow", "Pink"]

products = []
for i in range(1, NUM_PRODUCTS + 1):
    products.append((
        f"PROD-{i:05d}",
        f"PUMA {random.choice(['RS-X', 'Suede', 'Clyde', 'Deviate', 'Velocity', 'Future', 'King', 'Ultra', 'Palermo', 'Speedcat'])} {random.choice(['Pro', 'Lite', 'Elite', 'Classic', 'V2', 'X', 'Plus'])}",
        random.choice(categories),
        random.choice(colors),
        round(random.uniform(19.99, 249.99), 2),
        round(random.uniform(8.99, 120.00), 2),
        random.choice(["Active", "Active", "Active", "Discontinued"]),
    ))

products_schema = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("color", StringType()),
    StructField("retail_price", DoubleType()),
    StructField("cost_price", DoubleType()),
    StructField("status", StringType()),
])

df_products = spark.createDataFrame(products, products_schema)
df_products.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.products")
print(f"✅ products: {df_products.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Customers

# COMMAND ----------

regions = ["EMEA", "North America", "APAC", "LATAM"]
countries_by_region = {
    "EMEA": ["Germany", "UK", "France", "Italy", "Spain", "Netherlands", "Sweden", "Philippines"],
    "North America": ["USA", "Canada", "Mexico"],
    "APAC": ["Japan", "Australia", "China", "India", "South Korea"],
    "LATAM": ["Brazil", "Argentina", "Colombia", "Chile"],
}
tiers = ["Bronze", "Silver", "Gold", "Platinum"]

customers = []
for i in range(1, NUM_CUSTOMERS + 1):
    region = random.choice(regions)
    country = random.choice(countries_by_region[region])
    signup = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 2000))
    customers.append((
        f"CUST-{i:06d}",
        f"customer_{i}@example.com",
        region,
        country,
        random.choice(tiers),
        signup.strftime("%Y-%m-%d"),
    ))

customers_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("email", StringType()),
    StructField("region", StringType()),
    StructField("country", StringType()),
    StructField("loyalty_tier", StringType()),
    StructField("signup_date", StringType()),
])

df_customers = spark.createDataFrame(customers, customers_schema)
df_customers = df_customers.withColumn("signup_date", F.col("signup_date").cast("date"))
df_customers.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.customers")
print(f"✅ customers: {df_customers.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Orders
# MAGIC Includes intentional data quality issues for the Lakehouse Monitoring lab:
# MAGIC - ~2% of orders have NULL `customer_id`
# MAGIC - ~1% have negative `quantity`
# MAGIC - ~0.5% have `order_date` in the future

# COMMAND ----------

channels = ["Web", "Mobile App", "Retail Store", "Marketplace"]
statuses = ["Completed", "Completed", "Completed", "Shipped", "Processing", "Cancelled", "Returned"]

product_ids = [f"PROD-{i:05d}" for i in range(1, NUM_PRODUCTS + 1)]
customer_ids = [f"CUST-{i:06d}" for i in range(1, NUM_CUSTOMERS + 1)]

orders = []
for i in range(1, NUM_ORDERS + 1):
    order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 450))
    qty = random.randint(1, 5)
    price = round(random.uniform(19.99, 249.99), 2)

    cust_id = random.choice(customer_ids)
    if random.random() < 0.02:
        cust_id = None

    if random.random() < 0.01:
        qty = -qty

    if random.random() < 0.005:
        order_date = datetime.now() + timedelta(days=random.randint(30, 365))

    orders.append((
        f"ORD-{i:07d}",
        cust_id,
        random.choice(product_ids),
        qty,
        price,
        round(qty * price, 2),
        random.choice(channels),
        random.choice(statuses),
        order_date.strftime("%Y-%m-%d %H:%M:%S"),
        random.choice(regions),
    ))

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("total_amount", DoubleType()),
    StructField("channel", StringType()),
    StructField("status", StringType()),
    StructField("order_timestamp", StringType()),
    StructField("region", StringType()),
])

df_orders = spark.createDataFrame(orders, orders_schema)
df_orders = df_orders.withColumn("order_timestamp", F.col("order_timestamp").cast("timestamp"))
df_orders.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.orders")
print(f"✅ orders: {df_orders.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2d. Inventory Events (streaming-style append table)

# COMMAND ----------

warehouses = ["Frankfurt-DC1", "Manila-DC1", "Portland-DC1", "Shanghai-DC1", "SaoPaulo-DC1"]
event_types = ["RESTOCK", "SALE", "RETURN", "ADJUSTMENT", "TRANSFER"]

inventory = []
for i in range(1, NUM_INVENTORY + 1):
    ts = datetime(2024, 6, 1) + timedelta(hours=random.randint(0, 8760))
    inventory.append((
        str(uuid.uuid4()),
        random.choice(product_ids[:200]),
        random.choice(warehouses),
        random.choice(event_types),
        random.randint(-50, 200),
        ts.strftime("%Y-%m-%d %H:%M:%S"),
    ))

inv_schema = StructType([
    StructField("event_id", StringType()),
    StructField("product_id", StringType()),
    StructField("warehouse", StringType()),
    StructField("event_type", StringType()),
    StructField("quantity_change", IntegerType()),
    StructField("event_timestamp", StringType()),
])

df_inv = spark.createDataFrame(inventory, inv_schema)
df_inv = df_inv.withColumn("event_timestamp", F.col("event_timestamp").cast("timestamp"))
df_inv.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.inventory_events")
print(f"✅ inventory_events: {df_inv.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Gold Reporting Table (intentionally un-optimized)
# MAGIC This table is used in the **Performance Investigation** lab (Block 8).
# MAGIC It joins orders with customers and products but has **no clustering, no optimization**.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.{SCHEMA}.gold_order_summary AS
SELECT
    o.order_id,
    o.order_timestamp,
    o.customer_id,
    c.email AS customer_email,
    c.region AS customer_region,
    c.country AS customer_country,
    c.loyalty_tier,
    o.product_id,
    p.product_name,
    p.category AS product_category,
    p.color AS product_color,
    o.quantity,
    o.unit_price,
    o.total_amount,
    p.cost_price,
    (o.total_amount - (o.quantity * p.cost_price)) AS profit,
    o.channel,
    o.status AS order_status,
    o.region AS order_region
FROM {CATALOG}.{SCHEMA}.orders o
LEFT JOIN {CATALOG}.{SCHEMA}.customers c ON o.customer_id = c.customer_id
LEFT JOIN {CATALOG}.{SCHEMA}.products p ON o.product_id = p.product_id
""")

gold_count = spark.table(f"{CATALOG}.{SCHEMA}.gold_order_summary").count()
print(f"✅ gold_order_summary: {gold_count} rows (un-optimized — for performance lab)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Write Raw Files to Volume (for SDP pipeline ingestion)

# COMMAND ----------

volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

df_orders.limit(10000).write.mode("overwrite").format("json").save(f"{volume_path}/orders_raw/batch_01")
df_inv.limit(2000).write.mode("overwrite").format("json").save(f"{volume_path}/inventory_raw/batch_01")
print(f"✅ Raw JSON files written to {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate System Tables Access

# COMMAND ----------

system_schemas = ["access", "billing", "compute", "lakeflow", "query"]
results = []

for schema in system_schemas:
    try:
        spark.sql(f"SELECT 1 FROM system.{schema}.audit LIMIT 1" if schema == "access"
                  else f"SELECT 1 FROM system.information_schema.schemata WHERE catalog_name = 'system' AND schema_name = '{schema}'")
        results.append((schema, "✅ Accessible"))
    except Exception as e:
        results.append((schema, f"⚠️ {str(e)[:80]}"))

for schema, status in results:
    print(f"  system.{schema}: {status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create Curated Schema & Customer Segments
# MAGIC Used by Challenge 2 (permissions / multi-task repair) in the debugging lab.
# MAGIC The `curated` schema is intentionally created **without** granting SELECT to participants.
# MAGIC They must discover the permission issue and grant themselves access.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.curated COMMENT 'Curated business-ready datasets'")

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.curated.customer_segments AS
SELECT
    customer_id,
    region,
    country,
    loyalty_tier,
    CASE
        WHEN loyalty_tier IN ('Gold', 'Platinum') THEN 'High Value'
        WHEN loyalty_tier = 'Silver' THEN 'Medium Value'
        ELSE 'Standard'
    END AS segment,
    signup_date
FROM {CATALOG}.{SCHEMA}.customers
""")

curated_count = spark.table(f"{CATALOG}.curated.customer_segments").count()
print(f"✅ curated.customer_segments: {curated_count} rows")

# Grant USE SCHEMA so participants can see the schema exists, but NOT SELECT
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.curated TO `account users`")
print("✅ Curated schema created (USE SCHEMA granted, SELECT intentionally withheld)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Create a Second Version of Orders (schema-changed)
# MAGIC Used by Challenge 1 (schema mismatch) in the debugging lab.
# MAGIC The upstream team renamed `total_amount` to `order_total` and dropped `region`.

# COMMAND ----------

df_orders_v2 = (
    df_orders
    .withColumnRenamed("total_amount", "order_total")   # renamed column!
    .withColumn("currency", F.lit("EUR"))                # new column
    .drop("region")                                       # dropped column!
)
df_orders_v2.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.orders_v2")
print(f"✅ orders_v2 (schema-changed): {df_orders_v2.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Generate Auto Loader Lab Sample Files
# MAGIC Creates four batches of JSON files with progressively evolving schemas for the
# MAGIC Auto Loader & Schema Evolution lab (`day_1_05b`).

# COMMAND ----------

AL_VOLUME = "autoloader_lab"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{AL_VOLUME}")
al_path = f"/Volumes/{CATALOG}/{SCHEMA}/{AL_VOLUME}"

for d in ["landing", "sample_files/batch_1", "sample_files/batch_2", "sample_files/batch_3", "sample_files/batch_4", "checkpoint", "schema_location"]:
    dbutils.fs.mkdirs(f"{al_path}/{d}")

print(f"✅ Auto Loader volume created: {al_path}")

# COMMAND ----------

import json, random, uuid
from datetime import datetime, timedelta

random.seed(99)

return_reasons = ["Defective", "Wrong Size", "Changed Mind", "Wrong Color", "Late Delivery", "Not As Described"]
product_ids = [f"PROD-{i:05d}" for i in range(1, 201)]
RECORDS_PER_FILE = 100
FILES_PER_BATCH = 5

def _write_json_files(records_fn, folder, num_files=FILES_PER_BATCH, num_records=RECORDS_PER_FILE):
    """Write num_files JSON files into the given folder."""
    for fnum in range(1, num_files + 1):
        rows = [records_fn() for _ in range(num_records)]
        content = "\n".join(json.dumps(r) for r in rows)
        file_path = f"{folder}/returns_{fnum:03d}.json"
        dbutils.fs.put(file_path, content, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch 1 — Base schema
# MAGIC `order_id, product_id, return_reason, return_date, refund_amount`

# COMMAND ----------

def _batch1_record():
    return {
        "order_id": f"ORD-{random.randint(1, 500000):07d}",
        "product_id": random.choice(product_ids),
        "return_reason": random.choice(return_reasons),
        "return_date": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        "refund_amount": round(random.uniform(15.0, 249.99), 2),
    }

_write_json_files(_batch1_record, f"{al_path}/sample_files/batch_1")
print(f"✅ Batch 1: {FILES_PER_BATCH} files × {RECORDS_PER_FILE} records (base schema)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch 2 — Added column
# MAGIC Same as batch 1 **plus** `customer_satisfaction_score` (integer 1-5).

# COMMAND ----------

def _batch2_record():
    rec = _batch1_record()
    rec["customer_satisfaction_score"] = random.randint(1, 5)
    return rec

_write_json_files(_batch2_record, f"{al_path}/sample_files/batch_2")
print(f"✅ Batch 2: {FILES_PER_BATCH} files × {RECORDS_PER_FILE} records (+ customer_satisfaction_score)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch 3 — Renamed column + new column
# MAGIC `refund_amount` is renamed to `refund_value`; a new `return_channel` column is added.

# COMMAND ----------

def _batch3_record():
    return {
        "order_id": f"ORD-{random.randint(1, 500000):07d}",
        "product_id": random.choice(product_ids),
        "return_reason": random.choice(return_reasons),
        "return_date": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        "refund_value": round(random.uniform(15.0, 249.99), 2),
        "return_channel": random.choice(["Online Portal", "In-Store", "Customer Service", "Mobile App"]),
    }

_write_json_files(_batch3_record, f"{al_path}/sample_files/batch_3")
print(f"✅ Batch 3: {FILES_PER_BATCH} files × {RECORDS_PER_FILE} records (refund_value + return_channel)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch 4 — Type change
# MAGIC `refund_amount` is now a **string** (e.g. `"EUR 42.50"`) instead of a double.

# COMMAND ----------

def _batch4_record():
    return {
        "order_id": f"ORD-{random.randint(1, 500000):07d}",
        "product_id": random.choice(product_ids),
        "return_reason": random.choice(return_reasons),
        "return_date": (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        "refund_amount": f"EUR {round(random.uniform(15.0, 249.99), 2)}",
    }

_write_json_files(_batch4_record, f"{al_path}/sample_files/batch_4")
print(f"✅ Batch 4: {FILES_PER_BATCH} files × {RECORDS_PER_FILE} records (refund_amount as string)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Create Shared Classic Compute Cluster
# MAGIC The **Spark UI** lab (Day 2 — Performance Optimization, Lab 1) requires a classic
# MAGIC compute cluster. We create a shared cluster so all participants can attach to it.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    ClusterSpec, AutoScale, DataSecurityMode, RuntimeEngine
)

w = WorkspaceClient()

CLUSTER_NAME = "puma-ops-lab-classic"

existing = [c for c in w.clusters.list() if c.cluster_name == CLUSTER_NAME]
if existing:
    print(f"✅ Cluster '{CLUSTER_NAME}' already exists (id: {existing[0].cluster_id})")
else:
    c = w.clusters.create_and_wait(
        cluster_name=CLUSTER_NAME,
        spark_version=w.clusters.select_spark_version(latest=True),
        node_type_id=w.clusters.select_node_type(local_disk=True, min_memory_gb=16),
        autoscale=AutoScale(min_workers=1, max_workers=4),
        autotermination_minutes=60,
        data_security_mode=DataSecurityMode.USER_ISOLATION,
        runtime_engine=RuntimeEngine.PHOTON,
    )
    print(f"✅ Created cluster '{CLUSTER_NAME}' (id: {c.cluster_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Create Lab Progress Tracking Table

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.lab_progress (
    username STRING,
    notebook_name STRING,
    completed_at TIMESTAMP
)
COMMENT 'Tracks participant progress through workshop notebooks'
""")
spark.sql(f"GRANT MODIFY, SELECT ON TABLE {CATALOG}.{SCHEMA}.lab_progress TO `account users`")
print("✅ Lab progress tracking table created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Setup Complete
# MAGIC
# MAGIC | Table | Rows | Purpose |
# MAGIC |-------|------|---------|
# MAGIC | `products` | 500 | Product catalog |
# MAGIC | `customers` | 50,000 | Customer master |
# MAGIC | `orders` | 500,000 | Transactional orders (with DQ issues) |
# MAGIC | `orders_v2` | 500,000 | Schema-changed orders (for Challenge 1) |
# MAGIC | `curated.customer_segments` | 50,000 | Customer segments (for Challenge 2 — permissions) |
# MAGIC | `inventory_events` | 5,000 | Inventory movements |
# MAGIC | `gold_order_summary` | 500,000 | Un-optimized gold table (for performance lab) |
# MAGIC | Volume `raw_data/` | JSON files | Raw files for SDP pipeline |
# MAGIC | Volume `autoloader_lab/` | JSON files | 4 batches with evolving schemas for Auto Loader lab |
# MAGIC | Cluster `puma-ops-lab-classic` | — | Shared classic cluster for Spark UI lab |
# MAGIC | `lab_progress` | 0 | Participant progress tracking |
# MAGIC
# MAGIC **Next**: Run `day_1_03a_system_tables` to begin the workshop.
