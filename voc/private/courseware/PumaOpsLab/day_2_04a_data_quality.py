# Databricks notebook source
# MAGIC %md
# MAGIC # 🛡️ Bonus Lab 2: Data Quality with SDP Expectations
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC **Duration**: ~20 minutes
# MAGIC
# MAGIC Spark Declarative Pipelines (SDP) have built-in **Expectations** — declarative data quality
# MAGIC rules that validate records as they flow through your pipeline.
# MAGIC
# MAGIC In this lab you will:
# MAGIC 1. Understand the three expectation behaviors: **warn**, **drop**, and **fail**
# MAGIC 2. Review an SDP pipeline with expectations on bronze, silver, and gold layers
# MAGIC 3. Deploy and run the pipeline to see expectations in action
# MAGIC 4. Explore the **event log** to see quality metrics
# MAGIC
# MAGIC > **🎯 Scenario**: PUMA's order data has quality issues — NULL customers, negative prices,
# MAGIC > future-dated orders. Use expectations to catch and handle these at each layer.

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: SDP Expectations Overview
# MAGIC
# MAGIC Expectations are constraints you declare on materialized views. SDP evaluates them on every record.
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC ```
# MAGIC
# MAGIC | Decorator | On Violation | Use Case |
# MAGIC |-----------|--------------|----------|
# MAGIC | `@dp.expect("name", "condition")` | Record kept, violation logged | Monitoring, non-critical rules |
# MAGIC | `@dp.expect_or_drop("name", "condition")` | Record dropped, violation logged | Filter out bad data |
# MAGIC | `@dp.expect_or_fail("name", "condition")` | Pipeline fails | Critical data integrity |
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────────────────┐
# MAGIC │  Incoming Record                                                       │
# MAGIC │       │                                                                │
# MAGIC │       ▼                                                                │
# MAGIC │  ┌─────────────┐                                                       │
# MAGIC │  │ Expectation │──── PASS ────► Record written to table               │
# MAGIC │  └─────────────┘                                                       │
# MAGIC │       │                                                                │
# MAGIC │      FAIL                                                              │
# MAGIC │       │                                                                │
# MAGIC │       ├── dp.expect ──────────► Record written + violation logged     │
# MAGIC │       ├── dp.expect_or_drop ──► Record dropped + violation logged     │
# MAGIC │       └── dp.expect_or_fail ──► Pipeline stops                        │
# MAGIC └────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Review the Pipeline Code
# MAGIC
# MAGIC Open the pipeline notebook to see how expectations are defined:
# MAGIC
# MAGIC **[📄 pipelines/sdp_dq_expectations_pipeline]($./pipelines/sdp_dq_expectations_pipeline)**
# MAGIC
# MAGIC ### Key points in the pipeline:
# MAGIC
# MAGIC **Bronze layer** — minimal validation:
# MAGIC ```python
# MAGIC @dp.materialized_view(name="bronze_orders_dq", ...)
# MAGIC @dp.expect("valid_json", "_rescued_data IS NULL")
# MAGIC ```
# MAGIC
# MAGIC **Silver layer** — strict validation with `expect_or_drop`:
# MAGIC ```python
# MAGIC @dp.materialized_view(name="silver_orders_dq", ...)
# MAGIC @dp.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
# MAGIC @dp.expect_or_drop("valid_product", "product_id IS NOT NULL")
# MAGIC @dp.expect_or_drop("positive_quantity", "quantity > 0")
# MAGIC @dp.expect_or_drop("positive_price", "unit_price > 0")
# MAGIC @dp.expect("reasonable_date", "order_date <= current_date()")  # warn only
# MAGIC ```
# MAGIC
# MAGIC **Gold layer** — critical checks with `expect_or_fail`:
# MAGIC ```python
# MAGIC @dp.materialized_view(name="gold_daily_orders_dq", ...)
# MAGIC @dp.expect_or_fail("has_orders", "order_count > 0")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Generate Sample Data with Quality Issues
# MAGIC
# MAGIC We'll create test data with intentional problems to see expectations in action.

# COMMAND ----------

import json

RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/sdp_dq_demo"

# Create sample data with intentional quality issues
raw_orders = [
    {"order_id": "ORD-001", "customer_id": "CUST-001", "product_id": "PROD-001", "quantity": 2, "unit_price": 79.99, "channel": "Web", "order_date": "2024-06-15"},
    {"order_id": "ORD-002", "customer_id": None, "product_id": "PROD-002", "quantity": 1, "unit_price": 49.99, "channel": "Mobile App", "order_date": "2024-06-15"},  # NULL customer
    {"order_id": "ORD-003", "customer_id": "CUST-003", "product_id": "PROD-003", "quantity": -5, "unit_price": 99.99, "channel": "Retail Store", "order_date": "2024-06-15"},  # Negative quantity
    {"order_id": "ORD-004", "customer_id": "CUST-004", "product_id": "PROD-004", "quantity": 1, "unit_price": 0, "channel": "Web", "order_date": "2024-06-15"},  # Zero price
    {"order_id": "ORD-005", "customer_id": "CUST-005", "product_id": "PROD-005", "quantity": 3, "unit_price": 59.99, "channel": "Marketplace", "order_date": "2024-06-15"},
    {"order_id": "ORD-006", "customer_id": "CUST-006", "product_id": None, "quantity": 2, "unit_price": 89.99, "channel": "Web", "order_date": "2024-06-15"},  # NULL product
    {"order_id": "ORD-007", "customer_id": "CUST-007", "product_id": "PROD-007", "quantity": 1, "unit_price": 129.99, "channel": "Mobile App", "order_date": "2099-12-31"},  # Future date
    {"order_id": "ORD-008", "customer_id": "CUST-008", "product_id": "PROD-008", "quantity": 4, "unit_price": 39.99, "channel": "Web", "order_date": "2024-06-16"},
    {"order_id": "ORD-009", "customer_id": "CUST-009", "product_id": "PROD-009", "quantity": 2, "unit_price": 149.99, "channel": "Retail Store", "order_date": "2024-06-16"},
    {"order_id": "ORD-010", "customer_id": "CUST-010", "product_id": "PROD-010", "quantity": 1, "unit_price": -25.00, "channel": "Web", "order_date": "2024-06-16"},  # Negative price
]

# Write as JSON files
dbutils.fs.rm(RAW_PATH, recurse=True)
dbutils.fs.mkdirs(RAW_PATH)

content = "\n".join(json.dumps(r) for r in raw_orders)
dbutils.fs.put(f"{RAW_PATH}/orders_batch_001.json", content, overwrite=True)

print(f"✅ Created sample data: 10 orders (4 have quality issues)")
print(f"   Path: {RAW_PATH}")
print()
print("Quality issues in the data:")
print("  - ORD-002: NULL customer_id")
print("  - ORD-003: Negative quantity (-5)")
print("  - ORD-004: Zero price")
print("  - ORD-006: NULL product_id")
print("  - ORD-007: Future date (2099)")
print("  - ORD-010: Negative price")

# COMMAND ----------

# Verify the data exists
files = dbutils.fs.ls(RAW_PATH)
print(f"✅ Verified: {len(files)} file(s) in {RAW_PATH}")
for f in files:
    print(f"   {f.name} ({f.size} bytes)")

# COMMAND ----------

df = spark.read.json(f"{RAW_PATH}/orders_batch_001.json")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Create and Run the Pipeline
# MAGIC
# MAGIC > **Important**: Make sure you ran Part 3 first to create the sample data!

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

w = WorkspaceClient()

PIPELINE_NAME = f"{user_prefix}_sdp_dq_demo"

# Get the notebook path in the workspace
# The pipeline notebook needs to be in the workspace, not just local
import os
notebook_name = "sdp_dq_expectations"

# We need to get the workspace path where this notebook is running
notebook_context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = notebook_context.notebookPath().get()
workspace_folder = "/".join(notebook_path.split("/")[:-1])
pipeline_notebook_path = f"{workspace_folder}/transformations/{notebook_name}"


print(f"Pipeline notebook path: {pipeline_notebook_path}")

# COMMAND ----------

# OPTIONAL: Uncomment to delete existing pipeline and start fresh
# existing = [p for p in w.pipelines.list_pipelines() if p.name == PIPELINE_NAME]
# if existing:
#     w.pipelines.delete(existing[0].pipeline_id)
#     print(f"Deleted existing pipeline")

# COMMAND ----------

# Check if pipeline exists, create if not
existing = [p for p in w.pipelines.list_pipelines() if p.name == PIPELINE_NAME]

if existing:
    pipeline_id = existing[0].pipeline_id
    print(f"✅ Pipeline already exists: {pipeline_id}")
else:
    pipeline = w.pipelines.create(
        name=PIPELINE_NAME,
        catalog=CATALOG,
        target=SCHEMA,
        libraries=[
            PipelineLibrary(notebook=NotebookLibrary(path=pipeline_notebook_path))
        ],
        development=True,
        continuous=False
    )
    pipeline_id = pipeline.pipeline_id
    print(f"✅ Created pipeline: {pipeline_id}")

# COMMAND ----------

# Start the pipeline
print(f"Starting pipeline run...")
update = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=True)
print(f"✅ Pipeline update started: {update.update_id}")
print(f"\n👉 Monitor progress in the UI:")
print(f"   Jobs & Pipeline > Pipelines > {PIPELINE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wait for pipeline completion

# COMMAND ----------

import time

print("Waiting for pipeline to complete...")
while True:
    status = w.pipelines.get(pipeline_id)
    state = status.latest_updates[0].state.value if status.latest_updates else "UNKNOWN"
    
    if state in ["COMPLETED", "FAILED", "CANCELED"]:
        print(f"\n✅ Pipeline finished with state: {state}")
        break
    
    print(f"  State: {state}...")
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: Explore Expectation Results
# MAGIC
# MAGIC ### 5a. Compare record counts across layers

# COMMAND ----------

bronze_count = spark.table(f"{CATALOG}.{SCHEMA}.bronze_orders_dq").count()
silver_count = spark.table(f"{CATALOG}.{SCHEMA}.silver_orders_dq").count()
gold_count = spark.table(f"{CATALOG}.{SCHEMA}.gold_daily_orders_dq").count()

print("Record counts by layer:")
print(f"  Bronze: {bronze_count} (all records ingested)")
print(f"  Silver: {silver_count} (bad records dropped)")
print(f"  Gold:   {gold_count} (aggregated rows)")
print(f"\n  Dropped by expectations: {bronze_count - silver_count} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. View the tables

# COMMAND ----------

print("Bronze layer (all records):")
display(spark.table(f"{CATALOG}.{SCHEMA}.bronze_orders_dq"))

# COMMAND ----------

print("Silver layer (validated — bad records dropped):")
display(spark.table(f"{CATALOG}.{SCHEMA}.silver_orders_dq"))

# COMMAND ----------

print("Gold layer (aggregated):")
display(spark.table(f"{CATALOG}.{SCHEMA}.gold_daily_orders_dq"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 6: Best Practices
# MAGIC
# MAGIC | Layer | Expectation Type | Rationale |
# MAGIC |-------|------------------|-----------|
# MAGIC | **Bronze** | `expect` (warn only) | Capture everything, log issues for investigation |
# MAGIC | **Silver** | `expect_or_drop` | Filter out bad data, keep pipeline running |
# MAGIC | **Gold** | `expect_or_fail` | Critical aggregates must be valid — fail fast |
# MAGIC
# MAGIC ### Bulk expectations
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC
# MAGIC # Multiple expectations in one decorator
# MAGIC @dp.expect_all({
# MAGIC     "valid_id": "id IS NOT NULL",
# MAGIC     "valid_amount": "amount > 0",
# MAGIC     "valid_date": "date <= current_date()"
# MAGIC })
# MAGIC
# MAGIC # Drop if ANY fails
# MAGIC @dp.expect_all_or_drop({
# MAGIC     "not_null_a": "a IS NOT NULL",
# MAGIC     "not_null_b": "b IS NOT NULL"
# MAGIC })
# MAGIC
# MAGIC # Fail if ANY fails
# MAGIC @dp.expect_all_or_fail({
# MAGIC     "critical_1": "...",
# MAGIC     "critical_2": "..."
# MAGIC })
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# Delete pipeline
try:
    w.pipelines.delete(pipeline_id)
    print(f"✅ Deleted pipeline: {pipeline_id}")
except Exception as e:
    print(f"Could not delete pipeline: {e}")

# Delete tables
for table in ["bronze_orders_dq", "silver_orders_dq", "gold_daily_orders_dq"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{table}")
print("✅ Deleted demo tables")

# Delete raw data
dbutils.fs.rm(RAW_PATH, recurse=True)
print("✅ Deleted raw data")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC | Decorator | Behavior | Use When |
# MAGIC |-----------|----------|----------|
# MAGIC | `@dp.expect` | Log violation, keep record | Monitoring, non-blocking |
# MAGIC | `@dp.expect_or_drop` | Log violation, drop record | Data cleaning |
# MAGIC | `@dp.expect_or_fail` | Stop pipeline | Critical integrity rules |
# MAGIC
# MAGIC **Event log** (`__event_log`) gives you full visibility into pass/fail counts — use it for monitoring and alerting.
# MAGIC
# MAGIC ---
# MAGIC **Next**: [day_2_04b_sql_alerts — SQL Alerts Challenge]($./day_2_04b_sql_alerts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete

# COMMAND ----------

mark_notebook_complete("day_2_04a_data_quality")