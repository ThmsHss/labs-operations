# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1 — 04a: Delta Lake Foundations
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Understand how the Delta **transaction log** tracks every change
# MAGIC 2. See **ACID transactions** in action with INSERT, UPDATE, and DELETE
# MAGIC 3. Use **time travel** to query and restore previous versions of a table
# MAGIC 4. Use `DESCRIBE HISTORY` and `DESCRIBE DETAIL` for operational metadata

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ### Your personal lab table
# MAGIC Each participant gets their own table so you can work in parallel without conflicts.

# COMMAND ----------

DELTA_TABLE = f"{FQ}.{user_prefix}_delta_lab_orders"
print(f"Your Delta lab table: {DELTA_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: The Transaction Log
# MAGIC
# MAGIC Every Delta table stores a **transaction log** in a hidden `_delta_log/` directory.
# MAGIC Each write operation (INSERT, UPDATE, DELETE, MERGE, schema change, ...) creates a new
# MAGIC JSON file in the log. This is the **single source of truth** — it determines which
# MAGIC Parquet files belong to the current version of the table.
# MAGIC
# MAGIC Key concepts:
# MAGIC - Each commit = one JSON file (`00000000000000000000.json`, `...01.json`, ...)
# MAGIC - Every 10 commits, Delta writes a **checkpoint** (Parquet) for faster reads
# MAGIC - The log records **add** / **remove** actions, commit metadata, and schema info

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a. Create a working copy of orders
# MAGIC We take a small subset so the log stays easy to inspect.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {DELTA_TABLE}")

# Disable auto-compact / optimized writes so background OPTIMIZE operations
# don't insert extra versions and shift the version numbers we rely on below.
spark.sql(f"""
CREATE TABLE {DELTA_TABLE}
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'false',
  'delta.autoOptimize.optimizeWrite' = 'false'
)
AS
SELECT *
FROM orders
WHERE order_timestamp >= '2025-01-01'
LIMIT 5000
""")

display(spark.sql(f"SELECT format_number(count(*), 0) AS row_count FROM {DELTA_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: ACID Transactions
# MAGIC
# MAGIC Delta Lake provides full **ACID** guarantees:
# MAGIC - **Atomicity** — each operation either fully succeeds or fully rolls back
# MAGIC - **Consistency** — the table is always in a valid state
# MAGIC - **Isolation** — concurrent readers see a consistent snapshot
# MAGIC - **Durability** — committed data is never lost
# MAGIC
# MAGIC Let's generate several versions through mutations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. INSERT new rows (version 1)

# COMMAND ----------

spark.sql(f"""
INSERT INTO {DELTA_TABLE}
VALUES
  ('ORD-LAB-001', 'CUST-000001', 'PROD-00001', 2, 89.99, 179.98, 'Web', 'Completed', current_timestamp(), 'EMEA'),
  ('ORD-LAB-002', 'CUST-000002', 'PROD-00042', 1, 129.99, 129.99, 'Retail Store', 'Shipped', current_timestamp(), 'EMEA')
""")
print("Inserted 2 rows (version 1)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. UPDATE rows (version 2)

# COMMAND ----------

spark.sql(f"""
UPDATE {DELTA_TABLE}
SET status = 'Cancelled', total_amount = 0
WHERE order_id = 'ORD-LAB-001'
""")
print("Updated ORD-LAB-001 to Cancelled (version 2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. DELETE rows (version 3)

# COMMAND ----------

spark.sql(f"DELETE FROM {DELTA_TABLE} WHERE order_id = 'ORD-LAB-001'")
print("Deleted ORD-LAB-001 (version 3)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2d. DESCRIBE HISTORY — the full audit trail

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {DELTA_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Each row represents one version. Note the `operation`, `operationParameters`,
# MAGIC `operationMetrics` (rows affected, files added/removed), and `userName`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Time Travel
# MAGIC
# MAGIC Because Delta keeps every version in the transaction log, you can query **any**
# MAGIC historical version of a table — either by **version number** or by **timestamp**.
# MAGIC
# MAGIC This is invaluable for:
# MAGIC - Auditing what data looked like at a past point in time
# MAGIC - Recovering accidentally deleted or overwritten data
# MAGIC - Reproducing ML training datasets

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Query by version number

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS rows_at_v0 FROM {DELTA_TABLE} VERSION AS OF 0"))

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS rows_at_v1 FROM {DELTA_TABLE} VERSION AS OF 1"))

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS rows_current FROM {DELTA_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Query by timestamp
# MAGIC Useful when you know *when* something went wrong but not the exact version.

# COMMAND ----------

display(spark.sql(f"""
SELECT version, timestamp
FROM (DESCRIBE HISTORY {DELTA_TABLE})
WHERE version IN (0, 1, 2, 3)
ORDER BY version
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC You can use any of those timestamps:
# MAGIC ```sql
# MAGIC SELECT * FROM my_table TIMESTAMP AS OF '2026-03-19T14:00:00Z'
# MAGIC ```
# MAGIC *(Replace with an actual timestamp from the history above.)*

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. Compare two versions
# MAGIC A common pattern: how many rows changed between v1 and v3?

# COMMAND ----------

display(spark.sql(f"""
SELECT
  (SELECT count(*) FROM {DELTA_TABLE} VERSION AS OF 1) AS v1_count,
  (SELECT count(*) FROM {DELTA_TABLE} VERSION AS OF 3) AS v3_count,
  (SELECT count(*) FROM {DELTA_TABLE} VERSION AS OF 1) -
  (SELECT count(*) FROM {DELTA_TABLE} VERSION AS OF 3) AS rows_removed
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3d. Find the deleted row

# COMMAND ----------

display(spark.sql(f"""
SELECT * FROM {DELTA_TABLE} VERSION AS OF 1
WHERE order_id = 'ORD-LAB-001'
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3e. RESTORE TABLE
# MAGIC If you need to **roll back** the table to a previous version, use `RESTORE`.
# MAGIC This creates a *new* version that makes the table look like the target version.

# COMMAND ----------

spark.sql(f"RESTORE TABLE {DELTA_TABLE} TO VERSION AS OF 1")
print("Restored to version 1")

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS rows_after_restore FROM {DELTA_TABLE}"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {DELTA_TABLE} WHERE order_id LIKE 'ORD-LAB%'"))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {DELTA_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3f. DESCRIBE DETAIL — table metadata at a glance

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {DELTA_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Key fields in `DESCRIBE DETAIL`:
# MAGIC | Field | Meaning |
# MAGIC |-------|---------|
# MAGIC | `format` | Always `delta` |
# MAGIC | `location` | Cloud storage path |
# MAGIC | `numFiles` | Current number of data files |
# MAGIC | `sizeInBytes` | Total table size |
# MAGIC | `minReaderVersion` / `minWriterVersion` | Protocol version |
# MAGIC | `properties` | Table properties (e.g. `delta.enableChangeDataFeed`) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercises
# MAGIC
# MAGIC ### Exercise 1: Recover deleted data with time travel
# MAGIC
# MAGIC 1. Run the cell below to delete all "Returned" orders from the table.
# MAGIC 2. Verify that the returned orders are gone.
# MAGIC 3. Use **time travel** to find out how many "Returned" orders existed *before* the delete.
# MAGIC 4. **RESTORE** the table to get them back.

# COMMAND ----------

spark.sql(f"DELETE FROM {DELTA_TABLE} WHERE status = 'Returned'")
print("Deleted all 'Returned' orders")

# COMMAND ----------

display(spark.sql(f"""
SELECT status, count(*) AS cnt
FROM {DELTA_TABLE}
GROUP BY status
ORDER BY status
"""))

# COMMAND ----------

# TODO: Step 3 — query a previous version to find how many "Returned" orders there were


# COMMAND ----------

# TODO: Step 4 — RESTORE the table to recover the deleted rows


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Investigate table metadata
# MAGIC
# MAGIC Use `DESCRIBE HISTORY` and `DESCRIBE DETAIL` to answer these questions:
# MAGIC 1. How many **versions** does the table have now?
# MAGIC 2. What is the **total size in bytes** of the table?
# MAGIC 3. How many **data files** make up the current version?

# COMMAND ----------

# TODO: Write your queries here
# display(spark.sql(f"DESCRIBE HISTORY {DELTA_TABLE}"))
# display(spark.sql(f"DESCRIBE DETAIL {DELTA_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next**: [day_1_04b_autoloader_schema_evolution — Auto Loader with schema evolution exercises]($./day_1_04b_autoloader_schema_evolution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_04a_delta_foundations")
