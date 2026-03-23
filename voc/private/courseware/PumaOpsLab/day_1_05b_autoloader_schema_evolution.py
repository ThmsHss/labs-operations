# Databricks notebook source
# MAGIC %md
# MAGIC # Day 1 — 05b: Auto Loader & Schema Evolution
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Set up **Auto Loader** (`cloudFiles`) to incrementally ingest JSON files from a Volume
# MAGIC 2. Upload files and watch Auto Loader pick them up automatically
# MAGIC 3. Experiment with **schema evolution modes** — `addNewColumns`, `rescue`, `failOnNewColumns`, `none`
# MAGIC 4. Inspect the **`_rescued_data`** column for schema mismatches
# MAGIC 5. Check **what files were processed** in the last run
# MAGIC 6. Understand **checkpointing** — what it is, how to explore it, and how to reset it
# MAGIC
# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup: Create volume and sample data
# MAGIC This cell creates the `autoloader_lab` volume and generates four batches of
# MAGIC sample JSON files (each with a different schema variant) if they don't already exist.

# COMMAND ----------

import json, random
from datetime import datetime, timedelta

AL_VOLUME = "autoloader_lab"
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{AL_VOLUME}")

al_base     = f"/Volumes/{CATALOG}/{SCHEMA}/{AL_VOLUME}"
sample_path = f"{al_base}/sample_files"

def _sample_files_exist():
    try:
        for b in ["batch_1", "batch_2", "batch_3", "batch_4"]:
            if len(dbutils.fs.ls(f"{sample_path}/{b}")) == 0:
                return False
        return True
    except Exception:
        return False

if not _sample_files_exist():
    random.seed(99)
    return_reasons = ["Defective", "Wrong Size", "Changed Mind", "Wrong Color", "Late Delivery", "Not As Described"]
    product_ids_al = [f"PROD-{i:05d}" for i in range(1, 201)]
    RECORDS_PER_FILE, FILES_PER_BATCH = 100, 5

    def _write_json_files(records_fn, folder):
        for fnum in range(1, FILES_PER_BATCH + 1):
            rows = [records_fn() for _ in range(RECORDS_PER_FILE)]
            content = "\n".join(json.dumps(r) for r in rows)
            dbutils.fs.put(f"{folder}/returns_{fnum:03d}.json", content, overwrite=True)

    def _batch1():
        return {"order_id": f"ORD-{random.randint(1,500000):07d}", "product_id": random.choice(product_ids_al),
                "return_reason": random.choice(return_reasons),
                "return_date": (datetime(2025,1,1)+timedelta(days=random.randint(0,365))).strftime("%Y-%m-%d"),
                "refund_amount": round(random.uniform(15.0,249.99),2)}
    def _batch2():
        r = _batch1(); r["customer_satisfaction_score"] = random.randint(1,5); return r
    def _batch3():
        return {"order_id": f"ORD-{random.randint(1,500000):07d}", "product_id": random.choice(product_ids_al),
                "return_reason": random.choice(return_reasons),
                "return_date": (datetime(2025,1,1)+timedelta(days=random.randint(0,365))).strftime("%Y-%m-%d"),
                "refund_value": round(random.uniform(15.0,249.99),2),
                "return_channel": random.choice(["Online Portal","In-Store","Customer Service","Mobile App"])}
    def _batch4():
        return {"order_id": f"ORD-{random.randint(1,500000):07d}", "product_id": random.choice(product_ids_al),
                "return_reason": random.choice(return_reasons),
                "return_date": (datetime(2025,1,1)+timedelta(days=random.randint(0,365))).strftime("%Y-%m-%d"),
                "refund_amount": f"EUR {round(random.uniform(15.0,249.99),2)}"}

    for d in ["sample_files/batch_1","sample_files/batch_2","sample_files/batch_3","sample_files/batch_4"]:
        dbutils.fs.mkdirs(f"{al_base}/{d}")
    _write_json_files(_batch1, f"{sample_path}/batch_1")
    _write_json_files(_batch2, f"{sample_path}/batch_2")
    _write_json_files(_batch3, f"{sample_path}/batch_3")
    _write_json_files(_batch4, f"{sample_path}/batch_4")
    print("Generated sample files (4 batches x 5 files x 100 records)")
else:
    print("Sample files already exist — skipping generation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup paths
# MAGIC Each participant gets their own landing zone, checkpoint, and target table.

# COMMAND ----------

landing_path = f"{al_base}/{user_prefix}/landing"
checkpoint   = f"{al_base}/{user_prefix}/checkpoint/returns_bronze"
schema_loc   = f"{al_base}/{user_prefix}/schema_location/returns_bronze"
target_table = f"{CATALOG}.{SCHEMA}.{user_prefix}_product_returns_bronze"

print(f"Landing zone : {landing_path}")
print(f"Sample files : {sample_path}")
print(f"Checkpoint   : {checkpoint}")
print(f"Target table : {target_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC Let's confirm the sample batches are in place:

# COMMAND ----------

for batch in ["batch_1", "batch_2", "batch_3", "batch_4"]:
    files = dbutils.fs.ls(f"{sample_path}/{batch}")
    print(f"  {batch}: {len(files)} files")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Auto Loader Introduction
# MAGIC
# MAGIC **Auto Loader** (`cloudFiles`) is a Structured Streaming source that incrementally
# MAGIC processes new files as they land in cloud storage (including Unity Catalog Volumes).
# MAGIC
# MAGIC | Feature | Benefit |
# MAGIC |---------|---------|
# MAGIC | Incremental processing | Only processes **new** files, not the entire directory |
# MAGIC | Schema inference | Automatically infers and evolves the schema from files |
# MAGIC | Exactly-once guarantees | Checkpoint tracks which files have been processed |
# MAGIC | File notification or listing | Scales to millions of files via notification mode; listing mode works everywhere |
# MAGIC | Rescue column | Captures data that doesn't match the schema instead of failing |
# MAGIC
# MAGIC ### How it works (simplified)
# MAGIC
# MAGIC ```
# MAGIC   New files land in Volume
# MAGIC          |
# MAGIC          v
# MAGIC   Auto Loader detects them
# MAGIC   (checkpoint tracks state)
# MAGIC          |
# MAGIC          v
# MAGIC   Schema inference / evolution
# MAGIC          |
# MAGIC          v
# MAGIC   Write to Delta table
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a. Clean slate
# MAGIC Drop the target table and clear any previous checkpoint so we start fresh.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {target_table}")

dbutils.fs.rm(checkpoint, recurse=True)
dbutils.fs.rm(schema_loc, recurse=True)

try:
    for f in dbutils.fs.ls(landing_path):
        dbutils.fs.rm(f.path, recurse=True)
except Exception:
    pass

dbutils.fs.mkdirs(landing_path)
print(f"Clean slate: table dropped, checkpoint cleared, landing zone empty")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. Define the Auto Loader stream
# MAGIC
# MAGIC We start with the **default** schema evolution behavior. Key options:
# MAGIC - `cloudFiles.format` — the file format to read (`json`)
# MAGIC - `cloudFiles.schemaLocation` — where Auto Loader stores the inferred schema
# MAGIC - `cloudFiles.inferColumnTypes` — infer types (otherwise everything is string)

# COMMAND ----------

def start_autoloader_stream(schema_evolution_mode=None, rescue_enabled=True, max_retries=5):
    """Start (or restart) the Auto Loader stream with the given settings.

    When using addNewColumns mode, Auto Loader may throw an exception on the
    first encounter with new columns. It updates the schema automatically and
    a retry succeeds. This function retries up to max_retries times with a
    short delay to allow the schema file to be written.
    """
    import time
    for attempt in range(1, max_retries + 1):
        try:
            reader = (
                spark.readStream
                .format("cloudFiles")
                .option("cloudFiles.format", "json")
                .option("cloudFiles.schemaLocation", schema_loc)
                .option("cloudFiles.inferColumnTypes", "true")
            )

            if schema_evolution_mode:
                reader = reader.option("cloudFiles.schemaEvolutionMode", schema_evolution_mode)

            if rescue_enabled:
                reader = reader.option("rescuedDataColumn", "_rescued_data")

            df = reader.load(landing_path)

            query = (
                df.writeStream
                .format("delta")
                .option("checkpointLocation", checkpoint)
                .option("mergeSchema", "true")
                .outputMode("append")
                .trigger(availableNow=True)
                .toTable(target_table)
            )
            query.awaitTermination()
            return query
        except Exception as e:
            if "UNKNOWN_FIELD_EXCEPTION" in str(e) and attempt < max_retries:
                print(f"  Schema evolved — retrying ({attempt}/{max_retries})...")
                time.sleep(2)
                continue
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Exercise — First Data Landing
# MAGIC
# MAGIC **Your task**: Copy the `batch_1` files into the `landing/` zone, then run Auto Loader
# MAGIC to ingest them.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Copy batch 1 files into the landing zone

# COMMAND ----------

for f in dbutils.fs.ls(f"{sample_path}/batch_1"):
    dbutils.fs.cp(f.path, f"{landing_path}/{f.name}")
print("Batch 1 files copied to landing zone")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Run the Auto Loader stream

# COMMAND ----------

start_autoloader_stream()
print("Auto Loader stream completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Verify the results

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS total_rows FROM {target_table}"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {target_table} LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC You should see **500 rows** (5 files x 100 records) with columns:
# MAGIC `order_id`, `product_id`, `return_reason`, `return_date`, `refund_amount`, and
# MAGIC `_rescued_data` (all NULL since there are no mismatches yet).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Schema Evolution Modes
# MAGIC
# MAGIC Auto Loader can handle schema changes in incoming files in four ways:
# MAGIC
# MAGIC | Mode | Behavior | Use Case |
# MAGIC |------|----------|----------|
# MAGIC | **`addNewColumns`** | Automatically adds new columns to the table schema | Most flexible; good for evolving sources |
# MAGIC | **`failOnNewColumns`** | Fails the stream when new columns appear | Strict schemas; forces review before accepting changes |
# MAGIC | **`rescue`** | Puts data that doesn't fit the schema into `_rescued_data` | Safe default; nothing is lost but schema stays fixed |
# MAGIC | **`none`** | Ignores new columns entirely; only reads known columns | Minimal overhead; unknown data is silently dropped |
# MAGIC
# MAGIC The **`_rescued_data`** column is a JSON string that captures any field that couldn't
# MAGIC be mapped to the existing schema — renamed columns, new columns, type mismatches, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Exercise — Schema Evolution in Action
# MAGIC
# MAGIC We'll run three scenarios, each with a different batch and evolution mode.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario A: `addNewColumns` with Batch 2
# MAGIC
# MAGIC Batch 2 adds a new column: **`customer_satisfaction_score`** (integer 1-5).
# MAGIC
# MAGIC With `addNewColumns`, Auto Loader will automatically add this column to the table schema.

# COMMAND ----------

# MAGIC %md
# MAGIC **Step A1**: Reset checkpoint and schema to allow re-inference with the new mode.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {target_table}")
dbutils.fs.rm(checkpoint, recurse=True)
dbutils.fs.rm(schema_loc, recurse=True)

try:
    for f in dbutils.fs.ls(landing_path):
        dbutils.fs.rm(f.path, recurse=True)
except Exception:
    pass

dbutils.fs.mkdirs(landing_path)
print("Reset complete")

# COMMAND ----------

# MAGIC %md
# MAGIC **Step A2**: Copy batch 1 first (base schema), then batch 2 (with the new column).

# COMMAND ----------

for f in dbutils.fs.ls(f"{sample_path}/batch_1"):
    dbutils.fs.cp(f.path, f"{landing_path}/{f.name}")

start_autoloader_stream(schema_evolution_mode="addNewColumns")
print("Batch 1 ingested")

# COMMAND ----------

display(spark.sql(f"DESCRIBE {target_table}"))

# COMMAND ----------

for f in dbutils.fs.ls(f"{sample_path}/batch_2"):
    dbutils.fs.cp(f.path, f"{landing_path}/b2_{f.name}")

start_autoloader_stream(schema_evolution_mode="addNewColumns")
print("Batch 2 ingested with addNewColumns")

# COMMAND ----------

display(spark.sql(f"DESCRIBE {target_table}"))

# COMMAND ----------

display(spark.sql(f"""
SELECT
  customer_satisfaction_score,
  count(*) AS cnt
FROM {target_table}
GROUP BY customer_satisfaction_score
ORDER BY customer_satisfaction_score
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Result**: The `customer_satisfaction_score` column was added automatically.
# MAGIC > Batch 1 rows have `NULL` for this column (as expected), while batch 2 rows have values.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario B: `rescue` mode with Batch 3
# MAGIC
# MAGIC Batch 3 has a **renamed column** (`refund_amount` -> `refund_value`) and a **new column**
# MAGIC (`return_channel`). In `rescue` mode, any field that doesn't match the existing schema
# MAGIC goes into `_rescued_data` instead of creating a new column or failing.

# COMMAND ----------

# MAGIC %md
# MAGIC **Step B1**: Reset and re-ingest batch 1 as the base, this time with `rescue` mode.

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {target_table}")
dbutils.fs.rm(checkpoint, recurse=True)
dbutils.fs.rm(schema_loc, recurse=True)

try:
    for f in dbutils.fs.ls(landing_path):
        dbutils.fs.rm(f.path, recurse=True)
except Exception:
    pass

dbutils.fs.mkdirs(landing_path)

for f in dbutils.fs.ls(f"{sample_path}/batch_1"):
    dbutils.fs.cp(f.path, f"{landing_path}/{f.name}")

start_autoloader_stream(schema_evolution_mode="rescue")
print("Batch 1 ingested with rescue mode")

# COMMAND ----------

# MAGIC %md
# MAGIC **Step B2**: Copy batch 3 (renamed + new column) and run the stream.

# COMMAND ----------

for f in dbutils.fs.ls(f"{sample_path}/batch_3"):
    dbutils.fs.cp(f.path, f"{landing_path}/b3_{f.name}")

start_autoloader_stream(schema_evolution_mode="rescue")
print("Batch 3 ingested with rescue mode")

# COMMAND ----------

display(spark.sql(f"DESCRIBE {target_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Step B3**: Inspect the `_rescued_data` column to see what was captured.

# COMMAND ----------

display(spark.sql(f"""
SELECT
  order_id,
  refund_amount,
  _rescued_data
FROM {target_table}
WHERE _rescued_data IS NOT NULL
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Result**: For batch 3 rows, `refund_amount` is `NULL` (because the field was renamed
# MAGIC > to `refund_value` in the source). The `_rescued_data` column contains a JSON object
# MAGIC > with the unrecognized fields: `{"refund_value": 42.50, "return_channel": "Online Portal"}`.
# MAGIC >
# MAGIC > This is powerful: **no data is lost**, and you can parse `_rescued_data` later to
# MAGIC > reconcile the schema change.

# COMMAND ----------

display(spark.sql(f"""
SELECT
  order_id,
  _rescued_data,
  _rescued_data:refund_value::double AS rescued_refund_value,
  _rescued_data:return_channel::string AS rescued_return_channel
FROM {target_table}
WHERE _rescued_data IS NOT NULL
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario C: Your turn — experiment!
# MAGIC
# MAGIC **Exercise**: Try ingesting **batch 4** (where `refund_amount` is a string like
# MAGIC `"EUR 42.50"` instead of a double).
# MAGIC
# MAGIC 1. Reset the table, checkpoint, and schema location.
# MAGIC 2. Ingest batch 1 as the base.
# MAGIC 3. Choose a schema evolution mode — predict what will happen, then run it.
# MAGIC 4. Check the results: did the data land in the column or in `_rescued_data`?

# COMMAND ----------

# TODO: Reset the environment
# spark.sql(f"DROP TABLE IF EXISTS {target_table}")
# dbutils.fs.rm(checkpoint, recurse=True)
# dbutils.fs.rm(schema_loc, recurse=True)
# for f in dbutils.fs.ls(landing_path):
#     dbutils.fs.rm(f.path, recurse=True)
# dbutils.fs.mkdirs(landing_path)

# COMMAND ----------

# TODO: Ingest batch 1 as base


# COMMAND ----------

# TODO: Copy batch 4 into landing and run the stream with your chosen mode


# COMMAND ----------

# TODO: Inspect the results


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: Auto Loader Internals
# MAGIC
# MAGIC Understanding what Auto Loader did under the hood is essential for production operations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. What files were processed?
# MAGIC
# MAGIC The `cloud_files_state` table-valued function shows which files Auto Loader has
# MAGIC already processed for a given checkpoint path.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM cloud_files_state('{checkpoint}')"))

# COMMAND ----------

# MAGIC %md
# MAGIC Each row represents a file that Auto Loader has seen. Key columns:
# MAGIC | Column | Meaning |
# MAGIC |--------|---------|
# MAGIC | `path` | Full path to the file |
# MAGIC | `size` | File size in bytes |
# MAGIC | `create_timestamp` | When the file was created / detected |
# MAGIC | `commit_timestamp` | When it was committed to the target table |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Auto Loader Configuration Deep Dive
# MAGIC
# MAGIC Here are the most important `cloudFiles.*` options and when to use them:
# MAGIC
# MAGIC | Option | Default | Description |
# MAGIC |--------|---------|-------------|
# MAGIC | `cloudFiles.format` | *(required)* | File format: `json`, `csv`, `parquet`, `avro`, `text`, `binaryFile` |
# MAGIC | `cloudFiles.schemaLocation` | *(required for schema inference)* | Path where inferred schema is stored and tracked |
# MAGIC | `cloudFiles.inferColumnTypes` | `false` | If `true`, infer actual types; otherwise everything is `STRING` |
# MAGIC | `cloudFiles.schemaEvolutionMode` | `addNewColumns` | How to handle schema changes: `addNewColumns`, `rescue`, `failOnNewColumns`, `none` |
# MAGIC | `cloudFiles.maxFilesPerTrigger` | `1000` | Max files to process per micro-batch (throttling) |
# MAGIC | `cloudFiles.maxBytesPerTrigger` | *(unlimited)* | Max bytes per micro-batch |
# MAGIC | `cloudFiles.useNotifications` | `false` | Use file notification mode (SNS/SQS/EventGrid) instead of directory listing |
# MAGIC | `rescuedDataColumn` | *(none)* | Column name for rescued data (e.g. `_rescued_data`) |
# MAGIC
# MAGIC > **Production tip**: In production, always set `cloudFiles.schemaLocation` and
# MAGIC > `rescuedDataColumn`. This way schema is persisted across restarts and no data is
# MAGIC > ever silently lost.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. Checkpointing — How It Works
# MAGIC
# MAGIC The checkpoint directory stores the **state** of the streaming query:
# MAGIC - Which files have been processed
# MAGIC - The current offset (so it knows where to resume)
# MAGIC - Commit log (which micro-batches succeeded)
# MAGIC
# MAGIC Let's explore the checkpoint directory:

# COMMAND ----------

def list_recursive(path, indent=0):
    """Recursively list a directory tree."""
    try:
        entries = dbutils.fs.ls(path)
    except Exception:
        return
    for entry in entries[:15]:
        prefix = "  " * indent
        if entry.isDir():
            print(f"{prefix}  {entry.name}")
            list_recursive(entry.path, indent + 1)
        else:
            print(f"{prefix}  {entry.name} ({entry.size:,} bytes)")

list_recursive(checkpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC Key checkpoint sub-directories:
# MAGIC
# MAGIC | Directory | Purpose |
# MAGIC |-----------|---------|
# MAGIC | `offsets/` | Records the start offset of each micro-batch |
# MAGIC | `commits/` | Records which micro-batches have been committed |
# MAGIC | `sources/` | Auto Loader file-tracking state (which files were seen) |
# MAGIC | `metadata` | Stream metadata (query ID, etc.) |
# MAGIC
# MAGIC > **The golden rule**: If you delete the checkpoint, Auto Loader will **reprocess
# MAGIC > all files** from the landing zone from scratch. This is useful for recovery but
# MAGIC > means you may get duplicates if the target table still exists.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d. Schema Location — Inferred Schema Storage
# MAGIC
# MAGIC Auto Loader stores the inferred (and evolved) schema in the `schemaLocation` path:

# COMMAND ----------

try:
    for f in dbutils.fs.ls(schema_loc):
        print(f"  {f.name:>30s}  {f.size:>6,} bytes")
        if f.isDir():
            for child in dbutils.fs.ls(f.path):
                print(f"    {child.name:>28s}  {child.size:>6,} bytes")
except Exception as e:
    print(f"Schema location not found: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC The `_schemas/` directory contains JSON files with the inferred schema at each
# MAGIC evolution point. If you delete this along with the checkpoint, Auto Loader will
# MAGIC re-infer the schema from scratch on the next run.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5e. Exercise: Reset and Reprocess
# MAGIC
# MAGIC **Your task**:
# MAGIC 1. Note the current row count in your target table.
# MAGIC 2. Delete the **checkpoint** and **schema location** (but keep the table and landing zone files).
# MAGIC 3. Run the Auto Loader stream again.
# MAGIC 4. Check the row count — what happened? Why?
# MAGIC
# MAGIC > **Think about it**: After resetting the checkpoint, Auto Loader doesn't know which
# MAGIC > files it already processed. It will re-read all files in the landing zone and append
# MAGIC > them again — resulting in **duplicates** in the target table.
# MAGIC >
# MAGIC > In production, if you need to reset a checkpoint, you should also **truncate or
# MAGIC > recreate** the target table.

# COMMAND ----------

display(spark.sql(f"SELECT count(*) AS rows_before_reset FROM {target_table}"))

# COMMAND ----------

# TODO: Step 2 — delete checkpoint and schema location
# dbutils.fs.rm(checkpoint, recurse=True)
# dbutils.fs.rm(schema_loc, recurse=True)
# print("Checkpoint and schema location deleted")

# COMMAND ----------

# TODO: Step 3 — re-run the stream
# start_autoloader_stream()

# COMMAND ----------

# TODO: Step 4 — check the row count. Is it doubled? Why?
# display(spark.sql(f"SELECT count(*) AS rows_after_reset FROM {target_table}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Key Takeaway |
# MAGIC |---------|-------------|
# MAGIC | Auto Loader | Incrementally processes new files; exactly-once via checkpoint |
# MAGIC | Schema evolution modes | `addNewColumns` (flexible), `rescue` (safe), `failOnNewColumns` (strict), `none` (ignore) |
# MAGIC | `_rescued_data` | Captures any data that doesn't match the current schema — nothing is lost |
# MAGIC | `cloud_files_state` | Shows exactly which files were processed and when |
# MAGIC | Checkpoint | Tracks stream state; deleting it causes full reprocessing |
# MAGIC | Schema location | Stores inferred schema; delete alongside checkpoint for a full reset |
# MAGIC
# MAGIC ---
# MAGIC **End of Block 5**. Next up: Block 6 — Cluster Types, Costs, Autoscaling & Cluster Policies.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_05b_autoloader_schema_evolution")
