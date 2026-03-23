# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2 — 03: Performance Optimization
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC **Duration**: ~1 hour
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Understand table maintenance essentials — **OPTIMIZE** and **VACUUM**
# MAGIC 2. Learn the evolution from **partitioning** → **Z-ORDER** → **Liquid Clustering**
# MAGIC 3. See how **Predictive Optimization** automates all of it on managed tables
# MAGIC 4. **Lab 1**: Debug data skew on classic compute using the **Spark UI**
# MAGIC 5. **Lab 2**: Use the **Query Profile** on a SQL Warehouse to diagnose and fix a slow query
# MAGIC
# MAGIC > **⚠️ Compute switching guide:**
# MAGIC > - **Parts 1–3 + Lab 1**: Attach to **`puma-ops-lab-classic`** (classic cluster) — you need the Spark UI
# MAGIC > - **Lab 2**: Switch to **`PUMA_OPS_SHARED_WAREHOUSE`** (SQL Warehouse) — you need the Query Profile

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⏳ One-time setup: Pre-build the Query Profile lab table
# MAGIC
# MAGIC The cell below creates a shared **template table** (~250M rows) that you'll use in Lab 2.
# MAGIC It runs once and takes ~7 minutes. If the table already exists it skips the creation.
# MAGIC
# MAGIC **Run this cell now** — then continue with Part 1 while it builds in the background.

# COMMAND ----------

PERF_TEMPLATE = f"{CATALOG}.{SCHEMA}.perf_lab_gold_template"

if spark.catalog.tableExists(PERF_TEMPLATE):
    row_count = spark.sql(f"SELECT count(*) AS cnt FROM {PERF_TEMPLATE}").collect()[0]["cnt"]
    print(f"✅ Template table already exists: {PERF_TEMPLATE} ({row_count:,} rows) — nothing to do")
else:
    print(f"Creating {PERF_TEMPLATE} (~250M rows, ~7 minutes)...")
    spark.sql(f"""
    CREATE TABLE {PERF_TEMPLATE} AS
    SELECT g.*, uuid() AS batch_id
    FROM gold_order_summary g
    CROSS JOIN (SELECT explode(sequence(1, 50))) AS multiplier
    """)
    for i in range(9):
        spark.sql(f"""
            INSERT INTO {PERF_TEMPLATE}
            SELECT g.*, uuid() AS batch_id
            FROM gold_order_summary g
            CROSS JOIN (SELECT explode(sequence(1, 50))) AS multiplier
        """)
        print(f"  Batch {i+2}/10 inserted")
    row_count = spark.sql(f"SELECT count(*) AS cnt FROM {PERF_TEMPLATE}").collect()[0]["cnt"]
    print(f"✅ Created {PERF_TEMPLATE} with {row_count:,} rows (all regions mixed, no clustering)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Table Maintenance Essentials
# MAGIC
# MAGIC Every Delta table accumulates **small files** over time (from streaming appends, frequent
# MAGIC merges, or many small writes). These small files hurt read performance because the engine
# MAGIC spends more time opening/closing files than actually reading data.
# MAGIC
# MAGIC Two commands handle this:
# MAGIC
# MAGIC | Command | What It Does |
# MAGIC |---------|-------------|
# MAGIC | **`OPTIMIZE`** | Compacts small files into larger ones (target ~1 GB by default). Can also apply clustering. |
# MAGIC | **`VACUUM`** | Removes old data files no longer referenced by the current table version (default retention: 7 days). |
# MAGIC
# MAGIC > **Key insight**: On Unity Catalog **managed tables**, you should **not** be running these
# MAGIC > manually. That's what **Predictive Optimization** is for. But understanding what they do
# MAGIC > helps you reason about performance.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup: Create your personal copy of the gold table
# MAGIC Each participant gets their own table so the maintenance operations don't affect anyone else.

# COMMAND ----------

MAINT_TABLE = f"{FQ}.{user_prefix}_gold_maintenance"
spark.sql(f"DROP TABLE IF EXISTS {MAINT_TABLE}")
spark.sql(f"""
CREATE TABLE {MAINT_TABLE}
TBLPROPERTIES (
  'delta.autoOptimize.autoCompact' = 'false',
  'delta.autoOptimize.optimizeWrite' = 'false'
)
AS SELECT * FROM gold_order_summary
""")
row_count = spark.sql(f"SELECT count(*) AS cnt FROM {MAINT_TABLE}").collect()[0]["cnt"]
print(f"Created {MAINT_TABLE} with {row_count:,} rows (fresh — no optimizations applied)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a. Inspect the current state of your table
# MAGIC This table was just created with a single `CREATE TABLE ... AS SELECT` — no
# MAGIC maintenance has been performed on it yet.

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {MAINT_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC **What to note:**
# MAGIC - `numFiles` — how many data files make up the table
# MAGIC - `sizeInBytes` — total table size on storage
# MAGIC - `clusteringColumns` — empty means no clustering configured
# MAGIC - `properties` — any table properties set

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {MAINT_TABLE} LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. OPTIMIZE — compact small files
# MAGIC
# MAGIC `OPTIMIZE` rewrites small files into larger, more efficient ones.
# MAGIC After running it, queries scan fewer files → faster reads.

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {MAINT_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC Check the result — the output tells you how many files were compacted and the resulting sizes.
# MAGIC Let's verify with `DESCRIBE DETAIL`:

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {MAINT_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1c. VACUUM — clean up old files
# MAGIC
# MAGIC After OPTIMIZE, the old small files still exist on storage (they're needed for time travel).
# MAGIC `VACUUM` removes files older than the retention period (default 7 days).
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────┐
# MAGIC │  WRITE small files  →  OPTIMIZE merges  →  VACUUM     │
# MAGIC │  (many small)          (fewer large)       (removes    │
# MAGIC │                                            old files)  │
# MAGIC └────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > **⚠️ VACUUM is destructive** — once files are removed you can no longer time-travel
# MAGIC > to versions that depend on them. The 7-day default protects against accidental data loss.

# COMMAND ----------

display(spark.sql(f"VACUUM {MAINT_TABLE} DRY RUN"))

# COMMAND ----------

# MAGIC %md
# MAGIC Since our table was just created, there may not be much to vacuum yet. In production,
# MAGIC tables that receive daily merges accumulate hundreds of stale files over a week.
# MAGIC
# MAGIC > **Bottom line**: OPTIMIZE and VACUUM are essential — but running them manually on
# MAGIC > a schedule is operational toil. Let's see how Databricks automates this.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: From Partitioning → Automatic Liquid Clustering
# MAGIC
# MAGIC ### The evolution of data layout in Delta Lake
# MAGIC
# MAGIC | Generation | Technique | How It Works | Limitations |
# MAGIC |-----------|-----------|-------------|-------------|
# MAGIC | **Gen 1** | **Hive-style Partitioning** | Physically separates data into subdirectories by column value | Over-partitioning creates thousands of tiny directories; can't change partition column after creation |
# MAGIC | **Gen 2** | **Z-ORDER** | Co-locates related data within files using space-filling curves | Must run `OPTIMIZE ... ZORDER BY` manually; doesn't survive MERGE rewrites well |
# MAGIC | **Gen 3** | **Liquid Clustering** | Incrementally clusters data on chosen columns; works with OPTIMIZE | Fully flexible — change clustering columns anytime; preserved during merges. Still requires you to *choose* the right columns. |
# MAGIC | **Gen 4** | **Automatic Liquid Clustering** | Predictive Optimization analyzes query patterns and automatically selects + applies the best clustering columns | Requires Predictive Optimization enabled. No manual `CLUSTER BY` needed — Databricks picks the columns for you. |
# MAGIC
# MAGIC ### When to partition vs. when to use Liquid Clustering
# MAGIC
# MAGIC | | Hive Partitioning | Liquid Clustering (manual) | Auto Liquid Clustering |
# MAGIC |---|---|---|---|
# MAGIC | **Table size < 1 TB** | ❌ Don't partition | ✅ Good | ✅ Best — zero effort |
# MAGIC | **Table size ≥ 1 TB** | Consider if low-cardinality gives ≥ 1 GB/partition | ✅ Good | ✅ Best — adapts automatically |
# MAGIC | **Filter columns change over time** | ❌ Can't change partition column | ✅ `ALTER TABLE ... CLUSTER BY` | ✅ Adapts to new query patterns |
# MAGIC | **Frequent MERGE / UPDATE** | ❌ Z-ORDER gets fragmented | ✅ Clustering preserved | ✅ Clustering preserved |
# MAGIC | **Knowing which columns to pick** | Must choose at creation time | Must choose (can change later) | ✅ Databricks picks for you |
# MAGIC | **Predictive Optimization** | ⚠️ Limited | ✅ Supported | ✅ Fully automatic |
# MAGIC
# MAGIC **Recommendation**: For Unity Catalog managed tables, enable **Predictive Optimization** and
# MAGIC let **Automatic Liquid Clustering** handle the data layout. If you have strong opinions on
# MAGIC which columns to cluster by, you can still set `CLUSTER BY` explicitly — Predictive
# MAGIC Optimization will respect your choice while handling OPTIMIZE/VACUUM automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Add Liquid Clustering to the gold table
# MAGIC
# MAGIC Right now, data files in our table are laid out **randomly** — an EMEA order might sit
# MAGIC in the same file as a North America order. When an analyst queries `WHERE customer_region = 'EMEA'`,
# MAGIC Spark has to open **every file** because any of them might contain EMEA rows.
# MAGIC
# MAGIC **Liquid Clustering** solves this in two steps:
# MAGIC 1. **Define** which columns to cluster on (instant metadata change)
# MAGIC 2. **OPTIMIZE** physically rewrites the files so rows with similar values are co-located
# MAGIC
# MAGIC After clustering, all EMEA rows end up in the same files. Now the same query can
# MAGIC **skip entire files** that only contain other regions — reading a fraction of the data.

# COMMAND ----------

# Step 1: Define clustering columns (metadata only — instant)
spark.sql(f"ALTER TABLE {MAINT_TABLE} CLUSTER BY (customer_region, order_timestamp)")
print("Clustering columns set: (customer_region, order_timestamp)")

# COMMAND ----------

# Step 2: OPTIMIZE physically rewrites files, co-locating rows by the clustering columns
display(spark.sql(f"OPTIMIZE {MAINT_TABLE}"))

# COMMAND ----------

# Verify: clusteringColumns should now show our chosen columns
display(spark.sql(f"DESCRIBE DETAIL {MAINT_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC > **Unlike Z-ORDER**, Liquid Clustering is **incremental** — subsequent OPTIMIZE runs
# MAGIC > only recluster newly written files, not the entire table.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Change clustering columns (flexibility demo)
# MAGIC
# MAGIC What if next quarter the analytics team starts filtering by `product_category` instead?
# MAGIC With partitioning or Z-ORDER you'd need to rewrite the whole table. With Liquid Clustering:

# COMMAND ----------

spark.sql(f"ALTER TABLE {MAINT_TABLE} CLUSTER BY (customer_region, product_category, order_timestamp)")
print("Clustering columns changed to: (customer_region, product_category, order_timestamp)")

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {MAINT_TABLE}"))

# COMMAND ----------

spark.sql(f"ALTER TABLE {MAINT_TABLE} CLUSTER BY (customer_region, order_timestamp)")
print("Clustering columns reset to: (customer_region, order_timestamp)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup: Drop the maintenance demo table

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {MAINT_TABLE}")
print(f"Dropped {MAINT_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Predictive Optimization — Let Databricks Handle It
# MAGIC
# MAGIC **Predictive Optimization** is a managed service that automatically runs `OPTIMIZE` and
# MAGIC `VACUUM` on your Unity Catalog managed tables. It monitors table usage patterns and
# MAGIC triggers maintenance when beneficial.
# MAGIC
# MAGIC ### What it does
# MAGIC
# MAGIC | Action | Trigger | Benefit |
# MAGIC |--------|---------|---------|
# MAGIC | **Auto-OPTIMIZE** | Detects small file accumulation | Keeps read performance high |
# MAGIC | **Auto-VACUUM** | Detects stale files beyond retention | Reclaims storage, reduces costs |
# MAGIC | **Auto-compaction** | After writes that create small files | Prevents small file buildup |
# MAGIC
# MAGIC ### How to enable it
# MAGIC
# MAGIC An account admin can enable Predictive Optimization for **all metastores in the account**.
# MAGIC Catalogs and schemas inherit this setting by default, but you can override it at either level:
# MAGIC
# MAGIC ```sql
# MAGIC -- Override at catalog level (if not inherited from metastore):
# MAGIC ALTER CATALOG my_catalog ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC -- or at schema level:
# MAGIC ALTER SCHEMA my_catalog.my_schema ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC ```
# MAGIC
# MAGIC ### Monitoring Predictive Optimization
# MAGIC
# MAGIC Every action it takes is logged in a system table:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE catalog_name = 'my_catalog'
# MAGIC ORDER BY start_time DESC;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Check if Predictive Optimization is enabled

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the catalog-level setting
# MAGIC DESCRIBE CATALOG EXTENDED puma_ops_lab

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Review Predictive Optimization history
# MAGIC
# MAGIC If Predictive Optimization has been running, you'll see its operations here.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   operation_type,
# MAGIC   operation_status,
# MAGIC   operation_metrics,
# MAGIC   start_time,
# MAGIC   end_time
# MAGIC FROM system.storage.predictive_optimization_operations_history
# MAGIC WHERE catalog_name = 'puma_ops_lab'
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. The modern operational model
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────┐
# MAGIC │                  THE MODERN APPROACH                                 │
# MAGIC │                                                                      │
# MAGIC │  ┌────────────────────┐   ┌──────────────────────────────────────┐  │
# MAGIC │  │ You (the engineer) │   │ Predictive Optimization (Databricks) │  │
# MAGIC │  ├────────────────────┤   ├──────────────────────────────────────┤  │
# MAGIC │  │ • Use managed      │   │ • Runs OPTIMIZE automatically        │  │
# MAGIC │  │   tables (UC)      │   │ • Runs VACUUM automatically          │  │
# MAGIC │  │ • Write your data  │   │ • Auto Liquid Clustering — picks     │  │
# MAGIC │  │ • (Optionally) set │   │   the best columns from query        │  │
# MAGIC │  │   CLUSTER BY if    │   │   patterns and clusters for you      │  │
# MAGIC │  │   you know best    │   │ • Monitors file sizes & staleness    │  │
# MAGIC │  │ • Monitor via      │   │ • Adapts to write & query patterns   │  │
# MAGIC │  │   system tables    │   │ • Logs everything to system tables   │  │
# MAGIC │  └────────────────────┘   └──────────────────────────────────────┘  │
# MAGIC │                                                                      │
# MAGIC │  No more scheduling OPTIMIZE jobs. No more VACUUM cron tasks.        │
# MAGIC │  No more picking clustering columns. No more tuning Spark configs.   │
# MAGIC └──────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > **Key takeaway**: If you're creating new tables today, use **Unity Catalog managed tables**
# MAGIC > with **Predictive Optimization** enabled. Databricks handles OPTIMIZE, VACUUM, **and**
# MAGIC > choosing the right clustering columns — all automatically. You can still set `CLUSTER BY`
# MAGIC > explicitly if you want, but for most tables, the automatic approach is all you need.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Lab 1: Skew Debugging on Classic Compute (Spark UI)
# MAGIC
# MAGIC **Scenario**: A nightly ETL job joins the orders table with a heavily skewed lookup table.
# MAGIC Some tasks take 10x longer than others, dragging the whole job down.
# MAGIC Your mission: identify the skew using the **Spark UI** and fix it.
# MAGIC
# MAGIC ### ⚠️ Attach to the shared classic cluster
# MAGIC
# MAGIC This lab requires a **classic compute** cluster — **not** serverless.
# MAGIC
# MAGIC 1. Click the **Connect** dropdown at the top-right of the notebook
# MAGIC 2. Select the shared cluster: **`puma-ops-lab-classic`**
# MAGIC 3. Wait until it shows a green dot (running)
# MAGIC
# MAGIC **Why classic compute?** The **Spark UI** is only available on classic clusters. It gives
# MAGIC you low-level visibility into shuffle stages, per-task metrics, and executor behavior — the
# MAGIC level of detail you need to diagnose data skew. Serverless compute does not expose the
# MAGIC Spark UI; there you use **Query Insights** instead (covered in Lab 2).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create a skewed promotion table
# MAGIC
# MAGIC We'll simulate a real-world scenario where a few promo codes are applied to the vast
# MAGIC majority of orders (think: a site-wide "WELCOME10" code vs. niche codes).

# COMMAND ----------

SKEW_TABLE = f"{FQ}.{user_prefix}_skewed_promos"
SKEW_ORDERS = f"{FQ}.{user_prefix}_skew_lab_orders"

spark.sql(f"DROP TABLE IF EXISTS {SKEW_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {SKEW_ORDERS}")

# COMMAND ----------

from pyspark.sql import functions as F

promo_codes = ["WELCOME10", "SUMMER25", "VIP50", "FLASH15", "LOYALTY20",
               "SPORTS30", "PUMA2025", "MARATHON", "NEWUSER", "HOLIDAY40"]

promo_rows = [(code, f"Promo: {code}", round(5 + (i * 4.5), 2)) for i, code in enumerate(promo_codes)]
df_promos = spark.createDataFrame(promo_rows, ["promo_code", "promo_description", "discount_pct"])
df_promos.write.mode("overwrite").saveAsTable(SKEW_TABLE)

print(f"Created {SKEW_TABLE} with {len(promo_codes)} promo codes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create a large orders table with skewed promo code distribution
# MAGIC
# MAGIC **500 million rows** — enough to make skew clearly visible in the
# MAGIC Spark UI. ~70% of orders get "WELCOME10", ~15% get "SUMMER25", and the remaining 15%
# MAGIC are spread across the other 8 codes. We use `spark.range()` for fast parallel generation.

# COMMAND ----------

NUM_SKEW_ORDERS = 500_000_000

df_skew_orders = (
    spark.range(NUM_SKEW_ORDERS)
    .withColumn("rand", F.rand(seed=42))
    .withColumn("promo_code",
        F.when(F.col("rand") < 0.70, F.lit("WELCOME10"))
         .when(F.col("rand") < 0.85, F.lit("SUMMER25"))
         .when(F.col("rand") < 0.88, F.lit("VIP50"))
         .when(F.col("rand") < 0.90, F.lit("FLASH15"))
         .when(F.col("rand") < 0.92, F.lit("LOYALTY20"))
         .when(F.col("rand") < 0.94, F.lit("SPORTS30"))
         .when(F.col("rand") < 0.96, F.lit("PUMA2025"))
         .when(F.col("rand") < 0.975, F.lit("MARATHON"))
         .when(F.col("rand") < 0.99, F.lit("NEWUSER"))
         .otherwise(F.lit("HOLIDAY40"))
    )
    .withColumn("order_id", F.concat(F.lit("ORD-S-"), F.lpad(F.col("id").cast("string"), 8, "0")))
    .withColumn("order_total", F.round(F.rand(seed=99) * 280 + 20, 2))
    .withColumn("customer_id", F.concat(F.lit("CUST-"), F.lpad((F.rand(seed=7) * 50000).cast("int").cast("string"), 6, "0")))
    .withColumn("channel", F.element_at(F.array(*[F.lit(c) for c in ["Web", "Mobile App", "Retail Store", "Marketplace"]]), (F.rand(seed=3) * 4).cast("int") + 1))
    .withColumn("order_timestamp", F.date_add(F.lit("2024-01-01"), (F.rand(seed=5) * 450).cast("int")))
    .withColumn("shipping_address", F.concat(F.lit("Street "), (F.rand(seed=11) * 9999).cast("int").cast("string"), F.lit(", City "), (F.rand(seed=13) * 500).cast("int").cast("string")))
    .drop("id", "rand")
)

df_skew_orders.write.mode("overwrite").saveAsTable(SKEW_ORDERS)

print(f"Created {SKEW_ORDERS} with {NUM_SKEW_ORDERS:,} orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Verify the skew

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT promo_code,
               COUNT(*) AS order_count,
               ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 1) AS pct
        FROM {SKEW_ORDERS}
        GROUP BY promo_code
        ORDER BY order_count DESC
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC 70% of rows are concentrated on `WELCOME10`. When Spark joins on `promo_code`,
# MAGIC the task handling `WELCOME10` gets ~70% of the data while other tasks sit idle.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Force a shuffle join to expose the skew
# MAGIC
# MAGIC Normally Spark would auto-broadcast the tiny promo table (10 rows), which avoids the
# MAGIC shuffle entirely and hides the skew. To simulate a **real-world scenario** where both
# MAGIC tables are large and Spark must shuffle, we temporarily disable auto-broadcast and
# MAGIC AQE's skew optimization.

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", False)
print("Disabled auto-broadcast and AQE skew handling — Spark will use a SortMergeJoin")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Run the skewed join — then investigate in Spark UI
# MAGIC
# MAGIC Run this join and **immediately open the Spark UI** to observe the skew.
# MAGIC
# MAGIC **How to open the Spark UI:**
# MAGIC 1. After the cell completes, click **"Spark Jobs"** at the bottom of the cell output
# MAGIC 2. Click **"View"** next to the job — this opens the Spark UI directly on the relevant job
# MAGIC 3. Navigate to **Stages** → find the most recent shuffle stage
# MAGIC 4. Click into it → look at the **Task metrics** (Summary section)
# MAGIC
# MAGIC **What to look for:**
# MAGIC - In the Summary Metrics table, compare the **min** vs **max** for:
# MAGIC   - **Shuffle Read Size** — a huge disparity means skew
# MAGIC   - **Duration** — one task taking 10x longer than the median
# MAGIC - In the **Tasks** table, sort by Duration (descending) to find the straggler task

# COMMAND ----------

result = spark.sql(f"""
    SELECT o.order_id,
           o.promo_code,
           o.order_total,
           p.promo_description,
           p.discount_pct,
           o.order_total * (1 - p.discount_pct / 100) AS discounted_total
    FROM {SKEW_ORDERS} o
    JOIN {SKEW_TABLE} p ON o.promo_code = p.promo_code
""")

result.write.format("noop").mode("overwrite").save()
print("Join completed — go check the Spark UI!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Investigate in the Spark UI
# MAGIC
# MAGIC **Checklist — screenshot or note what you find:**
# MAGIC
# MAGIC | What to Check | Where in Spark UI | Skew Indicator |
# MAGIC |--------------|-------------------|----------------|
# MAGIC | Shuffle Read Size | Stages → Stage detail → Summary Metrics | Max ≫ Median |
# MAGIC | Task Duration | Stages → Stage detail → Tasks table | One task takes much longer |
# MAGIC | Task Input Size | Stages → Stage detail → Tasks table | One task reads far more data |
# MAGIC
# MAGIC > **Question**: Which promo code is causing the skew? How much more data does
# MAGIC > the straggler task process compared to the median?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: The fix — Broadcast the small table
# MAGIC
# MAGIC The promo table is tiny (10 rows). By using an explicit `broadcast()` hint we tell
# MAGIC Spark to send it to every executor — eliminating the shuffle entirely and making
# MAGIC the skew irrelevant.

# COMMAND ----------

from pyspark.sql.functions import broadcast

df_promos = spark.table(SKEW_TABLE)
df_orders = spark.table(SKEW_ORDERS)

result_fixed = df_orders.join(broadcast(df_promos), on="promo_code", how="inner")
result_fixed.write.format("noop").mode("overwrite").save()
print("Broadcast join completed — check Spark UI again!")

# COMMAND ----------

# MAGIC %md
# MAGIC **Compare in Spark UI:**
# MAGIC - The new job should have **no Exchange (shuffle) stage**
# MAGIC - All tasks should complete in roughly equal time
# MAGIC - Total job duration should be significantly lower
# MAGIC
# MAGIC > **Why did we disable auto-broadcast?** In real life, Spark *would* auto-broadcast
# MAGIC > a 10-row table and you'd never see the skew. We disabled it to simulate a scenario
# MAGIC > where **both** tables are large and a shuffle join is unavoidable. In that case, the
# MAGIC > alternatives are:
# MAGIC > - **AQE skew handling** (enabled by default in Databricks Runtime) — automatically
# MAGIC >   splits large partitions into smaller ones
# MAGIC > - **Skew hints**: `/*+ SKEW('table', 'column') */`
# MAGIC > - **Salting**: add a random suffix to the skewed key and do a two-pass aggregation
# MAGIC >
# MAGIC > For most workloads on Databricks, **AQE handles skew automatically**. You only need
# MAGIC > manual intervention for extreme cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Clean up skew lab tables and restore settings

# COMMAND ----------

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", True)

spark.sql(f"DROP TABLE IF EXISTS {SKEW_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {SKEW_ORDERS}")
print("Skew lab tables cleaned up, Spark settings restored to defaults")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Lab 2: Query Profile Deep Dive (SQL Warehouse)
# MAGIC
# MAGIC **Scenario**: The daily PUMA retail report query is slow. Use the **Query Profile** in
# MAGIC Databricks SQL to diagnose why, then fix it with Liquid Clustering.
# MAGIC
# MAGIC ### ⚠️ Switch to the SQL Warehouse
# MAGIC
# MAGIC 1. Click the **Connect** dropdown at the top-right of the notebook
# MAGIC 2. Select **`PUMA_OPS_SHARED_WAREHOUSE`**
# MAGIC 3. All `%sql` cells below will run on the warehouse, giving you access to the **Query Profile**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create your personal copy of the gold table (un-optimized)
# MAGIC
# MAGIC A ~250M row template table was **pre-created during setup** with all regions mixed in every
# MAGIC file. We `SHALLOW CLONE` it to get your own copy instantly — the clone shares the same
# MAGIC data files but you can modify it independently (ALTER, OPTIMIZE, DROP) without affecting others.

# COMMAND ----------

PERF_TEMPLATE = f"{CATALOG}.{SCHEMA}.perf_lab_gold_template"
PERF_TABLE = f"{FQ}.{user_prefix}_perf_lab_gold"

spark.sql(f"DROP TABLE IF EXISTS {PERF_TABLE}")
spark.sql(f"CREATE TABLE {PERF_TABLE} SHALLOW CLONE {PERF_TEMPLATE}")

row_count = spark.sql(f"SELECT count(*) AS cnt FROM {PERF_TABLE}").collect()[0]["cnt"]
print(f"✅ Created {PERF_TABLE} with {row_count:,} rows (shallow clone — instant)")

dbutils.widgets.text("perf_table", PERF_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Check the baseline table state

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL ${perf_table}

# COMMAND ----------

# MAGIC %md
# MAGIC **Note the values:**
# MAGIC - `numFiles` — how many data files?
# MAGIC - `sizeInBytes` — total size?
# MAGIC - `clusteringColumns` — should be empty (no clustering)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Run the "slow" analyst query
# MAGIC
# MAGIC This is the query the PUMA analytics team runs daily to generate their regional
# MAGIC performance report. Run it and note the execution time.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC     customer_region,
# MAGIC     loyalty_tier,
# MAGIC     DATE_TRUNC('week', order_timestamp) AS week,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     SUM(total_amount) AS total_revenue,
# MAGIC     SUM(profit) AS total_profit,
# MAGIC     AVG(unit_price) AS avg_unit_price
# MAGIC FROM ${perf_table}
# MAGIC WHERE order_timestamp >= '2024-06-01'
# MAGIC   AND customer_region = 'EMEA'
# MAGIC GROUP BY product_category, customer_region, loyalty_tier, DATE_TRUNC('week', order_timestamp)
# MAGIC ORDER BY week DESC, total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4: Open the Query Profile
# MAGIC
# MAGIC **How to access the Query Profile:**
# MAGIC 1. After the query completes, click **"See performance"** below the results
# MAGIC 2. Click the statement to open the query details panel
# MAGIC 3. Click **"See query profile"**
# MAGIC
# MAGIC ### What to investigate in the Query Profile
# MAGIC
# MAGIC | Check | What to Look For | Why It Matters |
# MAGIC |-------|-----------------|----------------|
# MAGIC | **Scan operator** | `rows_read` vs `rows_produced` — are we reading the whole table but returning a fraction? | Indicates missing file skipping |
# MAGIC | **Time spent** | Which operator dominates? Scan? Shuffle? Aggregate? | Points to the bottleneck |
# MAGIC | **Bytes scanned** | How much data was read from storage? | Clustering reduces this |
# MAGIC | **Top operators** | Click "Top operators" tab on the left | Shows most expensive operations |
# MAGIC | **Pruning %** | Filter icons show % of data pruned during scan | Low pruning = no clustering |
# MAGIC
# MAGIC > **Take note** of `rows_read` on the Scan operator — we'll compare it after optimization.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5: Apply Liquid Clustering and OPTIMIZE

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ${perf_table} CLUSTER BY (customer_region, order_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ${perf_table}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 6: Re-run the exact same query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     product_category,
# MAGIC     customer_region,
# MAGIC     loyalty_tier,
# MAGIC     DATE_TRUNC('week', order_timestamp) AS week,
# MAGIC     COUNT(*) AS order_count,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC     SUM(total_amount) AS total_revenue,
# MAGIC     SUM(profit) AS total_profit,
# MAGIC     AVG(unit_price) AS avg_unit_price
# MAGIC FROM ${perf_table}
# MAGIC WHERE order_timestamp >= '2024-06-01'
# MAGIC   AND customer_region = 'EMEA'
# MAGIC GROUP BY product_category, customer_region, loyalty_tier, DATE_TRUNC('week', order_timestamp)
# MAGIC ORDER BY week DESC, total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 7: Compare before vs. after in Query Profile
# MAGIC
# MAGIC Open the Query Profile for the **second** run and compare:
# MAGIC
# MAGIC | Metric | Before Clustering | After Clustering | Why |
# MAGIC |--------|------------------|-----------------|-----|
# MAGIC | **Bytes scanned** | Full table scan | Reduced | File pruning from clustering |
# MAGIC | **Rows read (Scan)** | All ~250M rows | Much fewer | Only files containing EMEA + date range are read |
# MAGIC | **Pruning %** | Low or 0% | High | Clustering enables min/max statistics-based skipping |
# MAGIC | **Execution time** | Slower | Faster | Less data to process |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 8: Verify improvement via system tables
# MAGIC
# MAGIC > **Note**: `system.query.history` has an ingestion delay of a few minutes. If this
# MAGIC > query returns no results, wait 2–3 minutes and re-run it.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     statement_id,
# MAGIC     start_time,
# MAGIC     total_duration_ms,
# MAGIC     execution_duration_ms,
# MAGIC     read_bytes,
# MAGIC     read_rows,
# MAGIC     produced_rows,
# MAGIC     SUBSTRING(statement_text, 1, 80) AS query_snippet
# MAGIC FROM system.query.history
# MAGIC WHERE start_time >= current_timestamp() - INTERVAL 30 MINUTES
# MAGIC   AND executed_by = current_user()
# MAGIC   AND statement_text LIKE '%perf_lab_gold%'
# MAGIC   AND statement_text LIKE '%EMEA%'
# MAGIC   AND statement_text NOT LIKE '%system.query%'
# MAGIC   AND statement_text NOT LIKE '%OPTIMIZE%'
# MAGIC   AND statement_text NOT LIKE '%DESCRIBE%'
# MAGIC ORDER BY start_time ASC

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected observations:**
# MAGIC - `read_bytes` decreases significantly (files that don't contain EMEA data are skipped)
# MAGIC - `read_rows` decreases (fewer rows scanned)
# MAGIC - `total_duration_ms` decreases
# MAGIC
# MAGIC > This is exactly what **Predictive Optimization** would do automatically on managed tables —
# MAGIC > keeping data well-clustered without any manual OPTIMIZE commands.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 9: Clean up

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {PERF_TABLE}")
dbutils.widgets.remove("perf_table")
print("Performance lab table cleaned up")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC | Topic | Legacy Approach | Modern Approach |
# MAGIC |-------|----------------|-----------------|
# MAGIC | **File compaction** | Schedule nightly OPTIMIZE jobs | Predictive Optimization runs it for you |
# MAGIC | **Stale file cleanup** | Schedule VACUUM cron jobs | Predictive Optimization handles it |
# MAGIC | **Data layout** | Hive partitioning + Z-ORDER | Auto Liquid Clustering picks the best columns for you |
# MAGIC | **Changing filter patterns** | Repartition the entire table | Auto Liquid Clustering adapts; or `ALTER TABLE ... CLUSTER BY` |
# MAGIC | **Skew on classic compute** | Diagnose via Spark UI task metrics | AQE handles most cases; broadcast small tables |
# MAGIC | **Slow SQL queries** | Guess and tune configs | Query Profile shows exactly where time is spent |
# MAGIC | **Spark configs** | Manually tune shuffle partitions, broadcast thresholds, etc. | Serverless + Photon + AQE handle it |
# MAGIC
# MAGIC ### The recipe for new tables
# MAGIC
# MAGIC ```sql
# MAGIC -- 1. Create as a Unity Catalog managed table
# MAGIC CREATE TABLE my_catalog.my_schema.my_table (...);
# MAGIC
# MAGIC -- 2. Enable Predictive Optimization at catalog or schema level
# MAGIC ALTER CATALOG my_catalog ENABLE PREDICTIVE OPTIMIZATION;
# MAGIC
# MAGIC -- 3. That's it. Write your data. Databricks handles the rest —
# MAGIC --    including picking the right clustering columns automatically.
# MAGIC --    (You CAN still set CLUSTER BY explicitly if you prefer.)
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC **Next**: [day_2_04_declarative_automation_bundles — Infrastructure-as-code for Databricks]($./day_2_04_declarative_automation_bundles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_2_03_performance_optimization")
