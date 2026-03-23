# Databricks notebook source
# MAGIC %md
# MAGIC # 📈 Block 6: Lakehouse Monitoring — Data Quality Profiling
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Understand what Lakehouse Monitoring provides
# MAGIC 2. Create a monitor on a Delta table
# MAGIC 3. Explore the auto-generated profile and drift metrics
# MAGIC 4. Define a custom metric
# MAGIC 5. Use monitoring data for root cause investigation
# MAGIC

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Lakehouse Monitoring?
# MAGIC
# MAGIC **Lakehouse Monitoring** is a built-in data quality profiling engine in Unity Catalog.
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────┐         ┌──────────────────────────────┐
# MAGIC │   Your Delta Table   │ ──────► │  Lakehouse Monitoring Engine │
# MAGIC │   (e.g., orders)     │         │  (serverless, automatic)     │
# MAGIC └──────────────────────┘         └──────────┬───────────────────┘
# MAGIC                                             │
# MAGIC                          ┌──────────────────┴──────────────────┐
# MAGIC                          │                                     │
# MAGIC                  ┌───────▼───────┐                 ┌───────────▼──────────┐
# MAGIC                  │ Profile Metrics│                 │    Drift Metrics      │
# MAGIC                  │ (per-column    │                 │ (distribution changes │
# MAGIC                  │  statistics)   │                 │  across time windows) │
# MAGIC                  └───────────────┘                 └──────────────────────┘
# MAGIC                          │                                     │
# MAGIC                          └──────────────┬──────────────────────┘
# MAGIC                                         │
# MAGIC                              ┌──────────▼──────────┐
# MAGIC                              │  Auto-generated      │
# MAGIC                              │  AI/BI Dashboard     │
# MAGIC                              └─────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Key features:
# MAGIC - **Zero infrastructure**: Fully serverless, no compute to manage
# MAGIC - **Automatic profiling**: Column-level statistics computed automatically
# MAGIC - **Drift detection**: Compares distributions across time windows
# MAGIC - **Custom metrics**: Define your own business-specific quality checks
# MAGIC - **Analysis types**: TimeSeries, Snapshot, Inference (ML models)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Create a Monitor on the Orders Table
# MAGIC
# MAGIC We'll monitor the `orders` table (which has intentional DQ issues from setup).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: Create via UI
# MAGIC 1. Navigate to **Catalog → puma_ops_lab → workshop → orders**
# MAGIC 2. Click the **"Quality"** tab
# MAGIC 3. Click **"Get started"** or **"Create monitor"**
# MAGIC 4. Configure:
# MAGIC    - **Profile type**: TimeSeries
# MAGIC    - **Timestamp column**: `order_timestamp`
# MAGIC    - **Granularities**: 1 day
# MAGIC    - **Baseline table**: *(leave empty for now)*
# MAGIC 5. Click **Create**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Create via Python SDK

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorTimeSeries

w = WorkspaceClient()
TABLE_NAME = f"{CATALOG}.{SCHEMA}.orders"

try:
    monitor = w.quality_monitors.get(table_name=TABLE_NAME)
    print(f"✅ Monitor already exists for {TABLE_NAME}")
    print(f"   Status: {monitor.status}")
    print(f"   Profile metrics table: {monitor.profile_metrics_table_name}")
    print(f"   Drift metrics table: {monitor.drift_metrics_table_name}")
except Exception:
    print(f"Creating monitor for {TABLE_NAME}...")
    monitor = w.quality_monitors.create(
        table_name=TABLE_NAME,
        time_series=MonitorTimeSeries(
            timestamp_col="order_timestamp",
            granularities=["1 day"],
        ),
        assets_dir=f"/Workspace/Users/{current_user}/monitoring/{SCHEMA}",
        output_schema_name=f"{CATALOG}.{SCHEMA}",
    )
    print(f"✅ Monitor created!")
    print(f"   Profile metrics: {monitor.profile_metrics_table_name}")
    print(f"   Drift metrics: {monitor.drift_metrics_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Refresh the monitor to compute latest metrics

# COMMAND ----------

# Trigger a refresh (this runs the profiling computation)
try:
    run = w.quality_monitors.run_refresh(table_name=TABLE_NAME)
    print(f"🔄 Refresh started: {run.refresh_id}")
    print("   This may take 1-3 minutes...")
except Exception as e:
    print(f"Note: {e}")
    print("The monitor may already be refreshing, or needs a moment after creation.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Explore Profile Metrics
# MAGIC
# MAGIC After the refresh completes, two tables are created:
# MAGIC - `<table>_profile_metrics` — Column-level statistics per time window
# MAGIC - `<table>_drift_metrics` — Distribution changes across windows

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Profile metrics: per-column statistics
# MAGIC SELECT
# MAGIC     window.start AS window_start,
# MAGIC     window.end AS window_end,
# MAGIC     column_name,
# MAGIC     data_type,
# MAGIC     num_nulls,
# MAGIC     null_percentage,
# MAGIC     num_zeros,
# MAGIC     distinct_count,
# MAGIC     min,
# MAGIC     max,
# MAGIC     mean,
# MAGIC     stddev
# MAGIC FROM puma_ops_lab.workshop.orders_profile_metrics
# MAGIC WHERE column_name IN ('customer_id', 'quantity', 'total_amount', 'order_timestamp')
# MAGIC ORDER BY window_start DESC, column_name
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Spot the Data Quality Issues!
# MAGIC
# MAGIC Remember, our `orders` table has intentional issues:
# MAGIC - **~2% NULL `customer_id`** — Look for `null_percentage` on `customer_id`
# MAGIC - **~1% negative `quantity`** — Look for `min` on `quantity` (should be negative)
# MAGIC - **~0.5% future `order_timestamp`** — Look for `max` on `order_timestamp`

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the DQ issues: NULL customer_id
# MAGIC SELECT
# MAGIC     window.start AS window_start,
# MAGIC     null_percentage,
# MAGIC     num_nulls,
# MAGIC     distinct_count
# MAGIC FROM puma_ops_lab.workshop.orders_profile_metrics
# MAGIC WHERE column_name = 'customer_id'
# MAGIC ORDER BY window_start DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find the DQ issues: negative quantities
# MAGIC SELECT
# MAGIC     window.start AS window_start,
# MAGIC     min,
# MAGIC     max,
# MAGIC     mean,
# MAGIC     percentile_25,
# MAGIC     percentile_75
# MAGIC FROM puma_ops_lab.workshop.orders_profile_metrics
# MAGIC WHERE column_name = 'quantity'
# MAGIC ORDER BY window_start DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Explore Drift Metrics
# MAGIC
# MAGIC Drift metrics compare the distribution of each column across consecutive time windows.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drift metrics: which columns changed the most between windows?
# MAGIC SELECT
# MAGIC     window.start AS window_start,
# MAGIC     column_name,
# MAGIC     drift_type,
# MAGIC     wasserstein_distance,
# MAGIC     ks_stat,
# MAGIC     chi_squared_stat
# MAGIC FROM puma_ops_lab.workshop.orders_drift_metrics
# MAGIC WHERE drift_type = 'CONSECUTIVE'
# MAGIC ORDER BY wasserstein_distance DESC NULLS LAST
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Custom Metrics
# MAGIC
# MAGIC Define business-specific metrics beyond the auto-generated ones.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: % of orders with negative amounts

# COMMAND ----------

from databricks.sdk.service.catalog import MonitorMetric, MonitorMetricType

custom_metrics = [
    MonitorMetric(
        type=MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
        name="pct_negative_quantity",
        input_columns=[":table"],
        definition="avg(CASE WHEN quantity < 0 THEN 1.0 ELSE 0.0 END)",
        output_data_type="DOUBLE",
    ),
    MonitorMetric(
        type=MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
        name="pct_future_orders",
        input_columns=[":table"],
        definition="avg(CASE WHEN order_timestamp > current_timestamp() THEN 1.0 ELSE 0.0 END)",
        output_data_type="DOUBLE",
    ),
]

try:
    w.quality_monitors.update(
        table_name=TABLE_NAME,
        custom_metrics=custom_metrics,
    )
    print("✅ Custom metrics added. Run a refresh to compute them.")
except Exception as e:
    print(f"Note: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: The Auto-generated Dashboard
# MAGIC
# MAGIC When you create a monitor, Databricks auto-generates an **AI/BI Dashboard** that visualizes:
# MAGIC - Column-level statistics over time
# MAGIC - Null percentages
# MAGIC - Distribution histograms
# MAGIC - Drift alerts
# MAGIC
# MAGIC Find it in the **"Quality"** tab of the table in the Catalog Explorer, or in the monitoring assets folder.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 When to Use What
# MAGIC
# MAGIC | Tool | Best For | Timing |
# MAGIC |------|---------|--------|
# MAGIC | **SDP Expectations** | Inline quality checks during pipeline processing | Real-time, per-batch |
# MAGIC | **Lakehouse Monitoring** | Post-hoc profiling, drift detection, trend analysis | Periodic (daily/hourly) |
# MAGIC | **SQL Alerts** | Proactive notification when thresholds are breached | Scheduled |
# MAGIC | **Custom queries on system tables** | Ad-hoc investigation | On-demand |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🏋️ Exercises
# MAGIC
# MAGIC ### Exercise 1
# MAGIC **Which day had the highest null percentage for `customer_id`? How many rows were affected?**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR QUERY HERE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2
# MAGIC **Create a monitor on the `gold_order_summary` table (Snapshot type, since it has no time column for time series). Refresh it and inspect the profile metrics.**

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3
# MAGIC **Write a query that could be used in a SQL Alert: check if the null percentage for `customer_id` exceeds 5% in any recent window.**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- YOUR QUERY HERE
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next**: `07_SQL_Alerts` — Set up proactive monitoring with SQL Alerts.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("06_Lakehouse_Monitoring")
