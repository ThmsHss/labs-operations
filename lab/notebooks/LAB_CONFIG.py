# Databricks notebook source
# MAGIC %md
# MAGIC # Lab Configuration
# MAGIC Central configuration for the **Databricks Operations Masterclass** workshop.
# MAGIC
# MAGIC **Run this cell in every notebook** by using `%run ./LAB_CONFIG` at the top.

# COMMAND ----------

# ── Catalog & Schema ─────────────────────────────────────────────────────────
CATALOG = "puma_ops_lab"
SCHEMA  = "workshop"
VOLUME  = "raw_data"

# Fully-qualified prefix for convenience
FQ = f"{CATALOG}.{SCHEMA}"

# ── Warehouse (used by Genie, Alerts, Dashboards) ───────────────────────────
# Leave as None to auto-detect; set explicitly if needed
SQL_WAREHOUSE_NAME = None

# ── Pipeline ID (set after deploying the SDP pipeline) ───────────────────────
SDP_PIPELINE_ID = None  # e.g. "abc123-def456"

# ── Tags ─────────────────────────────────────────────────────────────────────
WORKSHOP_TAG = "puma_ops_masterclass"

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"✅ Active context: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: get current user
# MAGIC Used to scope per-user resources during the lab.

# COMMAND ----------

current_user = spark.sql("SELECT current_user() AS user").collect()[0]["user"]
user_prefix = current_user.split("@")[0].replace(".", "_")
print(f"👤 User: {current_user}  |  Prefix: {user_prefix}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Helper: mark notebook as complete
# MAGIC Participants run this at the end of each notebook to log their progress.

# COMMAND ----------

def mark_notebook_complete(notebook_name):
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FQ}.lab_progress (
            username STRING,
            notebook_name STRING,
            completed_at TIMESTAMP
        )
    """)
    spark.sql(f"""
        MERGE INTO {FQ}.lab_progress AS target
        USING (SELECT '{current_user}' AS username, '{notebook_name}' AS notebook_name, current_timestamp() AS completed_at) AS source
        ON target.username = source.username AND target.notebook_name = source.notebook_name
        WHEN MATCHED THEN UPDATE SET completed_at = source.completed_at
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"✅ '{notebook_name}' marked as complete for {current_user}")
