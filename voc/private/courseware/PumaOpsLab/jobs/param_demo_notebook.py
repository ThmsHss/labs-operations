# Databricks notebook source
# MAGIC %md
# MAGIC # Parameter Demo Notebook
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC This notebook demonstrates receiving job parameters via widgets.
# MAGIC
# MAGIC - When run **interactively**: Uses widget defaults
# MAGIC - When run **as a job task**: Receives values from `base_parameters`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define Widgets with Defaults

# COMMAND ----------

# Define widgets with defaults (for interactive use)
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("processing_date", "2024-01-01", "Processing Date")
dbutils.widgets.text("region", "ALL", "Region")
dbutils.widgets.text("run_id", "INTERACTIVE", "Run ID")

print("✅ Widgets created - check the widget bar above!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Retrieve Parameter Values

# COMMAND ----------

# Get parameter values
# When run as a job, these come from base_parameters
# When run interactively, they use the widget defaults
env = dbutils.widgets.get("env")
processing_date = dbutils.widgets.get("processing_date")
region = dbutils.widgets.get("region")
run_id = dbutils.widgets.get("run_id")

print("=" * 60)
print("PARAMETERS RECEIVED")
print("=" * 60)
print(f"Environment:     {env}")
print(f"Processing Date: {processing_date}")
print(f"Region:          {region}")
print(f"Run ID:          {run_id}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Validate Parameters

# COMMAND ----------

from datetime import datetime

errors = []

# Validate environment
valid_envs = ["dev", "staging", "prod"]
if env not in valid_envs:
    errors.append(f"Invalid environment: {env}. Must be one of {valid_envs}")

# Validate date format
try:
    datetime.strptime(processing_date, "%Y-%m-%d")
except ValueError:
    errors.append(f"Invalid date format: {processing_date}. Expected YYYY-MM-DD")

# Validate region
valid_regions = ["ALL", "EMEA", "APAC", "AMER"]
if region not in valid_regions:
    errors.append(f"Invalid region: {region}. Must be one of {valid_regions}")

if errors:
    error_msg = "Parameter validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
    raise ValueError(error_msg)

print("✅ All parameters validated successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Simulate Processing

# COMMAND ----------

import json

# Simulate some data processing
print(f"\n🔄 Processing data for {region} region on {processing_date}...")
print(f"   Environment: {env}")
print(f"   Run ID: {run_id}")

# Build result
result = {
    "status": "SUCCESS",
    "parameters": {
        "env": env,
        "processing_date": processing_date,
        "region": region,
        "run_id": run_id
    },
    "records_processed": 1000,
    "output_path": f"/Volumes/puma_ops_lab/workshop/processed/{processing_date}/{region}"
}

print(f"\n📤 Processing complete!")
print(json.dumps(result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Return Result to Job
# MAGIC
# MAGIC Use `dbutils.notebook.exit()` to return data to:
# MAGIC - A parent notebook (if called via `dbutils.notebook.run()`)
# MAGIC - The job run output (visible in job run details)

# COMMAND ----------

# Return result as JSON string
# This value is accessible in downstream tasks or job run output
dbutils.notebook.exit(json.dumps(result))