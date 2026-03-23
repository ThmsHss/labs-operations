# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Lab: Job Parameters in Action
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC ⏱️ **Time allocation**: ~15 minutes
# MAGIC
# MAGIC ### Demo Objectives
# MAGIC - Create a job with parameters via SDK
# MAGIC - Pass static and dynamic parameters to notebooks
# MAGIC - Use job-level parameters with `{{params.name}}` syntax
# MAGIC - Test parameter override behavior

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## How Job Parameters Work
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                       JOB PARAMETER FLOW                                    │
# MAGIC ├─────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                             │
# MAGIC │   ┌─────────────────────────────────────────────────────────┐              │
# MAGIC │   │              JOB CONFIGURATION                          │              │
# MAGIC │   │  ┌───────────────────────────────────────────────────┐  │              │
# MAGIC │   │  │ parameters:                                       │  │              │
# MAGIC │   │  │   - name: environment                             │  │              │
# MAGIC │   │  │     default: "dev"                                │  │              │
# MAGIC │   │  │   - name: date                                    │  │              │
# MAGIC │   │  │     default: "{{job.start_time.iso_date}}"        │  │              │
# MAGIC │   │  └───────────────────────────────────────────────────┘  │              │
# MAGIC │   └─────────────────────────┬───────────────────────────────┘              │
# MAGIC │                             │                                              │
# MAGIC │                             ▼                                              │
# MAGIC │   ┌─────────────────────────────────────────────────────────┐              │
# MAGIC │   │              TASK base_parameters                       │              │
# MAGIC │   │  ┌───────────────────────────────────────────────────┐  │              │
# MAGIC │   │  │ base_parameters:                                  │  │              │
# MAGIC │   │  │   env: "{{params.environment}}"    ← References   │  │              │
# MAGIC │   │  │   date: "{{params.date}}"            job param    │  │              │
# MAGIC │   │  │   region: "EMEA"                   ← Static value │  │              │
# MAGIC │   │  └───────────────────────────────────────────────────┘  │              │
# MAGIC │   └─────────────────────────┬───────────────────────────────┘              │
# MAGIC │                             │                                              │
# MAGIC │                             ▼                                              │
# MAGIC │   ┌─────────────────────────────────────────────────────────┐              │
# MAGIC │   │              NOTEBOOK                                   │              │
# MAGIC │   │  ┌───────────────────────────────────────────────────┐  │              │
# MAGIC │   │  │ dbutils.widgets.text("env", "dev")                │  │              │
# MAGIC │   │  │ env = dbutils.widgets.get("env")  ← Gets "prod"   │  │              │
# MAGIC │   │  │                                     from job!     │  │              │
# MAGIC │   │  └───────────────────────────────────────────────────┘  │              │
# MAGIC │   └─────────────────────────────────────────────────────────┘              │
# MAGIC │                                                                             │
# MAGIC └─────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Dynamic Parameter Variables
# MAGIC
# MAGIC Databricks provides built-in variables you can use in job parameters:
# MAGIC
# MAGIC | Variable | Description | Example Value |
# MAGIC |----------|-------------|---------------|
# MAGIC | `{{job.start_time.iso_date}}` | Job start date | `2024-03-15` |
# MAGIC | `{{job.run_id}}` | Unique run identifier | `12345678` |
# MAGIC | `{{job.id}}` | Job ID | `987654` |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 1: Create a Job with Parameters via SDK

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task, NotebookTask, JobParameterDefinition, Source
)
import json

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the Job Configuration

# COMMAND ----------

YOUR_IDENTIFIER=''

job_name = f"{YOUR_IDENTIFIER}_puma-workshop-param-demo"

# Get current notebook path to determine where jobs/ folder is
current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
workshop_base_path = "/".join(current_notebook_path.split("/")[:-1])  # Parent folder of this notebook
jobs_notebook_path = f"{workshop_base_path}/jobs/param_demo_notebook"

print(f"📁 Workshop path: {workshop_base_path}")
print(f"📓 Target notebook: {jobs_notebook_path}")

job_config = {
    "name": job_name,
    "parameters": [
        {"name": "environment", "default": "dev"},
        {"name": "date", "default": "{{job.start_time.iso_date}}"},
        {"name": "region", "default": "APAC"}
    ],
    "tasks": [
        {
            "task_key": "process_data",
            "notebook_task": {
                "notebook_path": jobs_notebook_path,
                "base_parameters": {
                    "env": "dev",
                    "processing_date": "{{job.start_time.iso_date}}",
                    "region": "APAC",
                    "run_id": "{{job.run_id}}"
                },
                "source": "WORKSPACE"
            }
        }
    ]
}

print("📋 Job Configuration:")
print(json.dumps(job_config, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key Points in This Configuration
# MAGIC
# MAGIC | Element | Description |
# MAGIC |---------|-------------|
# MAGIC | `parameters` | Job-level params with defaults |
# MAGIC | `{{params.environment}}` | References job-level param |
# MAGIC | `{{job.start_time.iso_date}}` | Dynamic date at runtime |
# MAGIC | `{{job.run_id}}` | Unique identifier per run |
# MAGIC | `base_parameters` | Values passed to notebook |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 2: Review the Target Notebook
# MAGIC
# MAGIC The job will run the notebook at `jobs/param_demo_notebook`. This notebook:
# MAGIC
# MAGIC 1. **Defines widgets** with defaults for interactive use
# MAGIC 2. **Receives parameters** from job's `base_parameters`
# MAGIC 3. **Validates** all inputs
# MAGIC 4. **Returns results** via `dbutils.notebook.exit()`
# MAGIC
# MAGIC ### Key Pattern: Same Code Works Both Ways
# MAGIC
# MAGIC ```python
# MAGIC # Define widget with default
# MAGIC dbutils.widgets.text("env", "dev", "Environment")
# MAGIC
# MAGIC # Get value - works for BOTH interactive and job runs
# MAGIC env = dbutils.widgets.get("env")
# MAGIC #   Interactive run: env = "dev" (widget default)
# MAGIC #   Job run with base_parameters: env = "prod" (from job config)
# MAGIC ```

# COMMAND ----------

# View the target notebook location
print(f"📓 Target notebook: {jobs_notebook_path}")
print(f"\n📁 The notebook is located at: ./jobs/param_demo_notebook.py")
print("\n📋 Notebook structure:")
print("   1. Define widgets with defaults (env, processing_date, region, run_id)")
print("   2. Retrieve values via dbutils.widgets.get()")
print("   3. Validate parameters")
print("   4. Process data")
print("   5. Return result via dbutils.notebook.exit()")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 3: Create the Job via SDK

# COMMAND ----------


# Convert job_config to SDK objects
parameters = [
    JobParameterDefinition(name=p["name"], default=p["default"]) 
    for p in job_config["parameters"]
]

task_config = job_config["tasks"][0]
tasks = [
    Task(
        task_key=task_config["task_key"],
        notebook_task=NotebookTask(
            notebook_path=task_config["notebook_task"]["notebook_path"],
            base_parameters=task_config["notebook_task"]["base_parameters"],
            source=Source.WORKSPACE
        )
    )
]

try:
    job = w.jobs.create(
        name=job_config["name"],
        parameters=parameters,
        tasks=tasks
    )
    print(f"✅ Job created successfully!")
    print(f"   Job ID: {job.job_id}")
    print(f"   Job Name: {job_config['name']}")
    job_id = job.job_id
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"ℹ️ Job '{job_name}' already exists")
        existing = [j for j in w.jobs.list() if j.settings and j.settings.name == job_name]
        if existing:
            job_id = existing[0].job_id
            print(f"   Job ID: {job_id}")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 4: View Job Configuration

# COMMAND ----------

job_details = w.jobs.get(job_id)

print("📋 Job Details:")
print(f"   Name: {job_details.settings.name}")
print(f"   Job ID: {job_id}")
print()
print("📌 Job-Level Parameters:")
for param in job_details.settings.parameters or []:
    print(f"   • {param.name}: {param.default}")

print()
print("📋 Task Parameters:")
for task in job_details.settings.tasks or []:
    print(f"   Task: {task.task_key}")
    if task.notebook_task and task.notebook_task.base_parameters:
        for k, v in task.notebook_task.base_parameters.items():
            print(f"      {k}: {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 5: Run the Job with Parameter Overrides
# MAGIC
# MAGIC When triggering a job run, you can override the default parameters:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 1: Run with Default Parameters

# COMMAND ----------

# Uncomment to run with defaults
run_default = w.jobs.run_now(job_id=job_id)
print(f"🚀 Job run started with DEFAULT parameters")
print(f"   Run ID: {run_default.run_id}")

print("💡 To run with defaults:")
print(f'   w.jobs.run_now(job_id={job_id})')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Override Parameters at Runtime

# COMMAND ----------

# Uncomment to run with overrides
run_override = w.jobs.run_now(
    job_id=job_id,
    job_parameters={
        "environment": "prod",
        "date": "2026-03-20",
        "region": "APAC"
    }
)
print(f"🚀 Job run started with OVERRIDE parameters")
print(f"   Run ID: {run_override.run_id}")

print("💡 To run with parameter overrides:")
print(f'''   w.jobs.run_now(
       job_id={job_id},
       job_parameters={{
           "environment": "prod",
           "date": "2024-03-20",
           "region": "APAC"
       }}
   )''')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 6: YAML Configuration Reference
# MAGIC
# MAGIC Here's the equivalent YAML for Asset Bundles:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     param_demo_job:
# MAGIC       name: "puma-workshop-param-demo"
# MAGIC       
# MAGIC       # Job-level parameters with defaults
# MAGIC       parameters:
# MAGIC         - name: environment
# MAGIC           default: "dev"
# MAGIC         - name: date
# MAGIC           default: "{{job.start_time.iso_date}}"
# MAGIC         - name: region
# MAGIC           default: "EMEA"
# MAGIC       
# MAGIC       tasks:
# MAGIC         - task_key: process_data
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/notebooks/process_data
# MAGIC             # Pass job params to notebook using {{params.X}} syntax
# MAGIC             base_parameters:
# MAGIC               env: "{{params.environment}}"
# MAGIC               processing_date: "{{params.date}}"
# MAGIC               region: "{{params.region}}"
# MAGIC               run_id: "{{job.run_id}}"
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Parameter Precedence
# MAGIC
# MAGIC When multiple sources provide values, here's the priority:
# MAGIC
# MAGIC | Priority | Source | Example |
# MAGIC |----------|--------|---------|
# MAGIC | 1 (Highest) | `job_parameters` at run time | API/CLI override |
# MAGIC | 2 | Job-level `parameters` default | YAML/UI config |
# MAGIC | 3 | Task `base_parameters` | Static task values |
# MAGIC | 4 (Lowest) | Widget default in notebook | `dbutils.widgets.text("env", "dev")` |
# MAGIC
# MAGIC ```
# MAGIC Run-time override  →  Job default  →  Task base_param  →  Widget default
# MAGIC     (Wins!)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 7: Cleanup

# COMMAND ----------

# Uncomment to delete the demo job
# w.jobs.delete(job_id=job_id)
# print(f"✅ Job {job_id} deleted")

print("💡 To clean up, uncomment and run:")
print(f"   w.jobs.delete(job_id={job_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📋 Key Takeaways
# MAGIC
# MAGIC | Concept | Description |
# MAGIC |---------|-------------|
# MAGIC | **Job Parameters** | Define at job level, reuse across tasks |
# MAGIC | **`{{params.X}}`** | Reference job params in task config |
# MAGIC | **`{{job.X}}`** | Built-in dynamic values (date, run_id, etc.) |
# MAGIC | **base_parameters** | Pass values to notebook widgets |
# MAGIC | **Override at runtime** | `job_parameters` in API/CLI |
# MAGIC
# MAGIC ### Best Practices
# MAGIC
# MAGIC | Practice | Why |
# MAGIC |----------|-----|
# MAGIC | Define params at job level | Single source of truth |
# MAGIC | Use `{{job.start_time.iso_date}}` | Automatic date handling |
# MAGIC | Always set widget defaults | Enables interactive testing |
# MAGIC | Validate params early | Fail fast with clear errors |
# MAGIC | Use meaningful param names | Self-documenting configs |
# MAGIC
# MAGIC ---
# MAGIC **End of Day 1.** 🎉 See you tomorrow for Day 2!

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_06_job_parameters")