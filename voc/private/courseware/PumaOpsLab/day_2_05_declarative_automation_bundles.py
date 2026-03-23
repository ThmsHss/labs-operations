# Databricks notebook source
# MAGIC %md
# MAGIC # Day 2 — 05: Declarative Automation Bundles
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC **Duration**: ~30–45 minutes
# MAGIC
# MAGIC Declarative Automation Bundles (previously known as **Databricks Asset Bundles / DABs**) let you
# MAGIC define jobs, pipelines, dashboards, and other resources **as code** (YAML), deploy them to a
# MAGIC workspace, and manage them through source control. They bring **infrastructure-as-code**
# MAGIC practices to your Databricks projects.
# MAGIC
# MAGIC In this lab you will:
# MAGIC 1. Create a bundle in a **Git folder** in the workspace
# MAGIC 2. Add a notebook to the bundle
# MAGIC 3. Define a **job** resource in YAML that runs the notebook
# MAGIC 4. **Deploy** the bundle to `dev` and **run** the job
# MAGIC 5. Modify the bundle for **`prod`** with a different catalog and deploy
# MAGIC 6. Clean up by destroying the bundle deployment
# MAGIC
# MAGIC > 📖 **Reference**: [What are Declarative Automation Bundles?](https://docs.databricks.com/aws/en/dev-tools/bundles/)
# MAGIC
# MAGIC > **Note**: In this lab we use the **workspace UI** to author bundles for convenience. In practice,
# MAGIC > most teams use the **Databricks CLI** locally or in CI/CD pipelines (`databricks bundle validate`,
# MAGIC > `deploy`, `run`). The YAML and concepts are identical either way.

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Why Bundles?
# MAGIC
# MAGIC | Without Bundles | With Bundles |
# MAGIC |----------------|-------------|
# MAGIC | Resources created manually in the UI | Resources **defined as YAML** alongside source code |
# MAGIC | Hard to track who changed what | **Full Git history** — every change is a commit |
# MAGIC | Dev → Prod promotion = manual recreation | Deploy to `dev` or `prod` via a **single deployment pipeline execution** (CI/CD) |
# MAGIC | Team sharing = screenshots & Slack messages | Share a **Git folder** — everyone sees the same config |
# MAGIC
# MAGIC ### What can a bundle contain?
# MAGIC
# MAGIC | Resource Type | YAML Key | Example |
# MAGIC |--------------|----------|---------|
# MAGIC | Lakeflow Jobs | `jobs` | Scheduled ETL, ML training pipelines |
# MAGIC | Spark Declarative Pipelines | `pipelines` | Streaming tables, materialized views |
# MAGIC | AI/BI Dashboards | `dashboards` | Operational monitoring dashboards |
# MAGIC | Model Serving Endpoints | `model_serving_endpoints` | Real-time inference |
# MAGIC | MLflow Experiments | `experiments` | Experiment tracking |
# MAGIC | Schemas, Volumes | `schemas`, `volumes` | Data organization |
# MAGIC
# MAGIC ### Bundle structure (on disk)
# MAGIC
# MAGIC ```
# MAGIC my-bundle/
# MAGIC ├── databricks.yml            ← Main config: bundle name, variables, targets
# MAGIC ├── resources/
# MAGIC │   ├── my_job.job.yml        ← Job definition
# MAGIC │   └── my_pipeline.pipeline.yml
# MAGIC └── src/
# MAGIC     ├── notebooks/
# MAGIC     │   └── transform.py      ← Source code
# MAGIC     └── dashboards/
# MAGIC         └── ops.lvdash.json
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Create a Bundle in the Workspace
# MAGIC
# MAGIC In this lab we use the workspace UI to create and deploy bundles. This is great for getting
# MAGIC started and quick demos. In most real-world projects, teams use the **Databricks CLI** locally
# MAGIC or in CI/CD pipelines — the YAML configuration is the same either way.
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC Bundles live inside **Git folders**. We'll use a shared repository.
# MAGIC
# MAGIC ### Step 2.1 — Open the shared Git folder
# MAGIC
# MAGIC 1. In the left sidebar click **Workspace**
# MAGIC 2. Navigate to `/Workspace/Shared/PumaOpsDemoRepo`
# MAGIC 3. If you don't see the repo yet, click **⋮ → Create → Git folder** and enter the remote URL:
# MAGIC    `https://github.com/puma-databricks/PumaOpsDemoRepo.git`
# MAGIC 4. Create a **personal subfolder** inside the repo — e.g. `bundles-<your-name>` — to avoid
# MAGIC    conflicts with other participants
# MAGIC
# MAGIC ### Step 2.2 — Create the bundle
# MAGIC
# MAGIC 1. Open your personal subfolder inside the Git folder
# MAGIC 2. Click **Create → Asset bundle**
# MAGIC 3. In the dialog:
# MAGIC    - **Name**: `puma-orders-job` (only letters, numbers, dashes, underscores)
# MAGIC    - **Template**: select **Empty project**
# MAGIC 4. Click **Create and deploy**
# MAGIC
# MAGIC Databricks creates:
# MAGIC - A `databricks.yml` file — the main bundle configuration
# MAGIC - A `.gitignore` file
# MAGIC
# MAGIC Take a moment to read through the generated `databricks.yml`. You'll see a `targets` section
# MAGIC with `dev` (default) and `prod` — that's how bundles handle multi-environment deployments.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Add a Notebook
# MAGIC
# MAGIC ### Step 3.1 — Create a notebook inside the bundle
# MAGIC
# MAGIC 1. In the bundle editor, click the **Add notebook** tile
# MAGIC    *(or click the **⋮** kebab next to the bundle name → **Create Notebook**)*
# MAGIC 2. Rename the notebook to **`order_report`**
# MAGIC 3. Set the language to **Python**
# MAGIC 4. Paste the following code into the first cell:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # -- Paste this into the notebook you create inside the bundle --
# MAGIC from datetime import date
# MAGIC
# MAGIC catalog = "puma_ops_lab"
# MAGIC schema = "workshop"
# MAGIC
# MAGIC spark.sql(f"USE CATALOG {catalog}")
# MAGIC spark.sql(f"USE SCHEMA {schema}")
# MAGIC
# MAGIC df = spark.sql("""
# MAGIC     SELECT
# MAGIC         channel,
# MAGIC         status,
# MAGIC         COUNT(*) AS order_count,
# MAGIC         ROUND(SUM(total_amount), 2) AS total_revenue
# MAGIC     FROM orders
# MAGIC     GROUP BY channel, status
# MAGIC     ORDER BY total_revenue DESC
# MAGIC """)
# MAGIC
# MAGIC df.display()
# MAGIC print(f"Order report generated on {date.today()}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC > **💡 Tip**: You can run the notebook interactively right now to verify it works before
# MAGIC > wiring it up as a job.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Define a Job Resource
# MAGIC
# MAGIC Now let's tell the bundle to **run this notebook as a Lakeflow Job**.
# MAGIC
# MAGIC ### Step 4.1 — Create a job definition
# MAGIC
# MAGIC 1. Click the **deployment icon** (rocket / deploy icon) in the left panel to open the
# MAGIC    **Deployments** pane
# MAGIC 2. Under **Bundle resources** → click **Add** → **New job definition**
# MAGIC 3. Enter the job name: `order-report`
# MAGIC 4. Click **Add and deploy**
# MAGIC
# MAGIC This creates a file called `order-report.job.yml` with some placeholder YAML.
# MAGIC
# MAGIC ### Step 4.2 — Configure the job
# MAGIC
# MAGIC Replace the **entire contents** of `order-report.job.yml` with the following YAML:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     order_report:
# MAGIC       name: order-report
# MAGIC       queue:
# MAGIC         enabled: true
# MAGIC       tasks:
# MAGIC         - task_key: generate-report
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../order_report.ipynb
# MAGIC ```
# MAGIC
# MAGIC **What this does:**
# MAGIC
# MAGIC | Field | Purpose |
# MAGIC |-------|---------|
# MAGIC | `order_report` | Resource key — how the bundle references this job internally |
# MAGIC | `name` | Display name in the Databricks Jobs UI |
# MAGIC | `queue.enabled` | Queue runs so they don't overlap |
# MAGIC | `task_key` | Unique identifier for this task within the job |
# MAGIC | `notebook_path` | Path to the notebook (relative to the `.job.yml` file — `../` goes up one level) |
# MAGIC
# MAGIC > **⚠️ Path resolution**: Files in `resources/` use `../` to reference files at the bundle root.
# MAGIC > Files referenced from `databricks.yml` use `./`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: Deploy and Run
# MAGIC
# MAGIC ### Step 5.1 — Deploy the bundle
# MAGIC
# MAGIC 1. In the **Deployments** pane, make sure the target is set to **`dev`**
# MAGIC 2. Click **Deploy**
# MAGIC 3. A confirmation dialog shows what will be created — review it and click **Deploy**
# MAGIC 4. Watch the **Project output** window for deployment progress
# MAGIC
# MAGIC When deployment completes, the job appears under **Bundle resources**.
# MAGIC
# MAGIC ### Step 5.2 — Run the job
# MAGIC
# MAGIC 1. In the **Bundle resources** section, find your `order_report` job
# MAGIC 2. Click the **▶ play** icon to trigger a run
# MAGIC 3. Navigate to **Jobs & Pipelines** in the left sidebar to see the run
# MAGIC 4. Wait for the run to complete — click into the run to see the notebook output
# MAGIC
# MAGIC The job name is prefixed with your target and username, e.g.
# MAGIC `[dev your.name] order-report`.
# MAGIC
# MAGIC > **✅ Verify**: The run should show `Succeeded` status with the order report table output.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 6: Parameterize with Variables
# MAGIC
# MAGIC Right now the notebook has the catalog and schema hardcoded. To support deploying the same
# MAGIC bundle to different environments (dev vs. prod), we'll use **bundle variables**.
# MAGIC
# MAGIC ### Step 6.1 — Update `databricks.yml` with variables
# MAGIC
# MAGIC Open `databricks.yml` and update it to include variables and target-specific overrides.
# MAGIC Replace the contents with:
# MAGIC
# MAGIC ```yaml
# MAGIC bundle:
# MAGIC   name: puma-orders-job
# MAGIC
# MAGIC include:
# MAGIC   - resources/*.yml
# MAGIC
# MAGIC variables:
# MAGIC   catalog:
# MAGIC     default: "puma_ops_lab"
# MAGIC   schema:
# MAGIC     default: "workshop"
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     default: true
# MAGIC     mode: development
# MAGIC
# MAGIC   prod:
# MAGIC     mode: production
# MAGIC     variables:
# MAGIC       catalog: "puma_ops_lab_prod"
# MAGIC ```
# MAGIC
# MAGIC **What this does:**
# MAGIC
# MAGIC | Concept | What It Does |
# MAGIC |---------|-------------|
# MAGIC | `variables` | Declare parameterized values you can reference as `${var.catalog}` |
# MAGIC | `targets.dev` | `mode: development` — prefixes resource names with `[dev user]` |
# MAGIC | `targets.prod` | `mode: production` — clean names, overrides `catalog` to `puma_ops_lab_prod` |
# MAGIC
# MAGIC ### Step 6.2 — Pass variables to the job
# MAGIC
# MAGIC Update `order-report.job.yml` to pass the variables as notebook parameters:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     order_report:
# MAGIC       name: order-report
# MAGIC       queue:
# MAGIC         enabled: true
# MAGIC       tasks:
# MAGIC         - task_key: generate-report
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../order_report.ipynb
# MAGIC             base_parameters:
# MAGIC               catalog: ${var.catalog}
# MAGIC               schema: ${var.schema}
# MAGIC ```
# MAGIC
# MAGIC ### Step 6.3 — Update the notebook to read parameters
# MAGIC
# MAGIC Open the `order_report` notebook and replace the code with:
# MAGIC
# MAGIC ```python
# MAGIC from datetime import date
# MAGIC
# MAGIC dbutils.widgets.text("catalog", "puma_ops_lab")
# MAGIC dbutils.widgets.text("schema", "workshop")
# MAGIC catalog = dbutils.widgets.get("catalog")
# MAGIC schema = dbutils.widgets.get("schema")
# MAGIC
# MAGIC spark.sql(f"USE CATALOG {catalog}")
# MAGIC spark.sql(f"USE SCHEMA {schema}")
# MAGIC
# MAGIC df = spark.sql("""
# MAGIC     SELECT
# MAGIC         channel,
# MAGIC         status,
# MAGIC         COUNT(*) AS order_count,
# MAGIC         ROUND(SUM(total_amount), 2) AS total_revenue
# MAGIC     FROM orders
# MAGIC     GROUP BY channel, status
# MAGIC     ORDER BY total_revenue DESC
# MAGIC """)
# MAGIC
# MAGIC df.display()
# MAGIC print(f"Order report generated on {date.today()} — catalog: {catalog}, schema: {schema}")
# MAGIC ```
# MAGIC
# MAGIC Now the notebook dynamically reads which catalog/schema to use from the job parameters.
# MAGIC
# MAGIC ### Step 6.4 — Redeploy to dev and run
# MAGIC
# MAGIC 1. In the **Deployments** pane, make sure the target is **`dev`**
# MAGIC 2. Click **Deploy**
# MAGIC 3. Review the diff — it should show the `base_parameters` being added
# MAGIC 4. Confirm and **run the job** again
# MAGIC 5. Click into the run output and verify it says `catalog: puma_ops_lab`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7: Deploy to Production
# MAGIC
# MAGIC Now let's deploy the **same bundle** to `prod` — this time it will use the
# MAGIC `puma_ops_lab_prod` catalog automatically, because that's what we configured
# MAGIC in the `prod` target.
# MAGIC
# MAGIC ### Step 7.1 — Switch target and deploy
# MAGIC
# MAGIC 1. In the **Deployments** pane, change the target from `dev` to **`prod`**
# MAGIC 2. Click **Deploy**
# MAGIC 3. Review the deployment — notice the job name will **not** have a `[dev ...]` prefix
# MAGIC
# MAGIC ### Step 7.2 — Run the production job
# MAGIC
# MAGIC 1. Click the **▶ play** icon on the `order_report` job in the Bundle resources
# MAGIC 2. Navigate to **Jobs & Pipelines** — you should see a job named `order-report`
# MAGIC    (without the dev prefix)
# MAGIC 3. Click into the run and verify the output shows `catalog: puma_ops_lab_prod`
# MAGIC
# MAGIC > **Key takeaway**: Same code, same YAML, but the `prod` target overrides the catalog variable.
# MAGIC > In a real CI/CD pipeline, your deployment step would simply run:
# MAGIC > ```bash
# MAGIC > databricks bundle deploy -t prod
# MAGIC > databricks bundle run -t prod order_report
# MAGIC > ```
# MAGIC
# MAGIC ### Step 7.3 — Compare dev and prod jobs
# MAGIC
# MAGIC Open **Jobs & Pipelines** and compare the two jobs side by side:
# MAGIC
# MAGIC | | Dev | Prod |
# MAGIC |---|-----|------|
# MAGIC | **Job name** | `[dev your.name] order-report` | `order-report` |
# MAGIC | **Catalog used** | `puma_ops_lab` | `puma_ops_lab_prod` |
# MAGIC | **Managed by** | Bundle target `dev` | Bundle target `prod` |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 8: Modify and Redeploy
# MAGIC
# MAGIC One of the biggest advantages of bundles is that changes are just **file edits** — update the
# MAGIC YAML, redeploy, and the workspace resource is updated. No manual clicking.
# MAGIC
# MAGIC ### Step 8.1 — Add a schedule
# MAGIC
# MAGIC Open `order-report.job.yml` and add a `schedule` block. The full file should look like:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     order_report:
# MAGIC       name: order-report
# MAGIC       queue:
# MAGIC         enabled: true
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 8 * * ?"
# MAGIC         timezone_id: "Europe/Berlin"
# MAGIC       tasks:
# MAGIC         - task_key: generate-report
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../order_report.ipynb
# MAGIC             base_parameters:
# MAGIC               catalog: ${var.catalog}
# MAGIC               schema: ${var.schema}
# MAGIC ```
# MAGIC
# MAGIC This schedules the job to run **daily at 08:00 CET**.
# MAGIC
# MAGIC ### Step 8.2 — Add email notifications
# MAGIC
# MAGIC You can also add notifications. Extend the job definition:
# MAGIC
# MAGIC ```yaml
# MAGIC       email_notifications:
# MAGIC         on_failure:
# MAGIC           - your.email@puma.com
# MAGIC ```
# MAGIC
# MAGIC ### Step 8.3 — Redeploy
# MAGIC
# MAGIC 1. Go back to the **Deployments** pane
# MAGIC 2. Click **Deploy** (to `dev`)
# MAGIC 3. Review the diff — it should show the schedule and notification being added
# MAGIC 4. Confirm the deployment
# MAGIC
# MAGIC Now open the job in **Jobs & Pipelines** — you should see the schedule configured.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 9: Clean Up
# MAGIC
# MAGIC Bundles track what they deployed — so cleaning up is straightforward.
# MAGIC
# MAGIC ### Option A: From the workspace web terminal
# MAGIC
# MAGIC 1. Open any cluster's **Web Terminal** (Compute → select cluster → Apps → Web Terminal)
# MAGIC 2. Navigate to your bundle directory:
# MAGIC    ```bash
# MAGIC    cd /Workspace/Shared/PumaOpsDemoRepo/bundles-<your-name>/puma-orders-job
# MAGIC    ```
# MAGIC 3. Destroy both deployments:
# MAGIC    ```bash
# MAGIC    databricks bundle destroy -t dev --auto-approve
# MAGIC    databricks bundle destroy -t prod --auto-approve
# MAGIC    ```
# MAGIC
# MAGIC ### Option B: Delete the resources manually
# MAGIC
# MAGIC 1. Go to **Jobs & Pipelines** and delete both the `[dev ...] order-report` and `order-report` jobs
# MAGIC 2. Delete your personal subfolder from the shared Git folder
# MAGIC
# MAGIC > **💡 Tip**: In a real project, `databricks bundle destroy` is preferred because it tracks
# MAGIC > exactly which resources were deployed and removes only those.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Exercises
# MAGIC
# MAGIC ### Exercise 1: Add a second task to the job
# MAGIC
# MAGIC Modify `order-report.job.yml` to add a **second task** that depends on the first.
# MAGIC
# MAGIC 1. Create another notebook in the bundle called `order_summary` with a simple `SELECT count(*) FROM orders`
# MAGIC 2. Add a second task to the YAML that runs this notebook **after** the first task completes:
# MAGIC
# MAGIC ```yaml
# MAGIC         - task_key: summary-task
# MAGIC           depends_on:
# MAGIC             - task_key: generate-report
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../order_summary.ipynb
# MAGIC ```
# MAGIC
# MAGIC 3. Redeploy and run the job — verify both tasks appear in the job run

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2 (Bonus): Add an existing resource to the bundle
# MAGIC
# MAGIC Try adding an **existing job** from the workspace into your bundle:
# MAGIC
# MAGIC 1. In the **Deployments** pane, click **Add → Add existing job**
# MAGIC 2. Select any job from the dropdown
# MAGIC 3. Choose **Update on development deploys**
# MAGIC 4. Explore the generated YAML — this is a great way to learn the YAML syntax for complex jobs

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC | Concept | Summary |
# MAGIC |---------|---------|
# MAGIC | **What** | Declarative Automation Bundles = IaC for Databricks resources |
# MAGIC | **Where** | Bundle files live in Git folders — full version control |
# MAGIC | **How (UI)** | Create → deploy → run from the workspace editor (great for demos) |
# MAGIC | **How (CLI)** | `bundle validate` → `bundle deploy` → `bundle run` (typical for teams & CI/CD) |
# MAGIC | **Multi-env** | `targets` in `databricks.yml` — same code, different configs for `dev` and `prod` |
# MAGIC | **Variables** | `${var.catalog}` lets you parameterize and override per target |
# MAGIC | **Resources** | Jobs, pipelines, dashboards, model serving, schemas, volumes |
# MAGIC
# MAGIC ### Further reading
# MAGIC
# MAGIC - [What are Declarative Automation Bundles?](https://docs.databricks.com/aws/en/dev-tools/bundles/)
# MAGIC - [Bundle configuration reference](https://docs.databricks.com/aws/en/dev-tools/bundles/settings)
# MAGIC - [Supported resource types](https://docs.databricks.com/aws/en/dev-tools/bundles/resources)
# MAGIC - [Bundle examples on GitHub](https://github.com/databricks/bundle-examples)
# MAGIC
# MAGIC ---
# MAGIC **Previous**: [day_2_04_sql_alerts — SQL Alerts Challenge]($./day_2_04_sql_alerts)
# MAGIC
# MAGIC **End of Workshop.** 🎉 Thank you for attending the Databricks Operations Masterclass!

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_2_05_declarative_automation_bundles")
