# Databricks notebook source
# MAGIC %md
# MAGIC # 🧞 Day 1 — 03b: Genie on System Tables — Natural Language Debugging
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Set up a Genie Space connected to system tables
# MAGIC 2. Ask operational questions in natural language
# MAGIC 3. Explore how Genie translates questions to SQL
# MAGIC

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is Genie?
# MAGIC
# MAGIC **Genie** is Databricks' natural language interface for SQL data. You describe what you want in plain English, and Genie:
# MAGIC 1. Translates your question into SQL
# MAGIC 2. Executes it on a SQL warehouse
# MAGIC 3. Returns results conversationally
# MAGIC
# MAGIC When connected to **system tables**, Genie becomes a powerful **natural language debugging tool** for operations teams.
# MAGIC
# MAGIC > **⚠️ Disclaimer**: What we build in this notebook is a **very rudimentary** Genie Space — just enough to
# MAGIC > demonstrate the concept. To make it production-ready you would add more sample queries, richer instructions,
# MAGIC > and iteratively optimize based on the questions your team actually asks and the feedback Genie gives.
# MAGIC > Databricks is currently working on **out-of-the-box optimized Genie Spaces for system tables**, so stay tuned.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create a Genie Space for Operations
# MAGIC
# MAGIC We'll create a Genie Space backed by key system tables. Navigate to:
# MAGIC
# MAGIC **Workspace → New → Genie Space**
# MAGIC
# MAGIC ### Configuration:
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | **Name** | `PUMA Ops Debugger` |
# MAGIC | **SQL Warehouse** | *(select your shared warehouse)* |
# MAGIC | **Tables** | See below |
# MAGIC
# MAGIC ### Tables to add:
# MAGIC ```
# MAGIC system.lakeflow.job_run_timeline
# MAGIC system.lakeflow.jobs
# MAGIC system.lakeflow.job_task_run_timeline
# MAGIC system.billing.usage
# MAGIC system.query.history
# MAGIC system.access.audit
# MAGIC system.access.table_lineage
# MAGIC system.compute.clusters
# MAGIC ```
# MAGIC
# MAGIC ### Sample questions to configure:
# MAGIC ```
# MAGIC Which jobs failed in the last 24 hours?
# MAGIC What is our daily DBU spend this week?
# MAGIC Show me the top 10 slowest SQL queries yesterday
# MAGIC Which user ran the most expensive job this month?
# MAGIC What tables feed into the gold_order_summary table?
# MAGIC Are there any queries waiting in the queue right now?
# MAGIC Which warehouse has the highest average query duration?
# MAGIC Show me all permission denied errors in the last 3 days
# MAGIC ```
# MAGIC
# MAGIC ### Instructions to add (helps Genie generate better SQL):
# MAGIC ```
# MAGIC - When asked about "failed jobs", query system.lakeflow.job_run_timeline WHERE result_state = 'FAILED'
# MAGIC - When asked about "cost" or "spend", query system.billing.usage and SUM usage_quantity as DBUs
# MAGIC - When asked about "slow queries", query system.query.history ORDER BY total_duration_ms DESC
# MAGIC - The jobs table is SCD2; to get the latest version of each job, use ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1
# MAGIC - For lineage questions, use system.access.table_lineage
# MAGIC - Always filter on date partitions (event_date, usage_date, start_time) for performance
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Try It Out!
# MAGIC
# MAGIC Once the Genie Space is created, open it and try asking these questions:
# MAGIC
# MAGIC ### Warm-up questions (easy):
# MAGIC 1. *"How many jobs are in workspace 7474653647574759?"*
# MAGIC 2. *"Show me all job runs from today"*
# MAGIC 3. *"What is our total DBU usage this week?"*
# MAGIC
# MAGIC ### Debugging questions (intermediate):
# MAGIC 4. *"Which jobs failed in the last 48 hours? Show the job name and error."*
# MAGIC 5. *"What are the slowest queries on the shared warehouse in the last 3 days?"*
# MAGIC 6. *"Are any queries spending a lot of time waiting in the queue?"*
# MAGIC
# MAGIC ### Investigation questions (advanced):
# MAGIC 7. *"Which user consumed the most DBUs last week? Break down by compute type."*
# MAGIC 8. *"Show me the lineage for the gold_order_summary table — what are its upstream sources?"*
# MAGIC 9. *"Were there any permission denied errors for tables in the puma_ops_lab catalog?"*
# MAGIC 10. *"Compare this week's DBU usage to last week — are we trending up or down?"*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Inspect the Generated SQL
# MAGIC
# MAGIC After each question, click on **"View SQL"** in the Genie response to see the query it generated.
# MAGIC
# MAGIC 🔍 **Discussion points**:
# MAGIC - Is the SQL correct? Does it use the right joins and filters?
# MAGIC - Could you have written the query faster yourself, or did Genie save you time?
# MAGIC - What happens when you ask a follow-up question — does it maintain context?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Key Takeaways
# MAGIC
# MAGIC | Aspect | Detail |
# MAGIC |--------|--------|
# MAGIC | **Best for** | Ad-hoc operational questions, on-call triage, self-service for non-SQL users |
# MAGIC | **Backed by** | Standard SQL on system tables — no magic, fully auditable |
# MAGIC | **Limitations** | Works best with clear table descriptions and sample questions; complex multi-join queries may need manual tuning |
# MAGIC | **Share it** | Give the ops team access to the Genie Space — they can debug without writing SQL |
# MAGIC
# MAGIC ---
# MAGIC **Next**: [day_1_03c_debugging_surfaces — Debugging Surfaces: Where to Look When Jobs Fail]($./day_1_03c_debugging_surfaces)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_03b_genie")
