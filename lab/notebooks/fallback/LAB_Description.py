# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Operations Masterclass — PUMA
# MAGIC
# MAGIC Welcome to the **Databricks Operations Masterclass**. In this hands-on workshop you will learn
# MAGIC how to trace, debug, monitor, and optimize Databricks workloads using system tables, dashboards,
# MAGIC Lakehouse Monitoring, SQL Alerts, Genie, and the Spark UI / Query Profile.
# MAGIC
# MAGIC ### Environment Setup
# MAGIC - **Catalog:** `puma_ops_lab`
# MAGIC - **Schema:** `workshop`
# MAGIC - **Volume:** `/Volumes/puma_ops_lab/workshop/raw_data`
# MAGIC - **SQL Warehouse:** `PUMA_OPS_SHARED_WAREHOUSE`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Workshop Agenda
# MAGIC
# MAGIC | Block | Topic |
# MAGIC |-------|-------|
# MAGIC | **1** | [01 — System Tables & Dashboards]($./01_System_Tables_and_Dashboards) |
# MAGIC | **2** | [02 — Genie on System Tables]($./02_Genie_System_Tables) |
# MAGIC | **3a** | [03a — Job Tracing Basics]($./03a_Job_Tracing_Basics) |
# MAGIC | **3b** | [03b — Challenge: Fix Failing Jobs]($./03b_Challenge_Fix_Failing_Jobs) |
# MAGIC | **4a** | [04a — SDP Event Log Deep Dive]($./04a_SDP_Event_Log_Deep_Dive) |
# MAGIC | **4b** | [04b — SDP Debugging Challenge]($./04b_SDP_Debugging_Challenge) |
# MAGIC | **5** | [05 — SQL Warehouse Performance]($./05_SQL_Warehouse_Performance) |
# MAGIC | **6** | [06 — Lakehouse Monitoring]($./06_Lakehouse_Monitoring) |
# MAGIC | **7** | [07 — SQL Alerts]($./07_SQL_Alerts) |
# MAGIC | **8** | [08 — Performance Investigation]($./08_Performance_Investigation) |
# MAGIC | **9** | [09 — Operational Playbook]($./09_Operational_Playbook) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Getting Started
# MAGIC
# MAGIC The lab environment is already set up for you. All data, jobs, and pipelines are pre-deployed.
# MAGIC **Start with notebook 01** and work your way through the agenda.
# MAGIC
# MAGIC ### Your Environment
# MAGIC
# MAGIC | Resource | What's there |
# MAGIC |----------|-------------|
# MAGIC | **Catalog** | `puma_ops_lab` — retail data with products, customers, orders, inventory |
# MAGIC | **Jobs** | Several pre-configured jobs — some working, some with issues for you to investigate |
# MAGIC | **Pipelines** | Spark Declarative Pipelines for inventory data processing |
# MAGIC | **SQL Warehouse** | `PUMA_OPS_SHARED_WAREHOUSE` — shared serverless warehouse |
# MAGIC
# MAGIC ### Debugging Challenges (Block 3b)
# MAGIC
# MAGIC In Block 3b you'll investigate **5 failing jobs**. Your task: find the root cause using system tables,
# MAGIC logs, Spark UI, and Query Profile — then fix them.
# MAGIC
# MAGIC | Challenge | Difficulty |
# MAGIC |-----------|-----------|
# MAGIC | Challenge 1 | ⭐ |
# MAGIC | Challenge 2 | ⭐⭐ |
# MAGIC | Challenge 3 | ⭐⭐ |
# MAGIC | Challenge 4 | ⭐⭐ |
# MAGIC | Challenge 5 | ⭐⭐⭐ |
# MAGIC
# MAGIC If you get stuck, ask your instructor for a hint.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Quick Environment Check
# MAGIC Run this cell to validate your environment:

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

tables_to_check = ["products", "customers", "orders", "orders_v2", "inventory_events", "gold_order_summary"]
print("📋 Table inventory:")
for t in tables_to_check:
    try:
        count = spark.table(f"{FQ}.{t}").count()
        print(f"  ✅ {FQ}.{t}: {count:,} rows")
    except Exception as e:
        print(f"  ❌ {FQ}.{t}: {str(e)[:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Ready? Start with [01 — System Tables & Dashboards]($./01_System_Tables_and_Dashboards)**
