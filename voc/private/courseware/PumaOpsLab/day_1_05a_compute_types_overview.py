# Databricks notebook source
# MAGIC %md
# MAGIC # 💰 Day 1 — 05a: Compute Overview & Quick Wins
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC ⏱️ **Time**: ~10 minutes | 🎯 **Goal**: Find cost optimization opportunities in YOUR workspace

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔍 What compute is running in your workspace RIGHT NOW?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Active clusters and warehouses with running costs
# MAGIC SELECT 
# MAGIC     'Cluster' AS type,
# MAGIC     cluster_name AS name,
# MAGIC     owned_by AS owner,
# MAGIC     driver_node_type AS size,
# MAGIC     auto_termination_minutes AS auto_stop_mins,
# MAGIC     CASE WHEN auto_termination_minutes > 60 THEN '⚠️ HIGH' 
# MAGIC          WHEN auto_termination_minutes > 30 THEN '🟡 MEDIUM'
# MAGIC          ELSE '✅ OK' END AS cost_risk
# MAGIC FROM system.compute.clusters
# MAGIC WHERE delete_time IS NULL 
# MAGIC   AND change_time >= current_date() - 7
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC     'Warehouse' AS type,
# MAGIC     warehouse_name AS name,
# MAGIC     '' AS owner,
# MAGIC     warehouse_size AS size,
# MAGIC     auto_stop_minutes,
# MAGIC     CASE WHEN auto_stop_minutes > 30 THEN '⚠️ HIGH'
# MAGIC          WHEN auto_stop_minutes > 10 THEN '🟡 MEDIUM'
# MAGIC          ELSE '✅ OK' END AS cost_risk
# MAGIC FROM system.compute.warehouses
# MAGIC WHERE delete_time IS NULL
# MAGIC ORDER BY cost_risk DESC, type

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Compute Mix: What are you using?

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Last 30 days compute usage breakdown
# MAGIC SELECT 
# MAGIC     sku_name,
# MAGIC     COUNT(DISTINCT usage_date) AS days_used,
# MAGIC     ROUND(SUM(usage_quantity), 1) AS total_dbus,
# MAGIC     ROUND(SUM(usage_quantity) / COUNT(DISTINCT usage_date), 1) AS avg_dbus_per_day
# MAGIC FROM system.billing.usage
# MAGIC WHERE usage_date >= current_date() - 30
# MAGIC GROUP BY sku_name
# MAGIC ORDER BY total_dbus DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⚡ Quick Decision Guide
# MAGIC
# MAGIC | Your Need | Best Choice | Why |
# MAGIC |-----------|-------------|-----|
# MAGIC | Developing notebooks | **All-Purpose** (set 20 min auto-term!) | Interactive, but costly if left on |
# MAGIC | Running daily ETL | **Job Cluster** | Spins up, runs, terminates |
# MAGIC | BI dashboards | **SQL Warehouse (Serverless)** | Instant start, scales to zero |
# MAGIC | Variable workloads | **Serverless Compute** | No idle cost |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Action Items from This Notebook
# MAGIC
# MAGIC Run the queries above and note:
# MAGIC 1. **Any clusters with auto-term > 60 min?** → Reduce to 20-30 min
# MAGIC 2. **Classic warehouses for variable load?** → Switch to Serverless
# MAGIC 3. **Fixed-size clusters?** → Enable autoscaling
# MAGIC
# MAGIC ---
# MAGIC **Next**: [day_1_05b_sql_warehouse_sizing — SQL Warehouse Sizing from Query History]($./day_1_05b_sql_warehouse_sizing)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_05a_compute_types_overview")