# Databricks notebook source
# MAGIC %md
# MAGIC # 🚨 Day 2 — 04: SQL Alerts Challenge
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC ⏱️ **Time**: ~10 minutes | 🎯 **Goal**: Design your own monitoring alert

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏋️ Challenge: Design a SQL Alert
# MAGIC
# MAGIC **Your task**: Write a SQL query that returns a **single value** to monitor.
# MAGIC
# MAGIC ### Example Scenarios to Monitor:
# MAGIC - Failed jobs in the last hour
# MAGIC - Orders with null customer_id
# MAGIC - Data freshness (hours since last record)
# MAGIC - Cost threshold breach
# MAGIC - Row count anomaly

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Write Your Alert Query
# MAGIC
# MAGIC Your query should return a **single numeric value** that can be compared to a threshold.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 🎯 YOUR CHALLENGE: Write a query that monitors something important
# MAGIC -- Examples:
# MAGIC --   SELECT COUNT(*) AS failed_jobs FROM.  ...
# MAGIC --   SELECT TIMESTAMPDIFF(HOUR, MAX(order_timestamp), CURRENT_TIMESTAMP()) AS hours_stale FROM ...
# MAGIC --   SELECT SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_count FROM ...
# MAGIC
# MAGIC -- Write your query here:
# MAGIC SELECT 
# MAGIC     COUNT(*) AS alert_value
# MAGIC FROM puma_ops_lab.workshop.orders
# MAGIC WHERE customer_id IS NULL
# MAGIC   AND order_timestamp >= CURRENT_DATE() - 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Use This Alert Message Template
# MAGIC
# MAGIC When creating your alert in the UI, use this HTML template for the notification body.
# MAGIC Replace `XXX` with your specific context:
# MAGIC
# MAGIC ```html
# MAGIC 🚨 XXX Alert
# MAGIC
# MAGIC <h3>🚨 Summary</h3>
# MAGIC <p><strong>{{QUERY_RESULT_VALUE}}</strong> negative feedback event(s) detected in the last 24 hours.</p>
# MAGIC
# MAGIC <h3>⚠️ Action Required</h3>
# MAGIC <p>Users are dissatisfied with XXX. Please review.</p>
# MAGIC
# MAGIC <h3>📊 Next Steps</h3>
# MAGIC <ul>
# MAGIC   <li>Review flagged conversations in the audit log</li>
# MAGIC   <li>Identify common failure patterns</li>
# MAGIC   <li>Document findings and escalate if needed</li>
# MAGIC </ul>
# MAGIC
# MAGIC <hr>
# MAGIC
# MAGIC <p><a href="{{ALERT_URL}}">View Alert Details →</a></p>
# MAGIC ```
# MAGIC
# MAGIC ### Template Variables
# MAGIC
# MAGIC | Variable | Description |
# MAGIC |----------|-------------|
# MAGIC | `{{QUERY_RESULT_VALUE}}` | The value returned by your query |
# MAGIC | `{{ALERT_URL}}` | Link to the alert in Databricks |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Create the Alert in UI
# MAGIC
# MAGIC 1. Go to **SQL Editor** → **Alerts**
# MAGIC 2. Click **+ Create Alert**
# MAGIC 3. Configure:
# MAGIC    - **Name**: `[PUMA] Your Alert Name`
# MAGIC    - **Query**: Paste your SQL from above
# MAGIC    - **Schedule**: Every 15 minutes (or as needed)
# MAGIC    - **Trigger when**: `alert_value > 0` (or your threshold)
# MAGIC    - **Custom body**: Use the HTML template above
# MAGIC
# MAGIC ---
# MAGIC **Next**: [day_2_05_declarative_automation_bundles — Declarative Automation Bundles]($./day_2_05_declarative_automation_bundles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_2_04_sql_alerts")