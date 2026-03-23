# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Operations Masterclass — PUMA (INSTRUCTOR GUIDE)
# MAGIC
# MAGIC ### Environment Setup
# MAGIC - **Catalog:** `puma_ops_lab`
# MAGIC - **Schema:** `workshop`
# MAGIC - **Volume:** `/Volumes/puma_ops_lab/workshop/raw_data`
# MAGIC - **SQL Warehouse:** `PUMA_OPS_SHARED_WAREHOUSE`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Workshop Agenda (with timings)
# MAGIC
# MAGIC **Total: ~3 hours** (including breaks)
# MAGIC
# MAGIC | Block | Topic | Duration | Notes |
# MAGIC |-------|-------|----------|-------|
# MAGIC | **1** | [01 — System Tables & Dashboards]($../notebooks/01_System_Tables_and_Dashboards) | 30 min | 15 min guided demo + 15 min exercises |
# MAGIC | **2** | [02 — Genie on System Tables]($../notebooks/02_Genie_System_Tables) | 15 min | 5 min demo + 10 min exploration |
# MAGIC | **3a** | [03a — Job Tracing Basics]($../notebooks/03a_Job_Tracing_Basics) | 10 min | Instructor-guided walkthrough |
# MAGIC | **3b** | [03b — Challenge: Fix Failing Jobs]($../notebooks/03b_Challenge_Fix_Failing_Jobs) | 25 min | Participants work independently |
# MAGIC | | ☕ **Break** | 10 min | |
# MAGIC | **4a** | [04a — SDP Event Log Deep Dive]($../notebooks/04a_SDP_Event_Log_Deep_Dive) | 15 min | 10 min demo + 5 min exercises |
# MAGIC | **4b** | [04b — SDP Debugging Challenge]($../notebooks/04b_SDP_Debugging_Challenge) | 15 min | Participants fix broken pipeline |
# MAGIC | **5** | [05 — SQL Warehouse Performance]($../notebooks/05_SQL_Warehouse_Performance) | 20 min | Hands-on |
# MAGIC | **6** | [06 — Lakehouse Monitoring]($../notebooks/06_Lakehouse_Monitoring) | 20 min | 8 min demo + 12 min exercises |
# MAGIC | **7** | [07 — SQL Alerts]($../notebooks/07_SQL_Alerts) | 15 min | Hands-on |
# MAGIC | **8** | [08 — Performance Investigation]($../notebooks/08_Performance_Investigation) | 20 min | Hands-on, advanced |
# MAGIC | **9** | [09 — Operational Playbook]($../notebooks/09_Operational_Playbook) | 10 min | Wrap-up |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge Answer Key
# MAGIC
# MAGIC | Job | Bug | Root Cause | Fix |
# MAGIC |-----|-----|-----------|-----|
# MAGIC | `puma_ops_challenge_01_schema` | Column renamed upstream | `total_amount` → `order_total` in `orders_v2` | Update column reference |
# MAGIC | `puma_ops_challenge_02_permissions` | Missing SELECT on `curated` schema, then ambiguous column | Multi-task job: task 2 reads `curated.customer_segments` without SELECT → after GRANT, `AMBIGUOUS_REFERENCE` on `region` (both tables have it) | `GRANT SELECT ON SCHEMA puma_ops_lab.curated TO \`user\`` + **Repair Run**, then fix `region` → `df_staging["region"]` + **Repair Run** again |
# MAGIC | `puma_ops_challenge_04_params` (Challenge 3) | Typo in parameter | `"APEC"` instead of `"APAC"` → zero rows → assertion fails | Fix parameter value |
# MAGIC | `puma_ops_challenge_03_oom` (Challenge 4) | Cross-join instead of inner join | `crossJoin` causes cartesian product → runs forever or OOM. Spark UI may not help on single-node — participants must read the code | Replace `crossJoin` with `join(..., "customer_id")` |
# MAGIC | `puma_ops_challenge_05_slow_query` | Un-optimized table | Full scan on `gold_order_summary` (no OPTIMIZE, no clustering) | Run `OPTIMIZE` + add liquid clustering |
# MAGIC
# MAGIC ### SDP Pipeline Challenge
# MAGIC | Pipeline | Bug | Fix |
# MAGIC |---------|-----|-----|
# MAGIC | `puma_ops_inventory_pipeline_broken` | Typo in source path | `inventori_raw` → `inventory_raw` in bronze layer |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Hint Notebooks
# MAGIC
# MAGIC Located in your instructor folder at:
# MAGIC `/Workspace/Users/adminuser3541130@vocareum.com/instructor_hints/`
# MAGIC
# MAGIC | Hint | For |
# MAGIC |------|-----|
# MAGIC | `hint_challenge_01` | Challenge 1 — Schema mismatch |
# MAGIC | `hint_challenge_02` | Challenge 2 — Permission denied |
# MAGIC | `hint_challenge_03` | Challenge 3 — OOM / heavy shuffle |
# MAGIC | `hint_challenge_04` | Challenge 4 — Bad parameters |
# MAGIC | `hint_challenge_05` | Challenge 5 — Slow query |
# MAGIC | `hint_sdp_pipeline` | SDP broken pipeline |
