# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Day 1 — 05b: SQL Warehouse Sizing from Query History
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC ⏱️ **Time allocation**: ~20 minutes
# MAGIC
# MAGIC ### Learning Objectives
# MAGIC - Analyze query history to determine optimal SQL Warehouse size
# MAGIC - Aggregate query patterns by minute to understand concurrency needs
# MAGIC - Calculate MIN, MAX, and AVG cluster scaling recommendations
# MAGIC - Understand how warehouse size affects query throughput

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🗺️ Why Aggregate Query History?
# MAGIC
# MAGIC To properly size your SQL Warehouse, you need to understand your **query concurrency patterns**. By analyzing how many queries run per minute, you can determine:
# MAGIC
# MAGIC | Metric | What It Tells You |
# MAGIC |--------|-------------------|
# MAGIC | **MIN queries/minute** | Baseline load — minimum clusters needed |
# MAGIC | **MAX queries/minute** | Peak load — maximum scaling target |
# MAGIC | **AVG queries/minute** | Typical load — expected steady-state |
# MAGIC
# MAGIC ### The Sizing Formula
# MAGIC
# MAGIC ```
# MAGIC Clusters Needed = Queries per Minute ÷ Queries per Minute Capacity
# MAGIC ```
# MAGIC
# MAGIC Where **Queries per Minute Capacity** depends on warehouse size (see table below).

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📈 Warehouse Size vs Query Throughput
# MAGIC
# MAGIC SQL Warehouse throughput scales approximately **1.7x** per size increment:
# MAGIC
# MAGIC | Warehouse Size | Est. Queries/Min | Relative Scale |
# MAGIC |----------------|------------------|----------------|
# MAGIC | **2X-Small**   | ~50              | 0.2x           |
# MAGIC | **X-Small**    | ~86              | 0.34x          |
# MAGIC | **Small**      | ~147             | 0.59x          |
# MAGIC | **Medium**     | ~147             | 0.59x          |
# MAGIC | **Large**      | ~250             | 1.0x (baseline)|
# MAGIC | **X-Large**    | ~425             | 1.7x           |
# MAGIC | **2X-Large**   | ~722             | 2.9x           |
# MAGIC | **3X-Large**   | ~1,228           | 4.9x           |
# MAGIC | **4X-Large**   | ~2,087           | 8.3x           |
# MAGIC
# MAGIC > 💡 **Note**: These are estimates. Actual throughput depends on query complexity, data size, and caching.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Aggregate Queries by Minute
# MAGIC
# MAGIC First, let's aggregate your query history to the minute level to understand concurrency patterns.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 1: Aggregate query start times to the minute
# MAGIC -- This shows how many queries were running in each minute window
# MAGIC
# MAGIC WITH query_aggregate AS (
# MAGIC   SELECT
# MAGIC     make_timestamp(
# MAGIC       date_part('YEAR', start_time),
# MAGIC       date_part('MONTH', start_time),
# MAGIC       date_part('DAY', start_time),
# MAGIC       date_part('HOUR', start_time),
# MAGIC       date_part('MINUTE', start_time),
# MAGIC       0
# MAGIC     ) AS start_time_minute,
# MAGIC     COUNT(*) AS query_count
# MAGIC   FROM system.query.history
# MAGIC   WHERE start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY start_time_minute
# MAGIC )
# MAGIC SELECT
# MAGIC   start_time_minute,
# MAGIC   query_count
# MAGIC FROM query_aggregate
# MAGIC ORDER BY start_time_minute DESC
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 Key Observations
# MAGIC - Look for **peak minutes** — these drive your MAX scaling needs
# MAGIC - Identify **quiet periods** — potential for aggressive auto-suspend
# MAGIC - Notice **patterns** — are peaks during business hours? End of month?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Calculate Warehouse Scaling Recommendations
# MAGIC
# MAGIC Now let's calculate the recommended MIN, MAX, and AVG cluster scaling based on a **Large** warehouse (250 queries/min baseline).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate warehouse scaling recommendations for a LARGE warehouse
# MAGIC -- Adjust the divisor based on your target warehouse size
# MAGIC
# MAGIC WITH query_aggregate AS (
# MAGIC   SELECT
# MAGIC     make_timestamp(
# MAGIC       date_part('YEAR', start_time),
# MAGIC       date_part('MONTH', start_time),
# MAGIC       date_part('DAY', start_time),
# MAGIC       date_part('HOUR', start_time),
# MAGIC       date_part('MINUTE', start_time),
# MAGIC       0
# MAGIC     ) AS start_time_minute,
# MAGIC     COUNT(*) AS cnt
# MAGIC   FROM system.query.history
# MAGIC   WHERE start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY start_time_minute
# MAGIC )
# MAGIC SELECT
# MAGIC   MIN(cnt) / 250 AS min_scaling,
# MAGIC   -- Estimated 250 queries per minute on Large warehouse
# MAGIC   -- Scales linearly at 1.7x up or down per size
# MAGIC   -- Medium = 147, XL = 425
# MAGIC   MAX(cnt) / 250 AS max_scaling,
# MAGIC   AVG(cnt) / 250 AS avg_clusters,
# MAGIC   
# MAGIC   -- Additional context metrics
# MAGIC   MIN(cnt) AS min_queries_per_min,
# MAGIC   MAX(cnt) AS max_queries_per_min,
# MAGIC   ROUND(AVG(cnt), 1) AS avg_queries_per_min,
# MAGIC   COUNT(*) AS total_minutes_analyzed
# MAGIC FROM query_aggregate

# COMMAND ----------

# MAGIC %md
# MAGIC ### How to Interpret the Results
# MAGIC
# MAGIC | Metric | Interpretation | Warehouse Setting |
# MAGIC |--------|---------------|-------------------|
# MAGIC | **min_scaling** | Minimum clusters needed during quiet periods | Set as `min_num_clusters` |
# MAGIC | **max_scaling** | Maximum clusters needed at peak | Set as `max_num_clusters` |
# MAGIC | **avg_clusters** | Typical cluster count | Expected steady-state cost |
# MAGIC
# MAGIC > ⚠️ **Important**: Round UP for max_scaling to handle burst traffic!

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Sizing for Different Warehouse Sizes
# MAGIC
# MAGIC Let's calculate scaling for multiple warehouse sizes to help you choose the right one.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare scaling recommendations across warehouse sizes
# MAGIC
# MAGIC WITH query_aggregate AS (
# MAGIC   SELECT
# MAGIC     make_timestamp(
# MAGIC       date_part('YEAR', start_time),
# MAGIC       date_part('MONTH', start_time),
# MAGIC       date_part('DAY', start_time),
# MAGIC       date_part('HOUR', start_time),
# MAGIC       date_part('MINUTE', start_time),
# MAGIC       0
# MAGIC     ) AS start_time_minute,
# MAGIC     COUNT(*) AS cnt
# MAGIC   FROM system.query.history
# MAGIC   WHERE start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY start_time_minute
# MAGIC ),
# MAGIC stats AS (
# MAGIC   SELECT
# MAGIC     MIN(cnt) AS min_qpm,
# MAGIC     MAX(cnt) AS max_qpm,
# MAGIC     AVG(cnt) AS avg_qpm
# MAGIC   FROM query_aggregate
# MAGIC )
# MAGIC SELECT
# MAGIC   'Small' AS warehouse_size,
# MAGIC   147 AS queries_per_min_capacity,
# MAGIC   ROUND(min_qpm / 147, 2) AS min_clusters,
# MAGIC   ROUND(max_qpm / 147, 2) AS max_clusters,
# MAGIC   ROUND(avg_qpm / 147, 2) AS avg_clusters
# MAGIC FROM stats
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Medium',
# MAGIC   147,
# MAGIC   ROUND(min_qpm / 147, 2),
# MAGIC   ROUND(max_qpm / 147, 2),
# MAGIC   ROUND(avg_qpm / 147, 2)
# MAGIC FROM stats
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'Large',
# MAGIC   250,
# MAGIC   ROUND(min_qpm / 250, 2),
# MAGIC   ROUND(max_qpm / 250, 2),
# MAGIC   ROUND(avg_qpm / 250, 2)
# MAGIC FROM stats
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'X-Large',
# MAGIC   425,
# MAGIC   ROUND(min_qpm / 425, 2),
# MAGIC   ROUND(max_qpm / 425, 2),
# MAGIC   ROUND(avg_qpm / 425, 2)
# MAGIC FROM stats
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   '2X-Large',
# MAGIC   722,
# MAGIC   ROUND(min_qpm / 722, 2),
# MAGIC   ROUND(max_qpm / 722, 2),
# MAGIC   ROUND(avg_qpm / 722, 2)
# MAGIC FROM stats

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 Choosing the Right Size
# MAGIC
# MAGIC | If Your Max Clusters | Consider |
# MAGIC |---------------------|----------|
# MAGIC | < 1 | Smaller warehouse size |
# MAGIC | 1-3 | Current size is appropriate |
# MAGIC | 3-10 | Good for auto-scaling |
# MAGIC | > 10 | Consider larger warehouse size |
# MAGIC
# MAGIC **Rule of Thumb**: Aim for 2-5 clusters at peak for cost-effective auto-scaling.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Query Pattern Analysis by Hour
# MAGIC
# MAGIC Understanding when peaks occur helps with capacity planning.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze query patterns by hour of day
# MAGIC
# MAGIC WITH query_aggregate AS (
# MAGIC   SELECT
# MAGIC     date_part('HOUR', start_time) AS hour_of_day,
# MAGIC     make_timestamp(
# MAGIC       date_part('YEAR', start_time),
# MAGIC       date_part('MONTH', start_time),
# MAGIC       date_part('DAY', start_time),
# MAGIC       date_part('HOUR', start_time),
# MAGIC       date_part('MINUTE', start_time),
# MAGIC       0
# MAGIC     ) AS start_time_minute,
# MAGIC     COUNT(*) AS query_count
# MAGIC   FROM system.query.history
# MAGIC   WHERE start_time >= current_date() - INTERVAL 30 DAYS
# MAGIC   GROUP BY 1, 2
# MAGIC )
# MAGIC SELECT
# MAGIC   hour_of_day,
# MAGIC   ROUND(AVG(query_count), 1) AS avg_queries_per_min,
# MAGIC   MAX(query_count) AS peak_queries_per_min,
# MAGIC   COUNT(*) AS sample_count,
# MAGIC   ROUND(MAX(query_count) / 250, 2) AS peak_clusters_needed_large
# MAGIC FROM query_aggregate
# MAGIC GROUP BY hour_of_day
# MAGIC ORDER BY hour_of_day

# COMMAND ----------

# MAGIC %md
# MAGIC ### 💡 Pattern Insights
# MAGIC - **Peak hours**: When do you need maximum capacity?
# MAGIC - **Off-peak hours**: Opportunity for aggressive auto-suspend
# MAGIC - **Weekend patterns**: Consider different scaling for weekends

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📋 Summary
# MAGIC
# MAGIC | Step | Action |
# MAGIC |------|--------|
# MAGIC | 1. Aggregate | Group query history by minute |
# MAGIC | 2. Calculate | Divide by queries/min capacity for your warehouse size |
# MAGIC | 3. Configure | Set `min_num_clusters` and `max_num_clusters` based on results |
# MAGIC | 4. Monitor | Review periodically as workload patterns change |
# MAGIC
# MAGIC ### Quick Reference: Warehouse Capacity
# MAGIC
# MAGIC | Size | Queries/Min | Scale Factor |
# MAGIC |------|-------------|--------------|
# MAGIC | Small/Medium | 147 | 0.59x |
# MAGIC | **Large** | **250** | **1.0x** |
# MAGIC | X-Large | 425 | 1.7x |
# MAGIC | 2X-Large | 722 | 2.9x |
# MAGIC
# MAGIC ### Example Configuration
# MAGIC
# MAGIC If your analysis shows:
# MAGIC - MIN: 50 queries/min → `min_num_clusters = 1`
# MAGIC - MAX: 800 queries/min → `max_num_clusters = 4` (800 ÷ 250 = 3.2, round up)
# MAGIC - AVG: 200 queries/min → Expected 1 cluster most of the time
# MAGIC
# MAGIC ---
# MAGIC **Next**: [day_1_05c_cluster_policy — Cluster Policy Demo]($./day_1_05c_cluster_policy)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_05b_sql_warehouse_sizing")