# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Building ThermIQ – The Vaillant Multi-Agent Ecosystem
# MAGIC
# MAGIC In this lab, you will build **ThermIQ**, the primary service orchestrator for Vaillant field technicians. You will move from raw technical manuals and customer databases to a fully orchestrated system that provides unified repair plans.
# MAGIC
# MAGIC ### Environment Setup
# MAGIC - **Catalog:** `thermiq` 
# MAGIC - **Schema:** `agent` 
# MAGIC - **Live Data (Lakebase):** `thermiq_live.public.warehouse_stock` 
# MAGIC - **Manuals Volume:** `/Volumes/thermiq/agent/sharepoint_files`

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: Grounding in Technical Manuals
# MAGIC **Objective:** Transform static engineering PDFs into a searchable knowledge base.
# MAGIC
# MAGIC ### 1.1 Investigate the Source
# MAGIC Navigate to **Catalog Explorer** and locate your volume: `/Volumes/thermiq/agent/sharepoint_files`. 
# MAGIC These are the official Vaillant engineering manuals.
# MAGIC
# MAGIC ### 1.2 Build the Knowledge Assistant
# MAGIC 1. In the **Agents** UI, create a new **Knowledge Assistant**
# MAGIC 2. **Name:** `Knowledge-Assistant-Vaillant-Manuals` 
# MAGIC 3. **Description:** A specialist technical assistant designed to extract precise diagnostic data and repair workflows from official Vaillant engineering documentation.
# MAGIC
# MAGIC > **Tip:** If you don't want to wait 15 mins, use the already existing KA called Knowledge-Assistant-Vaillant-Manuals. Ping Me if you don't see it :)
# MAGIC
# MAGIC 4. **Test Query:** *"How do I bleed a Vaillant ecoTEC plus boiler and what is the correct system pressure?"*
# MAGIC
# MAGIC
# MAGIC
# MAGIC > **Tip:** Check the citations! It should point directly to a PDF page, not a general web result.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Customer History & Genie
# MAGIC **Objective:** Use natural language to analyze structured historical data.
# MAGIC
# MAGIC ### 2.1 The Analytical Agent
# MAGIC Open the pre-configured **Genie Space**: [Analytical Agent](https://dbc-2b922853-012b.cloud.databricks.com/genie/rooms/01f108f84826171e855779719dc88dbf?o=7474648766659500).
# MAGIC
# MAGIC This space holds `Accounts`, `Assets`, and `Cases`. Try these queries to see how Genie handles relational data:
# MAGIC - *"How many customers reported a 'F.28' fault code in the last 6 months?"*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Real-Time Logistics with UC Functions
# MAGIC **Objective:** Access live warehouse stock using a tool grounded in Unity Catalog.
# MAGIC
# MAGIC ### 3.1 Explore Lakebase
# MAGIC Your stock data is live in the Lakebase UI or in UC `thermiq_live.public.warehouse_stock`. Explore Lakebase (top right corner and query the data from the Postgres SQL Editor)
# MAGIC
# MAGIC ### 3.2 Test the Inventory Tool
# MAGIC Check the Tool Definition in Unity Catalog and afterwards run the following cell to test the logic of the tool defined in `thermiq.agent`. This tool is designed to be flexible: if a city is missing, it searches globally.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM thermiq.agent.check_live_part_stock('Remscheid', '0020219551');

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: External Intelligence (MCP)
# MAGIC **Objective:** Fetch real-time external data for issues not yet in the official manuals.
# MAGIC
# MAGIC ### 4.1 The Web Scout
# MAGIC Navigate to the [live_web_search MCP Connection](https://dbc-2b922853-012b.cloud.databricks.com/explore/connections/live_web_search?o=7474648766659500).
# MAGIC
# MAGIC Try this in the Agent Playground using the `mcp-live-web-search` tool:
# MAGIC - Navigate to playground, select your model and Add Tool -> External MCP -> live_web_search
# MAGIC - Ask *"Are there any known connectivity issues between the 2025 Nest Learning Thermostat and Vaillant eBUS systems?"*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: Orchestrating the Thermiq-Supervisor
# MAGIC **Objective:** Stitch all agents together into a Lead Orchestrator.
# MAGIC
# MAGIC ### 5.1 Create the Supervisor
# MAGIC 1. Create a new **Multi-Agent Supervisor** named `Thermiq-Supervisor-YourName`: The primary service orchestrator for Vaillant field technicians and support staff. It synthesizes technical documentation, customer history, and real-time logistics to provide a unified repair plan.
# MAGIC 2. **Add Agents/Tools:**
# MAGIC    - `agent-analytical-agent` (Genie): Combined tables of Accounts, Assets, and Cases. Provides customer service tiers, product installation dates, and historical case resolutions—specifically identifying 'Edge Cases' where standard manuals were insufficient or analytical analisis is needed.
# MAGIC    - `Knowledge-Assistant-Vaillant-Manuals" (Agent Bricks Knowledge Assistant): A specialist technical assistant designed to extract precise diagnostic data and repair workflows from official Vaillant engineering documentation.
# MAGIC    - `inventory-tool` (UC Function): Checks inventory. If city is null, searches across all regions. If search_term is null, lists all parts.
# MAGIC    - `mcp-live-web-search` (Tavily): Uses the Tavily MCP to fetch the latest technical bulletins, community-found fixes, or 3rd-party compatibility issues (e.g., Nest/Hive thermostats) not yet documented in official Vaillant PDFs.
# MAGIC
# MAGIC
# MAGIC
# MAGIC ### 5.2 Add Instructions (Optional Field Bottom)
# MAGIC Copy and paste this into the Supervisor's instructions:
# MAGIC
# MAGIC > "You are the ThermIQ Lead Orchestrator for Vaillant field staff. When a fault is reported:
# MAGIC > 1. Use **agent-analytical-agent** to analyze how often this specific issue happened in the past and if it is a known 'Edge Case'.
# MAGIC > 2. Use **Knowledge-Assistant-Vaillant-Manuals** to retrieve the official manufacturer's repair workflow.
# MAGIC > 3. Use **inventory-tool** to check live part availability in the customer's city (if no city is provided, search all regions).
# MAGIC > 4. If the user explicitly asks for a web search or if internal manuals lack recent data, use **mcp-live-web-search** to find external technical bulletins."
# MAGIC
# MAGIC ### 5.3 Final Test
# MAGIC **The Scenario:** *"Customer_7 got error code F.28, what could be the issue? Is this a common issue and what were common resolutions?"*
# MAGIC
# MAGIC **Please check if stock is available for replacement"*
# MAGIC
# MAGIC **Were there any similiar issues reported by other customers publicly? (internet)"*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Next Steps
# MAGIC - **Review Traces:** Use the MLflow Trace UI to see how the Supervisor called each agent in sequence.
# MAGIC - **App:** Checkout the Deployed App using a custom UI.
# MAGIC 🎉 **Congratulations! You've built your first industrial Multi-Agent system.**