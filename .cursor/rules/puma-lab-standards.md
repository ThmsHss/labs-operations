# PUMA Ops Lab — Quality Standards and Working Agreement

This rule applies to ALL files under `lab/` in the puma-manila workspace. Follow these standards without exception.

## Context

This is an enterprise customer workshop (PUMA). Every notebook will be read by experienced engineers in a live instructor-led setting. Factual errors, broken queries, or incorrect UI references destroy credibility and waste customer time.

## Non-Negotiable Standards

### 1. Factual Accuracy — Zero Tolerance for False Statements

- Never state a feature is available if it is not. When in doubt, verify against official docs or test it.
- Databricks UI changes frequently. Before referencing any sidebar name, button label, tab name, or panel location, verify it matches the current UI.
- Known critical distinctions to always get right:
  - **Spark UI** is available ONLY for Classic Compute. NOT for Serverless. NOT for SQL Warehouses.
  - **Query Profile** is available for SQL Warehouses and Serverless (via Query History). NOT for Classic Compute.
  - **Cluster logs, event log, compute metrics** are Classic Compute only.
  - The sidebar item is called **Jobs & Pipelines**, not "Workflows → Jobs".

### 2. Always Test SQL Queries via MCP Before Shipping

- Every SQL query that appears in a notebook MUST be tested against the actual workspace using the `execute_sql` MCP tool before the notebook is finalized.
- Test with realistic conditions (correct column names, valid time ranges, appropriate LIMIT).
- Common pitfalls to catch through testing:
  - `system.lakeflow.*` uses `period_start_time` / `period_end_time` (not `period.startTime`).
  - `system.query.history` uses `produced_rows` (not `rows_produced`), `execution_status` (not `status`), has no `produced_bytes` column.
  - `system.lakeflow.job_task_run_timeline` has `compute` (array of struct), not `compute_type`. Use `get(compute, 0).type` for safe access.
  - `EXTRACT(EPOCH FROM ...)` is NOT valid Databricks SQL. Use `TIMESTAMPDIFF(SECOND, start, end)`.
  - Array access like `col[0]` throws on empty arrays. Use `get(col, 0)` instead.

### 3. Research Before Writing

- Before writing or rewriting any content section:
  1. Read the relevant official Databricks documentation pages.
  2. Cross-reference with the actual workspace behavior (use MCP tools).
  3. If a doc link is provided in the content, verify the link works and the content matches what you wrote.
- Use the Databricks skills and MCP tools available in this project — they exist for this purpose.

### 4. Compute Type Awareness

- Every debugging surface, tool reference, or "where to look" instruction MUST specify which compute type(s) it applies to.
- Never tell a user to open Spark UI for a serverless job.
- Never tell a user to check cluster logs or compute metrics for serverless.
- The reference table in `day_1_03c` is the source of truth for what's available where.

### 5. UI References Must Be Precise

- Sidebar navigation: use exact names (e.g., "Jobs & Pipelines", not "Workflows → Jobs").
- Button/link locations: describe exactly where (e.g., "right-side panel under Compute" not "top right").
- Error display: the error banner is at the top of the **Output** section, not "top right of the page".
- Matrix view: it's a bar chart with bars (height = duration, color = status), not a simple grid of colored squares.

### 6. System Table Schemas — Always Verify

- Before using any `system.*` table in a query, run `DESCRIBE system.<schema>.<table>` via MCP to confirm column names and types.
- Column names, types, and availability change over time. Never assume from memory.
- Key tables and their gotchas:
  - `system.lakeflow.jobs`: partition by `workspace_id, job_id` when using ROW_NUMBER.
  - `system.lakeflow.job_run_timeline`: `run_duration_seconds` exists but may be NULL for older rows; use `TIMESTAMPDIFF` as fallback.
  - `system.lakeflow.job_task_run_timeline`: `compute` is an array; use `get()` for safe access.
  - `system.query.history`: no `produced_bytes` column; use `read_bytes`, `written_bytes`, `spilled_local_bytes`.

### 7. Progressive Learning Flow

- Don't dump all concepts upfront in a wall of text. Introduce each debugging surface in context as the user encounters the relevant challenge.
- Flow: navigate → see the error → learn what tools are available for THIS compute type → try it → move to next challenge.
- Clearly distinguish "you will use this now" from "you will use this later in Challenge N".

### 8. Notebook Structure

- Keep explanations concise. This is a workshop, not a textbook.
- Every SQL cell should have a brief markdown header above it explaining what it does.
- Investigation/scratch cells should be clearly marked.
- Link to official docs where appropriate, but don't duplicate entire doc pages.

## Workflow for Editing Lab Notebooks

1. **Read** the current notebook and all related files (challenge jobs, hints, config).
2. **Research** using docs, skills, and MCP tools.
3. **Write** the content.
4. **Test** every SQL query via `execute_sql` MCP.
5. **Verify** UI references against current Databricks behavior.
6. **Review** the full file one more time for consistency.
