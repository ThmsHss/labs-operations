#!/voc/course/venv/bin/python3
"""
Vocareum lab_setup override for PUMA Ops Masterclass.

Called when a lab session starts. Ensures:
- The data setup notebook has been run (tables exist)
- Challenge jobs are created and have been triggered once (failure history)
- SDP pipelines are created
- Dashboards are imported

Place this file at /voc/private/python/lab_setup.py to override the default.
"""

import json
import os
import sys
import time

from databricks.sdk import WorkspaceClient

WORKSPACE_PATH = "/Workspace/Shared/puma_ops_masterclass"
CATALOG = "puma_ops_lab"
SCHEMA = "workshop"


def trace_it():
    import traceback
    traceback.print_exc()


def main():
    workspace_url = os.getenv("VOC_DB_WORKSPACE_URL")
    token = os.getenv("VOC_DB_API_TOKEN")
    user = os.getenv("VOC_DB_USER_EMAIL")

    if not workspace_url or not token:
        print("ERROR: VOC_DB_WORKSPACE_URL and VOC_DB_API_TOKEN must be set")
        sys.exit(1)

    w = WorkspaceClient(host=workspace_url, token=token)
    print(f"Lab setup for user: {user}")

    # ── Check if sample data exists ──────────────────────────────────────────
    print("Checking if sample data exists ...")
    warehouse_id = _get_warehouse_id(w)
    if warehouse_id:
        try:
            result = w.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=f"SELECT COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.orders",
                wait_timeout="30s",
            )
            if result.result and result.result.data_array:
                count = result.result.data_array[0][0]
                print(f"  orders table: {count} rows")
                if int(count) > 0:
                    print("  ✅ Sample data already exists")
                else:
                    print("  ⚠️ Orders table empty — running setup notebook")
                    _run_setup_notebook(w)
            else:
                _run_setup_notebook(w)
        except Exception:
            print("  Sample data not found — running setup notebook")
            _run_setup_notebook(w)
    else:
        print("  ⚠️ No warehouse found — cannot validate data")

    # ── Create jobs if they don't exist ──────────────────────────────────────
    print("Ensuring challenge jobs exist ...")
    _ensure_jobs(w, warehouse_id)

    # ── Create pipelines if they don't exist ─────────────────────────────────
    print("Ensuring SDP pipelines exist ...")
    _ensure_pipelines(w)

    # ── Import dashboards if they don't exist ────────────────────────────────
    print("Ensuring dashboards exist ...")
    _ensure_dashboards(w, warehouse_id)

    # ── Trigger jobs once for system table history ───────────────────────────
    print("Triggering jobs for system table history ...")
    _trigger_jobs_for_history(w)

    print("✅ Lab setup complete")
    sys.exit(0)


def _get_warehouse_id(w):
    for wh in w.warehouses.list():
        if "PUMA_OPS" in wh.name.upper() or "SHARED" in wh.name.upper():
            return wh.id
    warehouses = list(w.warehouses.list())
    return warehouses[0].id if warehouses else None


def _run_setup_notebook(w):
    """Run the 00_Environment_Setup notebook."""
    notebook_path = f"{WORKSPACE_PATH}/notebooks/00_Environment_Setup"
    print(f"  Running {notebook_path} ...")
    try:
        run = w.jobs.submit(
            run_name="puma_ops_lab_setup",
            tasks=[{
                "task_key": "setup",
                "notebook_task": {"notebook_path": notebook_path, "source": "WORKSPACE"},
                "environment_key": "default",
            }],
            environments=[{"environment_key": "default", "spec": {"client": "1"}}],
        )
        print(f"  Submitted run {run.run_id}")
        result = w.jobs.get_run(run_id=run.run_id)
        timeout = 300
        elapsed = 0
        while result.state.life_cycle_state in ("PENDING", "RUNNING", "QUEUED") and elapsed < timeout:
            time.sleep(15)
            elapsed += 15
            result = w.jobs.get_run(run_id=run.run_id)
            print(f"    Status: {result.state.life_cycle_state} ({elapsed}s)")

        if result.state.result_state == "SUCCESS":
            print("  ✅ Setup notebook completed")
        else:
            print(f"  ⚠️ Setup notebook: {result.state.result_state} — {result.state.state_message}")
    except Exception as e:
        print(f"  ⚠️ Could not run setup notebook: {e}")


def _ensure_jobs(w, warehouse_id):
    """Create challenge jobs if they don't exist."""
    single_task_jobs = {
        "job_00_working_etl": ("jobs/job_00_working_etl", "serverless", {}),
        "puma_ops_challenge_01_schema": ("jobs/job_01_schema_mismatch", "serverless", {}),
        "puma_ops_challenge_03_oom": ("jobs/job_03_oom_heavy_shuffle", "job_cluster", {}),
        "puma_ops_challenge_04_params": ("jobs/job_04_bad_parameters", "serverless", {"region": "APEC", "min_date": "2024-01-01"}),
        "puma_ops_challenge_05_slow_query": ("jobs/job_05_slow_query", "serverless", {}),
    }

    multi_task_jobs = {
        "puma_ops_challenge_02_permissions": {
            "compute": "serverless",
            "tasks": [
                {"task_key": "extract_orders", "notebook": "jobs/job_02_task_extract"},
                {"task_key": "enrich_with_segments", "notebook": "jobs/job_02_task_enrich", "depends_on": ["extract_orders"]},
                {"task_key": "build_report", "notebook": "jobs/job_02_task_report", "depends_on": ["enrich_with_segments"]},
            ],
        },
    }

    existing_names = set()
    for j in w.jobs.list():
        if j.settings and j.settings.name:
            existing_names.add(j.settings.name)

    for job_name, (notebook, compute, params) in single_task_jobs.items():
        if job_name in existing_names:
            print(f"    Job '{job_name}' already exists")
            continue

        notebook_path = f"{WORKSPACE_PATH}/{notebook}"
        task = {
            "task_key": "main",
            "notebook_task": {
                "notebook_path": notebook_path,
                "source": "WORKSPACE",
                "base_parameters": params,
            },
        }
        job_settings = {
            "name": job_name,
            "tags": {"Workshop": "puma_ops_masterclass"},
        }

        if compute == "serverless":
            task["environment_key"] = "default"
            job_settings["environments"] = [
                {"environment_key": "default", "spec": {"client": "1"}}
            ]
        elif compute == "job_cluster":
            job_settings["job_clusters"] = [{
                "job_cluster_key": "lab_cluster",
                "new_cluster": {
                    "spark_version": "15.4.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 1,
                },
            }]
            task["job_cluster_key"] = "lab_cluster"

        job_settings["tasks"] = [task]
        try:
            job = w.jobs.create(**job_settings)
            print(f"    Created job '{job_name}' (id={job.job_id})")
        except Exception as e:
            print(f"    ⚠️ Could not create '{job_name}': {e}")

    for job_name, job_def in multi_task_jobs.items():
        if job_name in existing_names:
            print(f"    Job '{job_name}' already exists")
            continue

        compute = job_def["compute"]
        job_settings = {
            "name": job_name,
            "tags": {"Workshop": "puma_ops_masterclass"},
        }

        if compute == "serverless":
            job_settings["environments"] = [
                {"environment_key": "default", "spec": {"client": "1"}}
            ]

        tasks = []
        for td in job_def["tasks"]:
            tc = {
                "task_key": td["task_key"],
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_PATH}/{td['notebook']}",
                    "source": "WORKSPACE",
                },
            }
            if td.get("depends_on"):
                tc["depends_on"] = [{"task_key": k} for k in td["depends_on"]]
            if compute == "serverless":
                tc["environment_key"] = "default"
            tasks.append(tc)

        job_settings["tasks"] = tasks
        try:
            job = w.jobs.create(**job_settings)
            print(f"    Created job '{job_name}' (id={job.job_id})")
        except Exception as e:
            print(f"    ⚠️ Could not create '{job_name}': {e}")


def _ensure_pipelines(w):
    """Create SDP pipelines if they don't exist."""
    pipeline_defs = {
        "puma_ops_inventory_pipeline": [
            "pipelines/pipeline_bronze",
            "pipelines/pipeline_silver",
            "pipelines/pipeline_gold",
        ],
        "puma_ops_inventory_pipeline_broken": [
            "pipelines/pipeline_broken_bronze",
            "pipelines/pipeline_silver",
            "pipelines/pipeline_gold",
        ],
    }

    existing_names = set()
    for p in w.pipelines.list_pipelines():
        if p.name:
            existing_names.add(p.name)

    for name, notebooks in pipeline_defs.items():
        if name in existing_names:
            print(f"    Pipeline '{name}' already exists")
            continue
        try:
            libs = [{"path": f"{WORKSPACE_PATH}/{nb}"} for nb in notebooks]
            pipeline = w.pipelines.create(
                name=name,
                catalog=CATALOG,
                target=SCHEMA,
                libraries=libs,
                serverless=True,
                development=True,
                continuous=False,
            )
            print(f"    Created pipeline '{name}' (id={pipeline.pipeline_id})")
        except Exception as e:
            print(f"    ⚠️ Could not create '{name}': {e}")


def _ensure_dashboards(w, warehouse_id):
    """Import dashboards if they don't exist."""
    dashboard_dir = "/voc/private/courseware"
    import glob
    json_files = glob.glob(f"{dashboard_dir}/*dashboard*.json")
    if not json_files:
        dashboard_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboards")
        json_files = glob.glob(f"{dashboard_dir}/*dashboard*.json")

    existing_names = set()
    try:
        for d in w.lakeview.list():
            if d.display_name:
                existing_names.add(d.display_name)
    except Exception:
        pass

    for json_file in json_files:
        display_name = os.path.basename(json_file).replace(".json", "").replace("_", " ").title()
        if display_name in existing_names:
            print(f"    Dashboard '{display_name}' already exists")
            continue
        try:
            with open(json_file) as f:
                dash_json = json.load(f)
            result = w.lakeview.create(
                display_name=display_name,
                parent_path=f"{WORKSPACE_PATH}/dashboards",
                serialized_dashboard=json.dumps(dash_json),
                warehouse_id=warehouse_id,
            )
            w.lakeview.publish(dashboard_id=result.dashboard_id, warehouse_id=warehouse_id)
            print(f"    Created dashboard '{display_name}'")
        except Exception as e:
            print(f"    ⚠️ Could not create dashboard: {e}")


def _trigger_jobs_for_history(w):
    """Run working ETL and trigger challenge jobs to populate system tables."""
    job_names_to_trigger = [
        "job_00_working_etl",
        "puma_ops_challenge_01_schema",
        "puma_ops_challenge_02_permissions",
        "puma_ops_challenge_04_params",
    ]
    for name in job_names_to_trigger:
        for j in w.jobs.list(name=name):
            if j.settings and j.settings.name == name:
                try:
                    run = w.jobs.run_now(job_id=j.job_id)
                    print(f"    Triggered '{name}' (run_id={run.run_id})")
                except Exception as e:
                    print(f"    ⚠️ Could not trigger '{name}': {e}")
                break


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        trace_it()
        sys.exit(1)
