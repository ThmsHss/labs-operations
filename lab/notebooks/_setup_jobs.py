# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Create & Trigger Your Challenge Jobs
# MAGIC This notebook creates your personal challenge jobs and triggers them.
# MAGIC Called automatically via `%run` — no action needed from you.

# COMMAND ----------

import requests, json

host = spark.conf.get("spark.databricks.workspaceUrl", "")
if not host.startswith("http"):
    host = f"https://{host}"

token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
base_path = nb_path.rsplit("/", 1)[0]
jobs_base = f"{base_path}/jobs"

prefix = user_prefix

def _api(method, path, body=None):
    url = f"{host}/api/2.0{path}"
    resp = requests.request(method, url, headers=headers, json=body, timeout=30)
    resp.raise_for_status()
    return resp.json() if resp.text else {}

def _find_warehouse():
    for wh in _api("GET", "/sql/warehouses").get("warehouses", []):
        if wh.get("state") in ("RUNNING", "STARTING"):
            return wh["id"]
    for wh in _api("GET", "/sql/warehouses").get("warehouses", []):
        return wh["id"]
    return None

# ── Job definitions ──────────────────────────────────────────────────────────

JOB_DEFS = [
    {
        "suffix": "job_00_working_etl",
        "notebook": "job_00_working_etl",
        "compute": "serverless",
    },
    {
        "suffix": "challenge_01_schema",
        "notebook": "job_01_schema_mismatch",
        "compute": "serverless",
    },
    {
        "suffix": "challenge_02_permissions",
        "compute": "serverless",
        "tasks": [
            {"task_key": "extract_orders", "notebook": "job_02_task_extract"},
            {"task_key": "enrich_with_segments", "notebook": "job_02_task_enrich", "depends_on": ["extract_orders"]},
            {"task_key": "build_report", "notebook": "job_02_task_report", "depends_on": ["enrich_with_segments"]},
        ],
    },
    {
        "suffix": "challenge_03_oom",
        "notebook": "job_03_oom_heavy_shuffle",
        "compute": "job_cluster",
    },
    {
        "suffix": "challenge_04_params",
        "notebook": "job_04_bad_parameters",
        "compute": "serverless",
        "parameters": {"region": "APEC", "min_date": "2024-01-01"},
    },
]

# ── Create and trigger each job ──────────────────────────────────────────────

warehouse_id = None
created = 0
triggered = 0

print(f"🔑 Your job prefix: {prefix}")
print(f"📂 Job notebooks: {jobs_base}/")
print(f"   Creating and triggering jobs...\n")

for jd in JOB_DEFS:
    job_name = f"{prefix}_{jd['suffix']}"
    nb_full_path = f"{jobs_base}/{jd['notebook']}" if "notebook" in jd else None

    # Check if job already exists
    existing_id = None
    try:
        resp = _api("GET", f"/jobs/list?name={requests.utils.quote(job_name)}")
        for j in resp.get("jobs", []):
            if j.get("settings", {}).get("name") == job_name:
                existing_id = j["job_id"]
                break
    except Exception:
        pass

    if existing_id:
        try:
            run = _api("POST", "/jobs/run-now", {"job_id": existing_id})
            print(f"  ✅ {job_name} (exists) → run_id={run.get('run_id', '?')}")
            triggered += 1
        except Exception as e:
            print(f"  ⚠️  {job_name} — trigger failed: {e}")
        continue

    compute = jd["compute"]

    job_settings = {
        "name": job_name,
        "tags": {"Workshop": "puma_ops_masterclass", "participant": current_user},
    }

    if compute == "serverless":
        job_settings["environments"] = [
            {"environment_key": "default", "spec": {"client": "1"}}
        ]
    elif compute == "job_cluster":
        job_settings["job_clusters"] = [{
            "job_cluster_key": "lab_cluster",
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "m5.large",
                "num_workers": 0,
                "data_security_mode": "SINGLE_USER",
                "policy_id": "000FD2933ECD2F11",
                "aws_attributes": {"first_on_demand": 1, "availability": "SPOT_WITH_FALLBACK"},
                "custom_tags": {"Workshop": "puma_ops_masterclass"},
                "cluster_log_conf": {
                    "volumes": {
                        "destination": "/Volumes/puma_ops_lab/workshop/raw_data/cluster_logs"
                    }
                },
            },
        }]

    # Multi-task job
    if "tasks" in jd:
        tasks = []
        for td in jd["tasks"]:
            tc = {
                "task_key": td["task_key"],
                "notebook_task": {
                    "notebook_path": f"{jobs_base}/{td['notebook']}",
                    "source": "WORKSPACE",
                },
            }
            if td.get("depends_on"):
                tc["depends_on"] = [{"task_key": k} for k in td["depends_on"]]
            if compute == "serverless":
                tc["environment_key"] = "default"
            elif compute == "job_cluster":
                tc["job_cluster_key"] = "lab_cluster"
            tasks.append(tc)
        job_settings["tasks"] = tasks
    else:
        # Single-task job
        task_config = {
            "task_key": "main",
            "notebook_task": {
                "notebook_path": nb_full_path,
                "source": "WORKSPACE",
                "base_parameters": jd.get("parameters", {}),
            },
        }
        if compute == "serverless":
            task_config["environment_key"] = "default"
        elif compute == "job_cluster":
            task_config["job_cluster_key"] = "lab_cluster"
        elif compute == "sql_warehouse":
            if warehouse_id is None:
                warehouse_id = _find_warehouse()
            if warehouse_id:
                task_config["notebook_task"]["warehouse_id"] = warehouse_id
        job_settings["tasks"] = [task_config]

    try:
        result = _api("POST", "/jobs/create", job_settings)
        job_id = result.get("job_id")
        created += 1

        run = _api("POST", "/jobs/run-now", {"job_id": job_id})
        print(f"  ✅ {job_name} → created (id={job_id}), run_id={run.get('run_id', '?')}")
        triggered += 1
    except Exception as e:
        print(f"  ⚠️  {job_name} — {e}")

print(f"\n🎯 {created} jobs created, {triggered} jobs triggered!")
print("   They'll run in the background while you work through the exercises.")
