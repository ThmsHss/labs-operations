"""
Master deployment script for the PUMA Ops Masterclass lab.

Deploys all lab resources into a Databricks workspace:
  1. Upload notebooks (zip) to workspace
  2. Create catalog, schema, volume
  3. Enable system schemas
  4. Create SQL warehouse
  5. Create challenge jobs (5 broken + 1 working)
  6. Create SDP pipelines (working + broken)
  7. Import AI/BI dashboards
  8. Run the data setup notebook
  9. Run the working ETL job to seed system table history

Prerequisites:
  - Databricks CLI configured with a profile, OR
  - DATABRICKS_HOST and DATABRICKS_TOKEN env vars set
  - Python packages: databricks-sdk

Usage:
  python deploy_all.py [--profile PROFILE] [--run-setup] [--skip-jobs] [--skip-pipelines]
"""

import argparse
import base64
import json
import os
import sys
import time
import zipfile
from pathlib import Path

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.service.workspace import ImportFormat, Language


def _api(host, token, method, path, body=None):
    """Direct REST API call, bypassing SDK type strictness."""
    url = f"{host.rstrip('/')}/api/2.0{path}" if not path.startswith("http") else path
    headers = {"Authorization": f"Bearer {token}"}
    if method == "GET":
        r = requests.get(url, headers=headers, timeout=60)
    elif method == "POST":
        r = requests.post(url, headers=headers, json=body, timeout=120)
    elif method == "PUT":
        r = requests.put(url, headers=headers, json=body, timeout=60)
    elif method == "DELETE":
        r = requests.delete(url, headers=headers, timeout=60)
    else:
        raise ValueError(f"Unknown method: {method}")
    r.raise_for_status()
    if r.content:
        return r.json()
    return {}

SCRIPT_DIR = Path(__file__).parent
LAB_DIR = SCRIPT_DIR.parent
CONFIG_PATH = SCRIPT_DIR / "config.json"

def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def get_workspace_client(profile=None):
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


# ── 1. Upload notebooks ─────────────────────────────────────────────────────

def create_notebook_zip():
    """Create a zip of all lab notebooks, jobs, pipelines, and hints."""
    zip_path = LAB_DIR / "deploy" / "PumaOpsLab.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for folder in ["notebooks", "jobs", "pipelines", "hints"]:
            folder_path = LAB_DIR / folder
            if not folder_path.exists():
                continue
            for file_path in sorted(folder_path.rglob("*")):
                if file_path.is_file():
                    arcname = f"{folder}/{file_path.relative_to(folder_path)}"
                    zf.write(file_path, arcname)
    print(f"  ✅ Created {zip_path} ({zip_path.stat().st_size / 1024:.0f} KB)")
    return zip_path


def _import_file(w, file_path, target_path):
    """Import a single file (py or sql) into the workspace."""
    content = file_path.read_bytes()
    target_no_ext = target_path.rsplit(file_path.suffix, 1)[0]
    try:
        w.workspace.mkdirs(str(Path(target_no_ext).parent))
    except Exception:
        pass
    lang = Language.PYTHON if file_path.suffix == ".py" else Language.SQL
    w.workspace.import_(
        path=target_no_ext,
        content=base64.b64encode(content).decode(),
        format=ImportFormat.SOURCE,
        language=lang,
        overwrite=True,
    )


def upload_notebooks(w, config):
    """Upload shared notebooks (not jobs) to the workspace."""
    workspace_path = config["lab_config"]["workspace_path"]

    print(f"  Uploading shared notebooks to {workspace_path}/ ...")
    for folder in ["notebooks", "pipelines", "hints"]:
        folder_path = LAB_DIR / folder
        if not folder_path.exists():
            continue
        for file_path in sorted(folder_path.rglob("*")):
            if not file_path.is_file() or file_path.suffix not in (".py", ".sql"):
                continue
            relative = file_path.relative_to(LAB_DIR)
            target = f"{workspace_path}/{relative}"
            _import_file(w, file_path, target)

    print(f"  ✅ Shared notebooks uploaded to {workspace_path}/")


def upload_per_user_jobs(w, config, participants):
    """Upload job notebooks into each participant's personal folder."""
    workspace_path = config["lab_config"]["workspace_path"]
    jobs_dir = LAB_DIR / "jobs"
    if not jobs_dir.exists():
        return

    for participant in participants:
        user_jobs_path = f"{workspace_path}/{participant}/jobs"
        print(f"  Uploading job notebooks for {participant} ...")
        for file_path in sorted(jobs_dir.rglob("*")):
            if not file_path.is_file() or file_path.suffix != ".py":
                continue
            target = f"{user_jobs_path}/{file_path.name}"
            _import_file(w, file_path, target)

    print(f"  ✅ Per-user job notebooks uploaded for {len(participants)} participant(s)")


def upload_dashboard_json(w, config):
    """Upload dashboard JSON files to the workspace."""
    workspace_path = config["lab_config"]["workspace_path"]
    dashboard_dir = f"{workspace_path}/dashboards"
    try:
        w.workspace.mkdirs(dashboard_dir)
    except Exception:
        pass

    for dash_file in config.get("dashboards", []):
        file_path = LAB_DIR / dash_file.replace("dashboards/", "dashboards/")
        if not file_path.exists():
            print(f"  ⚠️  Dashboard file not found: {file_path}")
            continue
        import base64
        content = file_path.read_bytes()
        target = f"{dashboard_dir}/{file_path.name}"
        w.workspace.import_(
            path=target.rsplit(".json", 1)[0],
            content=base64.b64encode(content).decode(),
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            overwrite=True,
        )
    print(f"  ✅ Dashboard JSON files uploaded to {dashboard_dir}/")


# ── 2. Create catalog, schema, volume ────────────────────────────────────────

def setup_catalog(w, config):
    """Create catalog, schema, and volume."""
    catalog = config["lab_config"]["catalog"]
    schema = config["lab_config"]["schema"]
    volume = config["lab_config"]["volume"]

    print(f"  Creating catalog '{catalog}' ...")
    try:
        w.catalogs.create(name=catalog, comment="PUMA Ops Masterclass workshop catalog")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"    Catalog '{catalog}' already exists")
        else:
            raise

    print(f"  Creating schema '{catalog}.{schema}' ...")
    try:
        w.schemas.create(name=schema, catalog_name=catalog, comment="Workshop lab data")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"    Schema '{schema}' already exists")
        else:
            raise

    print(f"  Creating volume '{catalog}.{schema}.{volume}' ...")
    try:
        w.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=volume,
            volume_type=VolumeType.MANAGED,
            comment="Raw data for SDP pipeline ingestion",
        )
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"    Volume '{volume}' already exists")
        else:
            raise

    print(f"  ✅ Catalog/Schema/Volume ready")


# ── 3. Enable system schemas ────────────────────────────────────────────────

def enable_system_schemas(w, config):
    """Enable required system schemas for the lab."""
    schemas_to_enable = config.get("metastore_config", {}).get("enable_system_schemas", [])
    if not schemas_to_enable:
        return

    try:
        metastore_assignment = w.metastores.current()
        metastore_id = metastore_assignment.metastore_id
    except Exception as e:
        print(f"  ⚠️  Could not get current metastore: {e}")
        return

    for schema_name in schemas_to_enable:
        try:
            w.system_schemas.enable(metastore_id=metastore_id, schema_name=schema_name)
            print(f"    Enabled system.{schema_name}")
        except Exception as e:
            if "already enabled" in str(e).lower() or "already exists" in str(e).lower():
                print(f"    system.{schema_name} already enabled")
            else:
                print(f"    ⚠️  Could not enable system.{schema_name}: {e}")

    print(f"  ✅ System schemas enabled")


# ── 4. Create SQL Warehouse ─────────────────────────────────────────────────

def create_warehouse(w, config):
    """Create or find the SQL warehouse."""
    wh_config = config["user_config"]["warehouse"]
    wh_name = wh_config["name"]

    existing = list(w.warehouses.list())
    for wh in existing:
        if wh.name == wh_name:
            print(f"  ✅ SQL Warehouse '{wh_name}' already exists (id={wh.id})")
            return wh.id

    print(f"  Creating SQL Warehouse '{wh_name}' ...")
    from databricks.sdk.service.sql import EndpointTags, EndpointTagPair
    wh = w.warehouses.create_and_wait(
        name=wh_name,
        cluster_size=wh_config.get("cluster_size", "Small"),
        max_num_clusters=wh_config.get("max_num_clusters", 1),
        min_num_clusters=wh_config.get("min_num_clusters", 1),
        enable_serverless_compute=wh_config.get("enable_serverless_compute", True),
        auto_stop_mins=wh_config.get("auto_stop_mins", 30),
        tags=EndpointTags(custom_tags=[EndpointTagPair(key="Workshop", value="puma_ops_masterclass")]),
    )
    print(f"  ✅ SQL Warehouse '{wh_name}' created (id={wh.id})")
    return wh.id


# ── 5. Create Jobs ──────────────────────────────────────────────────────────

def _user_prefix(participant):
    """Derive a job name prefix from a participant identifier (email or username)."""
    return participant.split("@")[0].replace(".", "_")


def _delete_job_by_name(host, token, job_name):
    """Delete an existing job by exact name match."""
    try:
        existing = _api(host, token, "GET", f"/jobs/list?name={job_name}")
        for ej in existing.get("jobs", []):
            if ej.get("settings", {}).get("name") == job_name:
                _api(host, token, "POST", "/jobs/delete", {"job_id": ej["job_id"]})
                print(f"    Deleted existing job '{job_name}' (id={ej['job_id']})")
    except Exception:
        pass


def _build_task_config(notebook_path, compute, params, cluster_config=None, warehouse_id=None):
    """Build a single task config dict."""
    task_config = {
        "notebook_task": {
            "notebook_path": notebook_path,
            "source": "WORKSPACE",
            "base_parameters": params,
        },
    }
    if compute == "serverless":
        task_config["environment_key"] = "default"
    elif compute == "job_cluster":
        task_config["job_cluster_key"] = "lab_cluster"
    elif compute == "sql_warehouse" and warehouse_id:
        task_config["notebook_task"]["warehouse_id"] = warehouse_id
    return task_config


def _build_job_settings(job_name, compute, participant, cluster_config=None):
    """Build common job settings (environments, clusters, tags)."""
    job_settings = {
        "name": job_name,
        "tags": {"Workshop": "puma_ops_masterclass", "participant": participant},
    }
    if compute == "serverless":
        job_settings["environments"] = [
            {"environment_key": "default", "spec": {"client": "1"}}
        ]
    elif compute == "job_cluster" and cluster_config:
        new_cluster = {
            "spark_version": cluster_config["spark_version"],
            "node_type_id": cluster_config["node_type_id"],
            "num_workers": cluster_config.get("num_workers", 1),
            "data_security_mode": "SINGLE_USER",
            "custom_tags": cluster_config.get("custom_tags", {}),
        }
        if cluster_config.get("cluster_log_conf"):
            new_cluster["cluster_log_conf"] = cluster_config["cluster_log_conf"]
        job_settings["job_clusters"] = [{
            "job_cluster_key": "lab_cluster",
            "new_cluster": new_cluster,
        }]
    return job_settings


def create_jobs(w, config, participants):
    """Create per-user challenge jobs and working ETL jobs via REST API."""
    host = w.config.host
    token = w.config.token
    workspace_path = config["lab_config"]["workspace_path"]
    jobs_config = config.get("jobs", {})
    cluster_config = config["user_config"]["cluster_config"]
    warehouse_id = None

    for participant in participants:
        prefix = _user_prefix(participant)
        user_jobs_root = f"{workspace_path}/{participant}/jobs"
        print(f"  Creating jobs for {participant} (prefix={prefix}) ...")

        for job_template_name, job_def in jobs_config.items():
            if job_template_name.startswith("puma_ops_"):
                job_name = f"{prefix}_{job_template_name[len('puma_ops_'):]}"
            else:
                job_name = f"{prefix}_{job_template_name}"

            compute = job_def.get("compute", "serverless")
            params = job_def.get("parameters", {})

            if compute == "sql_warehouse" and warehouse_id is None:
                for wh in w.warehouses.list():
                    if wh.name == config["user_config"]["warehouse"]["name"]:
                        warehouse_id = wh.id
                        break

            job_settings = _build_job_settings(
                job_name, compute, participant, cluster_config
            )

            # Multi-task job (has "tasks" list in config)
            if "tasks" in job_def:
                tasks = []
                for task_def in job_def["tasks"]:
                    nb_basename = task_def["notebook"].split("/")[-1]
                    nb_path = f"{user_jobs_root}/{nb_basename}"
                    tc = _build_task_config(nb_path, compute, params, cluster_config, warehouse_id)
                    tc["task_key"] = task_def["task_key"]
                    if task_def.get("depends_on"):
                        tc["depends_on"] = [{"task_key": k} for k in task_def["depends_on"]]
                    tasks.append(tc)
                job_settings["tasks"] = tasks
            else:
                # Single-task job
                notebook_basename = job_def["notebook"].split("/")[-1]
                notebook_path = f"{user_jobs_root}/{notebook_basename}"
                tc = _build_task_config(notebook_path, compute, params, cluster_config, warehouse_id)
                tc["task_key"] = "main"
                job_settings["tasks"] = [tc]

            _delete_job_by_name(host, token, job_name)

            try:
                result = _api(host, token, "POST", "/jobs/create", job_settings)
                print(f"    Created job '{job_name}' (id={result.get('job_id')})")
            except Exception as e:
                print(f"    ⚠️  Could not create job '{job_name}': {e}")

    print(f"  ✅ All per-user jobs created")


# ── 6. Create Pipelines ─────────────────────────────────────────────────────

def create_pipelines(w, config):
    """Create SDP pipelines (working and broken) via REST API."""
    host = w.config.host
    token = w.config.token
    workspace_path = config["lab_config"]["workspace_path"]
    catalog = config["lab_config"]["catalog"]
    pipelines_config = config.get("pipelines", {})

    for pipeline_name, pipeline_def in pipelines_config.items():
        libraries = [
            {"notebook": {"path": f"{workspace_path}/{nb}"}}
            for nb in pipeline_def["source_notebooks"]
        ]
        target_schema = pipeline_def["target_schema"]
        serverless = pipeline_def.get("serverless", True)

        # Delete existing pipeline with same name
        try:
            existing = _api(host, token, "GET", f"/pipelines?filter=name+LIKE+'{pipeline_name}'")
            for ep in existing.get("statuses", []):
                if ep.get("name") == pipeline_name:
                    _api(host, token, "DELETE", f"/pipelines/{ep['pipeline_id']}")
                    print(f"    Deleted existing pipeline '{pipeline_name}'")
        except Exception:
            pass

        pipeline_spec = {
            "name": pipeline_name,
            "catalog": catalog,
            "target": target_schema,
            "libraries": libraries,
            "serverless": serverless,
            "development": True,
            "continuous": False,
            "channel": "CURRENT",
        }

        try:
            result = _api(host, token, "POST", "/pipelines", pipeline_spec)
            print(f"    Created pipeline '{pipeline_name}' (id={result.get('pipeline_id')})")
        except Exception as e:
            print(f"    ⚠️  Could not create pipeline '{pipeline_name}': {e}")

    print(f"  ✅ All pipelines created")


# ── 7. Import Dashboards ────────────────────────────────────────────────────

def import_dashboards(w, config):
    """Import AI/BI dashboards from JSON templates via REST API."""
    host = w.config.host
    token = w.config.token
    workspace_path = config["lab_config"]["workspace_path"]
    warehouse_id = None

    for wh in w.warehouses.list():
        if wh.name == config["user_config"]["warehouse"]["name"]:
            warehouse_id = wh.id
            break

    for dash_file in config.get("dashboards", []):
        file_path = LAB_DIR / dash_file
        if not file_path.exists():
            print(f"  ⚠️  Dashboard file not found: {file_path}")
            continue

        with open(file_path) as f:
            dashboard_json = json.load(f)

        display_name = file_path.stem.replace("_", " ").title()
        parent_path = f"{workspace_path}/dashboards"

        try:
            body = {
                "display_name": display_name,
                "parent_path": parent_path,
                "serialized_dashboard": json.dumps(dashboard_json),
                "warehouse_id": warehouse_id,
            }
            result = _api(host, token, "POST", "/lakeview/dashboards", body)
            dash_id = result.get("dashboard_id")
            print(f"    Created dashboard '{display_name}' (id={dash_id})")

            if dash_id and warehouse_id:
                _api(host, token, "POST", f"/lakeview/dashboards/{dash_id}/published",
                     {"warehouse_id": warehouse_id, "embed_credentials": True})
                print(f"    Published dashboard '{display_name}'")
        except Exception as e:
            print(f"    ⚠️  Could not create dashboard '{display_name}': {e}")

    print(f"  ✅ Dashboards imported")


# ── 8. Run setup notebook ───────────────────────────────────────────────────

def run_setup_notebook(w, config):
    """Run the 00_Environment_Setup notebook to generate sample data via REST API."""
    host = w.config.host
    token = w.config.token
    workspace_path = config["lab_config"]["workspace_path"]
    notebook_path = f"{workspace_path}/notebooks/00_Environment_Setup"

    print(f"  Running {notebook_path} ...")
    print("  (This generates sample data — may take 3-5 minutes)")

    submit_body = {
        "run_name": "puma_ops_lab_setup",
        "tasks": [{
            "task_key": "setup",
            "notebook_task": {
                "notebook_path": notebook_path,
                "source": "WORKSPACE",
            },
            "environment_key": "default",
        }],
        "environments": [
            {"environment_key": "default", "spec": {"client": "1"}}
        ],
    }
    result = _api(host, token, "POST", "/jobs/runs/submit", submit_body)
    run_id = result.get("run_id")
    print(f"  Submitted run {run_id}, waiting for completion...")

    while True:
        time.sleep(15)
        run_info = _api(host, token, "GET", f"/jobs/runs/get?run_id={run_id}")
        state = run_info.get("state", {})
        lc = state.get("life_cycle_state", "UNKNOWN")
        print(f"    Status: {lc}")
        if lc not in ("PENDING", "RUNNING", "QUEUED", "BLOCKED"):
            break

    result_state = state.get("result_state", "UNKNOWN")
    if result_state == "SUCCESS":
        print(f"  ✅ Setup notebook completed successfully")
    else:
        print(f"  ❌ Setup notebook failed: {state.get('state_message', 'unknown')}")
        print(f"     Check the run at: {run_info.get('run_page_url', 'N/A')}")


# ── 9. Run working ETL to seed system tables ────────────────────────────────

def run_working_etl(w, config, participants):
    """Run the working ETL job for each participant to populate system table history."""
    host = w.config.host
    token = w.config.token

    for participant in participants:
        prefix = _user_prefix(participant)
        job_name = f"{prefix}_job_00_working_etl"
        try:
            result = _api(host, token, "GET", f"/jobs/list?name={job_name}")
            job_id = None
            for j in result.get("jobs", []):
                if j.get("settings", {}).get("name") == job_name:
                    job_id = j["job_id"]
                    break
        except Exception:
            job_id = None

        if not job_id:
            print(f"  ⚠️  Working ETL job for {participant} not found, skipping")
            continue

        print(f"  Running working ETL for {participant} (id={job_id})...")
        for i in range(2):
            run = _api(host, token, "POST", "/jobs/run-now", {"job_id": job_id})
            print(f"    Triggered run {i+1}/2 (run_id={run.get('run_id')})")
            time.sleep(5)

    print(f"  ✅ Working ETL seeding triggered for all participants")


# ── 10. Run challenge jobs to create failure history ─────────────────────────

def run_challenge_jobs(w, config, participants):
    """Trigger the per-user challenge jobs so they fail and create history."""
    host = w.config.host
    token = w.config.token
    challenge_suffixes = [
        "challenge_01_schema",
        "challenge_02_permissions",
        "challenge_04_params",
        "challenge_05_slow_query",
    ]

    for participant in participants:
        prefix = _user_prefix(participant)
        print(f"  Triggering challenge jobs for {participant} ...")
        for suffix in challenge_suffixes:
            name = f"{prefix}_{suffix}"
            try:
                result = _api(host, token, "GET", f"/jobs/list?name={name}")
                for j in result.get("jobs", []):
                    if j.get("settings", {}).get("name") == name:
                        run = _api(host, token, "POST", "/jobs/run-now", {"job_id": j["job_id"]})
                        print(f"    Triggered '{name}' (run_id={run.get('run_id')})")
                        break
            except Exception as e:
                print(f"    ⚠️  Could not trigger '{name}': {e}")

    print(f"  ✅ Challenge jobs triggered for all participants")
    print(f"     Note: Challenge 03 (OOM) skipped — run manually if cluster is available")


# ── 11. Grant permissions ────────────────────────────────────────────────────

def grant_permissions(w, config):
    """Grant necessary permissions for workshop participants."""
    catalog = config["lab_config"]["catalog"]

    grants = [
        f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`",
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.workshop TO `account users`",
        f"GRANT SELECT ON SCHEMA {catalog}.workshop TO `account users`",
        f"GRANT MODIFY ON SCHEMA {catalog}.workshop TO `account users`",
        f"GRANT CREATE TABLE ON SCHEMA {catalog}.workshop TO `account users`",
        f"GRANT READ VOLUME ON VOLUME {catalog}.workshop.raw_data TO `account users`",
        f"GRANT WRITE VOLUME ON VOLUME {catalog}.workshop.raw_data TO `account users`",
        # Curated schema: USE SCHEMA only — SELECT intentionally withheld (Challenge 2)
        f"GRANT USE SCHEMA ON SCHEMA {catalog}.curated TO `account users`",
        "GRANT USE CATALOG ON CATALOG system TO `account users`",
        "GRANT USE SCHEMA ON SCHEMA system.access TO `account users`",
        "GRANT USE SCHEMA ON SCHEMA system.billing TO `account users`",
        "GRANT USE SCHEMA ON SCHEMA system.compute TO `account users`",
        "GRANT USE SCHEMA ON SCHEMA system.lakeflow TO `account users`",
        "GRANT USE SCHEMA ON SCHEMA system.query TO `account users`",
        "GRANT SELECT ON SCHEMA system.access TO `account users`",
        "GRANT SELECT ON SCHEMA system.billing TO `account users`",
        "GRANT SELECT ON SCHEMA system.compute TO `account users`",
        "GRANT SELECT ON SCHEMA system.lakeflow TO `account users`",
        "GRANT SELECT ON SCHEMA system.query TO `account users`",
    ]

    for grant_sql in grants:
        try:
            w.statement_execution.execute_statement(
                warehouse_id=_get_warehouse_id(w, config),
                statement=grant_sql,
                wait_timeout="30s",
            )
            print(f"    ✅ {grant_sql[:60]}...")
        except Exception as e:
            print(f"    ⚠️  {grant_sql[:50]}... → {str(e)[:80]}")

    print(f"  ✅ Permissions configured")


def _get_warehouse_id(w, config):
    wh_name = config["user_config"]["warehouse"]["name"]
    for wh in w.warehouses.list():
        if wh.name == wh_name:
            return wh.id
    return None


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Deploy PUMA Ops Masterclass lab")
    parser.add_argument("--profile", help="Databricks CLI profile name")
    parser.add_argument("--participants", help="Comma-separated participant names (overrides config.json)")
    parser.add_argument("--run-setup", action="store_true", help="Run the setup notebook to generate data")
    parser.add_argument("--run-challenges", action="store_true", help="Trigger challenge jobs to create failure history")
    parser.add_argument("--skip-jobs", action="store_true", help="Skip job creation")
    parser.add_argument("--skip-pipelines", action="store_true", help="Skip pipeline creation")
    parser.add_argument("--skip-dashboards", action="store_true", help="Skip dashboard import")
    parser.add_argument("--zip-only", action="store_true", help="Only create the notebook zip, don't deploy")
    args = parser.parse_args()

    config = load_config()

    participants = (
        [p.strip() for p in args.participants.split(",")]
        if args.participants
        else config.get("participants", [])
    )
    if not participants:
        print("⚠️  No participants configured. Add to config.json or use --participants")
        sys.exit(1)

    if args.zip_only:
        print("📦 Creating notebook zip...")
        create_notebook_zip()
        return

    print("🚀 Deploying PUMA Ops Masterclass Lab")
    print("=" * 60)
    w = get_workspace_client(args.profile)
    print(f"  Connected to: {w.config.host}")
    print(f"  Participants:  {', '.join(participants)}")

    print("\n📁 Step 1: Uploading shared notebooks...")
    upload_notebooks(w, config)

    print("\n📁 Step 2: Uploading per-user job notebooks...")
    upload_per_user_jobs(w, config, participants)

    print("\n🗄️  Step 3: Setting up catalog/schema/volume...")
    setup_catalog(w, config)

    print("\n🔓 Step 4: Enabling system schemas...")
    enable_system_schemas(w, config)

    print("\n🏭 Step 5: Creating SQL warehouse...")
    create_warehouse(w, config)

    if not args.skip_jobs:
        print("\n⚙️  Step 6: Creating per-user challenge jobs...")
        create_jobs(w, config, participants)

    if not args.skip_pipelines:
        print("\n🔄 Step 7: Creating SDP pipelines...")
        create_pipelines(w, config)

    if not args.skip_dashboards:
        print("\n📊 Step 8: Importing dashboards...")
        import_dashboards(w, config)

    print("\n🔑 Step 9: Configuring permissions...")
    grant_permissions(w, config)

    if args.run_setup:
        print("\n📊 Step 10: Running setup notebook (data generation)...")
        run_setup_notebook(w, config)

        print("\n🔄 Step 11: Seeding system tables with ETL runs...")
        run_working_etl(w, config, participants)

    if args.run_challenges:
        print("\n🛠️  Step 12: Triggering challenge jobs (creating failure history)...")
        run_challenge_jobs(w, config, participants)

    print("\n" + "=" * 60)
    print("✅ Deployment complete!")
    print(f"   Workspace path: {config['lab_config']['workspace_path']}")
    print(f"   Catalog:        {config['lab_config']['catalog']}")
    print(f"   Participants:   {', '.join(participants)}")
    if not args.run_setup:
        print("\n💡 Run with --run-setup to generate sample data")
    if not args.run_challenges:
        print("💡 Run with --run-challenges to trigger failing jobs for the debugging lab")


if __name__ == "__main__":
    main()
