"""
Teardown script for the PUMA Ops Masterclass lab.

Removes all lab resources from the workspace:
  - Jobs (challenge + working)
  - Pipelines
  - Dashboards
  - Catalog/schema/volume (optional)
  - Workspace notebooks (optional)

Usage:
  python teardown.py [--profile PROFILE] [--include-data] [--include-notebooks]
"""

import argparse
import json
from pathlib import Path

from databricks.sdk import WorkspaceClient

SCRIPT_DIR = Path(__file__).parent
CONFIG_PATH = SCRIPT_DIR / "config.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


def main():
    parser = argparse.ArgumentParser(description="Teardown PUMA Ops Masterclass lab")
    parser.add_argument("--profile", help="Databricks CLI profile name")
    parser.add_argument("--include-data", action="store_true",
                        help="Also drop the catalog and all data (DESTRUCTIVE)")
    parser.add_argument("--include-notebooks", action="store_true",
                        help="Also remove workspace notebooks")
    args = parser.parse_args()

    config = load_config()
    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()

    print("🧹 Tearing down PUMA Ops Masterclass Lab")
    print("=" * 60)

    # Delete jobs
    print("\n⚙️  Deleting jobs...")
    for job_name in config.get("jobs", {}):
        for j in w.jobs.list(name=job_name):
            if j.settings and j.settings.name == job_name:
                w.jobs.delete(job_id=j.job_id)
                print(f"    Deleted job '{job_name}' (id={j.job_id})")

    # Delete pipelines
    print("\n🔄 Deleting pipelines...")
    for pipeline_name in config.get("pipelines", {}):
        for p in w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'"):
            if p.name == pipeline_name:
                w.pipelines.delete(pipeline_id=p.pipeline_id)
                print(f"    Deleted pipeline '{pipeline_name}'")

    # Delete dashboards
    print("\n📊 Deleting dashboards...")
    for dash_file in config.get("dashboards", []):
        display_name = Path(dash_file).stem.replace("_", " ").title()
        try:
            for d in w.lakeview.list():
                if d.display_name == display_name:
                    w.lakeview.trash(dashboard_id=d.dashboard_id)
                    print(f"    Trashed dashboard '{display_name}'")
        except Exception as e:
            print(f"    ⚠️  Could not delete dashboard '{display_name}': {e}")

    # Delete warehouse
    print("\n🏭 Deleting SQL warehouse...")
    wh_name = config["user_config"]["warehouse"]["name"]
    for wh in w.warehouses.list():
        if wh.name == wh_name:
            w.warehouses.delete(id=wh.id)
            print(f"    Deleted warehouse '{wh_name}' (id={wh.id})")

    # Delete catalog (optional)
    if args.include_data:
        catalog = config["lab_config"]["catalog"]
        print(f"\n🗄️  Dropping catalog '{catalog}' (CASCADE)...")
        try:
            w.catalogs.delete(name=catalog, force=True)
            print(f"    Dropped catalog '{catalog}'")
        except Exception as e:
            print(f"    ⚠️  Could not drop catalog: {e}")

    # Delete workspace notebooks (optional)
    if args.include_notebooks:
        workspace_path = config["lab_config"]["workspace_path"]
        print(f"\n📁 Deleting workspace path '{workspace_path}'...")
        try:
            w.workspace.delete(path=workspace_path, recursive=True)
            print(f"    Deleted {workspace_path}")
        except Exception as e:
            print(f"    ⚠️  Could not delete workspace path: {e}")

    print("\n" + "=" * 60)
    print("✅ Teardown complete!")


if __name__ == "__main__":
    main()
