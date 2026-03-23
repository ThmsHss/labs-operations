#!/voc/course/venv/bin/python3
"""
Vocareum workspace_destroy override for PUMA Ops Masterclass.

Called when the workspace is being deprovisioned.
Cleans up all lab resources: jobs, pipelines, dashboards, warehouse, catalog.

Place this file at /voc/private/python/workspace_destroy.py to override the default.
"""

import os
import sys

from databricks.sdk import WorkspaceClient

WORKSPACE_PATH = "/Workspace/Shared/puma_ops_masterclass"
CATALOG = "puma_ops_lab"


def trace_it():
    import traceback
    traceback.print_exc()


def main():
    workspace_url = os.getenv("VOC_DB_WORKSPACE_URL")
    token = os.getenv("VOC_DB_API_TOKEN")

    if not workspace_url or not token:
        print("ERROR: Required env vars not set")
        sys.exit(1)

    w = WorkspaceClient(host=workspace_url, token=token)
    print("Workspace destroy — cleaning up PUMA Ops Masterclass")

    # Delete jobs
    print("Deleting jobs ...")
    for j in w.jobs.list():
        if j.settings and j.settings.name and (
            "puma_ops" in j.settings.name.lower()
            or j.settings.name.startswith("job_0")
        ):
            try:
                w.jobs.delete(job_id=j.job_id)
                print(f"  Deleted job '{j.settings.name}'")
            except Exception:
                pass

    # Delete pipelines
    print("Deleting pipelines ...")
    for p in w.pipelines.list_pipelines():
        if p.name and "puma_ops" in p.name.lower():
            try:
                w.pipelines.delete(pipeline_id=p.pipeline_id)
                print(f"  Deleted pipeline '{p.name}'")
            except Exception:
                pass

    # Delete dashboards
    print("Deleting dashboards ...")
    try:
        for d in w.lakeview.list():
            if d.display_name and any(kw in d.display_name.lower() for kw in
                                       ["job monitoring", "query performance", "billing", "cost"]):
                w.lakeview.trash(dashboard_id=d.dashboard_id)
                print(f"  Trashed dashboard '{d.display_name}'")
    except Exception:
        pass

    # Delete warehouse
    print("Deleting SQL warehouse ...")
    for wh in w.warehouses.list():
        if "PUMA_OPS" in wh.name.upper():
            try:
                w.warehouses.delete(id=wh.id)
                print(f"  Deleted warehouse '{wh.name}'")
            except Exception:
                pass

    # Drop catalog (cascade)
    print(f"Dropping catalog '{CATALOG}' ...")
    try:
        w.catalogs.delete(name=CATALOG, force=True)
        print(f"  Dropped catalog '{CATALOG}'")
    except Exception as e:
        print(f"  Could not drop catalog: {e}")

    # Delete workspace folder
    print(f"Deleting workspace path {WORKSPACE_PATH} ...")
    try:
        w.workspace.delete(path=WORKSPACE_PATH, recursive=True)
        print(f"  Deleted {WORKSPACE_PATH}")
    except Exception:
        pass

    print("✅ Workspace destroy complete")
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        trace_it()
        sys.exit(1)
