#!/voc/course/venv/bin/python3
"""
Vocareum lab_end override for PUMA Ops Masterclass.

Called when a user's lab session ends.
Stops running jobs and warehouse to save costs.

Place this file at /voc/private/python/lab_end.py to override the default.
"""

import os
import sys

from databricks.sdk import WorkspaceClient


def trace_it():
    import traceback
    traceback.print_exc()


def main():
    workspace_url = os.getenv("VOC_DB_WORKSPACE_URL")
    token = os.getenv("VOC_DB_API_TOKEN")
    user = os.getenv("VOC_DB_USER_EMAIL")
    end_lab_behavior = os.getenv("VOC_END_LAB_BEHAVIOR", "stop")

    if not workspace_url or not token:
        print("ERROR: Required env vars not set")
        sys.exit(1)

    w = WorkspaceClient(host=workspace_url, token=token)
    print(f"Lab end for user: {user} (behavior: {end_lab_behavior})")

    if end_lab_behavior == "terminate":
        # Cancel any running job runs
        print("Cancelling active runs ...")
        for run in w.jobs.list_runs(active_only=True):
            try:
                if run.run_name and "puma_ops" in run.run_name.lower():
                    w.jobs.cancel_run(run_id=run.run_id)
                    print(f"  Cancelled run {run.run_id}")
            except Exception:
                pass

        # Stop pipeline updates
        print("Stopping pipeline updates ...")
        for p in w.pipelines.list_pipelines():
            if p.name and "puma_ops" in p.name.lower():
                try:
                    w.pipelines.stop(pipeline_id=p.pipeline_id)
                    print(f"  Stopped pipeline '{p.name}'")
                except Exception:
                    pass

    # Stop warehouse (always, to save costs)
    print("Stopping SQL warehouse ...")
    for wh in w.warehouses.list():
        if "PUMA_OPS" in wh.name.upper():
            try:
                w.warehouses.stop(id=wh.id)
                print(f"  Stopped warehouse '{wh.name}'")
            except Exception:
                pass

    print("✅ Lab end complete")
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        trace_it()
        sys.exit(1)
