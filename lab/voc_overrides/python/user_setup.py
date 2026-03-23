#!/voc/course/venv/bin/python3
"""
Vocareum user_setup override for PUMA Ops Masterclass.

Called per-user when they start the lab. Ensures:
- User has workspace access to the lab folder
- Returns the redirect URL to the LAB_Description notebook

Place this file at /voc/private/python/user_setup.py to override the default.
"""

import json
import os
import sys

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import PermissionLevel

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
    ipc_data_file = os.getenv("VOC_IPC_DATA_FILE", "vocipcdata.txt")

    if not workspace_url or not token or not user:
        print("ERROR: Required env vars not set")
        sys.exit(1)

    w = WorkspaceClient(host=workspace_url, token=token)
    print(f"User setup for: {user}")

    # ── Ensure user has workspace folder permissions ─────────────────────────
    try:
        w.workspace.mkdirs(f"/Workspace/Users/{user}")
        print(f"  User home folder ensured")
    except Exception:
        pass

    # ── Ensure per-user grants on the catalog (in case account-level didn't work) ──
    warehouse_id = _get_warehouse_id(w)
    if warehouse_id:
        per_user_grants = [
            f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{user}`",
            f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{user}`",
            f"GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMA} TO `{user}`",
            f"GRANT MODIFY ON SCHEMA {CATALOG}.{SCHEMA} TO `{user}`",
            f"GRANT CREATE TABLE ON SCHEMA {CATALOG}.{SCHEMA} TO `{user}`",
        ]
        for sql in per_user_grants:
            try:
                w.statement_execution.execute_statement(
                    warehouse_id=warehouse_id, statement=sql, wait_timeout="15s",
                )
            except Exception:
                pass

    # ── Return redirect URL to LAB_Description ───────────────────────────────
    notebook_url = f"{workspace_url}#notebook/{WORKSPACE_PATH}/notebooks/LAB_Description"

    # Clean up the URL
    notebook_url = notebook_url.replace("//", "/").replace("https:/", "https://")

    print(f"  Redirecting to: {notebook_url}")

    if ipc_data_file:
        with open(ipc_data_file, "w") as f:
            f.write(json.dumps({"notebook_url": notebook_url}))

    print("✅ User setup complete")
    sys.exit(0)


def _get_warehouse_id(w):
    for wh in w.warehouses.list():
        if "PUMA_OPS" in wh.name.upper() or "SHARED" in wh.name.upper():
            return wh.id
    warehouses = list(w.warehouses.list())
    return warehouses[0].id if warehouses else None


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        trace_it()
        sys.exit(1)
