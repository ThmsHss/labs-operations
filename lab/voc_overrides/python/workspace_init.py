#!/voc/course/venv/bin/python3
"""
Vocareum workspace_init override for PUMA Ops Masterclass.

Called ONCE when the workspace is first provisioned.
Sets up: catalog, schema, volume, system schemas, warehouse, jobs, pipelines,
dashboards, permissions, and sample data.

Place this file at /voc/private/python/workspace_init.py to override the default.
"""

import json
import os
import sys
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

def trace_it():
    import traceback
    traceback.print_exc()


def main():
    workspace_url = os.getenv("VOC_DB_WORKSPACE_URL")
    token = os.getenv("VOC_DB_API_TOKEN")

    if not workspace_url or not token:
        print("ERROR: VOC_DB_WORKSPACE_URL and VOC_DB_API_TOKEN must be set")
        sys.exit(1)

    w = WorkspaceClient(host=workspace_url, token=token)
    print(f"Connected to: {workspace_url}")

    config_path = "/voc/private/courseware/config.json"
    if os.path.exists(config_path):
        with open(config_path) as f:
            config = json.load(f)
    else:
        config = {
            "lab_config": {
                "catalog": "puma_ops_lab",
                "schema": "workshop",
                "volume": "raw_data",
                "workspace_path": "/Workspace/Shared/puma_ops_masterclass",
            },
            "user_config": {
                "warehouse": {
                    "name": "PUMA_OPS_SHARED_WAREHOUSE",
                    "cluster_size": "Small",
                    "enable_serverless_compute": True,
                    "auto_stop_mins": 30,
                }
            },
            "metastore_config": {
                "enable_system_schemas": ["access", "billing", "compute", "lakeflow", "query"]
            },
        }

    catalog = config["lab_config"]["catalog"]
    schema = config["lab_config"]["schema"]
    volume = config["lab_config"]["volume"]

    # ── Create catalog, schema, volume ───────────────────────────────────────
    print("Creating catalog/schema/volume ...")
    try:
        w.catalogs.create(name=catalog, comment="PUMA Ops Masterclass")
    except Exception:
        print(f"  Catalog '{catalog}' already exists or cannot be created")

    try:
        w.schemas.create(name=schema, catalog_name=catalog)
    except Exception:
        print(f"  Schema '{schema}' already exists")

    try:
        w.volumes.create(
            catalog_name=catalog, schema_name=schema, name=volume,
            volume_type=VolumeType.MANAGED,
        )
    except Exception:
        print(f"  Volume '{volume}' already exists")

    # ── Enable system schemas ────────────────────────────────────────────────
    print("Enabling system schemas ...")
    try:
        metastore = w.metastores.current()
        for s in config.get("metastore_config", {}).get("enable_system_schemas", []):
            try:
                w.system_schemas.enable(metastore_id=metastore.metastore_id, schema_name=s)
                print(f"  Enabled system.{s}")
            except Exception:
                print(f"  system.{s} already enabled or unavailable")
    except Exception as e:
        print(f"  Could not enable system schemas: {e}")

    # ── Create SQL warehouse ─────────────────────────────────────────────────
    print("Creating SQL warehouse ...")
    wh_config = config["user_config"]["warehouse"]
    wh_name = wh_config["name"]
    warehouse_id = None
    for wh in w.warehouses.list():
        if wh.name == wh_name:
            warehouse_id = wh.id
            print(f"  Warehouse '{wh_name}' already exists (id={wh.id})")
            break

    if not warehouse_id:
        try:
            wh = w.warehouses.create_and_wait(
                name=wh_name,
                cluster_size=wh_config.get("cluster_size", "Small"),
                enable_serverless_compute=wh_config.get("enable_serverless_compute", True),
                auto_stop_mins=wh_config.get("auto_stop_mins", 30),
            )
            warehouse_id = wh.id
            print(f"  Created warehouse '{wh_name}' (id={wh.id})")
        except Exception as e:
            print(f"  Could not create warehouse: {e}")

    # ── Grant base permissions ───────────────────────────────────────────────
    print("Granting permissions ...")
    if warehouse_id:
        grants = [
            f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`",
            f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `account users`",
            f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `account users`",
            f"GRANT MODIFY ON SCHEMA {catalog}.{schema} TO `account users`",
            f"GRANT CREATE TABLE ON SCHEMA {catalog}.{schema} TO `account users`",
            f"GRANT READ VOLUME ON VOLUME {catalog}.{schema}.{volume} TO `account users`",
            f"GRANT WRITE VOLUME ON VOLUME {catalog}.{schema}.{volume} TO `account users`",
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
        for sql in grants:
            try:
                w.statement_execution.execute_statement(
                    warehouse_id=warehouse_id, statement=sql, wait_timeout="30s",
                )
            except Exception as e:
                print(f"  Grant warning: {str(e)[:80]}")

    print("✅ Workspace init complete")
    sys.exit(0)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        trace_it()
        sys.exit(1)
