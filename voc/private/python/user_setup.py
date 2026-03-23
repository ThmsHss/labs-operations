#!/voc/course/venv/bin/python3
"""
Custom user_setup override for Vaillant / PUMA labs.

Replaces the default dbacademy user_setup which fails with:
  [UC_NOT_ENABLED] Unity Catalog is not enabled on this cluster.

This version uses the Databricks SDK directly, bypassing dbacademy's
UC-dependent metadata SQL queries.
"""

import base64
import json
import os
import sys
import time
import traceback
import zipfile
import tempfile

ipc_data_file = os.getenv('VOC_IPC_DATA_FILE', 'vocipcdata.txt')
custom_data_file = os.getenv('VOC_CUSTOM_DATA_FILE', 'voccustomdata.txt')


def trace_it():
    traceback.print_exc()


def error_exit(code, msg):
    with open(custom_data_file, 'w') as data_file:
        d = {'error_code': str(code), 'message': msg}
        data_file.write(json.dumps(d))
        print(json.dumps(d, indent=2))
    sys.exit(1)


def load_config():
    """Load course config from the standard Vocareum config location."""
    config_paths = [
        '/voc/private/courseware/config.json',
        '/voc/course/config.json',
    ]
    for path in config_paths:
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f)
    return {}


def find_content_zip():
    """Find the lab content zip file."""
    config = load_config()
    zip_name = config.get('content', {}).get('src', '')
    search_paths = [
        f'/voc/private/courseware/{zip_name}',
        f'/voc/course/public/{zip_name}',
        f'/voc/private/courseware/',
    ]
    for path in search_paths:
        if os.path.isfile(path):
            return path
    if os.path.isdir(search_paths[-1]):
        import glob
        zips = glob.glob(f'{search_paths[-1]}/*.zip')
        if zips:
            return zips[0]
    return None


def import_notebooks(w, user, config):
    """Import the lab notebook(s) to the user's workspace folder."""
    zip_path = find_content_zip()
    entry = config.get('content', {}).get('entry', 'LAB_Description')

    user_folder = f'/Workspace/Users/{user}'
    course_folder = f'{user_folder}/lab'

    try:
        w.workspace.mkdirs(course_folder)
    except Exception:
        pass

    if zip_path and os.path.exists(zip_path):
        print(f"  Importing content from {zip_path} to {course_folder}")
        try:
            with open(zip_path, 'rb') as f:
                content = base64.b64encode(f.read()).decode()

            w.workspace.import_(
                path=course_folder,
                content=content,
                format='DBC',
                overwrite=True,
            )
            print(f"  ✅ Imported notebook archive to {course_folder}")
        except Exception as e:
            print(f"  ⚠️ DBC import failed, trying SOURCE import: {e}")
            try:
                with tempfile.TemporaryDirectory() as tmpdir:
                    with zipfile.ZipFile(zip_path, 'r') as zf:
                        zf.extractall(tmpdir)
                    for root, dirs, files in os.walk(tmpdir):
                        for fname in files:
                            if fname.endswith('.py'):
                                fpath = os.path.join(root, fname)
                                rel = os.path.relpath(fpath, tmpdir)
                                target = f"{course_folder}/{rel}".rsplit('.py', 1)[0]
                                with open(fpath, 'rb') as f:
                                    content_b64 = base64.b64encode(f.read()).decode()
                                try:
                                    w.workspace.mkdirs(os.path.dirname(target))
                                except Exception:
                                    pass
                                w.workspace.import_(
                                    path=target,
                                    content=content_b64,
                                    format='SOURCE',
                                    language='PYTHON',
                                    overwrite=True,
                                )
                print(f"  ✅ Imported notebooks via SOURCE format to {course_folder}")
            except Exception as e2:
                print(f"  ⚠️ SOURCE import also failed: {e2}")

    notebook_url_path = f'{course_folder}/{entry}'
    return notebook_url_path


def ensure_warehouse(w, config):
    """Find or create the SQL warehouse."""
    wh_config = config.get('user_config', {}).get('warehouse', {})
    wh_name = wh_config.get('name', '')

    if wh_name:
        for wh in w.warehouses.list():
            if wh.name == wh_name:
                if str(wh.state) not in ('RUNNING', 'STARTING'):
                    try:
                        w.warehouses.start(id=wh.id)
                        print(f"  Starting warehouse '{wh_name}'")
                    except Exception:
                        pass
                print(f"  ✅ Warehouse '{wh_name}' found (id={wh.id})")
                return wh.id

    for wh in w.warehouses.list():
        if str(wh.state) in ('RUNNING', 'STARTING'):
            print(f"  ✅ Using existing running warehouse '{wh.name}'")
            return wh.id

    warehouses = list(w.warehouses.list())
    if warehouses:
        wh = warehouses[0]
        try:
            w.warehouses.start(id=wh.id)
        except Exception:
            pass
        print(f"  ✅ Using warehouse '{wh.name}'")
        return wh.id

    if wh_name:
        try:
            wh = w.warehouses.create_and_wait(
                name=wh_name,
                cluster_size=wh_config.get('cluster_size', 'Small'),
                max_num_clusters=wh_config.get('max_num_clusters', 1),
                min_num_clusters=wh_config.get('min_num_clusters', 1),
                enable_serverless_compute=wh_config.get('enable_serverless_compute', True),
                auto_stop_mins=30,
            )
            print(f"  ✅ Created warehouse '{wh_name}' (id={wh.id})")
            return wh.id
        except Exception as e:
            print(f"  ⚠️ Could not create warehouse: {e}")

    return None


def ensure_cluster(w, user, config):
    """Find or create a cluster for the user."""
    cluster_config = config.get('user_config', {}).get('cluster_config', {})
    if not cluster_config:
        return None

    cluster_name = f"lab-{user.split('@')[0].replace('.', '-')}"

    for c in w.clusters.list():
        if c.cluster_name == cluster_name:
            if str(c.state) in ('TERMINATED',):
                try:
                    w.clusters.start(cluster_id=c.cluster_id)
                    print(f"  Starting cluster '{cluster_name}'")
                except Exception:
                    pass
            print(f"  ✅ Cluster '{cluster_name}' found (id={c.cluster_id})")
            return c.cluster_id

    try:
        result = w.clusters.create_and_wait(
            cluster_name=cluster_name,
            spark_version=cluster_config.get('spark_version', '15.4.x-scala2.12'),
            node_type_id=cluster_config.get('node_type_id', 'i3.xlarge'),
            num_workers=cluster_config.get('num_workers', 0),
            autotermination_minutes=60,
            runtime_engine=cluster_config.get('runtime_engine', 'PHOTON'),
            data_security_mode='SINGLE_USER',
            single_user_name=user,
        )
        print(f"  ✅ Created cluster '{cluster_name}' (id={result.cluster_id})")
        return result.cluster_id
    except Exception as e:
        print(f"  ⚠️ Could not create cluster: {e}")
        return None


def build_redirect_url(workspace_url, notebook_path):
    """Build the full URL for the notebook redirect."""
    base = workspace_url.rstrip('/')
    if not base.startswith('http'):
        base = f'https://{base}'
    return f'{base}#notebook{notebook_path}'


if __name__ == "__main__":
    user = os.getenv('VOC_DB_USER_EMAIL')
    workspace_url = os.getenv('VOC_DB_WORKSPACE_URL')
    token = os.getenv('VOC_DB_API_TOKEN')

    if not user:
        error_exit("INSUFFICIENT_ARGS", "VOC_DB_USER_EMAIL has not been supplied")

    if not ipc_data_file:
        error_exit("INSUFFICIENT_ARGS", "VOC_IPC_DATA_FILE has not been supplied")

    if not workspace_url or not token:
        error_exit("INSUFFICIENT_ARGS", "VOC_DB_WORKSPACE_URL or VOC_DB_API_TOKEN not set")

    print(f"Custom user_setup for: {user}")
    print(f"Workspace: {workspace_url}")

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient(host=workspace_url, token=token)
    except Exception as e:
        trace_it()
        error_exit("WORKSPACE_INIT_ERROR", f"Could not init WorkspaceClient: {e}")

    config = load_config()
    print(f"Config loaded: entry={config.get('content', {}).get('entry', 'N/A')}")

    # Step 1: Import notebooks
    try:
        notebook_path = import_notebooks(w, user, config)
        print(f"  Notebook path: {notebook_path}")
    except Exception as e:
        trace_it()
        notebook_path = f'/Workspace/Users/{user}/lab/LAB_Description'
        print(f"  ⚠️ Import error, using fallback path: {notebook_path}")

    # Step 2: Ensure SQL warehouse is running
    try:
        ensure_warehouse(w, config)
    except Exception as e:
        print(f"  ⚠️ Warehouse setup warning: {e}")

    # Step 3: Ensure cluster exists (with UC enabled via SINGLE_USER mode)
    try:
        ensure_cluster(w, user, config)
    except Exception as e:
        print(f"  ⚠️ Cluster setup warning: {e}")

    # Step 4: Return redirect URL
    redirect_url = build_redirect_url(workspace_url, notebook_path)
    print(f"  Redirect URL: {redirect_url}")

    if redirect_url:
        with open(ipc_data_file, 'w') as data_file:
            data_file.write(json.dumps({'notebook_url': redirect_url}))

    print("✅ Custom user_setup complete")
    sys.exit(0)
