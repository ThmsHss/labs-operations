# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 LIVE DEMO: Creating a Cluster Policy
# MAGIC **Databricks Operations Masterclass — PUMA**
# MAGIC
# MAGIC ### Demo Objectives
# MAGIC - Create a cost-controlled cluster policy via SDK
# MAGIC - Test the policy by creating a cluster
# MAGIC - Verify policy constraints are enforced

# COMMAND ----------

# MAGIC %run ./LAB_CONFIG

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 1: Create a Cluster Policy via SDK

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import CreatePolicy
import json

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the Policy JSON

# COMMAND ----------

policy_definition = {
    "cluster_type": {
        "type": "fixed",
        "value": "all-purpose"
    },
    "autoscale.min_workers": {
        "type": "fixed",
        "value": 1
    },
    "autoscale.max_workers": {
        "type": "range",
        "minValue": 1,
        "maxValue": 4,
        "defaultValue": 2
    },
    "autotermination_minutes": {
        "type": "range",
        "minValue": 10,
        "maxValue": 60,
        "defaultValue": 20
    },
    "spark_version": {
        "type": "regex",
        "pattern": "1[4-9]\\.[0-9]+\\.x-scala.*"
    },
    "aws_attributes.availability": {
        "type": "fixed",
        "value": "SPOT_WITH_FALLBACK"
    },
    "custom_tags.Team": {
        "type": "fixed",
        "value": "pm-ops-workshop",
        "hidden": True
    }
}

print("Policy Definition:")
print(json.dumps(policy_definition, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Policy

# COMMAND ----------

your_identifier = 'test'
policy_name = f"{your_identifier}_pm-workshop-dev-policy"

try:
    policy = w.cluster_policies.create(
        name=policy_name,
        definition=json.dumps(policy_definition),
        description="Cost-controlled development cluster policy for PUMA workshop"
    )
    print(f"✅ Policy created successfully!")
    print(f"   Policy ID: {policy.policy_id}")
    print(f"   Policy Name: {policy_name}")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"ℹ️ Policy '{policy_name}' already exists")
        existing = [p for p in w.cluster_policies.list() if p.name == policy_name]
        if existing:
            policy = existing[0]
            print(f"   Policy ID: {policy.policy_id}")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 2: List All Cluster Policies

# COMMAND ----------

print("📋 All Cluster Policies in Workspace:\n")
for p in w.cluster_policies.list():
    print(f"  • {p.name}")
    print(f"    ID: {p.policy_id}")
    print(f"    Created by: {p.creator_user_name}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 3: What Happens When Users Try to Violate the Policy?
# MAGIC
# MAGIC Let's see what happens when we try to create a cluster with settings outside the policy.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try #1: Create a cluster with 10 workers (policy allows max 4)
# MAGIC This will fail because our policy limits max_workers to 4.

# COMMAND ----------

from databricks.sdk.service.compute import AutoScale

try:
    oversized_cluster = w.clusters.create(
        cluster_name="puma-test-oversized",
        spark_version="14.3.x-scala2.12",
        node_type_id="i3.xlarge",
        autoscale=AutoScale(min_workers=1, max_workers=10),  # Exceeds policy!
        policy_id=policy.policy_id
    )
except Exception as e:
    print("❌ Cluster creation BLOCKED by policy:")
    print(f"   Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Try #2: Create a compliant cluster
# MAGIC This should succeed because all settings are within policy constraints.

# COMMAND ----------

from databricks.sdk.service.compute import AutoScale

try:
    compliant_cluster = w.clusters.create(
        cluster_name="puma-test-compliant",
        spark_version="14.3.x-scala2.12",
        node_type_id="i3.xlarge",
        autoscale=AutoScale(min_workers=1, max_workers=2),  # Within policy!
        autotermination_minutes=20,
        policy_id=policy.policy_id,
        custom_tags={
            "Environment": "Dev"
        }
    )
    print("✅ Cluster creation ALLOWED!")
    print(f"   Cluster ID: {compliant_cluster.cluster_id}")
    print(f"   Name: puma-test-compliant")
    print("\n⚠️ Remember to delete this test cluster after the demo!")
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 4: Verify Policy Tags Are Applied
# MAGIC
# MAGIC Check that the hidden tag "Team: puma-ops-workshop" was applied automatically.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if our test cluster has the required tags
# MAGIC SELECT 
# MAGIC     cluster_name,
# MAGIC     tags,
# MAGIC     change_time
# MAGIC FROM system.compute.clusters
# MAGIC WHERE lower(cluster_name) LIKE '%puma%'
# MAGIC   AND delete_time IS NULL
# MAGIC ORDER BY change_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Demo 5: Cleanup - Delete Test Cluster

# COMMAND ----------

# Uncomment to delete the test cluster
# w.clusters.permanent_delete(cluster_id=compliant_cluster.cluster_id)
# print("✅ Test cluster deleted")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📋 Key Takeaways
# MAGIC
# MAGIC | Demo | What We Learned |
# MAGIC |------|-----------------|
# MAGIC | Create Policy | Policies are JSON definitions applied via SDK/UI |
# MAGIC | Policy Enforcement | Users CANNOT violate policy constraints |
# MAGIC | Hidden Tags | Tags can be auto-applied without user knowledge |
# MAGIC | Cost Control | Policies prevent runaway costs before they happen |
# MAGIC
# MAGIC ### When to Use Policies
# MAGIC
# MAGIC | Scenario | Policy Strategy |
# MAGIC |----------|----------------|
# MAGIC | Development teams | Max 4 workers, 30 min auto-term, spot instances |
# MAGIC | Production ETL | Approved DBR versions, required tags, spot fallback |
# MAGIC | Data Science | GPU node allowlist, longer auto-term, larger clusters |
# MAGIC | Sandbox/Training | Very restrictive, single-node only, 10 min auto-term |
# MAGIC
# MAGIC ---
# MAGIC **Next**: [day_1_06_job_parameters — Job Parameters in Action]($./day_1_06_job_parameters)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Mark Notebook Complete
# MAGIC Run the cell below when you are done with this notebook.

# COMMAND ----------

mark_notebook_complete("day_1_05c_cluster_policy")