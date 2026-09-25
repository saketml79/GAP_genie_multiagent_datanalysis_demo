# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Grant CAN_RUN on all SC* Genie Agents + Supervisor Endpoint
# MAGIC %md
# MAGIC # Grant Permissions on SC* Genie Agents
# MAGIC
# MAGIC Grants **CAN_RUN** to the `users` group on all 20 SC* Genie agents and **CAN_QUERY** on the supervisor serving endpoint.
# MAGIC
# MAGIC Run each cell in order. Safe to re-run (PATCH is additive — existing permissions are preserved).

# COMMAND ----------

# DBTITLE 1,Discover all SC* Genie agents
import requests, time, json

host = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# Discover all Genie spaces starting with "SC"
resp = requests.get(f"https://{host}/api/2.0/genie/spaces", headers=headers)
assert resp.status_code == 200, f"List failed: {resp.status_code} {resp.text}"

all_spaces = resp.json().get("spaces", [])
sc_spaces = [(s["title"], s["space_id"]) for s in all_spaces if s.get("title", "").startswith("SC")]
sc_spaces.sort(key=lambda x: x[0])

print(f"Found {len(sc_spaces)} SC* Genie agents:\n")
for name, sid in sc_spaces:
    print(f"  {name}  ({sid})")

# COMMAND ----------

# DBTITLE 1,Grant CAN_RUN on all SC* Genie agents
# Grant CAN_RUN to the 'users' group on every SC* Genie agent
acl_payload = {
    "access_control_list": [
        {"group_name": "users", "permission_level": "CAN_RUN"}
    ]
}

success, failed = 0, 0
for name, space_id in sc_spaces:
    url = f"https://{host}/api/2.0/permissions/genie/{space_id}"
    resp = requests.patch(url, headers=headers, json=acl_payload)
    if resp.status_code == 200:
        success += 1
        print(f"OK   {name}")
    else:
        failed += 1
        print(f"FAIL {name} ({resp.status_code}: {resp.text[:120]})")
    time.sleep(0.2)

print(f"\n--- Genie agents: {success} succeeded, {failed} failed out of {len(sc_spaces)} ---")

# COMMAND ----------

# DBTITLE 1,Grant CAN_QUERY on Supervisor serving endpoint
# Grant CAN_QUERY on the supervisor serving endpoint
endpoint_name = "mas-c3b78ce3-endpoint"

# First get the endpoint ID
ep_resp = requests.get(f"https://{host}/api/2.0/serving-endpoints/{endpoint_name}", headers=headers)
assert ep_resp.status_code == 200, f"Endpoint lookup failed: {ep_resp.status_code} {ep_resp.text[:200]}"
endpoint_id = ep_resp.json()["id"]
print(f"Supervisor endpoint: {endpoint_name} (ID: {endpoint_id})")

# Grant CAN_QUERY to 'users' group
ep_acl = {
    "access_control_list": [
        {"group_name": "users", "permission_level": "CAN_QUERY"}
    ]
}
resp = requests.patch(
    f"https://{host}/api/2.0/permissions/serving-endpoints/{endpoint_id}",
    headers=headers, json=ep_acl
)
if resp.status_code == 200:
    print(f"OK   Supervisor endpoint granted CAN_QUERY")
else:
    print(f"FAIL Supervisor endpoint ({resp.status_code}: {resp.text[:200]})")

# COMMAND ----------

# DBTITLE 1,Verify permissions
# Verify: GET permissions on each agent and the endpoint
print("=" * 80)
print("VERIFICATION: Current 'users' group permissions")
print("=" * 80)

for name, space_id in sc_spaces:
    resp = requests.get(f"https://{host}/api/2.0/permissions/genie/{space_id}", headers=headers)
    if resp.status_code == 200:
        acls = resp.json().get("access_control_list", [])
        users_perms = [a for a in acls if a.get("group_name") == "users"]
        if users_perms:
            levels = [p["permission_level"] for ap in users_perms for p in ap.get("all_permissions", [])]
            print(f"OK   {name}: {', '.join(levels)}")
        else:
            print(f"WARN {name}: 'users' group not found in ACL")
    else:
        print(f"ERR  {name}: GET failed ({resp.status_code})")

# Verify endpoint
resp = requests.get(f"https://{host}/api/2.0/permissions/serving-endpoints/{endpoint_id}", headers=headers)
if resp.status_code == 200:
    acls = resp.json().get("access_control_list", [])
    users_perms = [a for a in acls if a.get("group_name") == "users"]
    if users_perms:
        levels = [p["permission_level"] for ap in users_perms for p in ap.get("all_permissions", [])]
        print(f"OK   Supervisor endpoint ({endpoint_name}): {', '.join(levels)}")
    else:
        print(f"WARN Supervisor endpoint: 'users' group not found")

print("\nDone. All workspace users now have access.")

# COMMAND ----------

# DBTITLE 1,Grant Databricks SQL Access entitlement to users group
# Grant 'Databricks SQL access' entitlement to the 'users' group via SCIM API.
# Without this entitlement, users cannot see Genie Agents in the left sidebar.
# This is the programmatic equivalent of:
#   Admin Console → Identity and access → Groups → users → Entitlements → Databricks SQL access

# Step 1: Find the 'users' group ID via SCIM
scim_resp = requests.get(
    f"https://{host}/api/2.0/preview/scim/v2/Groups",
    headers=headers,
    params={"filter": 'displayName eq "users"'}
)
assert scim_resp.status_code == 200, f"SCIM list failed: {scim_resp.status_code} {scim_resp.text[:200]}"

groups = scim_resp.json().get("Resources", [])
assert len(groups) > 0, "'users' group not found"
users_group_id = groups[0]["id"]
print(f"Found 'users' group (ID: {users_group_id})")

# Check current entitlements
current_entitlements = [e["value"] for e in groups[0].get("entitlements", [])]
print(f"Current entitlements: {current_entitlements}")

if "databricks-sql-access" in current_entitlements:
    print("\n✓ 'Databricks SQL access' already enabled — nothing to do.")
else:
    # Step 2: PATCH to add the entitlement
    patch_payload = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [
            {
                "op": "add",
                "path": "entitlements",
                "value": [{"value": "databricks-sql-access"}]
            }
        ]
    }
    patch_resp = requests.patch(
        f"https://{host}/api/2.0/preview/scim/v2/Groups/{users_group_id}",
        headers=headers,
        json=patch_payload
    )
    if patch_resp.status_code == 200:
        print("\n✓ 'Databricks SQL access' entitlement granted to 'users' group.")
        print("  All workspace users can now see SQL features + Genie Agents in the sidebar.")
    else:
        print(f"\n✗ PATCH failed: {patch_resp.status_code} {patch_resp.text[:300]}")