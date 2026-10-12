# Databricks notebook source
# MAGIC %md
# MAGIC # Share the demo with all users (last task of Full Setup)
# MAGIC
# MAGIC Runs after every setup so any user of the Curator Workbench app can run setup, iterations and teardown end to end:
# MAGIC * **Unity Catalog:** `account users` becomes the owner of the catalog, every schema, table, view, metric view, function and volume.
# MAGIC * **Genie agents:** the `users` group gets **CAN_MANAGE** on every `SC ...` agent (all pages).
# MAGIC * **Supervisor endpoints:** the `users` group gets **CAN_QUERY** on every `mas-*` endpoint.
# MAGIC * **Entitlement:** the `users` group gets *Databricks SQL access* (needed to see Genie agents).
# MAGIC
# MAGIC Safe to re-run. Problems are reported as warnings; this task never fails the setup job.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "GAP_Demo_Dev")
CATALOG = dbutils.widgets.get("catalog_name").strip().lower()
OWNER_GROUP = "account users"   # UC owner (UC grants need an account-level group)
SHARE_GROUP = "users"           # workspace group for agent / endpoint permissions

import requests, time
host = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
warnings = []

# COMMAND ----------

# DBTITLE 1,Unity Catalog: hand every object to account users
objs = spark.sql(f"""
SELECT CASE WHEN table_type IN ('VIEW','METRIC_VIEW') THEN 'VIEW'
            WHEN table_type = 'MATERIALIZED_VIEW' THEN 'MATERIALIZED VIEW' ELSE 'TABLE' END AS kind,
       table_schema || '.' || table_name AS name
FROM `{CATALOG}`.information_schema.tables WHERE table_schema <> 'information_schema' AND table_owner <> '{OWNER_GROUP}'
UNION ALL SELECT 'FUNCTION', routine_schema || '.' || routine_name FROM `{CATALOG}`.information_schema.routines
WHERE routine_owner <> '{OWNER_GROUP}'
UNION ALL SELECT 'VOLUME', volume_schema || '.' || volume_name FROM `{CATALOG}`.information_schema.volumes
WHERE volume_owner <> '{OWNER_GROUP}'""").collect()
schemas = [r[0] for r in spark.sql(f"""SELECT schema_name FROM `{CATALOG}`.information_schema.schemata
WHERE schema_name <> 'information_schema' AND schema_owner <> '{OWNER_GROUP}'""").collect()]
stmts = ([f"ALTER {r.kind} `{CATALOG}`.{r.name} OWNER TO `{OWNER_GROUP}`" for r in objs]      # children first
         + [f"ALTER SCHEMA `{CATALOG}`.{s} OWNER TO `{OWNER_GROUP}`" for s in schemas]
         + [f"ALTER CATALOG `{CATALOG}` OWNER TO `{OWNER_GROUP}`"])
ok = 0
for st in stmts:
    try:
        spark.sql(st); ok += 1
    except Exception as e:
        warnings.append(f"UC: {st} -> {str(e).splitlines()[0][:150]}")
print(f"Unity Catalog: {ok}/{len(stmts)} ownership changes applied to '{OWNER_GROUP}'")
left = spark.sql(f"""SELECT COUNT(*) FROM (
  SELECT table_owner o FROM `{CATALOG}`.information_schema.tables WHERE table_schema <> 'information_schema'
  UNION ALL SELECT routine_owner FROM `{CATALOG}`.information_schema.routines
  UNION ALL SELECT schema_owner FROM `{CATALOG}`.information_schema.schemata WHERE schema_name <> 'information_schema')
WHERE o <> '{OWNER_GROUP}'""").first()[0]
print(f"Objects not owned by '{OWNER_GROUP}': {left}")

# COMMAND ----------

# DBTITLE 1,Genie agents: CAN_MANAGE for all users (every page of agents)
def patch_acl(kind, oid, level):
    try:
        r = requests.patch(f"{host}/api/2.0/permissions/{kind}/{oid}", headers=headers, timeout=30,
                           json={"access_control_list": [{"group_name": SHARE_GROUP, "permission_level": level}]})
        return r.status_code == 200, f"{r.status_code}: {r.text[:120]}"
    except Exception as e:
        return False, str(e)[:120]

spaces, page = [], None
while True:
    r = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers, timeout=30,
                     params={"page_size": 100, **({"page_token": page} if page else {})})
    if r.status_code != 200:
        warnings.append(f"Genie list failed: {r.status_code} {r.text[:120]}"); break
    spaces += r.json().get("spaces", []); page = r.json().get("next_page_token")
    if not page: break
sc = [s for s in spaces if s.get("title", "").startswith("SC ")]
ok = 0
for s in sc:
    good, msg = patch_acl("genie", s["space_id"], "CAN_MANAGE")
    ok += good
    if not good: warnings.append(f"Agent {s['title']}: {msg}")
    time.sleep(0.2)
print(f"Genie agents: {ok}/{len(sc)} shared with '{SHARE_GROUP}' (CAN_MANAGE)")

# COMMAND ----------

# DBTITLE 1,Supervisor endpoints: CAN_QUERY for all users
r = requests.get(f"{host}/api/2.0/serving-endpoints", headers=headers, timeout=30)
eps = [e for e in (r.json().get("endpoints", []) if r.status_code == 200 else []) if e.get("name", "").startswith("mas-")]
ok = 0
for e in eps:
    good, msg = patch_acl("serving-endpoints", e["id"], "CAN_QUERY")
    ok += good
    if not good: warnings.append(f"Endpoint {e['name']}: {msg}")
print(f"Supervisor endpoints: {ok}/{len(eps)} shared with '{SHARE_GROUP}' (CAN_QUERY)")

# COMMAND ----------

# DBTITLE 1,Databricks SQL access entitlement for the users group
# Without this entitlement, users cannot see Genie agents in the left sidebar.
try:
    g = requests.get(f"{host}/api/2.0/preview/scim/v2/Groups", headers=headers, timeout=30,
                     params={"filter": f'displayName eq "{SHARE_GROUP}"'}).json().get("Resources", [])
    if not g:
        warnings.append(f"Group '{SHARE_GROUP}' not found")
    elif "databricks-sql-access" in [e["value"] for e in g[0].get("entitlements", [])]:
        print("Databricks SQL access: already enabled")
    else:
        pr = requests.patch(f"{host}/api/2.0/preview/scim/v2/Groups/{g[0]['id']}", headers=headers, timeout=30, json={
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "add", "path": "entitlements", "value": [{"value": "databricks-sql-access"}]}]})
        print("Databricks SQL access:", "granted" if pr.status_code == 200 else f"failed {pr.status_code}")
        if pr.status_code != 200: warnings.append(f"Entitlement: {pr.status_code} {pr.text[:120]}")
except Exception as e:
    warnings.append(f"Entitlement: {e}")

# COMMAND ----------

# DBTITLE 1,Summary
print("=" * 70)
print("WARNINGS:" if warnings else "All sharing steps succeeded.")
for w in warnings: print("  -", w)
dbutils.notebook.exit("OK" if not warnings else f"OK with {len(warnings)} warning(s)")
