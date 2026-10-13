# Databricks notebook source
# MAGIC %md
# MAGIC # Teardown: Remove All Demo Resources
# MAGIC **WARNING**: This will permanently delete the catalog, all Genie Spaces, and the Supervisor Agent.
# MAGIC Run this to clean up a deployment before redeploying, or to remove the demo entirely.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("user_alias", "", "User Alias (for per-user sandbox)")
CATALOG = dbutils.widgets.get("catalog_name")
USER_ALIAS = dbutils.widgets.get("user_alias").strip()

print(f"TEARDOWN STARTING for {CATALOG} (alias={USER_ALIAS or 'all'})")

# COMMAND ----------

import requests, json

try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host
    headers = w.config.authenticate()
    headers["Content-Type"] = "application/json"
except:
    host = f"https://{spark.conf.get('spark.databricks.workspaceUrl', '')}"
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# COMMAND ----------

# DBTITLE 1,Delete Supervisors (scoped to user)
# ---- Delete Supervisor Agents (all pages) + any orphaned Control Tower endpoints ----
_sups, _tok = [], None
for _ in range(50):
    resp = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers,
                        params={"page_size": 100, **({"page_token": _tok} if _tok else {})})
    if resp.status_code != 200:
        print(f"✗ Could not list supervisors: {resp.status_code}")
        break
    _sups += resp.json().get("supervisor_agents", [])
    _tok = resp.json().get("next_page_token")
    if not _tok:
        break
_sup_deleted = 0
for agent in _sups:
    dn = agent.get("display_name", "")
    # Scope to user alias if provided, otherwise delete all SC supervisors
    if USER_ALIAS and f"({USER_ALIAS})" not in dn:
        continue  # skip — belongs to another user
    if "Supply Chain" in dn or "Control Tower" in dn:
        del_resp = requests.delete(f"{host}/api/2.1/{agent['name']}", headers=headers)
        ok = del_resp.status_code in (200, 204)
        _sup_deleted += ok
        print(f"{'✓' if ok else '✗'} Deleted supervisor: {dn}")
# Safety net: endpoints this demo tagged (supervisor_description tag written by the iteration jobs)
eps = requests.get(f"{host}/api/2.0/serving-endpoints", headers=headers).json().get("endpoints", [])
for ep in eps:
    tags = {t.get("key"): t.get("value", "") for t in ep.get("tags", []) or []}
    if ep["name"].startswith("mas-") and "supervisor_description" in tags and "Supply Chain" in tags.get("display_name", ""):
        d = requests.delete(f"{host}/api/2.0/serving-endpoints/{ep['name']}", headers=headers)
        print(f"{'✓' if d.status_code in (200, 204) else '✗'} Deleted orphan endpoint: {ep['name']} ({tags.get('display_name')})")
print(f"Supervisors deleted: {_sup_deleted}")

# COMMAND ----------

# DBTITLE 1,Delete Genie Agents (scoped to user)
# ---- Delete ALL Genie Spaces (all iterations: Baseline, Iter 1, 2, 3, Evaluator) ----
# With per-iteration agent creation, there can be up to 21 agents:
#   5 × Baseline + 5 × Iteration 1 + 5 × Iteration 2 + 5 × Iteration 3 + 1 Evaluator
# The list API returns 20 agents per page -- read EVERY page first, then delete
# (deleting while paging would shift pages and skip agents).
_all_spaces, _page_token, _list_error = [], None, None
for _ in range(100):
    _params = {"page_size": 100, **({"page_token": _page_token} if _page_token else {})}
    resp = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers, params=_params)
    if resp.status_code != 200:
        _list_error = resp.status_code
        break
    _body = resp.json()
    _all_spaces += _body.get("spaces", [])
    _page_token = _body.get("next_page_token")
    if not _page_token:
        break
_deleted_count = 0
if _list_error is None:
    def _is_user_agent(title):
        """True if this agent belongs to the current user (or all, if no alias)."""
        is_sc = title.startswith("SC - ") or ("SC" in title and any(k in title for k in ["Demand", "Inventory", "Logistics", "Supplier", "Executive", "Evaluator"]))
        if not is_sc:
            return False
        if USER_ALIAS:
            return f"({USER_ALIAS})" in title
        return True  # no alias = delete all SC agents
    _targets = [s for s in _all_spaces if _is_user_agent(s.get("title", ""))]
    print(f"Found {len(_all_spaces)} Genie agents in total; {len(_targets)} SC agents to delete")
    for space in _targets:
        title = space.get("title", "")
        sid = space["space_id"]
        del_resp = requests.delete(f"{host}/api/2.0/genie/spaces/{sid}", headers=headers)
        status = "✓" if del_resp.status_code in (200, 204) else "✗"
        print(f"{status} Deleted space: {title}")
        if del_resp.status_code in (200, 204):
            _deleted_count += 1
    print(f"\n  Total deleted: {_deleted_count}/{len(_targets)} Genie agents")
else:
    print(f"✗ Could not list spaces: {_list_error}")

# COMMAND ----------

# ---- Drop demo catalog(s) (CASCADE: all schemas, tables, views, metric views, functions, volumes, history) ----
dbutils.widgets.text("extra_catalogs", "", "Extra demo catalogs to drop (comma-separated)")
_catalogs = [CATALOG] + [c.strip() for c in dbutils.widgets.get("extra_catalogs").split(",") if c.strip()]
for _cat in _catalogs:
    try:
        spark.sql(f"DROP CATALOG IF EXISTS `{_cat}` CASCADE")
        print(f"✓ Dropped catalog: {_cat}")
    except Exception as e:
        print(f"✗ Failed to drop catalog {_cat}: {e}")

# COMMAND ----------

# DBTITLE 1,Verify teardown (scoped to user)
# ---- Verify nothing is left (job FAILS if anything remains, so the app never reports a false success) ----
import time as _t
_t.sleep(5)
_left = []
_sp, _tok = [], None
for _ in range(100):
    r = requests.get(f"{host}/api/2.0/genie/spaces", headers=headers,
                     params={"page_size": 100, **({"page_token": _tok} if _tok else {})}).json()
    _sp += r.get("spaces", []); _tok = r.get("next_page_token")
    if not _tok:
        break
for s in _sp:
    _t = s.get("title", "")
    if not _t.startswith("SC - "):
        continue
    # Only flag agents that belong to this user (or all if no alias)
    if USER_ALIAS and f"({USER_ALIAS})" not in _t:
        continue
    _left.append(f"Genie agent: {_t}")
r = requests.get(f"{host}/api/2.1/supervisor-agents", headers=headers)
if r.status_code == 200:
    for a in r.json().get("supervisor_agents", []):
        _dn = a.get("display_name", "")
        if not ("Supply Chain" in _dn or "Control Tower" in _dn):
            continue
        if USER_ALIAS and f"({USER_ALIAS})" not in _dn:
            continue
        _left.append(f"Supervisor: {_dn}")
_cats = {c["name"].lower() for c in requests.get(f"{host}/api/2.1/unity-catalog/catalogs", headers=headers).json().get("catalogs", [])}
_left += [f"Catalog: {c}" for c in _catalogs if c.lower() in _cats]
print("UC Domains + Pages: created manually on the Discover page -- not removed by this job.")
if _left:
    print("✗ STILL PRESENT:"); [print("   " + x) for x in _left]
    raise RuntimeError(f"Teardown incomplete: {len(_left)} resource(s) remain")
print(f"\n✓ Teardown complete and verified: 0 SC agents, 0 Control Tower supervisors, catalogs dropped: {', '.join(_catalogs)}")