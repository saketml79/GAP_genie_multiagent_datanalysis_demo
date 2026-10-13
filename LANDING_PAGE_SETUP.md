# Genie Landing — Front Door for the Curator Workbench

## What This Is

A micro Databricks App (`genie-landing`) that sits in front of the main
`genie-curator-workbench` app. When a user visits, it:

1. Shows a branded loading page instantly (~5s cold start)
2. Checks the main app's status via the Databricks SDK (server-side, no exposed tokens)
3. Starts the main app if it's stopped/idle
4. Polls until the main app is ACTIVE, then auto-redirects

**Cost**: ~$0/month (scales to zero, Flask + 2 deps, fractions of a DBU per wake cycle).

## File Structure

```
src/genie-landing/
├── app.yaml              # Databricks App config (target app name + URL)
├── app.py                # Flask server: 4 routes (/, /api/status, /api/start, /api/config)
├── requirements.txt      # flask + databricks-sdk
└── static/
    └── index.html        # Dark-themed landing page with spinner + progress bar
```

## How to Deploy

### 1. Create the Databricks App (one-time)

```bash
databricks apps create genie-landing
```

### 2. Grant the Landing App Permission to Start the Main App

The landing app's service principal needs `CAN_MANAGE` on the main app so it can
call `apps.start()`.

```bash
# Find the landing app's SP name (created automatically)
databricks apps get genie-landing --output JSON | python3 -c "
import sys,json; d=json.load(sys.stdin)
print('SP name:', d['service_principal_name'])
print('SP id:', d['service_principal_id'])
"

# Grant it CAN_MANAGE on the main app
databricks apps update-permissions genie-curator-workbench \
  --json '{
    "access_control_list": [
      {
        "service_principal_name": "<SP_NAME_FROM_ABOVE>",
        "permission_level": "CAN_MANAGE"
      }
    ]
  }'
```

Replace `<SP_NAME_FROM_ABOVE>` (angle brackets included) with the actual SP name,
e.g. `app-45c6gp genie-landing`.

### 3. Deploy

```bash
databricks apps deploy genie-landing \
  --source-code-path /Workspace/Users/ssuman@bpcs.com/GAP_genie_multiagent_datanalysis_demo/src/genie-landing
```

### 4. Verify

```bash
# Check it's running
databricks apps get genie-landing

# Open the URL in your browser — it should show the landing page
```

### 5. Share the Landing URL

The landing page URL is the one you share with users:

```
https://genie-landing-1866518241053589.9.azure.databricksapps.com
```

Users visit this URL. If the main app is running, they're redirected instantly.
If it's stopped, they see a progress spinner while it wakes up (~30-60s).

## How It Works

```
User visits genie-landing URL
  │
  ├─ Landing app cold-starts (~5-8s, Flask + minimal deps)
  │
  ├─ Browser loads index.html (branded spinner page)
  │
  ├─ JS calls  GET /api/status  →  Flask  →  WorkspaceClient().apps.get()
  │   └─ Returns: { state: "STOPPED" | "ACTIVE", url: "..." }
  │
  ├─ If STOPPED:  JS calls  POST /api/start  →  Flask  →  WorkspaceClient().apps.start()
  │   └─ Returns: { ok: true }
  │
  ├─ JS polls  GET /api/status  every 3.5s (progress bar advances)
  │   └─ Shows: "Starting up... STOPPED • 12s elapsed"
  │
  └─ When state = ACTIVE:
      └─ Auto-redirect to main app URL after 3.5s buffer
```

**Key design**: All Databricks API calls happen server-side via the Databricks SDK.
The browser never sees a token. The landing app's own service principal authenticates
automatically (standard Databricks Apps auth).

## Configuration

Edit `app.yaml` to change the target app:

```yaml
env:
  - name: TARGET_APP_NAME
    value: "genie-curator-workbench"           # ← the main app's name
  - name: TARGET_APP_URL
    value: "https://genie-curator-workbench-1866518241053589.9.azure.databricksapps.com"
```

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| Landing page shows "Status check failed" | Landing app SP lacks permission on main app | Run the `apps update-permissions` command from step 2 |
| "Cannot reach landing server" | Landing app itself hasn't started yet | Refresh — Flask cold-starts in ~5-8s |
| Main app never reaches ACTIVE | Main app crashed during startup | Check main app logs: `databricks apps logs genie-curator-workbench` |
| Timeout after 5 minutes | Main app is stuck | Check main app deployment: `databricks apps get genie-curator-workbench` |
