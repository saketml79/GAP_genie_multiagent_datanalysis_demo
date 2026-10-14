"""
Genie Landing — Micro Front Door for the Curator Workbench.

A tiny Flask app that serves a branded loading page and proxies
Databricks REST API calls (status check + start) using the app SP.
Uses POST /apps/{name}/start which auto-deploys from
default_source_code_path.  The /deployments endpoint requires higher
privileges that group-based CAN_MANAGE cannot provide.

Compute: Liquid (Brickvisor) — 0 DBU/hr, scales to zero.
Cold start: ~5-8 seconds (Flask + minimal deps).
"""
import os, logging, requests as _req
from flask import Flask, jsonify, send_from_directory, request

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("genie-landing")

app = Flask(__name__, static_folder="static")

TARGET_APP_NAME = os.environ.get("TARGET_APP_NAME", "genie-curator-workbench")
TARGET_APP_URL  = os.environ.get("TARGET_APP_URL", "")

# Resolve workspace host once at startup from SP config
try:
    from databricks.sdk import WorkspaceClient as _WC
    _HOST = _WC().config.host.rstrip("/")
except Exception:
    _HOST = os.environ.get("DATABRICKS_HOST", "").rstrip("/")


def _auth_headers():
    """Return SP auth headers.  The landing app has Can Run
    on the target app via resource binding.  OBO is not possible
    because the apps API scope doesn't exist for space apps."""
    try:
        h = dict(_WC().config.authenticate())
        h["Content-Type"] = "application/json"
        return h
    except Exception:
        return {"Content-Type": "application/json"}


# ── Routes ──────────────────────────────────────────────────────────────────

@app.route("/")
def landing():
    """Serve the landing page."""
    return send_from_directory("static", "index.html")


@app.route("/api/status")
def status():
    """Return the target app's compute state, deployment progress, and URL."""
    try:
        headers = _auth_headers()
        log.info(f"Status check: host={_HOST}, auth_len={len(headers.get('Authorization',''))}")
        r = _req.get(f"{_HOST}/api/2.0/apps/{TARGET_APP_NAME}", headers=headers, timeout=10)
        if r.status_code != 200:
            body = r.text[:500]
            log.error(f"Status {r.status_code}: {body}")
            return jsonify({"state": "ERROR", "message": f"{r.status_code}: {body}", "url": TARGET_APP_URL}), r.status_code
        data = r.json()
        cs = data.get("compute_status", {})
        # Detect active / pending deployment
        pending = data.get("pending_deployment") or {}
        active_dep = data.get("active_deployment") or {}
        pend_st = pending.get("status", {})
        act_st = active_dep.get("status", {})
        deploying = bool(pending and pend_st.get("state") in ("IN_PROGRESS", "PENDING"))
        deploy_msg = pend_st.get("message", "") if deploying else ""
        deploy_state = pend_st.get("state", "") if deploying else act_st.get("state", "")
        return jsonify({
            "state": cs.get("state", "UNKNOWN"),
            "message": cs.get("message", ""),
            "url": TARGET_APP_URL or data.get("url", ""),
            "deploying": deploying,
            "deploy_message": deploy_msg,
            "deploy_state": deploy_state,
        })
    except Exception as e:
        log.error(f"Status check failed: {e}", exc_info=True)
        return jsonify({"state": "ERROR", "message": str(e), "url": TARGET_APP_URL}), 500


@app.route("/api/start", methods=["POST"])
def start():
    """Start the target app.

    Uses POST /apps/{name}/start which automatically deploys from
    the app's default_source_code_path.  No need to call /deployments
    separately (which requires higher privileges).
    """
    try:
        headers = _auth_headers()
        log.info(f"Start: host={_HOST}, auth_len={len(headers.get('Authorization',''))}")
        r = _req.post(
            f"{_HOST}/api/2.0/apps/{TARGET_APP_NAME}/start",
            headers=headers,
            timeout=15,
        )
        if r.status_code != 200:
            body = r.text[:500]
            log.error(f"Start {r.status_code}: {body}")
            return jsonify({"ok": False, "message": f"{r.status_code}: {body}"}), r.status_code
        state = r.json().get("compute_status", {}).get("state", "unknown")
        log.info(f"Start request sent: {TARGET_APP_NAME} -> {state}")
        return jsonify({"ok": True, "message": f"Start sent — {state}"})
    except Exception as e:
        log.error(f"Start failed: {e}", exc_info=True)
        return jsonify({"ok": False, "message": str(e)}), 500


@app.route("/api/config")
def config():
    """Hand the target URL to the front-end (no secrets)."""
    return jsonify({"targetUrl": TARGET_APP_URL, "targetApp": TARGET_APP_NAME})


@app.route("/api/ready")
def ready():
    """Probe the actual workbench URL using the user's OBO token.

    The Databricks API reports RUNNING as soon as compute is up,
    but the Dash app inside may still be initializing (502).
    This endpoint uses the caller's forwarded access token to hit
    the real workbench URL and returns ready=true only when it
    gets a non-5xx response.
    """
    obo = request.headers.get("X-Forwarded-Access-Token", "")
    if not obo:
        return jsonify({"ready": False, "reason": "no OBO token"})

    # Resolve target URL if not set
    url = TARGET_APP_URL
    if not url:
        try:
            h = _auth_headers()
            r = _req.get(f"{_HOST}/api/2.0/apps/{TARGET_APP_NAME}", headers=h, timeout=8)
            if r.status_code == 200:
                url = r.json().get("url", "")
        except Exception:
            pass
    if not url:
        return jsonify({"ready": False, "reason": "no target URL"})

    # Probe the workbench with the user's OBO token
    try:
        probe = _req.get(
            url,
            headers={"Authorization": f"Bearer {obo}"},
            timeout=10,
            allow_redirects=True,
        )
        log.info(f"Ready probe: {url} -> {probe.status_code}")
        if probe.status_code < 500:
            return jsonify({"ready": True, "status": probe.status_code})
        else:
            return jsonify({"ready": False, "status": probe.status_code})
    except Exception as e:
        log.info(f"Ready probe failed: {e}")
        return jsonify({"ready": False, "reason": str(e)})


@app.route("/api/whoami")
def whoami():
    """Diagnostic: dump the runtime identity the app is using."""
    import os
    result = {}
    try:
        w = _WC()
        me = w.current_user.me()
        result["current_user"] = {
            "id": me.id,
            "user_name": me.user_name,
            "display_name": me.display_name,
            "active": me.active,
            "application_id": getattr(me, 'application_id', None),
            "external_id": getattr(me, 'external_id', None),
            "groups": [g.display for g in (me.groups or [])],
            "entitlements": [e.value for e in (me.entitlements or [])],
            "roles": [r.value for r in (me.roles or [])],
        }
    except Exception as e:
        result["current_user_error"] = str(e)

    # Also try SCIM Me endpoint directly
    try:
        headers = _auth_headers()
        r = _req.get(f"{_HOST}/api/2.0/preview/scim/v2/Me", headers=headers, timeout=10)
        result["scim_me"] = r.json() if r.status_code == 200 else {"status": r.status_code, "body": r.text[:500]}
    except Exception as e:
        result["scim_me_error"] = str(e)

    # Env vars (redact secrets)
    safe_env = {}
    for k, v in os.environ.items():
        if any(s in k.upper() for s in ["SECRET", "TOKEN", "PASSWORD", "KEY"]):
            safe_env[k] = f"***REDACTED*** (len={len(v)})"
        elif k.startswith("DATABRICKS") or k.startswith("APP") or k in ["PORT", "HOME", "USER"]:
            safe_env[k] = v
    result["env"] = safe_env

    return jsonify(result)


# ── Entrypoint ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    log.info(f"Genie Landing starting on :{port}  |  target={TARGET_APP_NAME}")
    app.run(host="0.0.0.0", port=port)
