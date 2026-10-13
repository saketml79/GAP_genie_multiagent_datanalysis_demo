"""
Genie Landing — Micro Front Door for the Curator Workbench.

A tiny Flask app that serves a branded loading page and proxies two
Databricks REST API calls (status check + start) using the visitor's
visitor's OBO token (X-Forwarded-Access-Token) so the start call
runs as the logged-in user.  All designated users have CAN_MANAGE.

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
    """Return auth headers using the visitor's OBO token.
    The user has CAN_MANAGE on the target app so start/stop work.
    Falls back to SP only if no OBO token present."""
    token = request.headers.get("X-Forwarded-Access-Token", "")
    if token:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
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
    """Return the target app's compute state + its URL."""
    try:
        headers = _auth_headers()
        r = _req.get(f"{_HOST}/api/2.0/apps/{TARGET_APP_NAME}", headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()
        cs = data.get("compute_status", {})
        return jsonify({
            "state": cs.get("state", "UNKNOWN"),
            "message": cs.get("message", ""),
            "url": TARGET_APP_URL or data.get("url", ""),
        })
    except Exception as e:
        log.error(f"Status check failed: {e}")
        return jsonify({"state": "ERROR", "message": str(e), "url": TARGET_APP_URL}), 500


@app.route("/api/start", methods=["POST"])
def start():
    """Start the target app if it is stopped."""
    try:
        headers = _auth_headers()
        r = _req.post(f"{_HOST}/api/2.0/apps/{TARGET_APP_NAME}/start", headers=headers, timeout=15)
        r.raise_for_status()
        return jsonify({"ok": True, "message": f"Start request sent for {TARGET_APP_NAME}"})
    except Exception as e:
        log.error(f"Start failed: {e}")
        return jsonify({"ok": False, "message": str(e)}), 500


@app.route("/api/config")
def config():
    """Hand the target URL to the front-end (no secrets)."""
    return jsonify({"targetUrl": TARGET_APP_URL, "targetApp": TARGET_APP_NAME})


# ── Entrypoint ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    log.info(f"Genie Landing starting on :{port}  |  target={TARGET_APP_NAME}")
    app.run(host="0.0.0.0", port=port)
