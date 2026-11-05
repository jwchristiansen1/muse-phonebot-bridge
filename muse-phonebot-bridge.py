#!/usr/bin/env python3
"""
Muse Phonebot bridge — stands in for Grok's Cursor webhook.

Grok inbound today: Twilio Function -> Cursor platform webhook
(api2.cursor.sh/automations/webhook/<routine id>) -> wakes Phonebot.
Muse has no platform webhook, so this bridge is the replacement:

  Twilio Function -> POST /inbound (this bridge, Bearer BRIDGE_WAKE_KEY)
                  -> queued in a local JSON file
  Muse hook       -> GET /inbound/count?since=<last_id>  (count only,
                     no message content, no secret in the hook script)
                  -> wakes a Muse agent only when count > 0
  Muse agent      -> GET /inbound/messages?since=<last_id>
                     (Bearer BRIDGE_MUSE_TOKEN) -> relays to John / BK Boss
                  -> POST /inbound/ack {"ids": [...]}
  Muse agent      -> POST /send {"to": ..., "body": ..., "dry_run": false}
                     (Bearer BRIDGE_MUSE_TOKEN) -> bridge checks DNC,
                     formats "Alta Legal: ... Reply STOP to opt out.",
                     calls Twilio's API with the muse-dryrun key.

Volume: John sends/receives about 5-10 texts a day, so a queue file
is plenty. No database, no dependencies — Python 3 stdlib only.

Environment (set in the host's dashboard, never in this file):
  BRIDGE_WAKE_KEY         required. Bearer token the Twilio Function
                          sends. This is the NEW Muse WAKE_KEY — set it
                          in the Function only at cutover, never before.
                          Grok's WAKE_KEY stays untouched until then.
  BRIDGE_MUSE_TOKEN       required. Bearer token Muse uses for
                          /inbound/messages, /inbound/ack and /send.
  TWILIO_ACCOUNT_SID      required for /send.
  TWILIO_API_KEY_SID      preferred for /send, with TWILIO_API_KEY_SECRET.
  TWILIO_API_KEY_SECRET   preferred for /send.
  TWILIO_AUTH_TOKEN       fallback for /send, only if no API key pair.
  BRIDGE_FROM_NUMBER      default +13852339099. Never a /send recipient.
  BRIDGE_QUEUE_PATH       default queue.json next to this file.
  DNC_FILE                optional. One phone number per line (any
                          format); /send refuses those recipients.
  PORT                    default 8788. (8787 was Grok's old dead port.)

Run locally for testing:
  BRIDGE_WAKE_KEY=test-wake BRIDGE_MUSE_TOKEN=test-muse \
    python3 muse-phonebot-bridge.py

Deploy (example, Render free web service):
  Upload this one file, start command `python3 muse-phonebot-bridge.py`,
  set the env vars above in Render's dashboard. The queue file is
  ephemeral on a free service — that is acceptable here because the
  queue is short-lived (the hook polls every 60s and the agent acks
  within minutes), and Twilio's own message logs remain the source
  of truth if the bridge ever restarts with messages still queued.
  Do NOT repoint the live Twilio Function to this bridge until the
  synthetic-payload test and a spare-number test both pass.
"""

import base64
import json
import os
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

FROM_NUMBER = os.environ.get("BRIDGE_FROM_NUMBER", "+13852339099")
QUEUE_PATH = os.environ.get(
    "BRIDGE_QUEUE_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "queue.json"),
)
PORT = int(os.environ.get("PORT", "8788"))
PROBE_BODIES = {
    "webhook-live-probe-ignore",
    "wake-test-ignore",
    "tunnel-free-wake-test-ignore",
}
LOCK = threading.Lock()


def _load_queue():
    try:
        with open(QUEUE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def _save_queue(queue):
    tmp = QUEUE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(queue, f)
    os.replace(tmp, QUEUE_PATH)


def _norm_phone(raw):
    digits = re.sub(r"\D", "", raw or "")
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    if len(digits) == 10:
        return "+1" + digits
    return None


def _dnc_numbers():
    path = os.environ.get("DNC_FILE", "")
    if not path or not os.path.exists(path):
        return set()
    out = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            n = _norm_phone(line.strip())
            if n:
                out.add(n)
    return out


def _twilio_auth():
    sid = (os.environ.get("TWILIO_ACCOUNT_SID") or "").strip()
    key_sid = (os.environ.get("TWILIO_API_KEY_SID") or "").strip()
    key_secret = (os.environ.get("TWILIO_API_KEY_SECRET") or "").strip()
    auth_token = (os.environ.get("TWILIO_AUTH_TOKEN") or "").strip()
    if not sid:
        return None
    if key_sid and key_secret:
        return sid, key_sid, key_secret
    if auth_token:
        return sid, sid, auth_token
    return None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):  # keep logs quiet; no bodies logged
        pass

    def _send_json(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _bearer(self):
        auth = self.headers.get("Authorization", "")
        return auth[7:] if auth.startswith("Bearer ") else ""

    def _read_json(self):
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode())
        except Exception:
            return {}

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/health":
            with LOCK:
                n = len(_load_queue())
            return self._send_json(200, {"ok": True, "queued": n})
        if parsed.path == "/inbound/count":
            # Count only. No auth, no message content, no PII.
            # This is the endpoint the Muse hook polls.
            since = int(query.get("since", ["0"])[0] or 0)
            with LOCK:
                queue = _load_queue()
            new = [m for m in queue if m["id"] > since and not m["delivered"]]
            latest = max((m["id"] for m in queue), default=0)
            return self._send_json(200, {"new": len(new), "latest_id": latest})
        if parsed.path == "/inbound/messages":
            if self._bearer() != os.environ.get("BRIDGE_MUSE_TOKEN", ""):
                return self._send_json(401, {"error": "unauthorized"})
            since = int(query.get("since", ["0"])[0] or 0)
            with LOCK:
                queue = _load_queue()
            msgs = [m for m in queue if m["id"] > since]
            return self._send_json(200, {"messages": msgs})
        return self._send_json(404, {"error": "not found"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/inbound":
            if self._bearer() != os.environ.get("BRIDGE_WAKE_KEY", ""):
                return self._send_json(401, {"error": "unauthorized"})
            payload = self._read_json()
            body = (payload.get("Body") or "").strip()
            sid = (payload.get("MessageSid") or "").strip()
            if not body or body in PROBE_BODIES or not sid:
                # Probe / blank / already-handled test payloads: accept
                # (so Twilio sees 200) but do not queue.
                return self._send_json(200, {"ok": True, "queued": False})
            with LOCK:
                queue = _load_queue()
                if any(m.get("message_sid") == sid for m in queue):
                    return self._send_json(200, {"ok": True, "queued": False,
                                                  "duplicate": True})
                new_id = max((m["id"] for m in queue), default=0) + 1
                queue.append({
                    "id": new_id,
                    "from": payload.get("From"),
                    "to": payload.get("To"),
                    "body": body,
                    "message_sid": sid,
                    "status": payload.get("SmsStatus") or payload.get("status"),
                    "received_at": datetime.now(timezone.utc).isoformat(),
                    "delivered": False,
                })
                _save_queue(queue)
            return self._send_json(200, {"ok": True, "queued": True, "id": new_id})
        if parsed.path == "/inbound/ack":
            if self._bearer() != os.environ.get("BRIDGE_MUSE_TOKEN", ""):
                return self._send_json(401, {"error": "unauthorized"})
            ids = set(self._read_json().get("ids") or [])
            with LOCK:
                queue = _load_queue()
                for m in queue:
                    if m["id"] in ids:
                        m["delivered"] = True
                _save_queue(queue)
            return self._send_json(200, {"ok": True, "acked": len(ids)})
        if parsed.path == "/send":
            if self._bearer() != os.environ.get("BRIDGE_MUSE_TOKEN", ""):
                return self._send_json(401, {"error": "unauthorized"})
            payload = self._read_json()
            to = _norm_phone(payload.get("to"))
            if not to:
                return self._send_json(400, {"error": "invalid recipient"})
            if to == FROM_NUMBER:
                return self._send_json(400, {"error": "recipient is the from-number"})
            if to in _dnc_numbers():
                return self._send_json(400, {"error": "recipient is do-not-contact"})
            text = (payload.get("body") or "").strip()
            if not text:
                return self._send_json(400, {"error": "empty body"})
            formatted = f"Alta Legal: {text} Reply STOP to opt out."
            if payload.get("dry_run", True):
                return self._send_json(200, {"ok": True, "dry_run": True,
                                              "would_send": {"from": FROM_NUMBER,
                                                             "to": to,
                                                             "body": formatted}})
            auth = _twilio_auth()
            if not auth:
                return self._send_json(500, {"error": "twilio credentials not configured"})
            sid, user, password = auth
            data = urllib.parse.urlencode(
                {"From": FROM_NUMBER, "To": to, "Body": formatted}).encode()
            req = urllib.request.Request(
                f"https://api.twilio.com/2010-04-01/Accounts/{sid}/Messages.json",
                data=data, method="POST",
                headers={"Authorization": "Basic " + base64.b64encode(
                    f"{user}:{password}".encode()).decode()})
            try:
                with urllib.request.urlopen(req, timeout=30) as resp:
                    result = json.loads(resp.read().decode())
            except urllib.error.HTTPError as e:
                try:
                    detail = json.loads(e.read().decode()).get("message")
                except Exception:
                    detail = ""
                return self._send_json(e.code, {"error": detail or "twilio error"})
            return self._send_json(200, {"ok": True, "sid": result.get("sid"),
                                          "status": result.get("status")})
        return self._send_json(404, {"error": "not found"})


if __name__ == "__main__":
    for var in ("BRIDGE_WAKE_KEY", "BRIDGE_MUSE_TOKEN"):
        if not os.environ.get(var):
            raise SystemExit(f"Set {var} in the environment before starting the bridge.")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
