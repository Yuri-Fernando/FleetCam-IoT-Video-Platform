"""Readiness probe for a cold Compose start: API health alone is not enough, because the S3 store
(master, volume and filer) may still be starting. Waits until a real evidence upload confirms."""
import base64
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from fleet import event, request

BASE = "http://127.0.0.1:8080"
KEY = "local-demo-key"
DEADLINE = time.monotonic() + (float(sys.argv[1]) if len(sys.argv) > 1 else 240)
IMAGE = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")

def attempt(step, fn):
    last = None
    while time.monotonic() < DEADLINE:
        try:
            return fn()
        except (urllib.error.URLError, ConnectionError, OSError, AssertionError, KeyError) as exc:
            last = exc
            time.sleep(2)
    raise SystemExit(f"NOT READY: {step}: {last}")

def healthy():
    with urllib.request.urlopen(BASE + "/actuator/health", timeout=3) as r:
        assert json.load(r)["status"] == "UP"

attempt("api health", healthy)
device = "ready-" + uuid.uuid4().hex[:12]
item = event(1, device, 1, "healthy")
attempt("ingest", lambda: (request(BASE, KEY, "/api/v1/devices", {"id": device, "vehicle_id": "readiness", "model": "virtual", "firmware_version": "sim-1"}),
                           request(BASE, KEY, "/api/v1/ingest/heartbeat", item)))
meta = {"event_id": item["event_id"], "mime_type": "image/png", "byte_size": len(IMAGE),
        "sha256": hashlib.sha256(IMAGE).hexdigest(), "origin": "SYNTHETIC", "idempotency_key": str(uuid.uuid4())}

def evidence_roundtrip():
    ticket = request(BASE, KEY, "/api/v1/evidence/uploads", meta)
    if ticket["status"] != "AVAILABLE":
        with urllib.request.urlopen(urllib.request.Request(ticket["upload_url"], IMAGE, method="PUT"), timeout=10): pass
        assert request(BASE, KEY, f"/api/v1/evidence/{ticket['id']}/confirm", {})["status"] == "AVAILABLE"

attempt("object storage", evidence_roundtrip)
print("READY: API, Kafka-independent ingest and S3 evidence round trip")
