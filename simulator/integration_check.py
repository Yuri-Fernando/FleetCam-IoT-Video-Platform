"""Opt-in smoke test against an already running local Compose stack."""
import json
import time
import urllib.error
import urllib.request
import uuid
from fleet import event, request

base = "http://127.0.0.1:8080"
key = "local-demo-key"
device = "check-" + uuid.uuid4().hex[:12]
request(base, key, "/api/v1/devices", {"id": device, "vehicle_id": "check", "model": "virtual", "firmware_version": "sim-1"})
events = [event(42, device, n, "sd_failure") for n in (1, 3, 2)]
for item in events:
    assert request(base, key, "/api/v1/ingest/heartbeat", item)["status"] == "accepted"
assert request(base, key, "/api/v1/ingest/heartbeat", events[1])["status"] == "duplicate"
conflict = dict(events[1], source="DIFFERENT")
try:
    request(base, key, "/api/v1/ingest/heartbeat", conflict)
    raise AssertionError("expected conflict")
except urllib.error.HTTPError as exc:
    assert exc.code == 409, exc.code
for _ in range(60):
    req = urllib.request.Request(base + "/api/v1/devices", headers={"X-API-Key": key})
    with urllib.request.urlopen(req, timeout=10) as response:
        row = next(x for x in json.load(response) if x["id"] == device)
    if row["sequence_number"] == 3:
        assert row["storage_state"] == "HEALTHY", row  # One observed increment is not a sustained failure.
        print("PASS: accepted, duplicate, conflicting identity and delayed heartbeat")
        break
    time.sleep(1)
else:
    raise AssertionError("projector did not catch up")
