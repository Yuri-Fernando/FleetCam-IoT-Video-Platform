"""Accepted heartbeats replayed after decommission must not project state."""
import subprocess
import time
import uuid
from fleet import event, request

def compose(*args):
    return subprocess.run(["docker", "compose", *args], check=True, capture_output=True, text=True)

def sql(query):
    return compose("exec", "-T", "postgres", "psql", "-U", "fleetcam", "-d", "fleetcam", "-At", "-c", query).stdout.strip()

base, key = "http://127.0.0.1:8080", "local-demo-key"
device = "retired-replay-" + uuid.uuid4().hex[:12]
request(base, key, "/api/v1/devices", {"id": device, "vehicle_id": "test", "model": "virtual", "firmware_version": "sim-1"})
item = event(314, device, 1, "sd_failure")
eid = item["event_id"]
try:
    compose("stop", "kafka")
    assert request(base, key, "/api/v1/ingest/heartbeat", item)["status"] == "accepted"
    assert sql(f"select count(*) from outbox where event_id='{eid}' and delivered_at is null") == "1"
    request(base, key, "/api/v1/devices/" + device + "/lifecycle", {"state": "DECOMMISSIONED"})
finally:
    compose("start", "kafka")
end = time.monotonic() + 120
while time.monotonic() < end:
    if sql(f"select count(*) from ingest_events where event_id='{eid}' and processed_at is not null") == "1":
        break
    time.sleep(2)
else:
    raise AssertionError("replay did not complete")
assert sql(f"select count(*) from device_state where device_id='{device}'") == "0"
assert sql(f"select count(*) from incidents where device_id='{device}' and status='OPEN'") == "0"
print("PASS: delayed heartbeat after decommission is retained without projecting state or incidents")
