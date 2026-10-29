"""Opt-in fault test for a local Compose stack. Stops and restarts its Kafka service."""
import json
import subprocess
import time
import urllib.request
import uuid
from fleet import event, request

BASE = "http://127.0.0.1:8080"
KEY = "local-demo-key"

def compose(*args):
    return subprocess.run(["docker", "compose", *args], check=True, capture_output=True, text=True)

def sql(query):
    return compose("exec", "-T", "postgres", "psql", "-U", "fleetcam", "-d", "fleetcam", "-At", "-c", query).stdout.strip()

def eventually(predicate, seconds=120):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate(): return
        time.sleep(2)
    raise AssertionError("timed out waiting for recovery")

device = "fault-" + uuid.uuid4().hex[:12]
request(BASE, KEY, "/api/v1/devices", {"id": device, "vehicle_id": "fault-test", "model": "virtual", "firmware_version": "sim-1"})
item = event(314, device, 1, "healthy")
eid = item["event_id"]
try:
    compose("stop", "kafka")
    assert request(BASE, KEY, "/api/v1/ingest/heartbeat", item)["status"] == "accepted"
    assert sql(f"select count(*) from outbox where event_id='{eid}' and delivered_at is null") == "1"
finally:
    compose("start", "kafka")
eventually(lambda: sql(f"select count(*) from ingest_events where event_id='{eid}' and processed_at is not null") == "1")
eventually(lambda: sql(f"select count(*) from outbox where event_id='{eid}' and delivered_at is not null") == "1")
# Re-publish a previously processed record: emulates a publisher crash after broker
# acknowledgement and before the outbox delivery marker commits.
sql(f"update outbox set delivered_at=null where event_id='{eid}'")
eventually(lambda: sql(f"select count(*) from outbox where event_id='{eid}' and delivered_at is not null") == "1")
assert sql(f"select count(*) from ingest_events where event_id='{eid}'") == "1"
assert sql(f"select count(*) from device_state where device_id='{device}' and sequence_number=1") == "1"
print("PASS: Kafka outage retains accepted event, recovery and duplicate publication")
