"""Opt-in crash test: SIGKILLs the API process (publisher and consumer) mid-backlog, restarts it and
verifies each accepted event is projected exactly once. Stops/starts Kafka and kills the api service."""
import concurrent.futures
import json
import re
import subprocess
import time
import urllib.request
import uuid
from fleet import event, request

BASE = "http://127.0.0.1:8080"
KEY = "local-demo-key"
COUNT = 300

def compose(*args):
    return subprocess.run(["docker", "compose", *args], check=True, capture_output=True, text=True)

def sql(query):
    return compose("exec", "-T", "postgres", "psql", "-U", "fleetcam", "-d", "fleetcam", "-At", "-c", query).stdout.strip()

def eventually(predicate, seconds=180, interval=1.0):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if predicate(): return
        time.sleep(interval)
    raise AssertionError("timed out")

def healthy():
    try:
        with urllib.request.urlopen(BASE + "/actuator/health", timeout=3) as r: return json.load(r)["status"] == "UP"
    except Exception: return False

def metric(name):
    with urllib.request.urlopen(BASE + "/actuator/prometheus", timeout=10) as r: text = r.read().decode()
    found = re.search(rf"^{name}(?:{{[^}}]*}})? ([0-9.eE+-]+|NaN)$", text, re.M)
    return float(found.group(1)) if found else None

device = "crash-" + uuid.uuid4().hex[:12]
request(BASE, KEY, "/api/v1/devices", {"id": device, "vehicle_id": "crash-test", "model": "virtual", "firmware_version": "sim-1"})
processed = lambda: int(sql(f"select count(*) from ingest_events where device_id='{device}' and processed_at is not null"))
# Build a backlog while the broker is down so the restart window is wide enough to kill mid-flight.
try:
    compose("stop", "kafka")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda seq: request(BASE, KEY, "/api/v1/ingest/heartbeat", event(4242, device, seq, "healthy"))["status"], range(1, COUNT + 1)))
    assert results.count("accepted") == COUNT, results
finally:
    compose("start", "kafka")
eventually(lambda: processed() > 0, interval=0.3)
compose("kill", "-s", "SIGKILL", "api")
at_kill = {"processed": processed(),
    "outbox_delivered": int(sql(f"select count(*) from outbox where device_id='{device}' and delivered_at is not null"))}
print("at SIGKILL:", at_kill)
assert at_kill["processed"] < COUNT, "backlog finished before the kill; increase COUNT"
compose("start", "api")
eventually(healthy)
eventually(lambda: processed() == COUNT)
eventually(lambda: sql(f"select count(*) from outbox where device_id='{device}' and delivered_at is null") == "0")
assert sql(f"select count(*) from ingest_events where device_id='{device}'") == str(COUNT)
assert sql(f"select sequence_number from device_state where device_id='{device}'") == str(COUNT)
# A healthy device has exactly one transition (unknown -> healthy) and no incidents despite redelivery.
assert sql(f"select count(*) from state_transitions where device_id='{device}'") == "1"
assert sql(f"select count(*) from incidents where device_id='{device}' and type in ('STORAGE','CAPTURE')") == "0"
eventually(lambda: metric("fleetcam_kafka_consumer_lag") == 0.0, seconds=60)
print("after restart:", {"processed": processed(), "redeliveries_recognized": metric("fleetcam_projector_redeliveries_total"),
    "consumer_lag": metric("fleetcam_kafka_consumer_lag")})
print(f"PASS: SIGKILL mid-backlog, {COUNT} events projected once, lag returned to 0")
