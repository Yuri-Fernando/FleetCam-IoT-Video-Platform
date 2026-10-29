"""Synthetic fleet client; only Python standard library is required."""
import argparse
import datetime as dt
import json
import pathlib
import time
import urllib.error
import urllib.request
import uuid

BASE_TIME = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)

def request(base, key, path, body):
    req = urllib.request.Request(base + path, json.dumps(body).encode(),
        {"Content-Type": "application/json", "X-API-Key": key}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 409 and path == "/api/v1/devices":
            return {"status": "already_provisioned"}
        raise

def event(seed, device, seq, scenario):
    ident = f"{seed}:{device}:{seq}:{scenario}"
    sd = {"present": True, "free_bytes": 800, "total_bytes": 1000, "write_error_count": 0}
    if scenario == "sd_failure" and seq >= 2:
        sd["write_error_count"] = seq
    if scenario == "full_sd" and seq >= 2:
        sd["free_bytes"] = 50
    return {"schema_version": 1, "event_id": str(uuid.uuid5(uuid.NAMESPACE_URL, ident)),
        "tenant_id": "tenant-demo", "device_id": device, "boot_id": f"boot-{seed}",
        "sequence_number": seq, "event_kind": "device_heartbeat", "source": "VIRTUAL_FLEET",
        "simulated": True, "occurred_at": (BASE_TIME + dt.timedelta(seconds=seq * 15)).isoformat().replace("+00:00", "Z"),
        "payload": {"firmware_version": "sim-1.0", "sd": sd, "camera": {"capture_operational": True}}}

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://127.0.0.1:8080")
    p.add_argument("--key", default="local-demo-key")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--devices", type=int, default=20)
    p.add_argument("--scenario", choices=["healthy", "sd_failure", "full_sd", "duplicate_events", "delayed_events", "offline_reconnect", "hard_brake", "reboot_loop"], default="healthy")
    p.add_argument("--offline-seconds", type=float, default=0, help="wait with queued events before reconnecting")
    p.add_argument("--output", type=pathlib.Path, default=pathlib.Path("simulator/output"))
    p.add_argument("--resume", action="store_true", help="replay the existing spool without generating new events")
    args = p.parse_args()
    if not 0 <= args.offline_seconds <= 3600:
        p.error("offline-seconds must be between 0 and 3600")
    if not 1 <= args.devices <= 500:
        p.error("devices must be between 1 and 500")
    args.output.mkdir(parents=True, exist_ok=True)
    spool = args.output / "spool.jsonl"
    if args.resume:
        if not spool.exists():
            p.error("spool not found")
        events = [json.loads(line) for line in spool.read_text(encoding="utf-8").splitlines() if line]
        deliver(args, spool, events)
        return
    if spool.exists() and spool.stat().st_size:
        p.error("spool has unsent events; use --resume or a different --output")
    manifest = {"seed": args.seed, "devices": args.devices, "scenario": args.scenario,
                "offline_seconds": args.offline_seconds if args.scenario == "offline_reconnect" else 0,
                "note": "synthetic timestamps and signals; offline_reconnect queues locally before replay"}
    (args.output / "scenario_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    events = []
    for i in range(args.devices):
        device = f"cam-{args.seed:04d}-{i:03d}"
        request(args.base, args.key, "/api/v1/devices", {"id": device, "vehicle_id": f"vehicle-{i:03d}", "model": "virtual-camera", "firmware_version": "sim-1.0"})
        batch = [event(args.seed, device, j, args.scenario) for j in (1, 2, 3)]
        if args.scenario == "reboot_loop":
            for boot_index, message in enumerate(batch, 1):
                message["boot_id"] = f"boot-{args.seed}-{boot_index}"
                message["sequence_number"] = 1
                message["payload"]["firmware_version"] = "sim-1.2-regression"
        if args.scenario == "hard_brake":
            safety = dict(batch[-1], event_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{args.seed}:{device}:hard-brake")), event_kind="safety_event", event_type="hard_brake_simulated", source="SCENARIO_ENGINE", payload={"severity":"warning","deceleration_mps2":-4.0,"evidence_upload_status":"PENDING"})
            batch.append(safety)
        if args.scenario == "delayed_events": batch = [batch[0], batch[2], batch[1]]
        if args.scenario == "duplicate_events": batch.append(batch[-1])
        events.extend(batch)
    if args.scenario == "offline_reconnect":
        # A first heartbeat establishes that each camera was seen before it goes offline.
        events = events[::3] + [e for i, e in enumerate(events) if i % 3 != 0]
    spool.write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")
    ground_truth = [{"event_id": e["event_id"], "device_id": e["device_id"], "event_kind": e["event_kind"],
                     "sequence_number": e["sequence_number"], "simulated": True} for e in events]
    (args.output / "ground_truth.jsonl").write_text("".join(json.dumps(e) + "\n" for e in ground_truth), encoding="utf-8")
    expected = {"storage_state": {"sd_failure": "WRITE_ERROR", "full_sd": "LOW_CAPACITY"}.get(args.scenario, "HEALTHY"), "unique_events_per_device": 4 if args.scenario == "hard_brake" else 3}
    (args.output / "expected_results.json").write_text(json.dumps(expected, indent=2), encoding="utf-8")
    if args.scenario == "offline_reconnect":
        deliver(args, spool, events, limit=args.devices)
        if args.offline_seconds:
            print(f"Offline simulation: {args.offline_seconds} seconds; events remain in local spool.", flush=True)
            time.sleep(args.offline_seconds)
    deliver(args, spool, events)

def deliver(args, spool, events, limit=None):
    # Rewriting after each acknowledged event preserves unsent items if the process stops.
    sent = 0
    while events and (limit is None or sent < limit):
        result = request(args.base, args.key, "/api/v1/ingest/heartbeat", events[0])
        print(events[0]["event_id"], result["status"])
        events.pop(0)
        sent += 1
        temp = spool.with_suffix(".tmp")
        temp.write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")
        temp.replace(spool)

if __name__ == "__main__":
    main()
