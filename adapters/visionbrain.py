"""Local webhook bridge for native VisionBrain VisionEvent dictionaries.

Run only on a trusted machine. The webhook listener intentionally binds loopback.
Set FLEETCAM_API_KEY, FLEETCAM_DEVICE and optionally FLEETCAM_URL in the environment.
"""
import datetime as dt
import argparse
from contextlib import contextmanager
import hashlib
import importlib.util
import json
import os
import pathlib
import sqlite3
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer

DB = pathlib.Path(os.getenv("FLEETCAM_ADAPTER_DB", "visionbrain-spool.sqlite3"))
DEVICE = os.environ.get("FLEETCAM_DEVICE", "")
API_KEY = os.environ.get("FLEETCAM_API_KEY", "")
BASE = os.environ.get("FLEETCAM_URL", "http://127.0.0.1:8080")
IMAGE_ORIGIN = os.environ.get("FLEETCAM_IMAGE_ORIGIN", "UNKNOWN")
SNAPSHOT_ROOT = os.environ.get("FLEETCAM_SNAPSHOT_ROOT", "")

@contextmanager
def db():
    conn = sqlite3.connect(DB, timeout=10)
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        with conn:
            yield conn
    finally:
        conn.close()

def setup():
    with db() as c:
        c.execute("CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
        c.execute("CREATE TABLE IF NOT EXISTS queue (seq INTEGER PRIMARY KEY AUTOINCREMENT, digest TEXT UNIQUE NOT NULL, payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0)")
        if "snapshot_delivered" not in {row[1] for row in c.execute("PRAGMA table_info(queue)")}:
            c.execute("ALTER TABLE queue ADD COLUMN snapshot_delivered INTEGER NOT NULL DEFAULT 0")
        c.execute("INSERT OR IGNORE INTO settings VALUES ('boot_id', ?)", (str(uuid.uuid4()),))
        c.execute("INSERT OR IGNORE INTO settings VALUES ('device_id', ?)", (DEVICE,))
        c.execute("INSERT OR IGNORE INTO settings VALUES ('image_origin', ?)", (IMAGE_ORIGIN,))
        if c.execute("SELECT v FROM settings WHERE k='device_id'").fetchone()[0] != DEVICE:
            raise ValueError("Spool is bound to a different device; use a separate database")
        if c.execute("SELECT v FROM settings WHERE k='image_origin'").fetchone()[0] != IMAGE_ORIGIN:
            raise ValueError("Spool is bound to a different image origin; use a separate database")

def enqueue(native):
    if not isinstance(native, dict) or not isinstance(native.get("event_type"), str) or not native["event_type"].strip() or not isinstance(native.get("frame_index"), int) or not isinstance(native.get("payload"), dict):
        raise ValueError("invalid VisionEvent")
    timestamp = dt.datetime.fromisoformat(native["timestamp"].replace("Z", "+00:00"))
    if timestamp.tzinfo is None: raise ValueError("timestamp must include timezone")
    canonical = json.dumps(native,sort_keys=True,separators=(",", ":"),ensure_ascii=False)
    digest = hashlib.sha256((DEVICE + "\n" + canonical).encode()).hexdigest()
    with db() as c:
        if c.execute("SELECT count(*) FROM queue WHERE delivered=0").fetchone()[0] >= 10000:
            raise OverflowError("spool full")
        c.execute("INSERT OR IGNORE INTO queue(digest,payload) VALUES (?,?)",(digest,canonical))
    return digest

def replay():
    with db() as c:
        boot = c.execute("SELECT v FROM settings WHERE k='boot_id'").fetchone()[0]
        rows = c.execute("SELECT seq,digest,payload FROM queue WHERE delivered=0 ORDER BY seq LIMIT 100").fetchall()
    for seq,digest,payload in rows:
        native = json.loads(payload)
        envelope = {"schema_version":1,"event_id":str(uuid.uuid5(uuid.NAMESPACE_URL,"fleetcam-visionbrain:"+digest)),
            "tenant_id":"tenant-demo","device_id":DEVICE,"boot_id":boot,"sequence_number":seq,
            "event_kind":"technical_event","event_type":native["event_type"],"source":"VISIONBRAIN_ANALYSIS",
            "simulated":False,"occurred_at":native["timestamp"],
            "payload":{"native_event":native,"image_origin":IMAGE_ORIGIN,"inference_validated_for_road_safety":False}}
        req = urllib.request.Request(BASE+"/api/v1/ingest/events",json.dumps(envelope).encode(),
            {"Content-Type":"application/json","X-API-Key":API_KEY},method="POST")
        try:
            with urllib.request.urlopen(req,timeout=10) as response:
                if response.status != 202: return
            with db() as c: c.execute("UPDATE queue SET delivered=1 WHERE seq=?",(seq,))
        except (OSError, urllib.error.URLError):
            return

def replay_snapshots():
    """Upload native snapshot paths only when an explicit trusted root is configured."""
    if not SNAPSHOT_ROOT:
        return
    try:
        root = pathlib.Path(SNAPSHOT_ROOT).resolve(strict=True)
    except OSError:
        return
    spec = importlib.util.spec_from_file_location("fleetcam_snapshot_upload", pathlib.Path(__file__).with_name("upload_snapshot.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with db() as c:
        rows = c.execute("SELECT seq,digest,payload FROM queue WHERE delivered=1 AND snapshot_delivered=0 ORDER BY seq LIMIT 100").fetchall()
    for seq, digest, payload in rows:
        snapshot = json.loads(payload).get("payload", {}).get("snapshot")
        if not snapshot:
            with db() as c: c.execute("UPDATE queue SET snapshot_delivered=1 WHERE seq=?", (seq,))
            continue
        try:
            path = pathlib.Path(snapshot).resolve(strict=True)
            path.relative_to(root)
            event_id = uuid.uuid5(uuid.NAMESPACE_URL, "fleetcam-visionbrain:" + digest)
            origin = "SYNTHETIC" if IMAGE_ORIGIN == "SYNTHETIC" else "VISIONBRAIN_ANALYSIS"
            result = module.upload(event_id, path, origin, BASE, API_KEY, idempotency_key=event_id)
            if result.get("status") == "AVAILABLE":
                with db() as c: c.execute("UPDATE queue SET snapshot_delivered=1 WHERE seq=?", (seq,))
        except (OSError, ValueError, TypeError, urllib.error.URLError):
            # Preserve the item for retry; local paths outside the trusted root are never read.
            continue

def import_legacy_spool(path):
    """Import pending VisionBrain JSONL records, preserving the original file."""
    count = 0
    with pathlib.Path(path).open(encoding="utf-8") as source:
        for number, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if not isinstance(record, dict) or record.get("delivery_status") != "pending":
                    raise ValueError("expected pending VisionBrain spool record")
                enqueue(record["event"])
            except (ValueError, KeyError, TypeError, OverflowError) as exc:
                raise ValueError(f"invalid legacy spool record at line {number}: {exc}") from exc
            count += 1
    return count

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 1 <= length <= 262144: raise ValueError("invalid size")
            digest = enqueue(json.loads(self.rfile.read(length)))
            body = json.dumps({"status":"queued","digest":digest}).encode()
            self.send_response(202); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
        except (ValueError, KeyError, TypeError, json.JSONDecodeError, OverflowError):
            self.send_error(400,"Invalid event or full spool")

def main():
    parser = argparse.ArgumentParser(description="VisionBrain local webhook adapter")
    parser.add_argument("--import-spool", type=pathlib.Path)
    args = parser.parse_args()
    if not DEVICE or not API_KEY: raise SystemExit("Set FLEETCAM_DEVICE and FLEETCAM_API_KEY")
    setup()
    if args.import_spool:
        count = import_legacy_spool(args.import_spool)
        replay()
        replay_snapshots()
        print(f"Imported {count} records; undelivered events remain in the adapter spool.")
        return
    def worker():
        while True:
            replay(); replay_snapshots(); time.sleep(2)
    threading.Thread(target=worker,daemon=True).start()
    HTTPServer(("127.0.0.1", 8091),Handler).serve_forever()

if __name__ == "__main__": main()
