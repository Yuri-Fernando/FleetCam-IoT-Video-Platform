"""Attach an explicitly selected image to an already accepted FleetCam event."""
import argparse
import hashlib
import json
import os
import pathlib
import urllib.request
import uuid

def call(url, method, body=None, headers=None):
    req = urllib.request.Request(url, body, headers or {}, method=method)
    with urllib.request.urlopen(req, timeout=30) as response:
        data = response.read()
        return json.loads(data) if data else None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--event-id", type=uuid.UUID, required=True)
    parser.add_argument("--file", type=pathlib.Path, required=True)
    parser.add_argument("--origin", choices=["SYNTHETIC", "VISIONBRAIN_ANALYSIS"], required=True)
    parser.add_argument("--base", default=os.environ.get("FLEETCAM_URL", "http://127.0.0.1:8080"))
    args = parser.parse_args()
    key = os.environ.get("FLEETCAM_API_KEY")
    if not key: parser.error("FLEETCAM_API_KEY must be set")
    result = upload(args.event_id, args.file, args.origin, args.base, key)
    print(json.dumps(result))

def upload(event_id, file, origin, base, key, idempotency_key=None):
    with pathlib.Path(file).open("rb") as source:
        data = source.read(10485761)
    if not 0 < len(data) <= 10485760: raise ValueError("image must be between 1 byte and 10 MiB")
    if data.startswith(bytes.fromhex("89504e470d0a1a0a")): mime = "image/png"
    elif data.startswith(bytes.fromhex("ffd8ff")): mime = "image/jpeg"
    else: raise ValueError("only PNG or JPEG images are accepted")
    headers = {"X-API-Key":key,"Content-Type":"application/json"}
    meta = {"event_id":str(event_id),"mime_type":mime,"byte_size":len(data),"sha256":hashlib.sha256(data).hexdigest(),"origin":origin}
    if idempotency_key: meta["idempotency_key"] = str(idempotency_key)
    ticket = call(base+"/api/v1/evidence/uploads", "POST", json.dumps(meta).encode(), headers)
    if ticket["status"] == "AVAILABLE": return ticket
    call(ticket["upload_url"], "PUT", data)
    return call(base+"/api/v1/evidence/"+ticket["id"]+"/confirm", "POST", b"{}", headers)

if __name__ == "__main__": main()
