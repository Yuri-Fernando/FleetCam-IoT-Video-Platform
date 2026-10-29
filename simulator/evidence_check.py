"""Integration check: real MinIO upload, hash validation and private signed download."""
import base64
import hashlib
import json
import urllib.error
import urllib.request
import uuid
from fleet import event, request

base = "http://127.0.0.1:8080"
key = "local-demo-key"
device = "evidence-" + uuid.uuid4().hex[:12]
request(base,key,"/api/v1/devices",{"id":device,"vehicle_id":"evidence-test","model":"virtual","firmware_version":"sim-1"})
item = event(99,device,1,"healthy")
request(base,key,"/api/v1/ingest/heartbeat",item)
image = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")
digest = hashlib.sha256(image).hexdigest()
meta = {"event_id":item["event_id"],"mime_type":"image/png","byte_size":len(image),"sha256":digest,"origin":"SYNTHETIC"}
ticket = request(base,key,"/api/v1/evidence/uploads",meta)
# Confirmation before an upload is rejected and can be retried after recovery.
try:
    request(base,key,f"/api/v1/evidence/{ticket['id']}/confirm",{})
    raise AssertionError("missing upload unexpectedly confirmed")
except urllib.error.HTTPError as exc:
    assert exc.code == 503, exc.code
try:
    urllib.request.urlopen(urllib.request.Request(base+f"/api/v1/evidence/{ticket['id']}/access",headers={"X-API-Key":key}),timeout=10)
    raise AssertionError("pending evidence unexpectedly accessible")
except urllib.error.HTTPError as exc:
    assert exc.code == 409, exc.code
req = urllib.request.Request(ticket["upload_url"],image,method="PUT")
with urllib.request.urlopen(req,timeout=30) as response: assert response.status == 200
assert request(base,key,f"/api/v1/evidence/{ticket['id']}/confirm",{})["status"] == "AVAILABLE"
req = urllib.request.Request(base+f"/api/v1/evidence/{ticket['id']}/access",headers={"X-API-Key":key})
with urllib.request.urlopen(req,timeout=30) as response: url=json.load(response)["url"]
with urllib.request.urlopen(url,timeout=30) as response: assert response.read() == image
# The still-valid staging URL cannot modify the verified object.
with urllib.request.urlopen(urllib.request.Request(ticket["upload_url"],b"x"*len(image),method="PUT"),timeout=30): pass
with urllib.request.urlopen(url,timeout=30) as response: assert response.read() == image
# A mismatching image must never become available.
bad = dict(meta, sha256="0" * 64)
bad_ticket = request(base,key,"/api/v1/evidence/uploads",bad)
with urllib.request.urlopen(urllib.request.Request(bad_ticket["upload_url"],image,method="PUT"),timeout=30): pass
try:
    request(base,key,f"/api/v1/evidence/{bad_ticket['id']}/confirm",{})
    raise AssertionError("invalid hash unexpectedly accepted")
except urllib.error.HTTPError as exc:
    assert exc.code == 422, exc.code
try:
    urllib.request.urlopen(urllib.request.Request(base+f"/api/v1/evidence/{bad_ticket['id']}/access",headers={"X-API-Key":key}),timeout=10)
    raise AssertionError("failed evidence unexpectedly accessible")
except urllib.error.HTTPError as exc:
    assert exc.code == 409, exc.code
# An idempotency key cannot silently refer to different evidence metadata.
once = dict(meta, idempotency_key=str(uuid.uuid4()))
first = request(base,key,"/api/v1/evidence/uploads",once)
assert request(base,key,"/api/v1/evidence/uploads",once)["id"] == first["id"]
try:
    request(base,key,"/api/v1/evidence/uploads",dict(once,sha256="0"*64))
    raise AssertionError("idempotency conflict unexpectedly accepted")
except urllib.error.HTTPError as exc:
    assert exc.code == 409, exc.code
print("PASS: signed upload, integrity, immutable verified copy, failed hash and identity conflict")
