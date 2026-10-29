"""Integration check: interrupted evidence uploads are detected, reconciled to FAILED and retryable."""
import base64
import hashlib
import json
import urllib.error
import urllib.request
import uuid
from fleet import event, request

base = "http://127.0.0.1:8080"
key = "local-demo-key"

def get(path):
    with urllib.request.urlopen(urllib.request.Request(base+path,headers={"X-API-Key":key}),timeout=10) as r: return json.load(r)

def status(evidence_id):
    row = next(item for item in get("/api/v1/evidence") if item["id"] == evidence_id)
    return row["status"], row["failure_reason"]

def expect_http(code, fn):
    try:
        fn()
        raise AssertionError(f"expected HTTP {code}")
    except urllib.error.HTTPError as exc:
        assert exc.code == code, exc.code

def reconcile(seconds):
    return request(base,key,f"/api/v1/lab/evidence/reconcile?older_than_seconds={seconds}",{})

device = "reconcile-" + uuid.uuid4().hex[:12]
request(base,key,"/api/v1/devices",{"id":device,"vehicle_id":"reconcile-test","model":"virtual","firmware_version":"sim-1"})
item = event(77,device,1,"healthy")
request(base,key,"/api/v1/ingest/heartbeat",item)
image = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=")
meta = {"event_id":item["event_id"],"mime_type":"image/png","byte_size":len(image),"sha256":hashlib.sha256(image).hexdigest(),"origin":"SYNTHETIC"}

# A fresh ticket is outside a long reconciliation window and stays PENDING.
fresh = request(base,key,"/api/v1/evidence/uploads",dict(meta,idempotency_key=str(uuid.uuid4())))
reconcile(600)
assert status(fresh["id"]) == ("PENDING", None), status(fresh["id"])

# 1) Client never uploads: detected as UPLOAD_MISSING.
missing = request(base,key,"/api/v1/evidence/uploads",dict(meta,idempotency_key=str(uuid.uuid4())))
# 2) Upload interrupted after a partial body and never confirmed: UPLOAD_UNCONFIRMED.
retry_key = str(uuid.uuid4())
partial = request(base,key,"/api/v1/evidence/uploads",dict(meta,idempotency_key=retry_key))
with urllib.request.urlopen(urllib.request.Request(partial["upload_url"],image[:20],method="PUT"),timeout=30): pass
result = reconcile(0)
assert result["upload_missing"] >= 1 and result["upload_unconfirmed"] >= 1, result
assert status(missing["id"]) == ("FAILED", "UPLOAD_MISSING"), status(missing["id"])
assert status(partial["id"]) == ("FAILED", "UPLOAD_UNCONFIRMED"), status(partial["id"])
# The event never points to unconfirmed evidence, and an abandoned upload cannot be confirmed late.
for evidence_id in (missing["id"], partial["id"]):
    expect_http(409, lambda: get(f"/api/v1/evidence/{evidence_id}/access"))
    expect_http(409, lambda: request(base,key,f"/api/v1/evidence/{evidence_id}/confirm",{}))

# 3) Retrying with the same idempotency key issues a new ticket and completes normally.
again = request(base,key,"/api/v1/evidence/uploads",dict(meta,idempotency_key=retry_key))
assert again["id"] == partial["id"] and again["status"] == "PENDING"
assert status(partial["id"]) == ("PENDING", None)
with urllib.request.urlopen(urllib.request.Request(again["upload_url"],image,method="PUT"),timeout=30) as r: assert r.status == 200
assert request(base,key,f"/api/v1/evidence/{partial['id']}/confirm",{})["status"] == "AVAILABLE"
assert status(partial["id"]) == ("AVAILABLE", None)
url = get(f"/api/v1/evidence/{partial['id']}/access")["url"]
with urllib.request.urlopen(url,timeout=30) as r: assert r.read() == image
expect_http(403, lambda: urllib.request.urlopen(urllib.request.Request(base+"/api/v1/lab/evidence/reconcile?older_than_seconds=0",b"{}",
    {"Content-Type":"application/json","X-API-Key":"local-tenant-b-key"},method="POST"),timeout=10))
print("PASS: missing and partial uploads reconciled to FAILED, late confirm refused, retry completed")
