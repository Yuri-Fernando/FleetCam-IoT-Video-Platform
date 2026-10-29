"""Exercise invalid heartbeat fields and valid unknown telemetry against the local API."""
import urllib.error
import uuid
from fleet import event, request
base="http://127.0.0.1:8080"; key="local-demo-key"; device="validation-"+uuid.uuid4().hex[:12]
request(base,key,"/api/v1/devices",{"id":device,"vehicle_id":"validation","model":"virtual","firmware_version":"sim-1"})
original=event(1001,device,1,"healthy")
def invalid(item):
    try: request(base,key,"/api/v1/ingest/heartbeat",item)
    except urllib.error.HTTPError as exc:
        assert exc.code == 400, exc.code
        return
    raise AssertionError("invalid payload was accepted")
invalid(dict(original,schema_version="1"))
invalid(dict(original,payload={"sd":{"present":True,"total_bytes":100,"free_bytes":101,"write_error_count":0}}))
invalid(dict(original,payload={"sd":{"present":True,"write_error_count":-1}}))
invalid(dict(original,payload={"camera":{"capture_operational":"yes"}}))
unknown=dict(original,payload={"sd":None,"camera":None})
assert request(base,key,"/api/v1/ingest/heartbeat",unknown)["status"]=="accepted"
print("PASS: invalid telemetry rejected and missing telemetry accepted as unknown")
