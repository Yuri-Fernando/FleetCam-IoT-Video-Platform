"""Verify that fetching one device is not restricted to the first page."""
import json
import subprocess
import urllib.request
import uuid
prefix="zz-test-"+uuid.uuid4().hex[:12]+"-"
def sql(q):
    return subprocess.run(["docker","compose","exec","-T","postgres","psql","-v","ON_ERROR_STOP=1","-U","fleetcam","-d","fleetcam","-At","-c",q],check=True,capture_output=True,text=True).stdout.strip()
try:
    sql(f"INSERT INTO devices(tenant_id,id,vehicle_id,model,firmware_version) SELECT 'tenant-demo','{prefix}' || lpad(i::text,4,'0'),'fixture','virtual','sim-1' FROM generate_series(1,501) i")
    target=prefix+"0501"
    req=urllib.request.Request("http://127.0.0.1:8080/api/v1/devices/"+target,headers={"X-API-Key":"local-demo-key"})
    with urllib.request.urlopen(req,timeout=20) as response: assert json.load(response)["id"]==target
    print("PASS: device lookup beyond the first 500 rows")
finally:
    sql(f"DELETE FROM devices WHERE tenant_id='tenant-demo' AND id LIKE '{prefix}%'")
