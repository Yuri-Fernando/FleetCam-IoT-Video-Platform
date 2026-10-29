"""Test offline and two-heartbeat recovery using a lab-only server-time fixture."""
import json
import subprocess
import time
import urllib.request
import uuid
from fleet import event, request

base="http://127.0.0.1:8080"; key="local-demo-key"; device="connectivity-"+uuid.uuid4().hex[:12]
def sql(q):
    return subprocess.run(["docker","compose","exec","-T","postgres","psql","-v","ON_ERROR_STOP=1","-U","fleetcam","-d","fleetcam","-At","-c",q],check=True,capture_output=True,text=True).stdout.strip()
def get(path):
    req=urllib.request.Request(base+path,headers={"X-API-Key":key})
    with urllib.request.urlopen(req,timeout=10) as response: return json.load(response)
def wait_for(check, timeout=60):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        if check(): return
        time.sleep(2)
    raise AssertionError("timed out waiting for expected connectivity state")
request(base,key,"/api/v1/devices",{"id":device,"vehicle_id":"connectivity-test","model":"virtual","firmware_version":"sim-1"})
request(base,key,"/api/v1/ingest/heartbeat",event(900,device,1,"healthy"))
assert get("/api/v1/devices/"+device)["connectivity_state"]=="RECOVERING"
sql(f"UPDATE ingest_events SET received_at=now()-interval '90 seconds' WHERE device_id='{device}'")
wait_for(lambda: any(x["device_id"]==device and x["type"]=="CONNECTIVITY" and x["status"]=="OPEN" for x in get("/api/v1/incidents")))
assert get("/api/v1/devices/"+device)["connectivity_state"]=="OFFLINE"
request(base,key,"/api/v1/ingest/heartbeat",event(900,device,2,"healthy"))
assert get("/api/v1/devices/"+device)["connectivity_state"]=="RECOVERING"
request(base,key,"/api/v1/ingest/heartbeat",event(900,device,3,"healthy"))
wait_for(lambda: get("/api/v1/devices/"+device)["connectivity_state"]=="ONLINE")
assert any(x["device_id"]==device and x["type"]=="CONNECTIVITY" and x["status"]=="CLOSED" for x in get("/api/v1/incidents"))
print("PASS: offline incident and recovery after two accepted heartbeats")
