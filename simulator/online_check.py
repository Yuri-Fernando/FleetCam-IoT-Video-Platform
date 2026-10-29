"""Provision 20 independent simulated devices and verify their observed state."""
import concurrent.futures
import json
import urllib.request
import uuid
from fleet import event, request
base="http://127.0.0.1:8080"; key="local-demo-key"; prefix="online-"+uuid.uuid4().hex[:10]+"-"
def provision_and_send(i):
    device=prefix+str(i).zfill(2)
    request(base,key,"/api/v1/devices",{"id":device,"vehicle_id":"online-test-"+str(i),"model":"virtual","firmware_version":"sim-1"})
    for seq in (1,2): request(base,key,"/api/v1/ingest/heartbeat",event(2500,device,seq,"healthy"))
    return device
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool: devices=list(pool.map(provision_and_send,range(20)))
req=urllib.request.Request(base+"/api/v1/devices",headers={"X-API-Key":key})
with urllib.request.urlopen(req,timeout=20) as response: rows={d['id']:d for d in json.load(response)}
assert all(d in rows and rows[d]['connectivity_state']=='ONLINE' for d in devices), {d:rows.get(d,{}).get('connectivity_state') for d in devices}
print('PASS: 20 synthetic devices observed ONLINE after two heartbeats')
