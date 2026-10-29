import json
import time
import urllib.request
import uuid
from fleet import event, request

base='http://127.0.0.1:8080'; key='local-demo-key'; device='incident-'+uuid.uuid4().hex[:12]
request(base,key,'/api/v1/devices',{'id':device,'vehicle_id':'incident-test','model':'virtual','firmware_version':'sim-1'})
for seq in (1,2,3): request(base,key,'/api/v1/ingest/heartbeat',event(77,device,seq,'sd_failure'))
def read():
    req=urllib.request.Request(base+'/api/v1/incidents',headers={'X-API-Key':key})
    with urllib.request.urlopen(req,timeout=10) as r: return json.load(r)
deadline=time.monotonic()+90
while time.monotonic()<deadline:
    found=[x for x in read() if x['device_id']==device and x['type']=='STORAGE' and x['status']=='OPEN']
    if found: break
    time.sleep(2)
else: raise AssertionError('storage incident was not opened')
for seq in (4,5,6):
    recovery=event(77,device,seq,'healthy')
    request(base,key,'/api/v1/ingest/heartbeat',recovery)
deadline=time.monotonic()+90
while time.monotonic()<deadline:
    if any(x['id']==found[0]['id'] and x['status']=='CLOSED' for x in read()):
        print('PASS: storage incident opens and closes after recovery'); break
    time.sleep(2)
else: raise AssertionError('storage incident was not closed')
