"""Lifecycle transitions must suppress offline alerts and prevent new ingestion."""
import json
import time
import urllib.error
import urllib.request
import uuid
from fleet import event, request
base='http://127.0.0.1:8080'; key='local-demo-key'; device='lifecycle-'+uuid.uuid4().hex[:12]
request(base,key,'/api/v1/devices',{'id':device,'vehicle_id':'test','model':'virtual','firmware_version':'sim-1'})
request(base,key,'/api/v1/ingest/heartbeat',event(21,device,1,'healthy'))
assert request(base,key,'/api/v1/devices/'+device+'/lifecycle',{'state':'MAINTENANCE'})['status']=='MAINTENANCE'
def get(path):
    with urllib.request.urlopen(urllib.request.Request(base+path,headers={'X-API-Key':key}),timeout=10) as r: return json.load(r)
assert get('/api/v1/devices/'+device)['connectivity_state']=='MAINTENANCE'
assert request(base,key,'/api/v1/devices/'+device+'/lifecycle',{'state':'DECOMMISSIONED'})['status']=='DECOMMISSIONED'
assert get('/api/v1/devices/'+device)['connectivity_state']=='DECOMMISSIONED'
try: request(base,key,'/api/v1/ingest/heartbeat',event(21,device,2,'healthy'))
except urllib.error.HTTPError as exc: assert exc.code==409
else: raise AssertionError('decommissioned device accepted a heartbeat')
time.sleep(12)
assert not any(i['device_id']==device and i['type']=='CONNECTIVITY' and i['status']=='OPEN' for i in get('/api/v1/incidents'))
print('PASS: maintenance and decommission suppress connectivity alerts and reject later ingest')
