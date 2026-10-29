"""Check that a retired boot cannot overwrite a newer boot after replay."""
import json
import time
import urllib.request
import uuid
from fleet import event, request
base='http://127.0.0.1:8080'; key='local-demo-key'; device='reboot-'+uuid.uuid4().hex[:12]
request(base,key,'/api/v1/devices',{'id':device,'vehicle_id':'reboot-test','model':'virtual','firmware_version':'sim-1'})
a=event(400,device,1,'healthy')
b=event(400,device,2,'healthy'); b['boot_id']='boot-new'; b['sequence_number']=1
c=event(400,device,3,'sd_failure'); c['boot_id']=a['boot_id']; c['sequence_number']=2
for item in (a,b,c): request(base,key,'/api/v1/ingest/heartbeat',item)
end=time.monotonic()+90
while time.monotonic()<end:
    req=urllib.request.Request(base+'/api/v1/devices/'+device+'/timeline',headers={'X-API-Key':key})
    with urllib.request.urlopen(req,timeout=10) as response: timeline=json.load(response)
    if len(timeline)==3 and all(row['processed_at'] for row in timeline): break
    time.sleep(2)
else: raise AssertionError('events not processed')
req=urllib.request.Request(base+'/api/v1/devices/'+device,headers={'X-API-Key':key})
with urllib.request.urlopen(req,timeout=10) as response: state=json.load(response)
assert state['boot_id']=='boot-new' and state['sequence_number']==1 and state['storage_state']=='HEALTHY',state
print('PASS: retired boot replay recorded without regressing current state')
