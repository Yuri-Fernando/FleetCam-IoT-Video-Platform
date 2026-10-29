"""Cross-tenant authorization test for local lab keys."""
import hashlib
import base64
import json
import urllib.error
import urllib.request
import uuid
from fleet import event, request
base='http://127.0.0.1:8080'; a='local-demo-key'; b='local-tenant-b-key'; token=uuid.uuid4().hex[:12]
da='tenant-a-'+token; db='tenant-b-'+token
for key,device in ((a,da),(b,db)):
    request(base,key,'/api/v1/devices',{'id':device,'vehicle_id':'tenant-test','model':'virtual','firmware_version':'sim-1'})
ea=event(100,da,1,'healthy'); eb=event(101,db,1,'healthy'); eb['tenant_id']='tenant-b'
assert request(base,a,'/api/v1/ingest/heartbeat',ea)['status']=='accepted'
assert request(base,b,'/api/v1/ingest/heartbeat',eb)['status']=='accepted'
def get(path,key):
    with urllib.request.urlopen(urllib.request.Request(base+path,headers={'X-API-Key':key}),timeout=15) as r: return json.load(r)
def denied(method,path,key,body=None,code=404):
    req=urllib.request.Request(base+path,body,{'X-API-Key':key,'Content-Type':'application/json'},method=method)
    try: urllib.request.urlopen(req,timeout=15)
    except urllib.error.HTTPError as exc:
        assert exc.code==code,(path,exc.code)
        return
    raise AssertionError('cross-tenant request accepted: '+path)
assert da in [row['id'] for row in get('/api/v1/devices',a)]
assert da not in [row['id'] for row in get('/api/v1/devices',b)]
assert db in [row['id'] for row in get('/api/v1/devices',b)]
denied('GET','/api/v1/devices/'+da,b)
denied('GET','/api/v1/devices/'+db,a)
assert get('/api/v1/devices/'+da+'/timeline',b)==[]
denied('POST','/api/v1/ingest/heartbeat',b,json.dumps(ea).encode(),403)
denied('GET','/api/v1/lab/dlt',b,code=403)
denied('GET','/api/v1/devices','invalid-key',code=401)
image=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=')
ticket=request(base,a,'/api/v1/evidence/uploads',{'event_id':ea['event_id'],'mime_type':'image/png','byte_size':len(image),'sha256':hashlib.sha256(image).hexdigest(),'origin':'SYNTHETIC'})
with urllib.request.urlopen(urllib.request.Request(ticket['upload_url'],image,method='PUT'),timeout=30): pass
assert request(base,a,'/api/v1/evidence/'+ticket['id']+'/confirm',{})['status']=='AVAILABLE'
denied('POST','/api/v1/evidence/uploads',b,json.dumps({'event_id':ea['event_id'],'mime_type':'image/png','byte_size':len(image),'sha256':hashlib.sha256(image).hexdigest(),'origin':'SYNTHETIC'}).encode(),404)
denied('GET','/api/v1/evidence/'+ticket['id']+'/access',b)
assert ticket['id'] not in [str(x['id']) for x in get('/api/v1/evidence',b)]
print('PASS: tenant-bound keys isolate devices, events, evidence and lab administration')
