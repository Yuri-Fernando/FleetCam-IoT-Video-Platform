"""Fault-inject one device projection and verify audited redrive after repair."""
import json
import subprocess
import time
import urllib.request
import uuid
from fleet import event, request

base='http://127.0.0.1:8080'; key='local-demo-key'; token=uuid.uuid4().hex[:12]
device='redrive-'+token; trigger='test_projection_'+token; func='test_projection_fn_'+token
def sql(q):
    return subprocess.run(['docker','compose','exec','-T','postgres','psql','-v','ON_ERROR_STOP=1','-U','fleetcam','-d','fleetcam','-At','-c',q],check=True,text=True,capture_output=True).stdout.strip()
def get(path):
    req=urllib.request.Request(base+path,headers={'X-API-Key':key})
    with urllib.request.urlopen(req,timeout=10) as r: return json.load(r)
request(base,key,'/api/v1/devices',{'id':device,'vehicle_id':'redrive-test','model':'virtual','firmware_version':'sim-1'})
item=event(123,device,1,'healthy'); eid=item['event_id']
sql(f"CREATE FUNCTION {func}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.device_id = '{device}' THEN RAISE EXCEPTION 'test fault'; END IF; RETURN NEW; END $$; CREATE TRIGGER {trigger} BEFORE INSERT OR UPDATE ON device_state FOR EACH ROW EXECUTE FUNCTION {func}();")
try:
    request(base,key,'/api/v1/ingest/heartbeat',item)
    deadline=time.monotonic()+120
    while time.monotonic()<deadline:
        ids=sql(f"select id from dead_letters where payload like '%{eid}%' and status='OPEN' limit 1")
        if ids: break
        time.sleep(2)
    else: raise AssertionError('accepted event did not reach DLT')
finally:
    sql(f'DROP TRIGGER IF EXISTS {trigger} ON device_state; DROP FUNCTION IF EXISTS {func}()')
request(base,key,f'/api/v1/lab/dlt/{ids}/redrive',{})
deadline=time.monotonic()+120
while time.monotonic()<deadline:
    if sql(f"select count(*) from ingest_events where event_id='{eid}' and processed_at is not null")=='1': break
    time.sleep(2)
else: raise AssertionError('redrive did not process')
assert sql(f"select count(*) from audit_log where action='DLT_REDRIVE' and resource_id='{ids}'")=='1'
assert sql(f"select count(*) from device_state where device_id='{device}'")=='1'
print('PASS: accepted event reaches DLT, repair and audited redrive')
