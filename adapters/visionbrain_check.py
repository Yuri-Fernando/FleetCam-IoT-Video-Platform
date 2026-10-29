"""Exercise real VisionBrain quality rules on a synthetic frame and the adapter."""
import importlib.util
import json
import os
import pathlib
import sys
import tempfile
import urllib.request
import uuid
sys.path = [p for p in sys.path if pathlib.Path(p or '.').resolve() != pathlib.Path(__file__).parent.resolve()]
import numpy as np
from visionbrain.quality.metrics import analyze_frame_quality
from visionbrain.events.engine import EventEngine
from visionbrain.config import EventConfig
from visionbrain.events.sinks import DiskEventSink

device = 'vision-' + uuid.uuid4().hex[:12]
key = 'local-demo-key'
base = 'http://127.0.0.1:8080'
req = urllib.request.Request(base+'/api/v1/devices',json.dumps({'id':device,'vehicle_id':'vision-test','model':'virtual','firmware_version':'sim-1'}).encode(),{'Content-Type':'application/json','X-API-Key':key},method='POST')
with urllib.request.urlopen(req,timeout=15): pass
frame = np.zeros((64,64,3),dtype=np.uint8)
quality = analyze_frame_quality(frame)
events = EventEngine(EventConfig()).evaluate(frame_index=1,frame_shape=frame.shape,detections=[],quality=quality)
vision_event = next(e for e in events if e.event_type == 'capture_quality_degraded')
with tempfile.TemporaryDirectory() as folder:
    root=pathlib.Path(folder)
    sink=DiskEventSink(EventConfig(jsonl_path=str(root/'events.jsonl'),snapshots_dir=str(root/'snapshots')))
    assert sink.emit(vision_event,frame)
    native=vision_event.to_dict()
    os.environ.update(FLEETCAM_DEVICE=device,FLEETCAM_API_KEY=key,FLEETCAM_ADAPTER_DB=str(root/'spool.sqlite3'),FLEETCAM_IMAGE_ORIGIN='SYNTHETIC',FLEETCAM_SNAPSHOT_ROOT=str(root/'snapshots'))
    spec=importlib.util.spec_from_file_location('visionbrain_adapter',pathlib.Path(__file__).with_name('visionbrain.py'))
    adapter=importlib.util.module_from_spec(spec); spec.loader.exec_module(adapter)
    adapter.setup(); digest=adapter.enqueue(native); adapter.replay(); adapter.replay_snapshots()
    # Simulate interruption after server confirmation but before local acknowledgement.
    with adapter.db() as conn: conn.execute("UPDATE queue SET snapshot_delivered=0")
    adapter.replay_snapshots()
    with adapter.db() as conn: assert conn.execute('SELECT delivered,snapshot_delivered FROM queue').fetchone() == (1,1)
    req=urllib.request.Request(base+'/api/v1/devices/'+device+'/timeline',headers={'X-API-Key':key})
    with urllib.request.urlopen(req,timeout=15) as response: rows=json.load(response)
    payload=json.loads(rows[0]['payload'])
    assert payload['event_type']=='capture_quality_degraded' and payload['source']=='VISIONBRAIN_ANALYSIS'
    assert payload['payload']['image_origin']=='SYNTHETIC' and payload['payload']['native_event']==native
    event_id=str(uuid.uuid5(uuid.NAMESPACE_URL,'fleetcam-visionbrain:'+digest))
    req=urllib.request.Request(base+'/api/v1/evidence',headers={'X-API-Key':key})
    with urllib.request.urlopen(req,timeout=15) as response: evidence=json.load(response)
    matches=[x for x in evidence if str(x['event_id'])==event_id and x['status']=='AVAILABLE']
    assert len(matches)==1
    req=urllib.request.Request(base+'/api/v1/evidence/'+str(matches[0]['id'])+'/access',headers={'X-API-Key':key})
    with urllib.request.urlopen(req,timeout=15) as response: url=json.load(response)['url']
    with urllib.request.urlopen(url,timeout=15) as response: assert response.read()==pathlib.Path(native['payload']['snapshot']).read_bytes()
print('PASS: real VisionBrain analysis, native snapshot, adapter and idempotent evidence')
