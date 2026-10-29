"""Verify one trace links HTTP ingestion, the outbox and Kafka processing."""
import json
import time
import urllib.request
import uuid
from fleet import event, request

base="http://127.0.0.1:8080"; key="local-demo-key"; device="trace-"+uuid.uuid4().hex[:12]
request(base,key,"/api/v1/devices",{"id":device,"vehicle_id":"trace-test","model":"virtual","firmware_version":"sim-1"})
item=event(177,device,1,"healthy"); trace=uuid.uuid4().hex; parent=uuid.uuid4().hex[:16]
req=urllib.request.Request(base+"/api/v1/ingest/heartbeat",json.dumps(item).encode(),{"Content-Type":"application/json","X-API-Key":key,"traceparent":f"00-{trace}-{parent}-01"},method="POST")
with urllib.request.urlopen(req,timeout=15) as response: assert response.status==202
end=time.monotonic()+120
while time.monotonic()<end:
    try:
        with urllib.request.urlopen("http://localhost:16686/api/traces/"+trace,timeout=5) as response: result=json.load(response)
        if result.get("data"):
            spans=result["data"][0]["spans"]
            names=[span["operationName"] for span in spans]
            if len(spans)>=2 and any("fleetcam.ingestion" in name for name in names):
                req=urllib.request.Request(base+"/api/v1/devices/"+device+"/timeline",headers={"X-API-Key":key})
                with urllib.request.urlopen(req,timeout=10) as response: timeline=json.load(response)
                assert timeline[0]["traceparent"].split("-")[1] == trace
                print("PASS: same trace in HTTP, outbox, Kafka and Jaeger",names)
                break
    except (OSError,ValueError,KeyError): pass
    time.sleep(2)
else: raise AssertionError("trace with both HTTP and Kafka spans not found")
