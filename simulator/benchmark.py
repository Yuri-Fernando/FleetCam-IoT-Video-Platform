"""Measured local burst baseline; not an industrial capacity claim."""
import argparse
import concurrent.futures
import datetime as dt
import json
import math
import pathlib
import platform
import re
import threading
import time
import urllib.request
import uuid
from fleet import event, request

p=argparse.ArgumentParser()
p.add_argument('--count',type=int,default=30)
p.add_argument('--workers',type=int,default=1)
p.add_argument('--base',default='http://127.0.0.1:8080',help='use 127.0.0.1: on Windows localhost tries ::1 first and adds ~2 s per request')
p.add_argument('--output',type=pathlib.Path,default=pathlib.Path('simulator/output/benchmark.json'))
args=p.parse_args()
if not 1<=args.count<=500: p.error('count must be 1..500')
if not 1<=args.workers<=32: p.error('workers must be 1..32')
base=args.base; key='local-demo-key'; device='bench-'+uuid.uuid4().hex[:12]
request(base,key,'/api/v1/devices',{'id':device,'vehicle_id':'benchmark','model':'virtual','firmware_version':'sim-1'})
def send(seq):
    t=time.perf_counter()
    try: request(base,key,'/api/v1/ingest/heartbeat',event(500,device,seq,'healthy')); error=False
    except Exception: error=True
    return (time.perf_counter()-t)*1000,error
lag_samples=[]; sampling=threading.Event()
def sample_lag():
    # Server-side gauge refreshed every 5 s from broker offsets; coarse, but measured rather than inferred.
    while not sampling.is_set():
        try:
            with urllib.request.urlopen(base+'/actuator/prometheus',timeout=5) as r: text=r.read().decode()
            m=re.search(r'^fleetcam_kafka_consumer_lag(?:\{[^}]*\})? ([0-9.eE+-]+)$',text,re.M)
            if m: lag_samples.append(float(m.group(1)))
        except Exception: pass
        sampling.wait(1)
sampler=threading.Thread(target=sample_lag,daemon=True); sampler.start()
start=time.perf_counter()
with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
    outcomes=list(pool.map(send,range(1,args.count+1)))
times=[item[0] for item in outcomes]; errors=sum(item[1] for item in outcomes)
send_seconds=time.perf_counter()-start
deadline=time.monotonic()+120; processed=0; rows=[]
while time.monotonic()<deadline:
    req=urllib.request.Request(base+'/api/v1/devices/'+device+'/timeline',headers={'X-API-Key':key})
    with urllib.request.urlopen(req,timeout=10) as r: rows=json.load(r)
    processed=sum(1 for row in rows if row['processed_at'])
    if processed>=args.count-errors: break
    time.sleep(2)
def ms_between(row):
    return (dt.datetime.fromisoformat(row['processed_at'].replace('Z','+00:00'))-dt.datetime.fromisoformat(row['received_at'].replace('Z','+00:00'))).total_seconds()*1000
sampling.set(); sampler.join(timeout=10)
end_to_end=[ms_between(row) for row in rows if row['processed_at']]
def percentiles(values):
    if not values: return None
    values=sorted(values)
    return {f'p{int(q*100)}':values[max(0,math.ceil(q*len(values))-1)] for q in (.5,.95,.99)}
report={'device':device,'count':args.count,'workers':args.workers,'errors':errors,'processed':processed,'send_seconds':send_seconds,'end_to_reconciliation_seconds':time.perf_counter()-start,'http_latency_ms':percentiles(times),'accepted_to_processed_ms':percentiles(end_to_end),'kafka_consumer_lag':{'samples':len(lag_samples),'max':max(lag_samples) if lag_samples else None,'last':lag_samples[-1] if lag_samples else None},'base':base,'host':{'platform':platform.platform(),'cpu':platform.processor(),'python':platform.python_version()},'note':'synthetic single-device burst; latency is local to this run, not a capacity claim'}
args.output.parent.mkdir(parents=True,exist_ok=True); args.output.write_text(json.dumps(report,indent=2),encoding='utf-8'); print(json.dumps(report,indent=2))
if errors or processed != args.count: raise SystemExit(1)
