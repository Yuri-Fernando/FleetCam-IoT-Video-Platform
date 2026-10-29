"""Injects a malformed Kafka message into this local lab to verify DLT persistence."""
import json
import subprocess
import time
import urllib.request
import uuid

base='http://127.0.0.1:8080'; key='local-demo-key'; marker='poison-'+uuid.uuid4().hex
subprocess.run(['docker','compose','exec','-T','kafka','/opt/kafka/bin/kafka-console-producer.sh',
                '--bootstrap-server','localhost:9092','--topic','fleetcam.ingestion.v1'],
               input=marker+'\n',text=True,check=True,timeout=90)
deadline=time.monotonic()+90
while time.monotonic()<deadline:
    req=urllib.request.Request(base+'/api/v1/lab/dlt',headers={'X-API-Key':key})
    with urllib.request.urlopen(req,timeout=10) as response: rows=json.load(response)
    if any(row['status']=='OPEN' for row in rows):
        # Confirm this exact marker, rather than an unrelated older dead letter.
        p=subprocess.run(['docker','compose','exec','-T','postgres','psql','-U','fleetcam','-d','fleetcam','-At','-c',
            "select count(*) from dead_letters where payload='"+marker+"'"],capture_output=True,text=True,check=True)
        if p.stdout.strip()=='1': print('PASS: malformed message persisted in DLT'); break
    time.sleep(2)
else: raise AssertionError('dead letter not persisted')
