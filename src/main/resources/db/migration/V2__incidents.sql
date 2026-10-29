CREATE TABLE incidents (
 id uuid PRIMARY KEY, tenant_id text NOT NULL, device_id text NOT NULL,
 type text NOT NULL, status text NOT NULL, opened_at timestamptz NOT NULL DEFAULT now(),
 closed_at timestamptz, summary text NOT NULL, source_event_id uuid,
 FOREIGN KEY (tenant_id,device_id) REFERENCES devices(tenant_id,id)
);
CREATE UNIQUE INDEX one_open_incident_per_kind ON incidents(tenant_id,device_id,type) WHERE status='OPEN';
CREATE INDEX incidents_tenant_status ON incidents(tenant_id,status,opened_at DESC);
CREATE TABLE incident_timeline (
 id bigserial PRIMARY KEY, incident_id uuid NOT NULL REFERENCES incidents(id),
 occurred_at timestamptz NOT NULL DEFAULT now(), kind text NOT NULL, annotation text NOT NULL,
 event_id uuid
);
CREATE TABLE state_transitions (
 id bigserial PRIMARY KEY, tenant_id text NOT NULL, device_id text NOT NULL,
 old_storage text, new_storage text NOT NULL, old_capture text, new_capture text NOT NULL,
 event_id uuid NOT NULL, changed_at timestamptz NOT NULL DEFAULT now()
);
