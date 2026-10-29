CREATE TABLE devices (
 tenant_id text NOT NULL, id text NOT NULL, vehicle_id text NOT NULL,
 model text NOT NULL, firmware_version text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (tenant_id,id)
);
CREATE TABLE ingest_events (
 tenant_id text NOT NULL, event_id uuid NOT NULL, device_id text NOT NULL,
 boot_id text NOT NULL, sequence_number bigint NOT NULL, event_kind text NOT NULL,
 payload_hash text NOT NULL, payload jsonb NOT NULL, occurred_at timestamptz NOT NULL,
 received_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz,
 PRIMARY KEY (tenant_id,event_id),
 UNIQUE (tenant_id,device_id,boot_id,sequence_number,event_kind),
 FOREIGN KEY (tenant_id,device_id) REFERENCES devices(tenant_id,id)
);
CREATE INDEX ingest_device_time ON ingest_events (tenant_id,device_id,received_at DESC);
CREATE TABLE outbox (
 tenant_id text NOT NULL, event_id uuid NOT NULL, device_id text NOT NULL,
 payload text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 delivered_at timestamptz, attempts integer NOT NULL DEFAULT 0, last_error text,
 PRIMARY KEY (tenant_id,event_id), FOREIGN KEY (tenant_id,event_id) REFERENCES ingest_events(tenant_id,event_id)
);
CREATE TABLE device_state (
 tenant_id text NOT NULL, device_id text NOT NULL, boot_id text NOT NULL, sequence_number bigint NOT NULL,
 occurred_at timestamptz NOT NULL, storage_state text NOT NULL, capture_state text NOT NULL,
 firmware_version text, updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (tenant_id,device_id), FOREIGN KEY (tenant_id,device_id) REFERENCES devices(tenant_id,id)
);
CREATE INDEX outbox_pending ON outbox (created_at) WHERE delivered_at IS NULL;
