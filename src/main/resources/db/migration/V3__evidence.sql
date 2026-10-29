CREATE TABLE evidence (
 id uuid PRIMARY KEY, tenant_id text NOT NULL, event_id uuid NOT NULL,
 object_key text NOT NULL UNIQUE, mime_type text NOT NULL,
 byte_size bigint NOT NULL CHECK (byte_size > 0 AND byte_size <= 10485760),
 sha256 text NOT NULL, origin text NOT NULL, status text NOT NULL DEFAULT 'PENDING',
 created_at timestamptz NOT NULL DEFAULT now(), confirmed_at timestamptz,
 retention_until timestamptz NOT NULL DEFAULT now() + interval '7 days',
 FOREIGN KEY (tenant_id,event_id) REFERENCES ingest_events(tenant_id,event_id)
);
CREATE INDEX evidence_tenant_event ON evidence(tenant_id,event_id);
