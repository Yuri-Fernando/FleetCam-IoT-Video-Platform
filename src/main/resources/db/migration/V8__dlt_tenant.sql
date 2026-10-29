ALTER TABLE dead_letters ADD COLUMN tenant_id text;
CREATE INDEX dead_letters_tenant ON dead_letters(tenant_id,created_at DESC);
