ALTER TABLE evidence ADD COLUMN failure_reason text;
ALTER TABLE evidence ADD COLUMN upload_requested_at timestamptz NOT NULL DEFAULT now();
UPDATE evidence SET upload_requested_at = created_at;
CREATE INDEX evidence_pending_requested ON evidence(upload_requested_at) WHERE status = 'PENDING';
