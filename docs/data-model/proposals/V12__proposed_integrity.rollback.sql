-- Rollback da PROPOSTA V12. Remove só o que a proposta cria; não toca em dados.
SET lock_timeout = '5s';
ALTER TABLE incidents         DROP CONSTRAINT IF EXISTS ck_incidents_status;
ALTER TABLE evidence          DROP CONSTRAINT IF EXISTS ck_evidence_status;
ALTER TABLE dead_letters      DROP CONSTRAINT IF EXISTS ck_dead_letters_status;
ALTER TABLE state_transitions DROP CONSTRAINT IF EXISTS fk_state_transitions_device;
DROP INDEX IF EXISTS incident_timeline_incident;
DROP INDEX IF EXISTS state_transitions_device_time;
RESET lock_timeout;
