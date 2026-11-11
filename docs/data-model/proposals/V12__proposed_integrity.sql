-- PROPOSTA V12 — NÃO é uma migração aplicada pelo app.
-- Fica fora de src/main/resources/db/migration de propósito: o Flyway do app não a
-- enxerga. É aplicada só pelo laboratório (scripts/data-model/migration_lab.py)
-- sobre um banco descartável em V11, com forward + rollback verificados.
-- Ver docs/data-model/ddl-review.md (achados R1-R4).
--
-- Antes de virar V12 de verdade: confirmar que não existe V12 no repositório,
-- copiar para db/migration com o nome V12__..., rodar a suíte completa.
--
-- Idempotente. CHECKs em duas fases (NOT VALID + VALIDATE) para lock curto.

SET lock_timeout = '5s';

-- R1: domínios de status hoje só garantidos pelo código Kotlin
ALTER TABLE incidents    DROP CONSTRAINT IF EXISTS ck_incidents_status;
ALTER TABLE evidence     DROP CONSTRAINT IF EXISTS ck_evidence_status;
ALTER TABLE dead_letters DROP CONSTRAINT IF EXISTS ck_dead_letters_status;
ALTER TABLE incidents    ADD CONSTRAINT ck_incidents_status    CHECK (status IN ('OPEN','CLOSED')) NOT VALID;
ALTER TABLE evidence     ADD CONSTRAINT ck_evidence_status     CHECK (status IN ('PENDING','AVAILABLE','FAILED','EXPIRED')) NOT VALID;
ALTER TABLE dead_letters ADD CONSTRAINT ck_dead_letters_status CHECK (status IN ('OPEN','REDRIVEN')) NOT VALID;
ALTER TABLE incidents    VALIDATE CONSTRAINT ck_incidents_status;
ALTER TABLE evidence     VALIDATE CONSTRAINT ck_evidence_status;
ALTER TABLE dead_letters VALIDATE CONSTRAINT ck_dead_letters_status;

-- R2: FK sem índice — a timeline é lida por incident_id (FleetCam.kt, GET /incidents/{id})
CREATE INDEX IF NOT EXISTS incident_timeline_incident ON incident_timeline (incident_id, occurred_at);

-- R3: state_transitions sem FK nem índice por device (única tabela com tenant_id+device_id sem FK)
ALTER TABLE state_transitions DROP CONSTRAINT IF EXISTS fk_state_transitions_device;
ALTER TABLE state_transitions ADD CONSTRAINT fk_state_transitions_device
    FOREIGN KEY (tenant_id, device_id) REFERENCES devices (tenant_id, id) NOT VALID;
ALTER TABLE state_transitions VALIDATE CONSTRAINT fk_state_transitions_device;
CREATE INDEX IF NOT EXISTS state_transitions_device_time ON state_transitions (tenant_id, device_id, changed_at DESC);

RESET lock_timeout;
