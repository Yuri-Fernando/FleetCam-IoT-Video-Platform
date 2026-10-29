ALTER TABLE device_state ADD COLUMN sd_last_errors bigint;
ALTER TABLE device_state ADD COLUMN sd_error_streak integer NOT NULL DEFAULT 0;
ALTER TABLE device_state ADD COLUMN sd_recovery_streak integer NOT NULL DEFAULT 0;
