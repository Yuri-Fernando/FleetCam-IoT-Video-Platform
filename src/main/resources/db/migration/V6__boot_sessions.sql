CREATE TABLE device_boot_sessions (
 tenant_id text NOT NULL, device_id text NOT NULL, boot_id text NOT NULL,
 first_seen_at timestamptz NOT NULL DEFAULT now(), retired_at timestamptz,
 firmware_version text,
 PRIMARY KEY (tenant_id,device_id,boot_id),
 FOREIGN KEY (tenant_id,device_id) REFERENCES devices(tenant_id,id)
);
