-- Execute after substituting the tenant in psql; laboratory data only.
-- Accepted but not yet projected: distinguish consumer lag from device silence.
SELECT device_id, count(*) AS waiting, min(received_at) AS oldest_received
FROM ingest_events
WHERE tenant_id = 'tenant-demo' AND processed_at IS NULL
GROUP BY device_id ORDER BY waiting DESC;

-- Compare the most recent reported storage status by firmware.
SELECT coalesce(s.firmware_version, d.firmware_version) AS firmware,
       coalesce(s.storage_state, 'UNKNOWN') AS storage_state, count(*) AS devices
FROM devices d LEFT JOIN device_state s ON s.tenant_id=d.tenant_id AND s.device_id=d.id
WHERE d.tenant_id='tenant-demo'
GROUP BY 1, 2 ORDER BY 1, 2;

-- Publish backlog is independent of projection backlog.
SELECT count(*) AS pending, min(created_at) AS oldest, max(attempts) AS max_attempts
FROM outbox WHERE tenant_id='tenant-demo' AND delivered_at IS NULL;
