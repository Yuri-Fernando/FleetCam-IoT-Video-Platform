-- Associate older valid dead letters only when their entire payload matches an accepted event.
UPDATE dead_letters d SET tenant_id = i.tenant_id
FROM ingest_events i
WHERE d.tenant_id IS NULL
  AND i.payload = CASE WHEN pg_input_is_valid(d.payload, 'jsonb') THEN d.payload::jsonb ELSE NULL END;
