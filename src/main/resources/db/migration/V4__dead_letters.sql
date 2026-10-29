CREATE TABLE dead_letters (
 id uuid PRIMARY KEY, topic text NOT NULL, kafka_partition integer NOT NULL, kafka_offset bigint NOT NULL,
 message_key text, payload text NOT NULL, error text, status text NOT NULL DEFAULT 'OPEN',
 created_at timestamptz NOT NULL DEFAULT now(), redriven_at timestamptz,
 UNIQUE(topic,kafka_partition,kafka_offset)
);
CREATE TABLE audit_log (
 id bigserial PRIMARY KEY, tenant_id text NOT NULL, action text NOT NULL,
 resource_id text NOT NULL, actor text NOT NULL, occurred_at timestamptz NOT NULL DEFAULT now()
);
