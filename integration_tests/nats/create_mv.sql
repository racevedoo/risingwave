CREATE MATERIALIZED VIEW target_count AS
SELECT
    target_id,
    COUNT(*) AS target_count
FROM
    user_behaviors
GROUP BY
    target_id;

CREATE SINK target_count_postgres_sink
FROM
    target_count WITH (
        connector='nats',
        nats.server_url='0.0.0.0:4222',
        nats.subject='event1',
        type='append-only',
        force_append_only='true'
        nats.user='a',
        nats.password='b',
        nats.max_messages = 10,
        nats.max_bytes=1024,
        nats.max_messages_per_subject = 10,
        nats.max_consumers = 8,
        nats.max_message_size = 1024,
        primary_key = 'target_id'
    );

-- ingest back to RW
CREATE table rw_typed_data (
    id BIGINT PRIMARY KEY,
    varchar_column VARCHAR,
    text_column TEXT,
    integer_column INTEGER,
    smallint_column SMALLINT,
    bigint_column BIGINT,
    decimal_column DECIMAL,
    real_column REAL,
    double_column DOUBLE PRECISION,
    boolean_column BOOLEAN,
    date_column DATE,
    time_column TIME,
    timestamp_column TIMESTAMP,
    timestamptz_column TIMESTAMPTZ,
    interval_column INTERVAL,
    jsonb_column JSONB,
    bytea_column BYTEA,
    array_column VARCHAR[]
) WITH (
    connector='nats',
    nats.server_url='0.0.0.0:4222',
    nats.subject='event1',
    type='append-only',
    force_append_only='true'
    nats.user='a',
    nats.password='b',
    nats.max_messages = 10,
    nats.max_bytes=1024,
    nats.max_messages_per_subject = 10,
    nats.max_consumers = 8,
    nats.max_message_size = 1024,
);
