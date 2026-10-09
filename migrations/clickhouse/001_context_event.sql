CREATE TABLE IF NOT EXISTS anisp.context_event
(
    event_id UUID,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    source LowCardinality(String),
    event_type LowCardinality(String),
    project_id LowCardinality(String),
    service LowCardinality(String),
    environment LowCardinality(String),
    subject_type LowCardinality(String),
    subject_id String,
    title String,
    content String,
    trace_id String DEFAULT '',
    correlation_id String DEFAULT '',
    metadata_json String DEFAULT '{}'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY
(
    project_id,
    service,
    event_time,
    event_id
);
