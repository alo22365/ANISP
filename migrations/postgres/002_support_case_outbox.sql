CREATE TABLE IF NOT EXISTS support_case_outbox
(
    event_id UUID PRIMARY KEY,
    case_id UUID NOT NULL REFERENCES support_case(case_id),
    event_type VARCHAR(128) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS support_case_outbox_pending_idx
    ON support_case_outbox (created_at, event_id)
    WHERE published_at IS NULL;
