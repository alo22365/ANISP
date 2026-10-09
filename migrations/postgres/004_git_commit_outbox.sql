CREATE TABLE IF NOT EXISTS git_commit_outbox
(
    repository_id TEXT NOT NULL CHECK (btrim(repository_id) <> ''),
    commit_sha VARCHAR(64) NOT NULL
        CHECK (commit_sha ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'),
    event_id UUID NOT NULL UNIQUE,
    event_time TIMESTAMPTZ NOT NULL,
    payload_json JSONB NOT NULL CHECK (jsonb_typeof(payload_json) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_attempt_at TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at TIMESTAMPTZ,
    claimed_by UUID,
    exhausted BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (repository_id, commit_sha),
    CHECK ((claimed_at IS NULL) = (claimed_by IS NULL))
);

CREATE INDEX IF NOT EXISTS git_commit_outbox_pending_idx
    ON git_commit_outbox (next_attempt_at, created_at, event_id)
    WHERE published_at IS NULL AND exhausted = false;
