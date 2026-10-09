ALTER TABLE support_case_outbox
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS claimed_by VARCHAR(128),
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS exhausted_at TIMESTAMPTZ;

UPDATE support_case_outbox
SET next_attempt_at = COALESCE(last_attempt_at, created_at)
WHERE next_attempt_at IS NULL;

ALTER TABLE support_case_outbox
    ALTER COLUMN next_attempt_at SET DEFAULT NOW(),
    ALTER COLUMN next_attempt_at SET NOT NULL;

CREATE INDEX IF NOT EXISTS support_case_outbox_claimable_idx
    ON support_case_outbox (next_attempt_at, created_at, event_id)
    WHERE published_at IS NULL AND exhausted_at IS NULL;
