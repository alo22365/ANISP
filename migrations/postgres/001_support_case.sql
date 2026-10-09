CREATE TABLE IF NOT EXISTS support_case
(
    case_id UUID PRIMARY KEY,
    case_type VARCHAR(32) NOT NULL,
    title VARCHAR(256) NOT NULL,
    description TEXT NOT NULL,
    reporter_id VARCHAR(128) NOT NULL,
    assignee_id VARCHAR(128),
    project_id VARCHAR(128),
    service VARCHAR(128),
    environment VARCHAR(64),
    priority VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ
);

ALTER TABLE support_case
    ALTER COLUMN reporter_id TYPE VARCHAR(128),
    ALTER COLUMN assignee_id TYPE VARCHAR(128);
