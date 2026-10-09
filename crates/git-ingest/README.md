# Git Ingestion and ContextEvent Projection (MVP3 Day3)

```text
LocalGitCollector → ObservedGitCommit → GitIngestService
                                      ↓ GitOutboxRepository port
                             PostgresGitOutboxRepository
                                      ↓ durable identity + intent
                              git_commit_outbox
                                      ↓ claim/lease
                               GitOutboxPublisher
                                      ↓ EventService (internal prepared input)
                              ClickHouseEventRepository → ClickHouse
```

The pure Git Context Domain is unchanged. This application crate depends on
Git Context and Event interfaces, not git2, SQLx, ClickHouse Client, or Axum.
The PostgreSQL adapter belongs to `postgres-store`. The server starts an
outbox delivery task, **not a repository poller**. This crate adds no Git HTTP
route; Day4's read-only Git routes use published ClickHouse data through an
independent GitCommitReader/GitContextQueryService, not this outbox ledger.

## Identity and first observation

`GitIngestService::ingest(ObservedGitCommit)` returns `Accepted(event_id)` or
`AlreadyIngested(event_id)`. PostgreSQL's `(repository_id, commit_sha)` primary
key arbitrates concurrent ingests. The accepted record owns a UUIDv7 event ID;
duplicates reuse it, including after publication. A conflict-safe insert and
subsequent READ COMMITTED lookup preserve the winner without updating its
payload. Duplicate scanning is not an error.

Branch is **the observation branch of the first accepted commit**, not part of
identity. Later observations on another branch do not create a second event
or replace branch/observed_at. Keep published outbox rows: they are also the
durable ingestion identity ledger. Deleting them would allow a future rescan
to allocate a new logical event. No ledger cleanup is implemented.

## ContextEvent mapping and boundary

- source: `git`; event_type: `git.commit.created`.
- event_time: commit's committed_at, normalized to the Event Store's millisecond precision.
- subject_type: `commit`; subject_id: full commit SHA.
- project/service: repository labels, or `unassigned`; environment: `unassigned`.
- correlation_id and trace_id: None.
- title: first message line; if blank or absent, `Git commit <12-character SHA>`.
- content: commit message, or the same fallback for blank messages.

Metadata includes repository ID/name, branch, SHA, all parent SHAs, author,
observed_at, real changed_file_count, aggregate stats, changed_files, and
changes_truncated. Each changed file carries path, old_path, lowercase change
type, and optional stats. Unknown per-file counts make the corresponding
aggregate unknown (`null`); they are not counted as zero. No remote_url, full
diff, patch, source, or binary content is copied.

The metadata budget is the existing Event boundary, **128 KiB serialized
UTF-8 JSON**. changed_files retains a safe prefix. The original total count
and aggregate stats remain intact, and `changes_truncated=true` explicitly
indicates omission (possibly the entire list for a single oversized path).
The complete serialized mandatory metadata is budgeted before any file; flag
and array/comma overhead are reserved too. Mandatory metadata that cannot fit
is rejected, not silently truncated.
The existing title 256-byte, content 64-KiB, and project/service input limits
also apply. Oversized message/title/labels are rejected **before enqueue**;
they are not silently shortened. Rehydrated payloads are revalidated.

## Durable delivery

`004_git_commit_outbox.sql` stores the natural identity, fixed event ID/time,
JSONB projection, delivery timestamps, claim owner/lease, attempt count,
next_attempt_at, and exhausted flag. All request values are SQL parameters.
Eligible batches are claimed transactionally with `FOR UPDATE SKIP LOCKED`.
Each publisher has a UUIDv7 instance ID. Active claims are exclusive; expired
claims can be recovered. Acknowledgements/failures check both owner and attempt
generation so a stale owner cannot acknowledge a renewed claim.

Before publication, the publisher queries EventService by the fixed event ID.
An existing event is acknowledged; otherwise the internal prepared-event
capability inserts it before acknowledgement. The HTTP Event creation
contract still generates IDs server-side and does not expose prepared input.

Failures release ownership and schedule 1s, 2s, 4s, 8s, … backoff capped at 60s,
measured from the individual failure time (not the batch start).
At max attempts the row is retained and marked exhausted. Expired final
attempts abandoned by a crash are also exhausted rather than endlessly
reclaimed. Corrupt payloads receive bounded attempts/exhaustion without
blocking other rows in their batch. Exhausted rows need operator review;
there is no automatic reset/dead-letter system.

Delivery is **at-least-once processing with eventual ClickHouse projection**.
The PostgreSQL identity/intent write is atomic, but there is no cross-database
transaction or exactly-once guarantee. Pre-insert lookup narrows crash/retry
duplicates; it cannot eliminate an in-flight insert racing with lease expiry.
Choose lease and batch size to cover expected batch processing time.

Tracing records instance/event IDs, attempt/error category and batch
published/failed/pending counts/duration. It never logs payloads or message
content. Backlog/exhausted state does not change `/ready`, which still checks
only PostgreSQL and ClickHouse availability.

## One-shot developer workflow

```sh
make infra-up
make migrate             # includes 004; safe to repeat
make run                 # runs both Support and Git outbox publishers

# In another terminal: one bounded scan, no repository polling.
cargo run -p anisp-postgres-store --example ingest_local_git -- \
  /absolute/path/to/repository repo-xpa xpa xpa-finance - 100

# Equivalent safely quoted wrapper:
ANISP_GIT_LOCAL_PATH=/absolute/path/to/repository ANISP_GIT_REPOSITORY_ID=repo-xpa \
ANISP_GIT_PROJECT_ID=xpa ANISP_GIT_SERVICE=xpa-finance make ingest-local-git
curl http://127.0.0.1:3000/api/v1/internal/git/outbox/status
curl 'http://127.0.0.1:3000/api/v1/git/commits?project_id=xpa&service=xpa-finance'
```

Arguments are path, stable repository ID, project ID, service, optional local
branch (`-` means HEAD), optional limit (default 100, max 500). A project or
service `-` means no label. The example needs only PostgreSQL; ClickHouse can
be offline. Keep repository IDs stable across scans. It prints only identity
and acceptance outcome. Local collection is executed on a blocking worker.

Configuration: `ANISP_GIT_OUTBOX_POLL_INTERVAL_MS=1000`,
`ANISP_GIT_OUTBOX_BATCH_SIZE=100`, `ANISP_GIT_OUTBOX_CLAIM_LEASE_MS=30000`,
`ANISP_GIT_OUTBOX_MAX_ATTEMPTS=10`. These are independent of Support delivery.

## Operational visibility (Day6)

GitOutboxOperationalReader is aggregate-only and storage independent.
GitOutboxStatusService uses the publisher's configured lease. The internal
status endpoint returns pending_events (non-exhausted unpublished),
exhausted_events, active_claims (owner + unexpired lease),
oldest_pending_age_seconds (ingestion age, null if empty), and
last_publish_success_at (durable acknowledgement time, null if none).
No payload, email, message, files or claim details cross this read port.
Pending plus exhausted is all unpublished backlog; Day5 scoped freshness
counts both. Neither backlog nor exhaustion changes readiness. Protect the
internal endpoint at the deployment boundary. There is no reset API.

Published rows are never deleted in MVP3. Future retention **must first split
an independent ingestion identity ledger from the delivery outbox**, or a
rescan would allocate a new event ID. Operational reads do not replace the
ClickHouse public Git query model or imply exactly-once delivery.

## Verification

```sh
cargo test -p anisp-git-ingest
cargo test -p anisp-git-collector
make test-integration
```

Database tests are ignored by default. The real Git tests create temporary
repositories and verify repeat scanning, concurrent ingestion, fixed IDs,
first-branch metadata, parents/author/rename/binary stats, bounded metadata,
and publication. **The ignored recovery test temporarily stops the local
Compose ClickHouse service and restores it, including a best-effort panic
guard.** Run ignored tests serially and only against a disposable development
stack; `make test-integration` runs serially. Stop the ANISP server or other
Git publishers before these tests: they explicitly control delivery. Test
records remain as scoped fixtures.

Local Collector V1's SHA-1/UTF-8/bounded-history limitations still apply. Git
ingestion/trigger HTTP APIs, remote providers, continuous scans, MCP/Codex and
a complete Context Engine remain out of scope. Day5's Case enrichment uses
the published Git query service and a narrow window-scoped pending-count port
on this ledger; it exposes no delivery internals and makes no causality claim.
