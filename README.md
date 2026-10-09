# ANISP — AI Native IT Support

## Project Introduction

ANISP MVP1 is a small Context Event Store. It accepts immutable operational
context events over HTTP, writes them to ClickHouse, and supports exact-field
and time-range reads. MVP2 adds a SupportCase Domain, PostgreSQL persistence,
lifecycle APIs, and transactional publication of lifecycle events to the
Context Event Store. MVP3 adds on-demand local Git metadata collection,
idempotent projection, published Git queries and bounded SupportCase Git
enrichment by time and project/service labels, plus aggregate Git delivery
visibility and a one-shot developer workflow, without automatic diagnosis.

## MVP1 Scope

Implemented: Axum/Tokio server, environment configuration, process health and
ClickHouse readiness, event validation, UUIDv7 IDs, event insert/detail/list,
request IDs, tracing, bounded inputs, stable HTTP errors, and ClickHouse
timeouts. MVP1 **does not implement MCP, Codex, IT Support Ticket, or automatic
diagnosis**.

## Architecture

```text
HTTP / Axum → EventService → EventRepository trait
                                ↓
                       ClickHouseEventRepository → ClickHouse

HTTP / Axum → SupportService → SupportCaseRepository trait
                                  ↓
                     PostgresSupportCaseRepository
                                  ↓ one transaction
                         SupportCase + Outbox → PostgreSQL

Outbox Publisher → EventService → ClickHouseEventRepository → ClickHouse

GET Case Context → SupportCaseContextService
                    ├─ SupportService → PostgreSQL current state
                    ├─ EventService → ClickHouse lifecycle history
                    ├─ SupportContextSyncReader → PostgreSQL support pending count
                    ├─ GitContextQueryService → ClickHouse published Git commits
                    └─ GitContextSyncReader → PostgreSQL window-scoped Git pending count
```

The handler never talks to the database. The Event Domain owns the repository
interface and does not depend on ClickHouse. See [Architecture](docs/ARCHITECTURE.md)
for the boundaries and data flow.

## Project Structure

```text
apps/server/                 HTTP routes, AppState, request context, tracing
crates/common/               shared response types
crates/config/               environment configuration
crates/context/              SupportCase context query composition
crates/event/                ContextEvent, DTOs, query, service, repository trait
crates/git-context/          Git Domain, validated query/view and storage-independent reader/service
crates/git-collector/        on-demand local git2 adapter (not server-wired)
crates/git-ingest/           idempotent Git ingestion and outbox projection publisher
crates/k8s-context/          provider/storage-independent Kubernetes runtime facts
crates/k8s-collector/        one-shot kube-rs Deployment snapshot adapter (not server-wired)
crates/clickhouse-store/     ClickHouse client and repository adapter
crates/support/              SupportCase model, service, and repository trait
crates/postgres-store/       PostgreSQL pool and SupportCase repository adapter
crates/support-outbox/       asynchronous lifecycle event publisher
deploy/docker/               local ClickHouse and PostgreSQL Compose stack
migrations/clickhouse/       versioned SQL file, manually applied
migrations/postgres/         SupportCase and Git outbox SQL schemas, manually applied
scripts/                     migration, HTTP seed and one-shot Git ingest helpers
docs/                        architecture notes
Makefile                     local developer commands
```

## Requirements

- Rust/Cargo 1.85 or newer
- Docker with Compose
- Bash, Make, curl, and jq (`jq` is used by the seed script)

## Quick Start

From the repository root:

```sh
make infra-up           # waits until ClickHouse and PostgreSQL are healthy
make migrate            # applies both idempotent schemas
make run                # starts ANISP on port 3000 by default
```

In another terminal:

```sh
curl -i http://127.0.0.1:3000/health
curl -i http://127.0.0.1:3000/ready
make seed-events
```

The seed command prints its `seed_subject_id` and a query URL for that batch.
Stop the local container with `make infra-down`. This does **not** remove the
ClickHouse named volume; data survives `infra-down` and a subsequent
`infra-up`.

## Configuration

`.env.example` documents the environment variables. The Rust process reads
exported environment variables; it does not automatically load `.env`. Compose
also reads shell/Compose environment variables. Without overrides, local
defaults work together:

| Variable | Default | Purpose |
| --- | --- | --- |
| `ANISP_SERVER_HOST` | `0.0.0.0` | HTTP bind address |
| `ANISP_SERVER_PORT` | `3000` | HTTP port |
| `RUST_LOG` | `info` | tracing filter |
| `ANISP_CLICKHOUSE_URL` | `http://127.0.0.1:8123` | ClickHouse HTTP endpoint |
| `ANISP_CLICKHOUSE_DATABASE` | `anisp` | database |
| `ANISP_CLICKHOUSE_USER` | `anisp` | user |
| `ANISP_CLICKHOUSE_PASSWORD` | `anisp` | local password; do not use in production |
| `ANISP_CLICKHOUSE_REQUEST_TIMEOUT_MS` | `3000` | ping/insert/find/search timeout |
| `ANISP_POSTGRES_URL` | `postgres://anisp:anisp@127.0.0.1:5432/anisp` | PostgreSQL connection URL |
| `ANISP_POSTGRES_MAX_CONNECTIONS` | `10` | PostgreSQL pool limit |
| `ANISP_POSTGRES_REQUEST_TIMEOUT_MS` | `3000` | connect/ping/insert/find timeout |
| `ANISP_SUPPORT_OUTBOX_POLL_INTERVAL_MS` | `1000` | publisher polling interval |
| `ANISP_SUPPORT_OUTBOX_BATCH_SIZE` | `100` | maximum pending events per poll |
| `ANISP_SUPPORT_OUTBOX_CLAIM_LEASE_MS` | `30000` | abandoned publisher claim lease |
| `ANISP_SUPPORT_OUTBOX_MAX_ATTEMPTS` | `10` | failures before an event is exhausted |
| `ANISP_GIT_OUTBOX_POLL_INTERVAL_MS` | `1000` | Git outbox delivery interval; not repository scanning |
| `ANISP_GIT_OUTBOX_BATCH_SIZE` | `100` | Git claim batch size (1–500) |
| `ANISP_GIT_OUTBOX_CLAIM_LEASE_MS` | `30000` | abandoned Git claim lease |
| `ANISP_GIT_OUTBOX_MAX_ATTEMPTS` | `10` | maximum Git delivery attempts |
| `ANISP_CONTEXT_GIT_LOOKBACK_HOURS` | `24` | case-creation-anchored lookback, 1–720 hours |
| `ANISP_CONTEXT_GIT_MAX_COMMITS` | `50` | recent commits in Case Context, 1–500 |
| `ANISP_BASE_URL` | derived from server port | optional seed script target |

If using non-default credentials, export them before `make infra-up` and
`make run`. The seed script uses `ANISP_BASE_URL` when the server is not at its
local default address.

## Migration

`make migrate` runs `scripts/migrate.sh`, which applies the ClickHouse event
schema plus all PostgreSQL migrations, including the SupportCase and outbox
tables, using clients inside the Compose containers. The migrations use
idempotent DDL, so the command can be repeated. There is deliberately no
runtime migration runner.

## API

| Method | Path | Result |
| --- | --- | --- |
| GET | `/health` | static process health, 200 |
| GET | `/ready` | ClickHouse + PostgreSQL query health, 200 or 503 |
| POST | `/api/v1/events` | create event, 201 |
| GET | `/api/v1/events/{event_id}` | full event, 200 or 404 |
| GET | `/api/v1/events` | filtered list, 200 |
| GET | `/api/v1/git/commits` | published Git context list, 200 |
| GET | `/api/v1/git/commits/{repository_id}/{commit_sha}` | compound-identity Git detail, 200 or 404 |
| POST | `/api/v1/support/cases` | create SupportCase, 201 |
| GET | `/api/v1/support/cases` | filtered SupportCase list, 200 |
| GET | `/api/v1/support/cases/{case_id}` | full SupportCase, 200 or 404 |
| PATCH | `/api/v1/support/cases/{case_id}/status` | advance lifecycle status, 200 |
| GET | `/api/v1/support/cases/{case_id}/context` | case + lifecycle history + sync state |
| GET | `/api/v1/internal/support/outbox/status` | payload-free outbox operational statistics |

Create an event:

```sh
curl -i -X POST http://127.0.0.1:3000/api/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"event_time":"2026-09-21T10:31:22.123Z","source":"demo","event_type":"k8s.pod.restart","project_id":"xpa","service":"xpa-finance","environment":"prod","subject_type":"pod","subject_id":"pod-1","title":"Container restarted","content":"Synthetic demo event","metadata":{"restart_count":1}}'
```

Read the returned `event_id` with `GET /api/v1/events/{event_id}`. Every
response has `X-Request-ID`; error JSON contains the same `request_id`.

Create a support case:

```sh
curl -i -X POST http://127.0.0.1:3000/api/v1/support/cases \
  -H 'Content-Type: application/json' \
  -d '{"case_type":"incident","title":"xpa-finance unavailable","description":"Users cannot access xpa-finance production service.","reporter_id":"user-001","assignee_id":null,"project_id":"xpa","service":"xpa-finance","environment":"prod","priority":"high"}'
```

Case type, priority, and status are lowercase on HTTP. Identity, status, and
timestamps are generated by the server and cannot be supplied by clients.

Advance a case through the strict `open → investigating → resolved → closed`
lifecycle:

```sh
curl -i -X PATCH http://127.0.0.1:3000/api/v1/support/cases/CASE_ID/status \
  -H 'Content-Type: application/json' \
  -d '{"status":"investigating"}'
```

Each successful create or transition atomically writes the case and a
PostgreSQL outbox record. A background publisher later creates a
`support.{ticket|incident|request}.{created|investigating|resolved|closed}`
ContextEvent. ClickHouse downtime therefore does not block SupportCase writes.

Read SupportCase context without exposing outbox internals:

```sh
curl http://127.0.0.1:3000/api/v1/support/cases/CASE_ID/context
```

The response contains `case`, an oldest-first support `timeline`, independent
`history_sync`, and `git_context`. Support `history_sync.pending_events > 0`
is reported as `pending`; zero is `synced`. Context reads require both
PostgreSQL and ClickHouse; ordinary Case GET still only needs PostgreSQL.

Inspect the publisher backlog without exposing lifecycle payloads:

```sh
curl http://127.0.0.1:3000/api/v1/internal/support/outbox/status
```

The response contains `pending_events`, `oldest_pending_age_seconds`,
`exhausted_events`, and `last_publish_success_at`. Backlog size does not alter
`/ready`: readiness continues to describe only direct PostgreSQL and
ClickHouse availability.

## Event Model

`ContextEvent` contains `event_id`, `event_time`, `ingest_time`, `source`,
`event_type`, `project_id`, `service`, `environment`, `subject_type`,
`subject_id`, `title`, `content`, optional `trace_id` and `correlation_id`,
and `metadata`. The server generates UUIDv7 `event_id` and `ingest_time`.
Timestamps are stored to millisecond precision (ClickHouse `DateTime64(3)`).
`metadata` must be a JSON Object; the adapter stores it as `metadata_json`
without exposing that column through HTTP. Events are insert-only in MVP1.

## Event Type Convention

`event_type` remains a String and must match `domain.object.action`, using
three lowercase segments such as `git.commit.created`, `cicd.build.success`,
or `k8s.pod.restart`. The seed script emits six **synthetic** event types; it
is not a Git, Kubernetes, CI/CD, or log collector.

## Query Semantics

`GET /api/v1/events` supports `project_id`, `service`, `environment`,
`source`, `event_type`, `subject_type`, `subject_id`, `from`, `to`, and `limit`.
`[from, to)` means `event_time >= from` and `event_time < to`; `from <= to` is
required. Results are sorted by `event_time DESC, event_id DESC`. The default
limit is 100 and the maximum is 500. `count` is the number of returned items,
not the total number of matching rows. There is no cursor pagination.

```sh
curl --get http://127.0.0.1:3000/api/v1/events \
  --data-urlencode 'project_id=xpa' \
  --data-urlencode 'service=xpa-finance' \
  --data-urlencode 'from=2026-09-21T10:00:00Z' \
  --data-urlencode 'to=2026-09-22T00:00:00Z'
```

`GET /api/v1/support/cases` supports `case_type`, `status`, `priority`,
`reporter_id`, `assignee_id`, `project_id`, `service`, `environment`,
`created_from`, `created_to`, and `limit`. Its time interval is
`[created_from, created_to)`. Results use `created_at DESC, case_id DESC`;
the default limit is 100 and the maximum is 500.

## Error Model

| Code | HTTP | Meaning |
| --- | ---: | --- |
| `invalid_request` | 400 | malformed request / invalid ID |
| `invalid_event` | 400 | event validation failed |
| `invalid_event_query` | 400 | query validation failed |
| `invalid_git_commit_query` | 400 | invalid SHA, identity or Git filters |
| `git_commit_not_found` | 404 | published compound Git identity not found |
| `event_not_found` | 404 | event ID not found |
| `support_case_not_found` | 404 | support case ID not found |
| `invalid_support_case` | 400 | support case domain validation failed |
| `invalid_support_case_query` | 400 | support list query validation failed |
| `invalid_support_case_transition` | 409 | lifecycle transition is not allowed |
| `support_case_conflict` | 409 | stale concurrent status update |
| `payload_too_large` | 413 | POST body over 256 KiB |
| `service_unavailable` | 503 | required repository unavailable |
| `request_timeout` | 504 | required repository timed out |
| `internal_error` | 500 | decode or internal failure |

Unknown routes return a generic `not_found` 404. `/ready` preserves its
dependency-readiness JSON even on 503. ClickHouse client errors are not sent to
HTTP clients. Event text is limited to 64 KiB, serialized metadata to 128 KiB,
and other fields have narrower byte limits enforced by the Event Domain.

## Observability

The HTTP request span records server-generated `request_id`, method, path,
status, and duration in milliseconds. Successful event and support operations
log safe identifiers such as IDs, types, project, service, and status.
Descriptions, event content, metadata, authorization, cookies, passwords, and
secrets are not logged by default. Publisher batch logs include its unique
instance ID, batch/published/failed/pending counts, and duration. Per-event
failure logs include only event ID, attempt count, and an error category.
Configure verbosity with `RUST_LOG`. No OpenTelemetry exporter or Prometheus
metrics are included.

## Testing

```sh
make check              # format check and cargo check
make test               # all default workspace tests
make test-integration   # starts both databases, migrates, runs ignored tests
```

The real ClickHouse and PostgreSQL tests are ignored by default so `make test`
is fast and does not require Docker. `make test-integration` leaves both
persistent containers running; use `make infra-down` when finished. Ignored
tests run serially: the Git recovery and Query API E2E tests briefly stop/restore local
ClickHouse. Use a disposable local development stack, with the ANISP server
and other Git publishers stopped, for this command. Normal scoped test records
remain; deliberately corrupt Query E2E fixtures are removed by their exact
test-generated event IDs so unfiltered public reads are not poisoned.

## Git Ingestion (MVP3 Day3)

Local Git collection can be explicitly passed to `GitIngestService`, which
atomically records a fixed event ID and durable projection in PostgreSQL.
The natural key is `(repository_id, commit_sha)`; repeat scans return
`AlreadyIngested` with the existing ID. Branch is the first accepted observation
branch, not part of commit identity. A server-started outbox publisher delivers
`git.commit.created` to ClickHouse using the internal prepared-event capability,
claim/lease, bounded backoff, and exhausted retention. It does not scan repos.

Only changed_files may be explicitly truncated to fit the 128-KiB metadata
boundary; the real total is retained. No remote URL, patch, or source content
is copied. There is no collector trigger API. Delivery
is at-least-once and eventually consistent, not exactly-once. Published Git
outbox rows also retain the ingestion ledger. MVP3 prohibits deleting published
Git outbox rows; future retention requires separating the durable ingestion
ledger from the delivery outbox first.
See [Git ingestion contract and one-shot example](crates/git-ingest/README.md).

## Git Context Query (MVP3 Day4)

Both Git GET routes read **published ClickHouse `context_event` rows only**.
The PostgreSQL Git outbox remains a delivery/idempotency ledger, never a
public read model. Accepted commits are absent from this API until published.

```sh
curl 'http://127.0.0.1:3000/api/v1/git/commits?repository_id=repo-xpa&project_id=xpa&service=xpa-finance&branch=main&from=2026-10-07T00:00:00Z&to=2026-10-08T00:00:00Z&limit=100'
curl 'http://127.0.0.1:3000/api/v1/git/commits/repo-xpa/FULL_COMMIT_SHA'
```

List filters: `repository_id`, `project_id`, `service`, `branch`, `from`, `to`,
`limit`. Time is **commit time**, not observation time; the interval is
`[from,to)`, with `from <= to`. Sub-millisecond bounds are rounded upward to
preserve the half-open interval against DateTime64(3) stored values. Limit
defaults to 100 and must be 1–500. Sorting is committed_at descending, then
event_id descending for ties. Response is `{"items":[],"count":0}`; count is
the returned array length, not total matches. There is no cursor or full-text
search. Unknown query parameters are rejected.

Detail identity is **repository_id + full SHA**, never SHA alone. Forty- or
64-character hexadecimal SHAs are accepted; uppercase is normalized to
lowercase. URL-encode path segments/query values when necessary.

`branch` means **first observed branch** (the first observation accepted by
ANISP). `?branch=main` does **not** mean “commits currently reachable from
main”. Later observations do not rewrite the original branch.

The response contains Git fields: event_id, repository_id/name, project_id,
service, branch, commit_sha, parent_shas, author_name/email, message,
committed_at, observed_at, changed_file_count, additions/deletions,
changes_truncated, and changed_files (path, old_path, lowercase change_type,
nullable additions/deletions). Binary/unknown statistics stay JSON null.
When changes_truncated is true, changed_file_count still describes the full
commit; changed_files is an explicitly incomplete prefix. No ContextEvent
wrapper, metadata_json, remote URL, patch, or source contents are exposed.
The existing Day3 projection stores a fallback for blank commit messages;
the query returns that stored content as message and cannot reconstruct the
original blank/whitespace message.

Invalid SHA/query → 400 (`invalid_git_commit_query`); missing compound
identity → 404 (`git_commit_not_found`); unavailable → 503
(`service_unavailable`); timeout → 504 (`request_timeout`); corrupt metadata
or internal failure → 500 (`internal_error`). Required metadata fields,
including nullable fields, must exist. Wrong types/invalid file changes or
count/truncation mismatches are errors, not invented empty arrays or zeroes.
All responses retain X-Request-ID; errors include the same ID. ClickHouse
unavailability does not gate PostgreSQL-only SupportCase APIs.

The reader always restricts source=git, event_type=git.commit.created and
subject_type=commit. All user filters/identities are safely bound. There are
no repository trigger, poller, remote provider, or source retrieval endpoints.

## SupportCase Git Enrichment (MVP3 Day5)

`GET /api/v1/support/cases/{case_id}/context` now also returns:

```json
{
  "git_context": {
    "status": "synced",
    "window": {
      "from": "2026-10-06T12:00:00Z",
      "to": "2026-10-07T12:00:00Z"
    },
    "pending_events": 0,
    "count": 0,
    "commits": []
  }
}
```

Association requires both Case `project_id` and `service`, matched exactly to
the Git projection labels. All matching repositories/branches are eligible;
there is no repository or branch ranking. Missing or blank project/service
returns `status=unmapped`, `window=null`, `pending_events=0`, `count=0`,
`commits=[]`, without any global Git query. A mapped case with zero published
commits and no pending intents returns `synced` with an empty array, not
`unmapped`.

The fixed commit-time window is `[case.created_at - lookback, case.created_at)`.
It never uses current time or updated_at as its upper bound; later case
transitions do not move it. Defaults are 24 hours / 50 commits, with positive
upper bounds of 720 hours / 500 commits. The returned window preserves Case
microsecond precision; millisecond projection queries retain exact half-open
semantics. Out-of-window old/future commits are excluded even when recently
observed. Late observation of an in-window commit can subsequently enrich the
same fixed window.

Case Context selects the most recent bounded set, then returns it **oldest →
newest** (event ID ascending for equal commit times). Day4's standalone Git
List remains newest-first. `count` is the returned number, not all matches.
Commit DTOs retain the Day4 metadata fields, real changed_file_count,
changes_truncated and nullable binary/file/aggregate statistics. No patch or
source is retrieved.

`GitContextSyncReader` counts unpublished PostgreSQL Git outbox intents for
the same exact project/service and `[from,to)` **commit-time** window. Claimed,
backoff and exhausted unpublished records all count. Its implementation uses
bound SQL predicates over existing payload labels and event_time; no new
schema is needed. No payload, attempts, claims or other delivery internals are
exposed. Pending is read before the published ClickHouse projection, so a
publication racing with a read can conservatively return `pending` until the
next request. A new relevant intent ingested after that count can also race;
this is not a cross-database snapshot.

Any relevant pending count yields HTTP 200 / `git_context.status=pending`,
while still returning available commits. Zero yields `synced` only after a
successful Git query. This is projection freshness for accepted ANISP intents,
not proof that all local repositories were scanned or that every window
commit fits the configured limit. It does not replace support `history_sync`.
ClickHouse or the PostgreSQL freshness reader being unavailable fails Context
with 503; timeout remains 504, decode/internal failure 500. No empty Git
context is used to disguise a failed dependency. Readiness is unchanged.

This API expresses only **time and project/service association**, not
causality, root cause, risk, relevance scores or commit ranking. No new business
routes, collectors or diagnostic engine are added.

## Git Operations and Developer Workflow (MVP3 Day6)

`GET /api/v1/internal/git/outbox/status` reads PostgreSQL through the narrow
GitOutboxOperationalReader / GitOutboxStatusService boundary, not ClickHouse.
It returns only aggregate statistics:

```json
{
  "pending_events": 2,
  "exhausted_events": 0,
  "active_claims": 0,
  "oldest_pending_age_seconds": 12,
  "last_publish_success_at": "2026-10-08T04:00:00Z"
}
```

- pending_events: unpublished, non-exhausted intents, including backoff and claims.
- exhausted_events: unpublished intents no longer automatically retried.
- active_claims: pending intents with an owner and an unexpired configured lease;
  at the exact expiry boundary a claim is no longer active. This is a count,
  not an instance/event/owner listing.
- oldest_pending_age_seconds: whole non-negative seconds since ingestion
  (`created_at`), not historical commit time; null when there are no pending intents.
- last_publish_success_at: latest durable publication acknowledgement; null
  before any acknowledgement. It is not proof of a cross-database snapshot.

Total unpublished delivery backlog is pending_events + exhausted_events.
Day5's window-scoped Git freshness intentionally counts **both** categories,
so an exhausted in-window intent still yields `git_context.status=pending`.
Backlog/exhaustion never gate `/ready`; only live PostgreSQL/ClickHouse checks
do. The status endpoint retains Request ID and opaque 503/504/500 errors.
It returns no payload, commit message, author email, files or claim details.
Protect this internal route at the deployment boundary; no Identity/auth layer
is introduced. No exhausted reset or cleanup API exists.

One explicit bounded scan, with environment values safely passed as arguments:

```sh
make infra-up
make migrate
make run                    # terminal 1: delivers existing PostgreSQL intents

# terminal 2: stable identity, not a new ID on each scan
ANISP_GIT_LOCAL_PATH=/absolute/path/to/local/repository \
ANISP_GIT_REPOSITORY_ID=repo-xpa \
ANISP_GIT_PROJECT_ID=xpa ANISP_GIT_SERVICE=xpa-finance \
ANISP_GIT_BRANCH=main ANISP_GIT_LIMIT=100 make ingest-local-git

curl 'http://127.0.0.1:3000/api/v1/internal/git/outbox/status'
curl 'http://127.0.0.1:3000/api/v1/git/commits?project_id=xpa&service=xpa-finance'
curl 'http://127.0.0.1:3000/api/v1/git/commits/repo-xpa/FULL_COMMIT_SHA'

case_id=$(curl --fail-with-body -sS -H 'Content-Type: application/json' \
  -d '{"case_type":"incident","title":"xpa-finance unavailable","description":"Git context acceptance","reporter_id":"user-001","project_id":"xpa","service":"xpa-finance","environment":"prod","priority":"high"}' \
  http://127.0.0.1:3000/api/v1/support/cases | jq -er '.case_id')
curl "http://127.0.0.1:3000/api/v1/support/cases/$case_id/context"
```

The wrapper delegates to the existing `ingest_local_git` Rust example. PATH
and REPOSITORY_ID are required; project/service default to `-` (unassigned),
branch defaults to `-` (HEAD, including detached HEAD), limit defaults to 100
and is bounded at 500. PostgreSQL credentials come from existing AppConfig,
not another hard-coded connection string. Export desired ANISP configuration
before these commands; `.env.example` is a template, not automatically loaded.
The CLI reports identity/event ID and accepted/already_ingested only. The
publisher polls delivery intents, **never local repositories**. Repeating the
scan on another branch preserves event IDs and the first accepted branch.

Local diff semantics remain: root = empty tree → commit; normal and merge =
first parent → commit, while retaining **all** merge parent SHAs. Rename
detection preserves old/new paths; binary/unknown line counts remain null.
No full diff/patch/source is retained or returned. The projection first
reserves the actual serialized mandatory-metadata budget (including original
count, aggregate stats and truncation flag), then fits a changed_files prefix
inside the remaining 128-KiB boundary, reserving flag/array delimiter overhead.
Mandatory fields that cannot fit reject ingestion; they are never silently
discarded. changes_truncated=true always retains the real changed_file_count.

For failure recovery, stop local ClickHouse, run another explicit scan, and
observe pending intents while ordinary SupportCase GET still works. Publisher
failure schedules bounded 1s/2s/4s/.../60s retries without panicking the server.
After `make infra-up`, non-exhausted events publish under the same event IDs,
Git Query becomes visible and Context freshness converges. At max attempts
records remain exhausted and visible to operators; restoration does not reset
them. Published Git rows must remain indefinitely in MVP3: deleting delivery
rows would also erase ingestion deduplication. Future retention must first
separate an independent Git ingestion ledger from the delivery outbox.

The final ignored E2E uses real local Git, PostgreSQL and ClickHouse: two repos,
old/A/B/C/Incident/future, A/B/C-only ascending Context, unchanged ledger/IDs/
first branch after duplicate scans, batch count = distinct event IDs, and two
new intents accepted while ClickHouse is down then delivered after recovery.
Run `make check`, `make test`, then stop all ANISP servers/publishers before
`make test-integration` (outage tests run serially and restore ClickHouse).
Operational aggregate fixtures use a retained isolated test schema; normal
Git ingestion ledgers are never cleaned up by these commands.

Recent Git Context expresses **time/project/service association, not causality**.
No collector trigger API, repository poller, remote provider, commit ranking,
risk/root-cause field or new business domain is part of this workflow.

## Transactional Outbox and Consistency

SupportCase creation and transitions write the PostgreSQL case row and outbox
row in one transaction. PostgreSQL is therefore the source of truth for the
current case and the durable publication intent. ClickHouse is an eventually
consistent lifecycle-event projection.

Each server publisher has a unique UUIDv7 instance ID. A PostgreSQL
transaction claims eligible rows with `FOR UPDATE SKIP LOCKED`; an unexpired
claim cannot be taken by another instance, while a claim older than the
configured lease can be recovered after a crash. Failed delivery uses bounded
exponential backoff (`1s, 2s, 4s, ...`, capped at `60s`). At the configured
attempt limit the row is retained and marked exhausted; it is never silently
deleted.

Processing is **at-least-once**, not exactly-once. Before insertion the
publisher checks the final outbox `event_id` in ClickHouse, which closes the
normal crash/retry duplicate window. This is not a distributed transaction or
a database-enforced uniqueness guarantee in ClickHouse. Likewise,
`history_sync=synced` means that PostgreSQL currently has no unpublished row
for that case; it does not promise a cross-database strongly consistent
snapshot.

## Failure Recovery

When ClickHouse is unavailable, SupportCase POST/PATCH continue to commit to
PostgreSQL and leave lifecycle events pending. The publisher logs categorized
failures, schedules a later attempt, and never panics the server. After
ClickHouse recovers, eligible rows are reclaimed and published. An exhausted
event requires explicit operator action; there is no dead-letter system or
automatic exhausted-event reset in MVP2.

## Current Limitations

No automatic migration, dead-letter queue, automatic outbox cleanup, cursor
pagination, arbitrary case editing, full-text or vector search, standalone
Timeline API, automatic collectors, MCP, Codex, automatic diagnosis, message bus,
metrics, exporter, or frontend. Context composition is deliberately limited
to SupportCase current state, support lifecycle events and bounded time/label-
associated Git commits. It does not diagnose causes. The internal
operational endpoints have no application-level authentication and must
be protected by the deployment boundary.
Git operational aggregates and window-scoped JSON label predicates currently
scan the retained ledger; future scale may require indexing/read projections.
Published Git outbox grows without retention until the identity ledger and
delivery storage are separated. Lease expiry racing an in-flight ClickHouse
insert can still create duplicates: at-least-once is not exactly-once.
An HTTP timeout may leave an insert outcome ambiguous; the client may not have
received the generated event ID, so manual replay can create duplicates.

## Kubernetes Snapshot Collector (MVP4 Day2)

`KubernetesSnapshotCollector::new(client, cluster_id)` accepts a caller-owned
`kube::Client` and an explicit business cluster ID; kubeconfig context names
are not business identity. `collect_deployment_snapshot(&RuntimeTarget)` is a
one-shot, read-only operation returning Deployment, Pod and Kubernetes Event
Domain observations. Only Deployment is supported; StatefulSet/DaemonSet
return `UnsupportedWorkloadKind` before accessing the API.

Collection reads the namespaced Deployment, lists Pods using its full standard
selector, then lists namespace Events once and filters by Deployment/Pod UID.
All observations share one `observed_at`, while resourceVersion and Kubernetes
fact times remain independent. Missing status counts stay `None`, Running does
not imply Ready, and absent containerStatuses are not synthesized from spec.
This is not an atomic snapshot across resource types. No server wiring,
readiness dependency, Watch, poller, persistence, event projection, HTTP API,
logs or metrics are added. See [Collector contract](crates/k8s-collector/README.md).

Normal tests use a mock HTTP service behind the real kube client:

```sh
cargo test -p anisp-k8s-context -p anisp-k8s-collector
```

An optional ignored test reads an existing Deployment using `Client::try_default()`.
It requires get permission for deployments and list permission for pods and
core/v1 events in the namespace. It never installs a cluster or creates resources:

```sh
ANISP_K8S_TEST_CLUSTER_ID=business-cluster \
ANISP_K8S_TEST_NAMESPACE=xpa \
ANISP_K8S_TEST_DEPLOYMENT=xpa-finance \
cargo test -p anisp-k8s-collector --test real_cluster -- --ignored
```

## Roadmap

MVP1 ends at a reliable Context Event Store and its developer workflow. MVP2
adds SupportCase create/detail/list/status/context APIs, strict lifecycle,
transactional outbox delivery, multi-instance-safe publishing, bounded retry,
and operational visibility. MVP3 Day1–6 add a pure Git Context Domain, an
on-demand local Git adapter, idempotent PostgreSQL ingestion, and asynchronous
ContextEvent projection, published Git query APIs, SupportCase Git enrichment,
Git outbox operational visibility and final full-chain/recovery acceptance.
See [Local Git Collector V1](crates/git-collector/README.md)
and [Git Ingestion](crates/git-ingest/README.md) for contracts and limitations.
Automatic collection, agentic
diagnosis, MCP/Codex, and a broader Context Engine remain outside this milestone.
