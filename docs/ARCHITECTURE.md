# ANISP Architecture

## Request and data flow

```text
HTTP Adapter (Axum routes/handlers)
    → EventService (validation, UUIDv7, ingest timestamp)
    → EventRepository trait (Domain-owned port)
    → ClickHouseEventRepository (infrastructure adapter)
    → ClickHouse (anisp.context_event)
```

`apps/server` wires `AppState`, routes, request middleware, and HTTP error
mapping. `crates/event` owns `ContextEvent`, request/response DTOs,
`EventQuery`, `EventService`, `EventRepository`, and domain errors.
`crates/clickhouse-store` owns `clickhouse::Client`, SQL, and
`ContextEventRow`. `crates/config` reads environment settings, while
`crates/common` contains shared transport response types.

## Architecture rules

1. Handlers call `EventService`; they do not access the database or repository
   directly.
2. The Event Domain has no ClickHouse dependency. `EventRepository` is defined
   in the Domain, with insert/find/search only for MVP1.
3. `ContextEventRow` is separate from the Domain's `ContextEvent`. The adapter
   converts both ways and rejects malformed stored metadata instead of
   silently replacing it.
4. `ContextEvent` is immutable by default: MVP1 exposes creation and reads,
   not update or delete. The server generates UUIDv7 event IDs and ingestion
   timestamps. DateTime64(3) storage uses millisecond precision.
5. Infrastructure errors are translated to domain repository errors and then
   stable HTTP responses. Raw ClickHouse errors never enter response bodies.

## MVP2 SupportCase boundary

```text
HTTP Adapter → SupportService → SupportCaseRepository trait
                                    ↓
                       PostgresSupportCaseRepository
                                    ↓ one PostgreSQL transaction
                         SupportCase row + Outbox row

Background Outbox Publisher
    → SupportOutboxRepository (transactional claim/lease)
    → EventService (internal prepared-event capability)
    → ClickHouseEventRepository
    → ClickHouse

GET SupportCase Context
    → SupportCaseContextService (pure application composition)
        ├─ SupportService → PostgreSQL current state
        ├─ EventService → ClickHouse support lifecycle events
        └─ SupportContextSyncReader → PostgreSQL pending count
```

`crates/support` owns the aggregate, persistence rehydration contract,
repository port, and storage-neutral repository errors. It has no SQLx or
PostgreSQL dependency. `crates/postgres-store` owns the connection pool,
parameterized SQL, `SupportCaseRow`, and enum string mapping. Rehydration
preserves persisted identity, timestamps, and status while rechecking Domain
invariants. `AppState` exposes `SupportService`, never `PgPool` or SQLx client
types. The HTTP adapter owns lowercase enum conversion and never calls the
repository directly.

`crates/context` depends only on the Support and Event application/domain
interfaces. It has no Axum, SQLx, PostgreSQL, ClickHouse client, or row types.
It requests events with `source=support`, `subject_type=support_case`, and
`subject_id=case_id`, then returns them oldest-first without changing the
existing Event List descending-order contract.

`SupportCase` owns the only valid state sequence: `Open → Investigating →
Resolved → Closed`. Transitions update `updated_at`, and entering `Resolved`
sets `resolved_at`. Creating or transitioning a case produces a storage-neutral
`SupportCaseLifecycleEvent`; the Support Domain does not depend on the Event
Domain.

Case mutation and outbox insertion happen inside the same PostgreSQL
transaction. `SupportService` never performs a PostgreSQL write followed by a
ClickHouse write. The outbox event UUID is allocated before the transaction
and becomes the final ContextEvent UUID. Its payload omits the case
description.

Each publisher instance owns a UUIDv7 instance ID. It atomically claims a
bounded eligible batch inside a PostgreSQL transaction using `FOR UPDATE SKIP
LOCKED`, storing `claimed_at` and `claimed_by`. Other instances skip active
claims. A claim older than the configured lease is eligible again, allowing a
new publisher to recover work after its previous owner crashes.

The publisher first checks ClickHouse by the fixed event ID, inserts only when
it is absent, and then marks the owned outbox row published. This narrows the
duplicate window but does not claim exactly-once delivery. Processing is
eventual and at-least-once. ClickHouse errors release the claim and set
`next_attempt_at` with exponential backoff (`1s, 2s, 4s, 8s, ...`, capped at
60 seconds). Once `ANISP_SUPPORT_OUTBOX_MAX_ATTEMPTS` is reached, the durable
row is marked exhausted and excluded from automatic claims. No business event
is deleted.

`SupportOutboxOperationalReader` is a narrow storage-independent read port.
`SupportOutboxStatusService` exposes only pending count, oldest pending age,
exhausted count, and last successful publication time. The internal HTTP
adapter never exposes `payload_json`, attempt details, or case description.

SupportCase search uses a Domain-owned `SupportCaseQuery`. SQLx
`QueryBuilder` appends only fixed columns and operators; every request value is
bound. The created-time interval is `[created_from, created_to)`, and the
stable order is `created_at DESC, case_id DESC` with a 1–500 limit.

Status updates use the previous status and previous `updated_at` as an
optimistic concurrency token. A stale write returns storage-neutral
`SupportRepositoryError::Conflict`, mapped to HTTP 409
`support_case_conflict` instead of 500.

## MVP3 local Git boundary (Day1–2)

`crates/git-context` expresses immutable repository, commit, file-change, and
observation facts without storage/provider dependencies. `crates/git-collector`
is a synchronous local git2 adapter returning `ObservedGitCommit` through the
existing validating constructors. Its native handles remain private; neither
Git Context Domain nor Server runtime acquires a git2 dependency (real E2E
tests use it as a dev dependency). The collector itself
has no Event mapping, persistence, HTTP endpoint, association, or polling task.

The adapter opens the explicit path without ancestor discovery, resolves local
branches or HEAD, and preserves all parent SHAs. Root commits diff against an
empty tree; other commits, including merges, diff against their first parent.
Rename detection is enabled, binary/gitlink line counts remain unknown, and
patch/source contents are not retained. Recent collection uses a bounded lazy
ancestry frontier, not a preloaded native time-sorted revwalk. See the
[collector contract](../crates/git-collector/README.md) for ordering, object
format, encoding, and incomplete-history limitations.

## MVP3 Git ingestion boundary (Day3)

```text
ObservedGitCommit → GitIngestService → GitOutboxRepository (application port)
                                           ↓
                                PostgresGitOutboxRepository
                                           ↓ atomic identity + intent
                                  PostgreSQL git_commit_outbox
                                           ↓ transactional claim/lease
                                    GitOutboxPublisher
                                           ↓ prepared EventService input
                                 ClickHouseEventRepository → ClickHouse
```

`git-ingest` is application composition, not a Git Domain dependency. Its
interfaces have no git2, SQLx, ClickHouse client, or Axum types. The adapter
belongs to `postgres-store`. The server starts a Git outbox delivery task but
no repository scanner or ingestion HTTP route. The explicit one-shot developer
example runs the collector on a blocking worker and enqueues using PostgreSQL
even if ClickHouse is down.

The natural-key primary key `(repository_id, commit_sha)` ensures one durable
logical Event ID. Conflict-safe insertion keeps the winning UUIDv7 and payload;
duplicates read it through a subsequent READ COMMITTED statement. Branch and
observed_at describe the first accepted observation, even if later scans see
other branches. Published rows must remain as the identity ledger.

Projection uses `source=git`, `event_type=git.commit.created`, committed_at as
millisecond-aligned event_time, full SHA as subject_id, and no correlation_id.
Unknown labels become `unassigned`. Metadata preserves commit/author/parents,
observation time, original file count and aggregate statistics. changed_files
alone may be truncated to a safe prefix with `changes_truncated=true`; all
Event input limits are checked before enqueue and rechecked on delivery.
Remote URLs, full diff/patch, and source content are never copied or logged.

Claim uses PostgreSQL transactions and `FOR UPDATE SKIP LOCKED`, a unique
publisher instance ID, expiring lease, and generation-checked acknowledgements.
Failed attempts use 1s/2s/4s/... backoff capped at 60s. Final failed or abandoned
attempts become retained exhausted rows. Corrupt payloads cannot poison the
entire batch indefinitely. Publication first checks the fixed ID in EventService,
inserts via prepared input only when missing, then marks the outbox published.
This is eventual projection and at-least-once processing, not exactly-once:
ClickHouse has no Event ID uniqueness constraint, and lease expiry/in-flight
insert races remain possible. Outbox backlog does not alter readiness.

See [Git ingestion contract](../crates/git-ingest/README.md) for configuration,
metadata limits, and real integration/recovery test behavior.

## MVP3 published Git query boundary (Day4)

```text
Git GET HTTP Adapter → GitContextQueryService → GitCommitReader (Git port)
                                                    ↓
                                    ClickHouseGitCommitReader
                                                    ↓
                                   ClickHouse context_event (published)
```

GitCommitQuery, GitCommitView, GitCommitReader and GitContextQueryService live
in git-context without Event, SQL, database clients or HTTP frameworks.
The immutable validated read view reuses existing Git constructors for SHA,
author and file invariants. HTTP query/response DTOs stay in the Server adapter.
AppState exposes only the query service to handlers. PostgreSQL Git outbox
rows are not a public query source and are not combined into a partial result.

The ClickHouse adapter owns its independent GitEventRow and typed metadata
decoder. Every selected row must be source=git, event_type=git.commit.created,
subject_type=commit. Detail simultaneously filters normalized subject SHA and
metadata.repository_id. All input values, including JSON-field predicates,
range and limit, use the client's escaped bind API; dynamic SQL contains only
fixed clauses. JSON predicates can exclude a row whose identity/filter value
is absent or damaged; every row actually selected is strictly decoded, with
no default strings, arrays or counts. Missing nullable keys differ from
explicit null. Invalid SHA/file metadata, subject/metadata SHA mismatch and
inconsistent file counts/truncation return Decode.

Lists are newest-first by committed_at/event_time and event ID (tie-breaker),
bounded 1–500/default 100, with [from,to) commit-time boundaries. Rounding
query bounds up preserves precise half-open comparisons to millisecond rows.
Count means returned items only. Branch filtering concerns the first accepted
observation, not current reachability. Binary stats remain null, and projected
prefixes retain original file counts and explicit truncation. No full diff or
source is read. message uses Day3's stored content, including its pre-existing
blank-message fallback; the original blank message is not recoverable.

Both reads use the existing configured ClickHouse request timeout. Adapter
Unavailable/Timeout/Decode/Internal categories map to opaque 503/504/500
responses; query validation is 400 and missing identity is 404. Existing
request-ID middleware/tracing covers both routes without logging message,
file contents or metadata. ClickHouse downtime fails Git queries but not
PostgreSQL-only SupportCase writes/reads. Publication remains eventual and
at-least-once; the reader adds no exactly-once or snapshot guarantee.

Real ignored E2E runs Collector → Ingest → Outbox Publisher → ClickHouse →
Axum requests. It checks A/B/C order, same physical commit under two repository
identities, rename/binary/truncation, unprojected intent invisibility, strict
source predicates, decode errors and outage isolation. Run ignored tests
serially without active publishers: the local Compose ClickHouse is briefly
stopped and restored.

## MVP3 SupportCase Git enrichment boundary (Day5)

SupportCaseContextService combines the existing support state, lifecycle
history and history_sync with GitContextQueryService and the storage-neutral
GitContextSyncReader port. The context crate has no Axum, SQLx, ClickHouse
client, git2 or ingestion/publisher dependency. A validated ContextGitConfig
is supplied at construction; configuration reads environment only at startup.
HTTP DTOs stay in Server, and reuse the Day4 Git commit DTO to preserve nullable
stats and original file counts rather than duplicating lossy mapping logic.

Association is exact Case project_id AND service. Missing/blank mapping skips
both Git reads, returning unmapped with a null window and empty commits; it
never widens to global data. Mapped empty results are synced if no relevant
intent remains unpublished. The window is fixed at case.created_at, minus
configured lookback (default 24 hours, positive max 720). It is [from,to),
compared against committed_at/event_time, not observed_at/outbox.created_at or
the request clock. Query bounds preserve exact semantics between PostgreSQL
microsecond Case timestamps and ClickHouse millisecond commit timestamps.
Checked subtraction converts an impossible persisted window to an internal
error rather than panicking.

The most recent max_commits (default 50, positive max 500) from all matching
repositories are presented oldest-first with event ID as the equal-time
tie-breaker. This is time ordering, not ranking. Standalone Git List remains
newest-first. A file list's truncation flag/true count and all binary null
stats survive composition. No diff/source access or new association schema is
introduced. Branch keeps its first-observation semantics.

PostgresGitOutboxRepository implements GitContextSyncReader using bound
project/service payload predicates, the event_time window, and published_at
IS NULL. Claimed/backoff/exhausted rows remain relevant unpublished intents.
The count is deliberately independent of the publisher's eligible queue size.
Only a count crosses this port; delivery payload/attempts/claims never reach
HTTP. Git views continue to come exclusively from ClickHouse. Pending is read
before the Git query; races can conservatively return pending even if the
event has since appeared. Subsequent ingestion can race the count too: synced
does not mean a cross-database strong snapshot, exhaustive collection, or an
unbounded result. Git freshness and support history_sync are independent.

Failed Git query/freshness reads propagate opaque unavailable/timeout/decode/
internal errors (503/504/500), not fabricated empty results. Ordinary Case GET
remains PostgreSQL-only, readiness remains direct PostgreSQL + ClickHouse.
Tracing adds only safe counts and Git sync status; it does not log commit
messages, metadata or Case descriptions. There are no relevance/risk/causality
fields, collectors, Timeline API or Context Engine.

## MVP3 Git operational visibility and final workflow (Day6)

```text
Internal status HTTP → GitOutboxStatusService → GitOutboxOperationalReader
                                                ↓
                                PostgresGitOutboxRepository
                                                ↓ aggregate only
                                    git_commit_outbox
```

The application-owned read port has no SQLx/client/HTTP types and is narrower
than the ingestion/delivery write port. AppState holds the status service;
the handler never accesses a repository or database. One bound aggregate query
returns pending_events, exhausted_events, active_claims, oldest pending age
and latest published_at. Pending excludes exhausted but includes backoff/
claimed intents; all unpublished backlog is the sum of pending and exhausted.
Day5 freshness includes both, independently of delivery eligibility.

Active claims require an owner and claimed_at newer than now minus the same
configured Git lease used by the publisher. Exact expiry/expired/orphaned
claims do not count active. Age uses ingestion created_at, clamps future
creation timestamps to zero and is null with no pending intents. Latest
success is the durable acknowledgement timestamp, not an exactly-once proof.
Statistics never select or expose payload, messages, emails, changed files,
claim owners or event/claim details. Operational tracing records counts and
duration only. Opaque errors retain 503/504/500 and request IDs.

Git outbox is simultaneously a delivery state store **and durable ingestion
idempotency ledger**. MVP3 cannot delete even published Git rows. Future
retention cleanup must first split identity/event-ID arbitration into an
independent ledger; a delivery table alone may then have a retention policy.
No cleanup/reset/re-ingestion control or migration is added here. Aggregate
queries over a growing ledger may need future indexes/read projections.

`make ingest-local-git` passes explicit environment arguments to the existing
bounded one-shot collector example. Collection runs on a blocking worker,
and ingestion requires PostgreSQL only. `make run` starts delivery publishers,
not repository pollers. Fixed IDs and first-observation branches survive
duplicates before/after publication. Developer workflow continues through
the published Git API and Case Context, without a collect HTTP endpoint.

The metadata budget starts with all serialized fixed metadata before adding
files. Flag/delimiter overhead stays reserved inside the 128-KiB limit. Only
the changed-files list can shrink; real totals, aggregate unknown/null stats
and explicit truncation survive. Root diffs use the empty tree, normal/merge
diffs use first parent, while all parent SHAs are kept. No patch/source is saved.

Final E2E checks two real repos through collector/ingest/ledger/publisher/
ClickHouse/Git API/Support Context, rescan identity/branch stability, and
COUNT(*) = COUNT(DISTINCT event_id) for its batch. ClickHouse downtime accepts
new D/E intents, preserves ordinary Case reads and exposes pending delivery;
bounded retry recovers the same events after restoration. Exhaustion retains
the ledger and stops claiming; the status read shows it without exposing body
data or adding reset operations. `/ready` remains direct PostgreSQL + ClickHouse
availability, never backlog-based. This is eventual projection and at-least-once
processing, not a cross-database snapshot or exactly-once guarantee. Time/
project/service-associated Git commits are not asserted causes of incidents.

## Reliability boundaries

The outer HTTP middleware generates a new UUIDv7 request ID for every request,
adds `X-Request-ID`, and records method, path, status, and duration in a
tracing span. Error JSON includes the same ID. The path is logged without the
query string. Content, metadata, authorization, cookies, passwords, and
secrets are not logged by default.

Event validation happens before repository insertion. The POST body is capped
at 256 KiB; domain fields and serialized metadata have byte-length limits.
`ClickHouseStore` applies the configured request timeout to ping, and
`ClickHouseEventRepository` applies it to insert/find/search. A repository
timeout maps to HTTP 504; `/ready` reports dependency unavailability as 503.
No retry occurs, so a timed-out insert may have an uncertain server-side
outcome.

## Query and storage

The ClickHouse table is defined in
`migrations/clickhouse/001_context_event.sql` and manually applied. The
`MergeTree` ordering key is `(project_id, service, event_time, event_id)`.
Search parameters are bound rather than interpolated into SQL. Filters are
exact matches. The time interval is `[from, to)` and results are ordered by
`event_time DESC, event_id DESC`, with a validated limit of 1–500. Metadata is
a JSON Object in the Domain and a JSON String in the ClickHouse row.

## Operational checks

`GET /health` is static process health and remains 200 if ClickHouse is down.
`GET /ready` concurrently executes real `SELECT 1` checks against ClickHouse
and PostgreSQL. It returns 503 if either dependency fails or times out and
identifies each dependency independently. `GET /health` remains static.
Support APIs depend only on PostgreSQL, not on the aggregate readiness result.
Consequently, with PostgreSQL available and ClickHouse unavailable, readiness
is 503 while SupportCase POST and PATCH still commit atomically to PostgreSQL;
their lifecycle events are published after ClickHouse recovers.
The SupportCase Context API is different: it requires PostgreSQL current state
and ClickHouse history, so either dependency failing returns 503. A healthy
ClickHouse with pending outbox rows returns 200 and a conservative
`history_sync.status=pending`; it never presents an unavailable history store
as an empty, complete timeline.

`history_sync=synced` is deliberately narrow: it means PostgreSQL has no
unpublished outbox row for that case at the time of the sync read. It does not
mean the PostgreSQL current-state read and ClickHouse history read form a
cross-database strongly consistent snapshot. Exhausted rows remain
unpublished, so they keep the case history state pending.

Outbox backlog is operational state, not dependency readiness. `/ready`
continues to report only live PostgreSQL and ClickHouse checks; pending or
exhausted counts never change its result.
The developer scripts start persistent containers, apply both migrations, and
seed events **through the HTTP API**. They do not add a runtime migration
runner or a direct seed INSERT.

## Kubernetes runtime snapshot boundary (MVP4 Day2)

`k8s-context` expresses provider-independent immutable RuntimeTarget,
Deployment/Pod/Container observations and Kubernetes source Events. Domain
UIDs are opaque source strings, never generated identifiers. Deployment UID,
Pod UID and Event UID survive mapping; each observation keeps the provider's
resourceVersion when present. Unknown Event counts are optional, not zero.

`k8s-collector` is the sole kube-rs/k8s-openapi adapter. It accepts a Client
and explicit cluster_id and only supports one-shot Deployment collection:

```text
Caller → KubernetesSnapshotCollector → Kubernetes API
             ├─ GET Deployment(namespace, name)
             ├─ LIST Pods(full validated Deployment label selector)
             └─ LIST namespace Events once → filter involvedObject UID
          → DeploymentSnapshot(existing validated Domain observations)
```

The snapshot captures one shared observed_at without rewriting any Kubernetes
fact time. It is a sequential, non-atomic multi-resource observation, not a
strongly consistent snapshot. Absent status counters remain None; Pod Running
is independent of Ready; missing container status remains absent. A required
API/selector/mapping failure is a stable adapter error and fails the whole
operation, without leaking provider error bodies.

This collector is not wired into AppState, HTTP, readiness, storage or any
ContextEvent publisher. There is no Watch, poller, log/metric reader or Support
association. See [mapping and test contract](../crates/k8s-collector/README.md).
