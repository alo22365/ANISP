use std::time::Duration;

use anisp_git_context::{GitContextSyncReader, GitReaderError};
use anisp_git_ingest::{
    ClaimedGitEvent, GitCommitProjection, GitFailureDisposition, GitIngestOutcome, GitOutboxError,
    GitOutboxOperationalReader, GitOutboxOperationalStatus, GitOutboxRepository, GitOutboxStatus,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row, postgres::PgRow};
use tokio::time::timeout;
use uuid::Uuid;

use crate::PostgresStore;

#[derive(Clone)]
pub struct PostgresGitOutboxRepository {
    pool: PgPool,
    request_timeout: Duration,
}
impl PostgresGitOutboxRepository {
    pub fn new(store: &PostgresStore) -> Self {
        Self {
            pool: store.pool.clone(),
            request_timeout: store.request_timeout,
        }
    }
}

#[async_trait]
impl GitOutboxOperationalReader for PostgresGitOutboxRepository {
    async fn read_operational_status(
        &self,
        now: DateTime<Utc>,
        lease_expired_before: DateTime<Utc>,
    ) -> Result<GitOutboxOperationalStatus, GitOutboxError> {
        if lease_expired_before >= now {
            return Err(GitOutboxError::Internal);
        }
        // A single aggregate statement gives internally consistent delivery
        // statistics. It never selects payload, Git metadata, or claim owners.
        let row = timeout(self.request_timeout, sqlx::query(r#"
            SELECT
                COUNT(*) FILTER (WHERE published_at IS NULL AND NOT exhausted) AS pending_events,
                COUNT(*) FILTER (WHERE published_at IS NULL AND exhausted) AS exhausted_events,
                COUNT(*) FILTER (WHERE published_at IS NULL AND NOT exhausted
                    AND claimed_by IS NOT NULL AND claimed_at > $1) AS active_claims,
                MIN(created_at) FILTER (WHERE published_at IS NULL AND NOT exhausted) AS oldest_pending_at,
                MAX(published_at) AS last_publish_success_at
            FROM git_commit_outbox
        "#).bind(lease_expired_before).fetch_one(&self.pool))
            .await.map_err(|_| GitOutboxError::Timeout)?.map_err(map_error)?;
        let count = |name| -> Result<u64, GitOutboxError> {
            let value: i64 = row.try_get(name).map_err(|_| GitOutboxError::Decode)?;
            u64::try_from(value).map_err(|_| GitOutboxError::Decode)
        };
        let oldest: Option<DateTime<Utc>> = row
            .try_get("oldest_pending_at")
            .map_err(|_| GitOutboxError::Decode)?;
        Ok(GitOutboxOperationalStatus {
            pending_events: count("pending_events")?,
            exhausted_events: count("exhausted_events")?,
            active_claims: count("active_claims")?,
            oldest_pending_age_seconds: oldest.map(|time| (now - time).num_seconds().max(0) as u64),
            last_publish_success_at: row
                .try_get("last_publish_success_at")
                .map_err(|_| GitOutboxError::Decode)?,
        })
    }
}

#[async_trait]
impl GitContextSyncReader for PostgresGitOutboxRepository {
    async fn pending_count_for_window(
        &self,
        project_id: &str,
        service: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<u64, GitReaderError> {
        if project_id.trim().is_empty() || service.trim().is_empty() || from > to {
            return Err(GitReaderError::Internal);
        }
        // This reads only delivery freshness, never Git views. Exhausted rows
        // remain unpublished and must not produce a false synced result.
        let count: i64 = timeout(self.request_timeout, sqlx::query_scalar(
            "SELECT COUNT(*) FROM git_commit_outbox WHERE published_at IS NULL AND payload_json ->> 'project_id' = $1 AND payload_json ->> 'service' = $2 AND event_time >= $3 AND event_time < $4")
            .bind(project_id).bind(service).bind(from).bind(to).fetch_one(&self.pool))
            .await.map_err(|_| GitReaderError::Timeout)?.map_err(|error| match map_error(error) {
                GitOutboxError::Unavailable => GitReaderError::Unavailable,
                GitOutboxError::Timeout => GitReaderError::Timeout,
                GitOutboxError::Decode => GitReaderError::Decode,
                GitOutboxError::Internal | GitOutboxError::Conflict => GitReaderError::Internal,
            })?;
        u64::try_from(count).map_err(|_| GitReaderError::Decode)
    }
}

#[async_trait]
impl GitOutboxRepository for PostgresGitOutboxRepository {
    async fn find_ingested_event_id(
        &self,
        repository_id: &str,
        commit_sha: &str,
    ) -> Result<Option<Uuid>, GitOutboxError> {
        timeout(self.request_timeout, sqlx::query_scalar("SELECT event_id FROM git_commit_outbox WHERE repository_id = $1 AND commit_sha = $2")
            .bind(repository_id).bind(commit_sha).fetch_optional(&self.pool)).await.map_err(|_| GitOutboxError::Timeout)?.map_err(map_error)
    }
    async fn insert_if_absent(
        &self,
        projection: &GitCommitProjection,
    ) -> Result<GitIngestOutcome, GitOutboxError> {
        projection.validate().map_err(|_| GitOutboxError::Decode)?;
        let payload = serde_json::to_string(projection).map_err(|_| GitOutboxError::Internal)?;
        timeout(self.request_timeout, async {
            let mut tx = self.pool.begin().await?;
            sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED").execute(&mut *tx).await?;
            let inserted: Option<Uuid> = sqlx::query_scalar(
                "INSERT INTO git_commit_outbox (repository_id, commit_sha, event_id, event_time, payload_json) VALUES ($1, $2, $3, $4, CAST($5 AS JSONB)) ON CONFLICT (repository_id, commit_sha) DO NOTHING RETURNING event_id")
                .bind(projection.repository_id()).bind(projection.commit_sha()).bind(projection.event_id()).bind(projection.event_time()).bind(&payload)
                .fetch_optional(&mut *tx).await?;
            let outcome = if let Some(id) = inserted { GitIngestOutcome::Accepted(id) } else {
                // Separate statement gets a fresh READ COMMITTED snapshot after
                // waiting for the winning insert. A one-statement CTE UNION can
                // miss the winning row in its original concurrent snapshot.
                let id = sqlx::query_scalar("SELECT event_id FROM git_commit_outbox WHERE repository_id = $1 AND commit_sha = $2")
                    .bind(projection.repository_id()).bind(projection.commit_sha()).fetch_one(&mut *tx).await?;
                GitIngestOutcome::AlreadyIngested(id)
            };
            tx.commit().await?;
            Ok::<_, sqlx::Error>(outcome)
        }).await.map_err(|_| GitOutboxError::Timeout)?.map_err(map_error)
    }
    async fn claim_pending(
        &self,
        limit: u32,
        instance_id: Uuid,
        now: DateTime<Utc>,
        lease_expired_before: DateTime<Utc>,
        max_attempts: u32,
    ) -> Result<Vec<ClaimedGitEvent>, GitOutboxError> {
        if !(1..=500).contains(&limit) || max_attempts == 0 || max_attempts > i32::MAX as u32 {
            return Err(GitOutboxError::Internal);
        }
        let rows = timeout(self.request_timeout, async {
            let mut tx = self.pool.begin().await?;
            // An owner may crash on its final attempt. Exhaust expired final
            // claims too, so crashes cannot bypass the configured retry bound.
            sqlx::query("UPDATE git_commit_outbox SET exhausted = true, claimed_at = NULL, claimed_by = NULL WHERE published_at IS NULL AND exhausted = false AND attempt_count >= $1 AND (claimed_at IS NULL OR claimed_at <= $2)")
                .bind(i64::from(max_attempts)).bind(lease_expired_before).execute(&mut *tx).await?;
            let rows = sqlx::query(r#"
                WITH candidates AS (
                    SELECT event_id FROM git_commit_outbox
                    WHERE published_at IS NULL AND exhausted = false
                      AND attempt_count < $5
                      AND next_attempt_at <= $3
                      AND (claimed_at IS NULL OR claimed_at <= $4)
                    ORDER BY created_at ASC, event_id ASC
                    FOR UPDATE SKIP LOCKED LIMIT $1
                )
                UPDATE git_commit_outbox AS outbox
                SET claimed_at = $3, claimed_by = $2,
                    attempt_count = outbox.attempt_count + 1, last_attempt_at = $3
                FROM candidates WHERE outbox.event_id = candidates.event_id
                RETURNING outbox.repository_id, outbox.commit_sha, outbox.event_id,
                    outbox.event_time, outbox.payload_json::text AS payload_json, outbox.attempt_count
                "#).bind(i64::from(limit)).bind(instance_id).bind(now).bind(lease_expired_before).bind(i64::from(max_attempts)).fetch_all(&mut *tx).await?;
            tx.commit().await?;
            Ok::<_, sqlx::Error>(rows)
        }).await.map_err(|_| GitOutboxError::Timeout)?.map_err(map_error)?;
        rows.into_iter().map(decode_claim).collect()
    }
    async fn mark_published(
        &self,
        claim: &ClaimedGitEvent,
        instance_id: Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<(), GitOutboxError> {
        let result = timeout(self.request_timeout, sqlx::query(
            "UPDATE git_commit_outbox SET published_at = $4, claimed_at = NULL, claimed_by = NULL WHERE event_id = $1 AND claimed_by = $2 AND attempt_count = $3 AND published_at IS NULL")
            .bind(claim.event_id).bind(instance_id).bind(i64::from(claim.attempt_count)).bind(published_at).execute(&self.pool))
            .await.map_err(|_| GitOutboxError::Timeout)?.map_err(map_error)?;
        expect_owned(result.rows_affected())
    }
    async fn record_failure(
        &self,
        claim: &ClaimedGitEvent,
        instance_id: Uuid,
        disposition: GitFailureDisposition,
    ) -> Result<(), GitOutboxError> {
        let (next_attempt_at, exhausted) = match disposition {
            GitFailureDisposition::RetryAt(time) => (Some(time), false),
            GitFailureDisposition::Exhausted => (None, true),
        };
        let result = timeout(self.request_timeout, sqlx::query(
            "UPDATE git_commit_outbox SET claimed_at = NULL, claimed_by = NULL, next_attempt_at = COALESCE($4, next_attempt_at), exhausted = $5 WHERE event_id = $1 AND claimed_by = $2 AND attempt_count = $3 AND published_at IS NULL")
            .bind(claim.event_id).bind(instance_id).bind(i64::from(claim.attempt_count)).bind(next_attempt_at).bind(exhausted).execute(&self.pool))
            .await.map_err(|_| GitOutboxError::Timeout)?.map_err(map_error)?;
        expect_owned(result.rows_affected())
    }
    async fn operational_status(&self) -> Result<GitOutboxStatus, GitOutboxError> {
        let row = timeout(self.request_timeout, sqlx::query("SELECT COUNT(*) FILTER (WHERE published_at IS NULL AND exhausted = false) AS pending_events, COUNT(*) FILTER (WHERE published_at IS NULL AND exhausted = true) AS exhausted_events FROM git_commit_outbox").fetch_one(&self.pool))
            .await.map_err(|_| GitOutboxError::Timeout)?.map_err(map_error)?;
        let pending: i64 = row
            .try_get("pending_events")
            .map_err(|_| GitOutboxError::Decode)?;
        let exhausted: i64 = row
            .try_get("exhausted_events")
            .map_err(|_| GitOutboxError::Decode)?;
        Ok(GitOutboxStatus {
            pending_events: u64::try_from(pending).map_err(|_| GitOutboxError::Decode)?,
            exhausted_events: u64::try_from(exhausted).map_err(|_| GitOutboxError::Decode)?,
        })
    }
}

fn decode_claim(row: PgRow) -> Result<ClaimedGitEvent, GitOutboxError> {
    let event_id = row
        .try_get("event_id")
        .map_err(|_| GitOutboxError::Decode)?;
    let attempts: i32 = row
        .try_get("attempt_count")
        .map_err(|_| GitOutboxError::Decode)?;
    let projection = decode_projection(&row, event_id);
    Ok(ClaimedGitEvent {
        event_id,
        projection,
        attempt_count: u32::try_from(attempts).map_err(|_| GitOutboxError::Decode)?,
    })
}
fn decode_projection(row: &PgRow, event_id: Uuid) -> Result<GitCommitProjection, GitOutboxError> {
    let payload: String = row
        .try_get("payload_json")
        .map_err(|_| GitOutboxError::Decode)?;
    let projection: GitCommitProjection =
        serde_json::from_str(&payload).map_err(|_| GitOutboxError::Decode)?;
    let repository_id: String = row
        .try_get("repository_id")
        .map_err(|_| GitOutboxError::Decode)?;
    let commit_sha: String = row
        .try_get("commit_sha")
        .map_err(|_| GitOutboxError::Decode)?;
    let event_time: DateTime<Utc> = row
        .try_get("event_time")
        .map_err(|_| GitOutboxError::Decode)?;
    if projection.repository_id() != repository_id
        || projection.commit_sha() != commit_sha
        || projection.event_id() != event_id
        || projection.event_time() != event_time
    {
        return Err(GitOutboxError::Decode);
    }
    projection.validate().map_err(|_| GitOutboxError::Decode)?;
    Ok(projection)
}
fn expect_owned(affected: u64) -> Result<(), GitOutboxError> {
    if affected == 1 {
        Ok(())
    } else {
        Err(GitOutboxError::Conflict)
    }
}
fn map_error(error: sqlx::Error) -> GitOutboxError {
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => GitOutboxError::Unavailable,
        sqlx::Error::PoolTimedOut => GitOutboxError::Timeout,
        _ => GitOutboxError::Internal,
    }
}

#[cfg(test)]
mod sync_tests {
    use super::*;
    fn repository() -> PostgresGitOutboxRepository {
        PostgresGitOutboxRepository {
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
                .unwrap(),
            request_timeout: Duration::from_millis(20),
        }
    }
    #[tokio::test]
    async fn sync_reader_rejects_invalid_scopes_without_database_access() {
        let repository = repository();
        let to = Utc::now();
        let from = to - chrono::Duration::hours(1);
        for (p, s, a, b) in [
            ("", "service", from, to),
            ("project", " ", from, to),
            ("project", "service", to, from),
        ] {
            assert_eq!(
                repository
                    .pending_count_for_window(p, s, a, b)
                    .await
                    .unwrap_err(),
                GitReaderError::Internal
            );
        }
    }
    #[tokio::test]
    async fn sync_reader_closed_pool_maps_to_opaque_unavailable() {
        let repository = repository();
        repository.pool.close().await;
        let to = Utc::now();
        assert_eq!(
            repository
                .pending_count_for_window("project", "service", to - chrono::Duration::hours(1), to)
                .await
                .unwrap_err(),
            GitReaderError::Unavailable
        );
    }

    #[tokio::test]
    async fn operational_reader_rejects_invalid_lease_and_hides_closed_pool_error() {
        let repository = repository();
        let now = Utc::now();
        assert_eq!(
            repository
                .read_operational_status(now, now)
                .await
                .unwrap_err(),
            GitOutboxError::Internal
        );
        repository.pool.close().await;
        assert_eq!(
            repository
                .read_operational_status(now, now - chrono::Duration::seconds(30))
                .await
                .unwrap_err(),
            GitOutboxError::Unavailable
        );
    }

    #[tokio::test]
    #[ignore = "requires migrated PostgreSQL; aggregate fixtures are retained in an isolated schema"]
    async fn real_operational_status_lease_boundaries_age_and_publication() {
        let config = anisp_config::AppConfig::from_env().unwrap();
        let admin = sqlx::postgres::PgPoolOptions::new()
            .connect(config.postgres.url())
            .await
            .unwrap();
        let schema = format!("git_ops_test_{}", Uuid::now_v7().simple());
        // Only generated alphanumeric identifiers are interpolated; all row
        // values and search_path are bound. No ledger rows are deleted.
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query(&format!(
            "CREATE TABLE {schema}.git_commit_outbox (LIKE public.git_commit_outbox INCLUDING ALL)"
        ))
        .execute(&admin)
        .await
        .unwrap();
        let fixture_schema = schema.clone();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .after_connect(move |connection, _| {
                let fixture_schema = fixture_schema.clone();
                Box::pin(async move {
                    sqlx::query("SELECT set_config('search_path', $1, false)")
                        .bind(fixture_schema)
                        .execute(connection)
                        .await?;
                    Ok(())
                })
            })
            .connect(config.postgres.url())
            .await
            .unwrap();
        let repository = PostgresGitOutboxRepository {
            pool: pool.clone(),
            request_timeout: Duration::from_secs(3),
        };
        let now = DateTime::from_timestamp_micros(Utc::now().timestamp_micros()).unwrap();
        let expired = now - chrono::Duration::seconds(30);
        assert_eq!(
            repository
                .read_operational_status(now, expired)
                .await
                .unwrap(),
            Default::default()
        );
        let published = now - chrono::Duration::seconds(5);
        for index in 0..9 {
            let claimed_at = match index {
                2 => Some(expired + chrono::Duration::microseconds(1)),
                3 => Some(expired),
                4 => Some(expired - chrono::Duration::microseconds(1)),
                _ => None,
            };
            let owner = claimed_at.is_some().then(Uuid::now_v7);
            let created = match index {
                1 => now - chrono::Duration::seconds(90),
                6 => now + chrono::Duration::seconds(60),
                _ => now - chrono::Duration::seconds(10),
            };
            sqlx::query("INSERT INTO git_commit_outbox (repository_id, commit_sha, event_id, event_time, payload_json, created_at, published_at, exhausted, claimed_at, claimed_by, next_attempt_at) VALUES ($1,$2,$3,$4,'{}'::jsonb,$5,$6,$7,$8,$9,$10)")
                .bind("isolated-operational-fixture").bind(format!("{index:040x}")).bind(Uuid::now_v7()).bind(now - chrono::Duration::days(100))
                .bind(created).bind((index == 0).then_some(published)).bind(index == 5).bind(claimed_at).bind(owner)
                .bind(now + chrono::Duration::hours(1)).execute(&pool).await.unwrap();
        }
        let status = repository
            .read_operational_status(now, expired)
            .await
            .unwrap();
        assert_eq!(
            status,
            GitOutboxOperationalStatus {
                pending_events: 7,
                exhausted_events: 1,
                active_claims: 1,
                oldest_pending_age_seconds: Some(90),
                last_publish_success_at: Some(published)
            }
        );
        // Passage of the configured lease expires active claims without any
        // mutation. Historical commit time never inflates pending age.
        assert_eq!(
            repository
                .read_operational_status(
                    now + chrono::Duration::minutes(1),
                    now + chrono::Duration::seconds(30)
                )
                .await
                .unwrap()
                .active_claims,
            0
        );
        sqlx::query("UPDATE git_commit_outbox SET published_at = $1, claimed_at = NULL, claimed_by = NULL WHERE NOT exhausted").bind(now).execute(&pool).await.unwrap();
        let status = repository
            .read_operational_status(now, expired)
            .await
            .unwrap();
        assert_eq!(
            (
                status.pending_events,
                status.exhausted_events,
                status.active_claims
            ),
            (0, 1, 0)
        );
        assert_eq!(status.oldest_pending_age_seconds, None);
        assert_eq!(status.last_publish_success_at, Some(now));
        let rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM git_commit_outbox")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 9);
        println!(
            "GIT_OPS_STATUS pending=7 exhausted=1 active_claims=1 age=90; lease boundary/expiry/null statistics verified; 9 ledger rows retained in {schema}"
        );
    }
}
