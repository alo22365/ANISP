use anisp_context::SupportContextSyncReader;
use anisp_support::{
    ClaimedSupportLifecycleEvent, OutboxFailureDisposition, StoredSupportCaseLifecycleEvent,
    SupportCaseLifecycleEvent, SupportCaseLifecycleKind, SupportOutboxOperationalReader,
    SupportOutboxOperationalStatus, SupportOutboxRepository, SupportRepositoryError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Postgres, Transaction, postgres::PgRow};
use tokio::time::timeout;
use uuid::Uuid;

use crate::support_repository::{
    PostgresSupportCaseRepository, case_type_to_str, decode_column, map_query_error,
    parse_case_type, parse_priority, parse_status, priority_to_str, status_to_str,
};

pub(crate) struct OutboxInsert {
    event_id: Uuid,
    case_id: Uuid,
    event_type: String,
    event_time: DateTime<Utc>,
    payload_json: String,
    created_at: DateTime<Utc>,
}

impl TryFrom<&SupportCaseLifecycleEvent> for OutboxInsert {
    type Error = serde_json::Error;

    fn try_from(event: &SupportCaseLifecycleEvent) -> Result<Self, Self::Error> {
        let payload = OutboxPayload {
            case_type: case_type_to_str(event.case_type()).to_owned(),
            kind: lifecycle_kind_to_str(event.kind()).to_owned(),
            previous_updated_at: event.previous_updated_at(),
            previous_status: event
                .previous_status()
                .map(status_to_str)
                .map(str::to_owned),
            current_status: status_to_str(event.current_status()).to_owned(),
            title: event.title().to_owned(),
            reporter_id: event.reporter_id().to_owned(),
            assignee_id: event.assignee_id().map(str::to_owned),
            project_id: event.project_id().map(str::to_owned),
            service: event.service().map(str::to_owned),
            environment: event.environment().map(str::to_owned),
            priority: priority_to_str(event.priority()).to_owned(),
        };

        Ok(Self {
            event_id: event.event_id(),
            case_id: event.case_id(),
            event_type: event.event_type(),
            event_time: event.occurred_at(),
            payload_json: serde_json::to_string(&payload)?,
            created_at: Utc::now(),
        })
    }
}

pub(crate) async fn insert_outbox(
    transaction: &mut Transaction<'_, Postgres>,
    row: &OutboxInsert,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO support_case_outbox
        (
            event_id, case_id, event_type, event_time, payload_json,
            created_at, published_at, attempt_count, last_attempt_at
        )
        VALUES ($1, $2, $3, $4, CAST($5 AS JSONB), $6, NULL, 0, NULL)
        "#,
    )
    .bind(row.event_id)
    .bind(row.case_id)
    .bind(&row.event_type)
    .bind(row.event_time)
    .bind(&row.payload_json)
    .bind(row.created_at)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

#[async_trait]
impl SupportOutboxRepository for PostgresSupportCaseRepository {
    async fn claim_pending(
        &self,
        limit: u32,
        instance_id: &str,
        claimed_at: DateTime<Utc>,
        lease_expired_before: DateTime<Utc>,
    ) -> Result<Vec<ClaimedSupportLifecycleEvent>, SupportRepositoryError> {
        let rows = timeout(self.request_timeout, async {
            let mut transaction = self.pool.begin().await?;
            let rows = sqlx::query(
                r#"
                    WITH candidates AS
                    (
                        SELECT event_id
                        FROM support_case_outbox
                        WHERE published_at IS NULL
                          AND exhausted_at IS NULL
                          AND next_attempt_at <= $3
                          AND (claimed_at IS NULL OR claimed_at <= $4)
                        ORDER BY created_at ASC, event_id ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT $1
                    )
                    UPDATE support_case_outbox AS outbox
                    SET claimed_at = $3,
                        claimed_by = $2,
                        attempt_count = outbox.attempt_count + 1,
                        last_attempt_at = $3
                    FROM candidates
                    WHERE outbox.event_id = candidates.event_id
                    RETURNING
                        outbox.event_id,
                        outbox.case_id,
                        outbox.event_type,
                        outbox.event_time,
                        outbox.payload_json::text AS payload_json,
                        outbox.attempt_count
                    "#,
            )
            .bind(i64::from(limit))
            .bind(instance_id)
            .bind(claimed_at)
            .bind(lease_expired_before)
            .fetch_all(&mut *transaction)
            .await?;
            transaction.commit().await?;
            Ok::<_, sqlx::Error>(rows)
        })
        .await
        .map_err(SupportRepositoryError::timeout_with_source)?
        .map_err(map_query_error)?;

        rows.into_iter()
            .map(PendingOutboxRow::try_from)
            .map(|row| row.and_then(ClaimedSupportLifecycleEvent::try_from))
            .collect()
    }

    async fn mark_published(
        &self,
        event_id: Uuid,
        instance_id: &str,
        published_at: DateTime<Utc>,
    ) -> Result<(), SupportRepositoryError> {
        let result = timeout(
            self.request_timeout,
            sqlx::query(
                r#"
                UPDATE support_case_outbox
                SET published_at = $3, claimed_at = NULL, claimed_by = NULL
                WHERE event_id = $1 AND claimed_by = $2 AND published_at IS NULL
                "#,
            )
            .bind(event_id)
            .bind(instance_id)
            .bind(published_at)
            .execute(&self.pool),
        )
        .await
        .map_err(SupportRepositoryError::timeout_with_source)?
        .map_err(map_query_error)?;
        expect_one_claimed_row(result.rows_affected())
    }

    async fn record_failure(
        &self,
        event_id: Uuid,
        instance_id: &str,
        failed_at: DateTime<Utc>,
        disposition: OutboxFailureDisposition,
    ) -> Result<(), SupportRepositoryError> {
        let (next_attempt_at, exhausted_at) = match disposition {
            OutboxFailureDisposition::RetryAt(next_attempt_at) => (Some(next_attempt_at), None),
            OutboxFailureDisposition::Exhausted => (None, Some(failed_at)),
        };
        let result = timeout(
            self.request_timeout,
            sqlx::query(
                r#"
                UPDATE support_case_outbox
                SET claimed_at = NULL,
                    claimed_by = NULL,
                    last_attempt_at = $3,
                    next_attempt_at = COALESCE($4, next_attempt_at),
                    exhausted_at = $5
                WHERE event_id = $1 AND claimed_by = $2 AND published_at IS NULL
                "#,
            )
            .bind(event_id)
            .bind(instance_id)
            .bind(failed_at)
            .bind(next_attempt_at)
            .bind(exhausted_at)
            .execute(&self.pool),
        )
        .await
        .map_err(SupportRepositoryError::timeout_with_source)?
        .map_err(map_query_error)?;
        expect_one_claimed_row(result.rows_affected())
    }
}

fn expect_one_claimed_row(rows_affected: u64) -> Result<(), SupportRepositoryError> {
    if rows_affected == 1 {
        Ok(())
    } else {
        Err(SupportRepositoryError::conflict())
    }
}

#[async_trait]
impl SupportOutboxOperationalReader for PostgresSupportCaseRepository {
    async fn operational_status(
        &self,
        now: DateTime<Utc>,
    ) -> Result<SupportOutboxOperationalStatus, SupportRepositoryError> {
        let row = timeout(
            self.request_timeout,
            sqlx::query(
                r#"
                SELECT
                    COUNT(*) FILTER
                        (WHERE published_at IS NULL AND exhausted_at IS NULL)::BIGINT
                        AS pending_events,
                    MIN(created_at) FILTER
                        (WHERE published_at IS NULL AND exhausted_at IS NULL)
                        AS oldest_pending_created_at,
                    COUNT(*) FILTER
                        (WHERE published_at IS NULL AND exhausted_at IS NOT NULL)::BIGINT
                        AS exhausted_events,
                    MAX(published_at) AS last_publish_success_at
                FROM support_case_outbox
                "#,
            )
            .fetch_one(&self.pool),
        )
        .await
        .map_err(SupportRepositoryError::timeout_with_source)?
        .map_err(map_query_error)?;

        let pending_events: i64 = decode_column(&row, "pending_events")?;
        let oldest_pending_created_at: Option<DateTime<Utc>> =
            decode_column(&row, "oldest_pending_created_at")?;
        let exhausted_events: i64 = decode_column(&row, "exhausted_events")?;
        let last_publish_success_at = decode_column(&row, "last_publish_success_at")?;
        let oldest_pending_age_seconds = oldest_pending_created_at.map(|created_at| {
            u64::try_from(now.signed_duration_since(created_at).num_seconds().max(0))
                .expect("non-negative i64 seconds fit in u64")
        });

        Ok(SupportOutboxOperationalStatus::new(
            u64::try_from(pending_events).map_err(SupportRepositoryError::decode_with_source)?,
            oldest_pending_age_seconds,
            u64::try_from(exhausted_events).map_err(SupportRepositoryError::decode_with_source)?,
            last_publish_success_at,
        ))
    }
}

#[async_trait]
impl SupportContextSyncReader for PostgresSupportCaseRepository {
    async fn pending_count_for_case(&self, case_id: Uuid) -> Result<u64, SupportRepositoryError> {
        let count: i64 = timeout(
            self.request_timeout,
            sqlx::query_scalar(
                r#"
                SELECT COUNT(*)
                FROM support_case_outbox
                WHERE case_id = $1 AND published_at IS NULL
                "#,
            )
            .bind(case_id)
            .fetch_one(&self.pool),
        )
        .await
        .map_err(SupportRepositoryError::timeout_with_source)?
        .map_err(map_query_error)?;
        u64::try_from(count).map_err(SupportRepositoryError::decode_with_source)
    }
}

struct PendingOutboxRow {
    event_id: Uuid,
    case_id: Uuid,
    event_type: String,
    event_time: DateTime<Utc>,
    payload_json: String,
    attempt_count: i32,
}

impl TryFrom<PgRow> for PendingOutboxRow {
    type Error = SupportRepositoryError;

    fn try_from(row: PgRow) -> Result<Self, Self::Error> {
        Ok(Self {
            event_id: decode_column(&row, "event_id")?,
            case_id: decode_column(&row, "case_id")?,
            event_type: decode_column(&row, "event_type")?,
            event_time: decode_column(&row, "event_time")?,
            payload_json: decode_column(&row, "payload_json")?,
            attempt_count: decode_column(&row, "attempt_count")?,
        })
    }
}

impl TryFrom<PendingOutboxRow> for SupportCaseLifecycleEvent {
    type Error = SupportRepositoryError;

    fn try_from(row: PendingOutboxRow) -> Result<Self, Self::Error> {
        let payload: OutboxPayload = serde_json::from_str(&row.payload_json)
            .map_err(SupportRepositoryError::decode_with_source)?;
        let event = SupportCaseLifecycleEvent::rehydrate(StoredSupportCaseLifecycleEvent {
            event_id: row.event_id,
            case_id: row.case_id,
            case_type: parse_case_type(&payload.case_type)?,
            kind: parse_lifecycle_kind(&payload.kind)?,
            occurred_at: row.event_time,
            previous_updated_at: payload.previous_updated_at,
            previous_status: payload
                .previous_status
                .as_deref()
                .map(parse_status)
                .transpose()?,
            current_status: parse_status(&payload.current_status)?,
            title: payload.title,
            reporter_id: payload.reporter_id,
            assignee_id: payload.assignee_id,
            project_id: payload.project_id,
            service: payload.service,
            environment: payload.environment,
            priority: parse_priority(&payload.priority)?,
        });
        if event.event_type() != row.event_type {
            return Err(SupportRepositoryError::decode());
        }
        Ok(event)
    }
}

impl TryFrom<PendingOutboxRow> for ClaimedSupportLifecycleEvent {
    type Error = SupportRepositoryError;

    fn try_from(row: PendingOutboxRow) -> Result<Self, Self::Error> {
        let attempt_count =
            u32::try_from(row.attempt_count).map_err(SupportRepositoryError::decode_with_source)?;
        let event = SupportCaseLifecycleEvent::try_from(row)?;
        Ok(Self::new(event, attempt_count))
    }
}

#[derive(Serialize, Deserialize)]
struct OutboxPayload {
    case_type: String,
    kind: String,
    previous_updated_at: Option<DateTime<Utc>>,
    previous_status: Option<String>,
    current_status: String,
    title: String,
    reporter_id: String,
    assignee_id: Option<String>,
    project_id: Option<String>,
    service: Option<String>,
    environment: Option<String>,
    priority: String,
}

fn lifecycle_kind_to_str(kind: SupportCaseLifecycleKind) -> &'static str {
    match kind {
        SupportCaseLifecycleKind::Created => "created",
        SupportCaseLifecycleKind::InvestigationStarted => "investigation_started",
        SupportCaseLifecycleKind::Resolved => "resolved",
        SupportCaseLifecycleKind::Closed => "closed",
    }
}

fn parse_lifecycle_kind(value: &str) -> Result<SupportCaseLifecycleKind, SupportRepositoryError> {
    match value {
        "created" => Ok(SupportCaseLifecycleKind::Created),
        "investigation_started" => Ok(SupportCaseLifecycleKind::InvestigationStarted),
        "resolved" => Ok(SupportCaseLifecycleKind::Resolved),
        "closed" => Ok(SupportCaseLifecycleKind::Closed),
        _ => Err(SupportRepositoryError::decode()),
    }
}

#[cfg(test)]
mod tests {
    use anisp_support::{CaseType, CreateSupportCaseCommand, Priority, SupportCase};

    use super::*;

    #[test]
    fn outbox_payload_round_trips_without_description() {
        let support_case = SupportCase::create(CreateSupportCaseCommand {
            case_type: CaseType::Incident,
            title: "xpa-finance unavailable".to_owned(),
            description: "secret diagnostic details".to_owned(),
            reporter_id: "user-001".to_owned(),
            assignee_id: None,
            project_id: Some("xpa".to_owned()),
            service: Some("xpa-finance".to_owned()),
            environment: Some("prod".to_owned()),
            priority: Priority::High,
        })
        .unwrap();
        let event = support_case.created_event();
        let row = OutboxInsert::try_from(&event).unwrap();

        assert!(!row.payload_json.contains("secret diagnostic details"));
        let decoded = SupportCaseLifecycleEvent::try_from(PendingOutboxRow {
            event_id: row.event_id,
            case_id: row.case_id,
            event_type: row.event_type,
            event_time: row.event_time,
            payload_json: row.payload_json,
            attempt_count: 1,
        })
        .unwrap();
        assert_eq!(decoded, event);
    }
}
