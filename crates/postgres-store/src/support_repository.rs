use std::{error::Error, fmt, time::Duration};

use anisp_support::{
    CaseStatus, CaseType, Priority, StoredSupportCase, SupportCase, SupportCaseQuery,
    SupportCaseRepository, SupportRepositoryError,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, QueryBuilder, Row, postgres::PgRow};
use tokio::time::timeout;
use uuid::Uuid;

use crate::PostgresStore;
use crate::outbox_repository::{OutboxInsert, insert_outbox};

#[derive(Clone)]
pub struct PostgresSupportCaseRepository {
    pub(crate) pool: PgPool,
    pub(crate) request_timeout: Duration,
}

impl PostgresSupportCaseRepository {
    pub fn new(store: &PostgresStore) -> Self {
        Self {
            pool: store.pool.clone(),
            request_timeout: store.request_timeout,
        }
    }
}

impl fmt::Debug for PostgresSupportCaseRepository {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresSupportCaseRepository")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SupportCaseRepository for PostgresSupportCaseRepository {
    async fn insert(
        &self,
        support_case: &SupportCase,
        lifecycle_event: &anisp_support::SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError> {
        if lifecycle_event.case_id() != support_case.case_id()
            || lifecycle_event.current_status() != support_case.status()
        {
            return Err(SupportRepositoryError::internal());
        }
        let row = SupportCaseRow::from(support_case);
        let outbox = OutboxInsert::try_from(lifecycle_event)
            .map_err(SupportRepositoryError::internal_with_source)?;
        timeout(self.request_timeout, async {
            let mut transaction = self.pool.begin().await?;
            sqlx::query(
                r#"
                    INSERT INTO support_case
                    (
                        case_id, case_type, title, description, reporter_id,
                        assignee_id, project_id, service, environment, priority,
                        status, created_at, updated_at, resolved_at
                    )
                    VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    "#,
            )
            .bind(row.case_id)
            .bind(&row.case_type)
            .bind(&row.title)
            .bind(&row.description)
            .bind(&row.reporter_id)
            .bind(&row.assignee_id)
            .bind(&row.project_id)
            .bind(&row.service)
            .bind(&row.environment)
            .bind(&row.priority)
            .bind(&row.status)
            .bind(row.created_at)
            .bind(row.updated_at)
            .bind(row.resolved_at)
            .execute(&mut *transaction)
            .await?;
            insert_outbox(&mut transaction, &outbox).await?;
            transaction.commit().await
        })
        .await
        .map_err(SupportRepositoryError::timeout_with_source)?
        .map_err(map_query_error)?;

        Ok(())
    }

    async fn update(
        &self,
        support_case: &SupportCase,
        lifecycle_event: &anisp_support::SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError> {
        if lifecycle_event.case_id() != support_case.case_id()
            || lifecycle_event.current_status() != support_case.status()
        {
            return Err(SupportRepositoryError::internal());
        }
        let previous_status = lifecycle_event
            .previous_status()
            .ok_or_else(SupportRepositoryError::internal)?;
        let previous_updated_at = lifecycle_event
            .previous_updated_at()
            .ok_or_else(SupportRepositoryError::internal)?;
        let outbox = OutboxInsert::try_from(lifecycle_event)
            .map_err(SupportRepositoryError::internal_with_source)?;
        let updated = timeout(self.request_timeout, async {
            let mut transaction = self.pool.begin().await?;
            let result = sqlx::query(
                r#"
                UPDATE support_case
                SET status = $2, updated_at = $3, resolved_at = $4
                WHERE case_id = $1 AND status = $5 AND updated_at = $6
                "#,
            )
            .bind(support_case.case_id())
            .bind(status_to_str(support_case.status()))
            .bind(support_case.updated_at())
            .bind(support_case.resolved_at())
            .bind(status_to_str(previous_status))
            .bind(previous_updated_at)
            .execute(&mut *transaction)
            .await?;
            if result.rows_affected() != 1 {
                transaction.rollback().await?;
                return Ok(false);
            }
            insert_outbox(&mut transaction, &outbox).await?;
            transaction.commit().await?;
            Ok(true)
        })
        .await
        .map_err(SupportRepositoryError::timeout_with_source)?
        .map_err(map_query_error)?;

        if !updated {
            return Err(SupportRepositoryError::conflict());
        }
        Ok(())
    }

    async fn find_by_id(
        &self,
        case_id: Uuid,
    ) -> Result<Option<SupportCase>, SupportRepositoryError> {
        let row = timeout(
            self.request_timeout,
            sqlx::query(
                r#"
                SELECT
                    case_id, case_type, title, description, reporter_id,
                    assignee_id, project_id, service, environment, priority,
                    status, created_at, updated_at, resolved_at
                FROM support_case
                WHERE case_id = $1
                "#,
            )
            .bind(case_id)
            .fetch_optional(&self.pool),
        )
        .await
        .map_err(SupportRepositoryError::timeout_with_source)?
        .map_err(map_query_error)?;

        row.map(SupportCaseRow::try_from)
            .transpose()?
            .map(SupportCase::try_from)
            .transpose()
    }

    async fn search(
        &self,
        query: &SupportCaseQuery,
    ) -> Result<Vec<SupportCase>, SupportRepositoryError> {
        // Every appended SQL fragment is fixed. User values are always bound.
        let filters = query.filters();
        let mut builder = QueryBuilder::<Postgres>::new(
            r#"
            SELECT
                case_id, case_type, title, description, reporter_id,
                assignee_id, project_id, service, environment, priority,
                status, created_at, updated_at, resolved_at
            FROM support_case
            WHERE TRUE
            "#,
        );
        if let Some(value) = filters.case_type {
            builder
                .push(" AND case_type = ")
                .push_bind(case_type_to_str(value));
        }
        if let Some(value) = filters.status {
            builder
                .push(" AND status = ")
                .push_bind(status_to_str(value));
        }
        if let Some(value) = filters.priority {
            builder
                .push(" AND priority = ")
                .push_bind(priority_to_str(value));
        }
        for (value, column) in [
            (&filters.reporter_id, "reporter_id"),
            (&filters.assignee_id, "assignee_id"),
            (&filters.project_id, "project_id"),
            (&filters.service, "service"),
            (&filters.environment, "environment"),
        ] {
            if let Some(value) = value {
                builder
                    .push(" AND ")
                    .push(column)
                    .push(" = ")
                    .push_bind(value);
            }
        }
        if let Some(from) = query.created_from() {
            builder.push(" AND created_at >= ").push_bind(from);
        }
        if let Some(to) = query.created_to() {
            builder.push(" AND created_at < ").push_bind(to);
        }
        builder
            .push(" ORDER BY created_at DESC, case_id DESC LIMIT ")
            .push_bind(i64::from(query.limit()));

        let rows = timeout(self.request_timeout, builder.build().fetch_all(&self.pool))
            .await
            .map_err(SupportRepositoryError::timeout_with_source)?
            .map_err(map_query_error)?;

        rows.into_iter()
            .map(SupportCaseRow::try_from)
            .map(|row| row.and_then(SupportCase::try_from))
            .collect()
    }
}

pub(crate) fn map_query_error(error: sqlx::Error) -> SupportRepositoryError {
    match error {
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => SupportRepositoryError::unavailable_with_source(error),
        _ => SupportRepositoryError::internal_with_source(error),
    }
}

#[derive(Debug, Clone)]
struct SupportCaseRow {
    case_id: Uuid,
    case_type: String,
    title: String,
    description: String,
    reporter_id: String,
    assignee_id: Option<String>,
    project_id: Option<String>,
    service: Option<String>,
    environment: Option<String>,
    priority: String,
    status: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    resolved_at: Option<DateTime<Utc>>,
}

impl From<&SupportCase> for SupportCaseRow {
    fn from(support_case: &SupportCase) -> Self {
        Self {
            case_id: support_case.case_id(),
            case_type: case_type_to_str(support_case.case_type()).to_owned(),
            title: support_case.title().to_owned(),
            description: support_case.description().to_owned(),
            reporter_id: support_case.reporter_id().to_owned(),
            assignee_id: support_case.assignee_id().map(str::to_owned),
            project_id: support_case.project_id().map(str::to_owned),
            service: support_case.service().map(str::to_owned),
            environment: support_case.environment().map(str::to_owned),
            priority: priority_to_str(support_case.priority()).to_owned(),
            status: status_to_str(support_case.status()).to_owned(),
            created_at: support_case.created_at(),
            updated_at: support_case.updated_at(),
            resolved_at: support_case.resolved_at(),
        }
    }
}

impl TryFrom<PgRow> for SupportCaseRow {
    type Error = SupportRepositoryError;

    fn try_from(row: PgRow) -> Result<Self, Self::Error> {
        Ok(Self {
            case_id: decode_column(&row, "case_id")?,
            case_type: decode_column(&row, "case_type")?,
            title: decode_column(&row, "title")?,
            description: decode_column(&row, "description")?,
            reporter_id: decode_column(&row, "reporter_id")?,
            assignee_id: decode_column(&row, "assignee_id")?,
            project_id: decode_column(&row, "project_id")?,
            service: decode_column(&row, "service")?,
            environment: decode_column(&row, "environment")?,
            priority: decode_column(&row, "priority")?,
            status: decode_column(&row, "status")?,
            created_at: decode_column(&row, "created_at")?,
            updated_at: decode_column(&row, "updated_at")?,
            resolved_at: decode_column(&row, "resolved_at")?,
        })
    }
}

pub(crate) fn decode_column<T>(row: &PgRow, name: &'static str) -> Result<T, SupportRepositoryError>
where
    for<'row> T: sqlx::Decode<'row, sqlx::Postgres> + sqlx::Type<sqlx::Postgres>,
{
    row.try_get(name)
        .map_err(SupportRepositoryError::decode_with_source)
}

impl TryFrom<SupportCaseRow> for SupportCase {
    type Error = SupportRepositoryError;

    fn try_from(row: SupportCaseRow) -> Result<Self, Self::Error> {
        let case_type = parse_case_type(&row.case_type)?;
        let priority = parse_priority(&row.priority)?;
        let status = parse_status(&row.status)?;

        SupportCase::rehydrate(StoredSupportCase {
            case_id: row.case_id,
            case_type,
            title: row.title,
            description: row.description,
            reporter_id: row.reporter_id,
            assignee_id: row.assignee_id,
            project_id: row.project_id,
            service: row.service,
            environment: row.environment,
            priority,
            status,
            created_at: row.created_at,
            updated_at: row.updated_at,
            resolved_at: row.resolved_at,
        })
        .map_err(SupportRepositoryError::decode_with_source)
    }
}

pub(crate) fn case_type_to_str(value: CaseType) -> &'static str {
    match value {
        CaseType::Ticket => "ticket",
        CaseType::Incident => "incident",
        CaseType::Request => "request",
    }
}

pub(crate) fn priority_to_str(value: Priority) -> &'static str {
    match value {
        Priority::Low => "low",
        Priority::Medium => "medium",
        Priority::High => "high",
        Priority::Critical => "critical",
    }
}

pub(crate) fn status_to_str(value: CaseStatus) -> &'static str {
    match value {
        CaseStatus::Open => "open",
        CaseStatus::Investigating => "investigating",
        CaseStatus::Resolved => "resolved",
        CaseStatus::Closed => "closed",
    }
}

pub(crate) fn parse_case_type(value: &str) -> Result<CaseType, SupportRepositoryError> {
    match value {
        "ticket" => Ok(CaseType::Ticket),
        "incident" => Ok(CaseType::Incident),
        "request" => Ok(CaseType::Request),
        _ => Err(decode_enum_error("case_type", value)),
    }
}

pub(crate) fn parse_priority(value: &str) -> Result<Priority, SupportRepositoryError> {
    match value {
        "low" => Ok(Priority::Low),
        "medium" => Ok(Priority::Medium),
        "high" => Ok(Priority::High),
        "critical" => Ok(Priority::Critical),
        _ => Err(decode_enum_error("priority", value)),
    }
}

pub(crate) fn parse_status(value: &str) -> Result<CaseStatus, SupportRepositoryError> {
    match value {
        "open" => Ok(CaseStatus::Open),
        "investigating" => Ok(CaseStatus::Investigating),
        "resolved" => Ok(CaseStatus::Resolved),
        "closed" => Ok(CaseStatus::Closed),
        _ => Err(decode_enum_error("status", value)),
    }
}

fn decode_enum_error(field: &'static str, value: &str) -> SupportRepositoryError {
    SupportRepositoryError::decode_with_source(PersistedEnumError {
        field,
        value: value.to_owned(),
    })
}

#[derive(Debug)]
struct PersistedEnumError {
    field: &'static str,
    value: String,
}

impl fmt::Display for PersistedEnumError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown persisted {} value: {}",
            self.field, self.value
        )
    }
}

impl Error for PersistedEnumError {}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn row() -> SupportCaseRow {
        SupportCaseRow {
            case_id: Uuid::now_v7(),
            case_type: "incident".to_owned(),
            title: "xpa-finance unavailable".to_owned(),
            description: "Customers cannot reach the service".to_owned(),
            reporter_id: "user-001".to_owned(),
            assignee_id: None,
            project_id: Some("xpa".to_owned()),
            service: Some("xpa-finance".to_owned()),
            environment: Some("prod".to_owned()),
            priority: "high".to_owned(),
            status: "open".to_owned(),
            created_at: Utc.with_ymd_and_hms(2026, 9, 24, 10, 0, 0).unwrap(),
            updated_at: Utc.with_ymd_and_hms(2026, 9, 24, 10, 0, 0).unwrap(),
            resolved_at: None,
        }
    }

    #[test]
    fn enums_round_trip_through_stable_strings() {
        for value in [CaseType::Ticket, CaseType::Incident, CaseType::Request] {
            assert_eq!(parse_case_type(case_type_to_str(value)).unwrap(), value);
        }
        for value in [
            Priority::Low,
            Priority::Medium,
            Priority::High,
            Priority::Critical,
        ] {
            assert_eq!(parse_priority(priority_to_str(value)).unwrap(), value);
        }
        for value in [
            CaseStatus::Open,
            CaseStatus::Investigating,
            CaseStatus::Resolved,
            CaseStatus::Closed,
        ] {
            assert_eq!(parse_status(status_to_str(value)).unwrap(), value);
        }
    }

    #[test]
    fn invalid_case_type_is_decode_error() {
        let mut row = row();
        row.case_type = "problem".to_owned();
        assert!(matches!(
            SupportCase::try_from(row).unwrap_err(),
            SupportRepositoryError::Decode(_)
        ));
    }

    #[test]
    fn invalid_priority_is_decode_error() {
        let mut row = row();
        row.priority = "urgent".to_owned();
        assert!(matches!(
            SupportCase::try_from(row).unwrap_err(),
            SupportRepositoryError::Decode(_)
        ));
    }

    #[test]
    fn invalid_status_is_decode_error() {
        let mut row = row();
        row.status = "pending".to_owned();
        assert!(matches!(
            SupportCase::try_from(row).unwrap_err(),
            SupportRepositoryError::Decode(_)
        ));
    }
}
