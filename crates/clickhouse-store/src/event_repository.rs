use anisp_event::{ContextEvent, EventQuery, EventRepository, RepositoryError, StoredContextEvent};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

use crate::ClickHouseStore;

#[derive(Clone)]
pub struct ClickHouseEventRepository {
    client: Client,
    request_timeout: Duration,
}

impl ClickHouseEventRepository {
    pub fn new(store: &ClickHouseStore) -> Self {
        Self {
            client: store.client.clone(),
            request_timeout: store.request_timeout,
        }
    }
}

impl std::fmt::Debug for ClickHouseEventRepository {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ClickHouseEventRepository")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl EventRepository for ClickHouseEventRepository {
    async fn insert(&self, event: &ContextEvent) -> Result<(), RepositoryError> {
        let row = ContextEventRow::try_from(event)?;
        let mut insert = self
            .client
            .insert("context_event")
            .map_err(RepositoryError::unavailable_with_source)?;

        timeout(self.request_timeout, async {
            insert.write(&row).await?;
            insert.end().await
        })
        .await
        .map_err(RepositoryError::timeout_with_source)?
        .map_err(RepositoryError::unavailable_with_source)
    }

    async fn find_by_id(&self, event_id: Uuid) -> Result<Option<ContextEvent>, RepositoryError> {
        let row = timeout(
            self.request_timeout,
            self.client
                .query("SELECT ?fields FROM context_event WHERE event_id = toUUID(?) LIMIT 1")
                .bind(event_id.to_string())
                .fetch_optional::<ContextEventRow>(),
        )
        .await
        .map_err(RepositoryError::timeout_with_source)?
        .map_err(RepositoryError::unavailable_with_source)?;

        row.map(ContextEvent::try_from).transpose()
    }

    async fn search(&self, query: &EventQuery) -> Result<Vec<ContextEvent>, RepositoryError> {
        // Only fixed SQL fragments are appended. Every user-provided value is
        // passed through the client's escaped bind API.
        let filters = query.filters();
        let mut sql = String::from("SELECT ?fields FROM context_event WHERE 1 = 1");
        for (value, clause) in [
            (&filters.project_id, " AND project_id = ?"),
            (&filters.service, " AND service = ?"),
            (&filters.environment, " AND environment = ?"),
            (&filters.source, " AND source = ?"),
            (&filters.event_type, " AND event_type = ?"),
            (&filters.subject_type, " AND subject_type = ?"),
            (&filters.subject_id, " AND subject_id = ?"),
        ] {
            if value.is_some() {
                sql.push_str(clause);
            }
        }
        if query.from().is_some() {
            sql.push_str(" AND event_time >= fromUnixTimestamp64Milli(?)");
        }
        if query.to().is_some() {
            sql.push_str(" AND event_time < fromUnixTimestamp64Milli(?)");
        }
        sql.push_str(" ORDER BY event_time DESC, event_id DESC LIMIT ?");

        let mut request = self.client.query(&sql);
        for value in [
            &filters.project_id,
            &filters.service,
            &filters.environment,
            &filters.source,
            &filters.event_type,
            &filters.subject_type,
            &filters.subject_id,
        ] {
            if let Some(value) = value {
                request = request.bind(value);
            }
        }
        if let Some(from) = query.from() {
            request = request.bind(from.timestamp_millis());
        }
        if let Some(to) = query.to() {
            request = request.bind(to.timestamp_millis());
        }
        let rows = timeout(
            self.request_timeout,
            request.bind(query.limit()).fetch_all::<ContextEventRow>(),
        )
        .await
        .map_err(RepositoryError::timeout_with_source)?
        .map_err(RepositoryError::unavailable_with_source)?;

        rows.into_iter().map(ContextEvent::try_from).collect()
    }
}

#[derive(Debug, Row, Serialize, Deserialize)]
struct ContextEventRow {
    #[serde(with = "clickhouse::serde::uuid")]
    event_id: Uuid,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    event_time: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    ingest_time: DateTime<Utc>,
    source: String,
    event_type: String,
    project_id: String,
    service: String,
    environment: String,
    subject_type: String,
    subject_id: String,
    title: String,
    content: String,
    trace_id: String,
    correlation_id: String,
    metadata_json: String,
}

impl TryFrom<ContextEventRow> for ContextEvent {
    type Error = RepositoryError;

    fn try_from(row: ContextEventRow) -> Result<Self, Self::Error> {
        let metadata: Value = serde_json::from_str(&row.metadata_json)
            .map_err(RepositoryError::decode_with_source)?;

        ContextEvent::from_stored(StoredContextEvent {
            event_id: row.event_id,
            event_time: row.event_time,
            ingest_time: row.ingest_time,
            source: row.source,
            event_type: row.event_type,
            project_id: row.project_id,
            service: row.service,
            environment: row.environment,
            subject_type: row.subject_type,
            subject_id: row.subject_id,
            title: row.title,
            content: row.content,
            trace_id: (!row.trace_id.is_empty()).then_some(row.trace_id),
            correlation_id: (!row.correlation_id.is_empty()).then_some(row.correlation_id),
            metadata,
        })
        .map_err(RepositoryError::decode_with_source)
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn row(metadata_json: &str) -> ContextEventRow {
        ContextEventRow {
            event_id: Uuid::now_v7(),
            event_time: Utc.with_ymd_and_hms(2026, 9, 21, 10, 31, 22).unwrap(),
            ingest_time: Utc.with_ymd_and_hms(2026, 9, 21, 10, 31, 23).unwrap(),
            source: "kubernetes".into(),
            event_type: "k8s.pod.restart".into(),
            project_id: "xpa".into(),
            service: "xpa-finance".into(),
            environment: "prod".into(),
            subject_type: "pod".into(),
            subject_id: "pod-1".into(),
            title: "Restarted".into(),
            content: "A restart occurred".into(),
            trace_id: String::new(),
            correlation_id: String::new(),
            metadata_json: metadata_json.into(),
        }
    }

    #[test]
    fn row_decodes_metadata_and_empty_ids() {
        let event = ContextEvent::try_from(row("{\"count\":1}")).unwrap();
        assert_eq!(event.metadata()["count"], 1);
        assert_eq!(event.trace_id(), None);
        assert_eq!(event.correlation_id(), None);
    }

    #[test]
    fn damaged_metadata_is_decode_error() {
        for metadata in ["{broken", "[]"] {
            let error = ContextEvent::try_from(row(metadata)).unwrap_err();
            assert!(error.is_decode());
        }
    }
}

impl TryFrom<&ContextEvent> for ContextEventRow {
    type Error = RepositoryError;

    fn try_from(event: &ContextEvent) -> Result<Self, Self::Error> {
        let metadata_json = serde_json::to_string(event.metadata())
            .map_err(RepositoryError::internal_with_source)?;

        Ok(Self {
            event_id: event.event_id(),
            event_time: event.event_time(),
            ingest_time: event.ingest_time(),
            source: event.source().to_owned(),
            event_type: event.event_type().to_owned(),
            project_id: event.project_id().to_owned(),
            service: event.service().to_owned(),
            environment: event.environment().to_owned(),
            subject_type: event.subject_type().to_owned(),
            subject_id: event.subject_id().to_owned(),
            title: event.title().to_owned(),
            content: event.content().to_owned(),
            trace_id: event.trace_id().unwrap_or_default().to_owned(),
            correlation_id: event.correlation_id().unwrap_or_default().to_owned(),
            metadata_json,
        })
    }
}
