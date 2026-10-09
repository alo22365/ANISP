use std::sync::Arc;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    ContextEvent, CreateEventRequest, EventQuery, EventRepository, EventServiceError,
    PreparedContextEvent, ValidationError,
};

pub const MAX_EVENT_METADATA_BYTES: usize = 128 * 1024;

impl PreparedContextEvent {
    /// Shared input boundary for internal producers, without inserting an Event.
    pub fn validate(&self) -> Result<(), ValidationError> {
        validate_request(&CreateEventRequest {
            event_time: self.event_time,
            source: self.source.clone(),
            event_type: self.event_type.clone(),
            project_id: self.project_id.clone(),
            service: self.service.clone(),
            environment: self.environment.clone(),
            subject_type: self.subject_type.clone(),
            subject_id: self.subject_id.clone(),
            title: self.title.clone(),
            content: self.content.clone(),
            trace_id: self.trace_id.clone(),
            correlation_id: self.correlation_id.clone(),
            metadata: self.metadata.clone(),
        })
    }
}

#[derive(Clone)]
pub struct EventService {
    repository: Arc<dyn EventRepository>,
}

impl EventService {
    pub fn new(repository: Arc<dyn EventRepository>) -> Self {
        Self { repository }
    }

    pub async fn create_event(
        &self,
        request: CreateEventRequest,
    ) -> Result<ContextEvent, EventServiceError> {
        validate_request(&request)?;

        let event = ContextEvent::new(
            Uuid::now_v7(),
            truncate_to_millis(request.event_time),
            truncate_to_millis(Utc::now()),
            request.source,
            request.event_type,
            request.project_id,
            request.service,
            request.environment,
            request.subject_type,
            request.subject_id,
            request.title,
            request.content,
            normalize_optional(request.trace_id),
            normalize_optional(request.correlation_id),
            request.metadata,
        );

        self.repository.insert(&event).await?;
        Ok(event)
    }

    pub async fn get_event(
        &self,
        event_id: Uuid,
    ) -> Result<Option<ContextEvent>, EventServiceError> {
        self.repository
            .find_by_id(event_id)
            .await
            .map_err(Into::into)
    }

    /// Persists an internally prepared event while preserving its identity and
    /// occurrence time. No HTTP route exposes this capability.
    pub async fn persist_prepared_event(
        &self,
        prepared: PreparedContextEvent,
    ) -> Result<ContextEvent, EventServiceError> {
        let request = CreateEventRequest {
            event_time: prepared.event_time,
            source: prepared.source,
            event_type: prepared.event_type,
            project_id: prepared.project_id,
            service: prepared.service,
            environment: prepared.environment,
            subject_type: prepared.subject_type,
            subject_id: prepared.subject_id,
            title: prepared.title,
            content: prepared.content,
            trace_id: prepared.trace_id,
            correlation_id: prepared.correlation_id,
            metadata: prepared.metadata,
        };
        validate_request(&request)?;

        let event = ContextEvent::new(
            prepared.event_id,
            truncate_to_millis(request.event_time),
            truncate_to_millis(Utc::now()),
            request.source,
            request.event_type,
            request.project_id,
            request.service,
            request.environment,
            request.subject_type,
            request.subject_id,
            request.title,
            request.content,
            normalize_optional(request.trace_id),
            normalize_optional(request.correlation_id),
            request.metadata,
        );

        self.repository.insert(&event).await?;
        Ok(event)
    }

    pub async fn search_events(
        &self,
        query: &EventQuery,
    ) -> Result<Vec<ContextEvent>, EventServiceError> {
        self.repository.search(query).await.map_err(Into::into)
    }
}

fn truncate_to_millis(time: DateTime<Utc>) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(time.timestamp_millis())
        .expect("valid datetime remains in range")
}

fn validate_request(request: &CreateEventRequest) -> Result<(), ValidationError> {
    for (field, value) in [
        ("source", request.source.as_str()),
        ("project_id", request.project_id.as_str()),
        ("service", request.service.as_str()),
        ("environment", request.environment.as_str()),
        ("subject_type", request.subject_type.as_str()),
        ("subject_id", request.subject_id.as_str()),
        ("title", request.title.as_str()),
        ("content", request.content.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(ValidationError::new(field, "must not be empty"));
        }
    }

    for (field, value, maximum) in [
        ("source", request.source.as_str(), 64),
        ("event_type", request.event_type.as_str(), 128),
        ("project_id", request.project_id.as_str(), 128),
        ("service", request.service.as_str(), 128),
        ("environment", request.environment.as_str(), 64),
        ("subject_type", request.subject_type.as_str(), 64),
        ("subject_id", request.subject_id.as_str(), 256),
        ("title", request.title.as_str(), 256),
        ("content", request.content.as_str(), 64 * 1024),
    ] {
        if value.len() > maximum {
            return Err(ValidationError::new(field, "exceeds maximum byte length"));
        }
    }

    for (field, value) in [
        ("trace_id", request.trace_id.as_deref()),
        ("correlation_id", request.correlation_id.as_deref()),
    ] {
        if value.is_some_and(|value| value.len() > 128) {
            return Err(ValidationError::new(field, "exceeds maximum byte length"));
        }
    }

    if !valid_event_type(&request.event_type) {
        return Err(ValidationError::new(
            "event_type",
            "must match domain.object.action using lowercase segments",
        ));
    }

    if !request.metadata.is_object() {
        return Err(ValidationError::new("metadata", "must be a JSON object"));
    }

    let metadata_bytes = serde_json::to_vec(&request.metadata)
        .map_err(|_| ValidationError::new("metadata", "could not serialize"))?;
    if metadata_bytes.len() > MAX_EVENT_METADATA_BYTES {
        return Err(ValidationError::new(
            "metadata",
            "exceeds maximum serialized byte length",
        ));
    }

    Ok(())
}

fn valid_event_type(value: &str) -> bool {
    let segments: Vec<_> = value.split('.').collect();
    segments.len() == 3 && segments.into_iter().all(valid_event_type_segment)
}

fn valid_event_type_segment(segment: &str) -> bool {
    let mut characters = segment.chars();
    matches!(characters.next(), Some(first) if first.is_ascii_lowercase())
        && characters.all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || character == '_'
                || character == '-'
        })
}

fn normalize_optional(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use chrono::{Duration, TimeZone, Utc};
    use serde_json::json;

    use crate::{EventFilters, EventQuery, EventRepository, RepositoryError};

    use super::*;

    #[derive(Default)]
    struct FakeRepository {
        inserted: Mutex<Vec<ContextEvent>>,
        fail: bool,
    }

    #[async_trait]
    impl EventRepository for FakeRepository {
        async fn insert(&self, event: &ContextEvent) -> Result<(), RepositoryError> {
            if self.fail {
                return Err(RepositoryError::unavailable());
            }
            self.inserted.lock().unwrap().push(event.clone());
            Ok(())
        }

        async fn find_by_id(
            &self,
            event_id: Uuid,
        ) -> Result<Option<ContextEvent>, RepositoryError> {
            if self.fail {
                return Err(RepositoryError::unavailable());
            }
            Ok(self
                .inserted
                .lock()
                .unwrap()
                .iter()
                .find(|event| event.event_id() == event_id)
                .cloned())
        }

        async fn search(&self, query: &EventQuery) -> Result<Vec<ContextEvent>, RepositoryError> {
            if self.fail {
                return Err(RepositoryError::unavailable());
            }
            let filters = query.filters();
            let mut items: Vec<_> = self
                .inserted
                .lock()
                .unwrap()
                .iter()
                .filter(|event| {
                    [
                        (filters.project_id.as_deref(), event.project_id()),
                        (filters.service.as_deref(), event.service()),
                        (filters.environment.as_deref(), event.environment()),
                        (filters.source.as_deref(), event.source()),
                        (filters.event_type.as_deref(), event.event_type()),
                        (filters.subject_type.as_deref(), event.subject_type()),
                        (filters.subject_id.as_deref(), event.subject_id()),
                    ]
                    .into_iter()
                    .all(|(expected, actual)| expected.is_none_or(|expected| expected == actual))
                        && query.from().is_none_or(|from| event.event_time() >= from)
                        && query.to().is_none_or(|to| event.event_time() < to)
                })
                .cloned()
                .collect();
            items.sort_by(|left, right| {
                right
                    .event_time()
                    .cmp(&left.event_time())
                    .then_with(|| right.event_id().cmp(&left.event_id()))
            });
            items.truncate(usize::from(query.limit()));
            Ok(items)
        }
    }

    fn valid_request() -> CreateEventRequest {
        CreateEventRequest {
            event_time: Utc.with_ymd_and_hms(2026, 9, 21, 10, 31, 22).unwrap()
                + Duration::milliseconds(123),
            source: "kubernetes".to_owned(),
            event_type: "k8s.pod.restart".to_owned(),
            project_id: "xpa".to_owned(),
            service: "xpa-finance".to_owned(),
            environment: "prod".to_owned(),
            subject_type: "pod".to_owned(),
            subject_id: "xpa-finance-675894c5fc-ddgzh".to_owned(),
            title: "Container restarted".to_owned(),
            content: "Container restart detected.".to_owned(),
            trace_id: None,
            correlation_id: None,
            metadata: json!({ "namespace": "xpa", "restart_count": 5 }),
        }
    }

    #[tokio::test]
    async fn valid_event_is_inserted() {
        let repository = Arc::new(FakeRepository::default());
        let service = EventService::new(repository.clone());

        let event = service.create_event(valid_request()).await.unwrap();

        assert_eq!(event.event_type(), "k8s.pod.restart");
        assert_eq!(repository.inserted.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn invalid_event_type_is_rejected() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        let mut request = valid_request();
        request.event_type = "restart".to_owned();

        let error = service.create_event(request).await.unwrap_err();

        assert!(matches!(
            error,
            EventServiceError::Validation(ref error) if error.field() == "event_type"
        ));
    }

    #[tokio::test]
    async fn invalid_metadata_is_rejected() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        let mut request = valid_request();
        request.metadata = json!(["not", "an", "object"]);

        let error = service.create_event(request).await.unwrap_err();

        assert!(matches!(
            error,
            EventServiceError::Validation(ref error) if error.field() == "metadata"
        ));
    }

    #[tokio::test]
    async fn uuid_v7_and_ingest_time_are_generated() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        let before = Utc::now();

        let event = service.create_event(valid_request()).await.unwrap();
        let after = Utc::now();

        assert_eq!(event.event_id().get_version_num(), 7);
        assert!(event.ingest_time() >= truncate_to_millis(before));
        assert!(event.ingest_time() <= truncate_to_millis(after));
    }

    #[tokio::test]
    async fn prepared_event_preserves_event_id_and_event_time() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        let event_id = Uuid::now_v7();
        let event_time =
            Utc.with_ymd_and_hms(2026, 9, 24, 10, 0, 0).unwrap() + Duration::milliseconds(123);
        let request = valid_request();

        let event = service
            .persist_prepared_event(PreparedContextEvent {
                event_id,
                event_time,
                source: request.source,
                event_type: request.event_type,
                project_id: request.project_id,
                service: request.service,
                environment: request.environment,
                subject_type: request.subject_type,
                subject_id: request.subject_id,
                title: request.title,
                content: request.content,
                trace_id: request.trace_id,
                correlation_id: request.correlation_id,
                metadata: request.metadata,
            })
            .await
            .unwrap();

        assert_eq!(event.event_id(), event_id);
        assert_eq!(event.event_time(), event_time);
    }

    #[tokio::test]
    async fn repository_failure_is_propagated() {
        let service = EventService::new(Arc::new(FakeRepository {
            inserted: Mutex::default(),
            fail: true,
        }));

        let error = service.create_event(valid_request()).await.unwrap_err();

        assert!(matches!(error, EventServiceError::Repository(_)));
    }

    #[tokio::test]
    async fn get_existing_and_missing_event() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        let created = service.create_event(valid_request()).await.unwrap();

        assert_eq!(
            service.get_event(created.event_id()).await.unwrap(),
            Some(created)
        );
        assert_eq!(service.get_event(Uuid::now_v7()).await.unwrap(), None);
    }

    #[tokio::test]
    async fn empty_search_and_repository_failure() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        assert!(
            service
                .search_events(&EventQuery::default())
                .await
                .unwrap()
                .is_empty()
        );

        let failing = EventService::new(Arc::new(FakeRepository {
            inserted: Mutex::default(),
            fail: true,
        }));
        assert!(matches!(
            failing.get_event(Uuid::now_v7()).await.unwrap_err(),
            EventServiceError::Repository(_)
        ));
        assert!(matches!(
            failing
                .search_events(&EventQuery::default())
                .await
                .unwrap_err(),
            EventServiceError::Repository(_)
        ));
    }

    #[tokio::test]
    async fn search_filters_and_limit() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        let first = service.create_event(valid_request()).await.unwrap();
        let mut second_request = valid_request();
        second_request.event_time += Duration::milliseconds(1);
        second_request.project_id = "other".into();
        second_request.service = "other-service".into();
        second_request.environment = "staging".into();
        second_request.source = "git".into();
        second_request.event_type = "git.commit.created".into();
        second_request.subject_type = "commit".into();
        second_request.subject_id = "abc".into();
        let second = service.create_event(second_request).await.unwrap();

        let all = service.search_events(&EventQuery::default()).await.unwrap();
        assert_eq!(
            all.iter().map(ContextEvent::event_id).collect::<Vec<_>>(),
            vec![second.event_id(), first.event_id()]
        );

        for filters in [
            EventFilters {
                project_id: Some("xpa".into()),
                ..Default::default()
            },
            EventFilters {
                service: Some("xpa-finance".into()),
                ..Default::default()
            },
            EventFilters {
                environment: Some("prod".into()),
                ..Default::default()
            },
            EventFilters {
                source: Some("kubernetes".into()),
                ..Default::default()
            },
            EventFilters {
                event_type: Some("k8s.pod.restart".into()),
                ..Default::default()
            },
            EventFilters {
                subject_type: Some("pod".into()),
                ..Default::default()
            },
            EventFilters {
                subject_id: Some("xpa-finance-675894c5fc-ddgzh".into()),
                ..Default::default()
            },
        ] {
            let query = EventQuery::new(filters, None, None, None).unwrap();
            let matches = service.search_events(&query).await.unwrap();
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].event_id(), first.event_id());
        }

        let limit = EventQuery::new(EventFilters::default(), None, None, Some(1)).unwrap();
        assert_eq!(service.search_events(&limit).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn search_time_range_is_half_open() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        let first = service.create_event(valid_request()).await.unwrap();
        let mut later = valid_request();
        later.event_time += Duration::milliseconds(1);
        service.create_event(later).await.unwrap();

        let start = first.event_time();
        let end = start + Duration::milliseconds(1);
        let query = EventQuery::new(EventFilters::default(), Some(start), Some(end), None).unwrap();
        let matches = service.search_events(&query).await.unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].event_id(), first.event_id());
    }

    #[tokio::test]
    async fn event_timestamps_are_millisecond_aligned() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        let mut request = valid_request();
        request.event_time += Duration::microseconds(456);
        let event = service.create_event(request).await.unwrap();
        assert_eq!(event.event_time().timestamp_subsec_nanos() % 1_000_000, 0);
        assert_eq!(event.ingest_time().timestamp_subsec_nanos() % 1_000_000, 0);
    }

    #[tokio::test]
    async fn rejects_oversized_fields_at_domain_boundary() {
        let service = EventService::new(Arc::new(FakeRepository::default()));
        let mut cases = Vec::new();

        let mut request = valid_request();
        request.source = "x".repeat(65);
        cases.push(("source", request));
        let mut request = valid_request();
        request.trace_id = Some("x".repeat(129));
        cases.push(("trace_id", request));
        let mut request = valid_request();
        request.correlation_id = Some("x".repeat(129));
        cases.push(("correlation_id", request));
        let mut request = valid_request();
        request.metadata = json!({"blob": "x".repeat(128 * 1024)});
        cases.push(("metadata", request));

        for (field, request) in cases {
            let error = service.create_event(request).await.unwrap_err();
            assert!(matches!(
                error,
                EventServiceError::Validation(ref error) if error.field() == field
            ));
        }
    }
}
