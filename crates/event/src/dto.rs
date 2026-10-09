use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use uuid::Uuid;

use crate::{ContextEvent, EventFilters, EventQuery, ValidationError};

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CreateEventRequest {
    pub event_time: DateTime<Utc>,
    pub source: String,
    pub event_type: String,
    pub project_id: String,
    pub service: String,
    pub environment: String,
    pub subject_type: String,
    pub subject_id: String,
    pub title: String,
    pub content: String,
    #[serde(default)]
    pub trace_id: Option<String>,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default = "empty_metadata")]
    pub metadata: Value,
}

/// Internal application input for persisting an event with a preassigned ID.
///
/// This type is intentionally not deserializable and is never accepted by the
/// public HTTP API.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedContextEvent {
    pub event_id: Uuid,
    pub event_time: DateTime<Utc>,
    pub source: String,
    pub event_type: String,
    pub project_id: String,
    pub service: String,
    pub environment: String,
    pub subject_type: String,
    pub subject_id: String,
    pub title: String,
    pub content: String,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub metadata: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateEventResponse {
    pub event_id: Uuid,
    pub status: String,
}

impl CreateEventResponse {
    pub fn created(event_id: Uuid) -> Self {
        Self {
            event_id,
            status: "created".to_owned(),
        }
    }
}

/// HTTP query parameters, kept separate from the validated domain query.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SearchEventsRequest {
    pub project_id: Option<String>,
    pub service: Option<String>,
    pub environment: Option<String>,
    pub source: Option<String>,
    pub event_type: Option<String>,
    pub subject_type: Option<String>,
    pub subject_id: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    pub limit: Option<u32>,
}

impl TryFrom<SearchEventsRequest> for EventQuery {
    type Error = ValidationError;

    fn try_from(request: SearchEventsRequest) -> Result<Self, Self::Error> {
        Self::new(
            EventFilters {
                project_id: request.project_id,
                service: request.service,
                environment: request.environment,
                source: request.source,
                event_type: request.event_type,
                subject_type: request.subject_type,
                subject_id: request.subject_id,
            },
            request.from,
            request.to,
            request.limit,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EventResponse {
    pub event_id: Uuid,
    pub event_time: DateTime<Utc>,
    pub ingest_time: DateTime<Utc>,
    pub source: String,
    pub event_type: String,
    pub project_id: String,
    pub service: String,
    pub environment: String,
    pub subject_type: String,
    pub subject_id: String,
    pub title: String,
    pub content: String,
    pub trace_id: Option<String>,
    pub correlation_id: Option<String>,
    pub metadata: Value,
}

impl From<ContextEvent> for EventResponse {
    fn from(event: ContextEvent) -> Self {
        Self {
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
            trace_id: event.trace_id().map(str::to_owned),
            correlation_id: event.correlation_id().map(str::to_owned),
            metadata: event.metadata().clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SearchEventsResponse {
    pub count: usize,
    pub items: Vec<EventResponse>,
}

impl SearchEventsResponse {
    pub fn from_events(events: Vec<ContextEvent>) -> Self {
        let items: Vec<_> = events.into_iter().map(EventResponse::from).collect();
        Self {
            count: items.len(),
            items,
        }
    }
}

fn empty_metadata() -> Value {
    Value::Object(Map::new())
}
