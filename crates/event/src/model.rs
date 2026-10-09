use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

/// Framework- and storage-independent context event.
#[derive(Debug, Clone, PartialEq)]
pub struct ContextEvent {
    event_id: Uuid,
    event_time: DateTime<Utc>,
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
    trace_id: Option<String>,
    correlation_id: Option<String>,
    metadata: Value,
}

/// Storage-neutral values used to reconstruct an event returned by a repository.
pub struct StoredContextEvent {
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

impl ContextEvent {
    pub fn from_stored(stored: StoredContextEvent) -> Result<Self, crate::ValidationError> {
        if !stored.metadata.is_object() {
            return Err(crate::ValidationError::new(
                "metadata",
                "must be a JSON object",
            ));
        }

        Ok(Self::new(
            stored.event_id,
            stored.event_time,
            stored.ingest_time,
            stored.source,
            stored.event_type,
            stored.project_id,
            stored.service,
            stored.environment,
            stored.subject_type,
            stored.subject_id,
            stored.title,
            stored.content,
            stored.trace_id,
            stored.correlation_id,
            stored.metadata,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        event_id: Uuid,
        event_time: DateTime<Utc>,
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
        trace_id: Option<String>,
        correlation_id: Option<String>,
        metadata: Value,
    ) -> Self {
        Self {
            event_id,
            event_time,
            ingest_time,
            source,
            event_type,
            project_id,
            service,
            environment,
            subject_type,
            subject_id,
            title,
            content,
            trace_id,
            correlation_id,
            metadata,
        }
    }

    pub fn event_id(&self) -> Uuid {
        self.event_id
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        self.event_time
    }

    pub fn ingest_time(&self) -> DateTime<Utc> {
        self.ingest_time
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn event_type(&self) -> &str {
        &self.event_type
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn service(&self) -> &str {
        &self.service
    }

    pub fn environment(&self) -> &str {
        &self.environment
    }

    pub fn subject_type(&self) -> &str {
        &self.subject_type
    }

    pub fn subject_id(&self) -> &str {
        &self.subject_id
    }

    pub fn title(&self) -> &str {
        &self.title
    }

    pub fn content(&self) -> &str {
        &self.content
    }

    pub fn trace_id(&self) -> Option<&str> {
        self.trace_id.as_deref()
    }

    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    pub fn metadata(&self) -> &Value {
        &self.metadata
    }
}
