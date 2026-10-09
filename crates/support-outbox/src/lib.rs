//! Operationally hardened publication of SupportCase lifecycle outbox records.

use std::{error::Error, fmt, sync::Arc, time::Duration};

use anisp_event::{EventService, EventServiceError, PreparedContextEvent, RepositoryError};
use anisp_support::{
    CaseStatus, CaseType, OutboxFailureDisposition, Priority, SupportCaseLifecycleEvent,
    SupportCaseLifecycleKind, SupportOutboxOperationalReader, SupportOutboxOperationalStatus,
    SupportOutboxRepository, SupportRepositoryError,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};
use uuid::Uuid;

#[derive(Clone)]
pub struct SupportOutboxStatusService {
    reader: Arc<dyn SupportOutboxOperationalReader>,
}

impl SupportOutboxStatusService {
    pub fn new(reader: Arc<dyn SupportOutboxOperationalReader>) -> Self {
        Self { reader }
    }

    pub async fn status(&self) -> Result<SupportOutboxOperationalStatus, SupportRepositoryError> {
        self.reader.operational_status(Utc::now()).await
    }
}

#[derive(Clone)]
pub struct SupportOutboxPublisher {
    outbox: Arc<dyn SupportOutboxRepository>,
    event_service: EventService,
    poll_interval: Duration,
    batch_size: u32,
    claim_lease: Duration,
    max_attempts: u32,
    instance_id: String,
}

impl SupportOutboxPublisher {
    pub fn new(
        outbox: Arc<dyn SupportOutboxRepository>,
        event_service: EventService,
        poll_interval: Duration,
        batch_size: u32,
        claim_lease: Duration,
        max_attempts: u32,
    ) -> Self {
        Self::with_instance_id(
            outbox,
            event_service,
            poll_interval,
            batch_size,
            claim_lease,
            max_attempts,
            Uuid::now_v7().to_string(),
        )
    }

    pub fn with_instance_id(
        outbox: Arc<dyn SupportOutboxRepository>,
        event_service: EventService,
        poll_interval: Duration,
        batch_size: u32,
        claim_lease: Duration,
        max_attempts: u32,
        instance_id: String,
    ) -> Self {
        Self {
            outbox,
            event_service,
            poll_interval,
            batch_size,
            claim_lease,
            max_attempts,
            instance_id,
        }
    }

    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    pub async fn publish_batch(&self) -> Result<PublishBatchResult, PublisherError> {
        self.publish_batch_at(Utc::now()).await
    }

    /// Deterministic clock entry point used by unit and integration tests.
    pub async fn publish_batch_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<PublishBatchResult, PublisherError> {
        let started = std::time::Instant::now();
        let lease = chrono::Duration::from_std(self.claim_lease)
            .map_err(PublisherError::InvalidClaimLease)?;
        let claimed = self
            .outbox
            .claim_pending(self.batch_size, &self.instance_id, now, now - lease)
            .await
            .map_err(PublisherError::Outbox)?;
        let batch_size = claimed.len();
        let mut published_count = 0;
        let mut failed_count = 0;
        let mut exhausted_count = 0;

        for claimed_event in claimed {
            let attempt_count = claimed_event.attempt_count();
            let lifecycle_event = claimed_event.event();
            let event_id = lifecycle_event.event_id();
            let publication = self.publish_one(lifecycle_event).await;

            match publication {
                Ok(already_exists) => {
                    self.outbox
                        .mark_published(event_id, &self.instance_id, Utc::now())
                        .await
                        .map_err(PublisherError::Outbox)?;
                    published_count += 1;
                    info!(
                        publisher_instance_id = %self.instance_id,
                        event_id = %event_id,
                        attempt_count,
                        already_exists,
                        operation = "support.outbox.publish",
                        result = "success",
                        "support lifecycle event published"
                    );
                }
                Err(error) => {
                    failed_count += 1;
                    let exhausted = attempt_count >= self.max_attempts;
                    let disposition = if exhausted {
                        exhausted_count += 1;
                        OutboxFailureDisposition::Exhausted
                    } else {
                        OutboxFailureDisposition::RetryAt(now + retry_backoff(attempt_count))
                    };
                    self.outbox
                        .record_failure(event_id, &self.instance_id, Utc::now(), disposition)
                        .await
                        .map_err(PublisherError::Outbox)?;
                    warn!(
                        publisher_instance_id = %self.instance_id,
                        event_id = %event_id,
                        attempt_count,
                        error_category = event_error_category(&error),
                        exhausted,
                        operation = "support.outbox.publish",
                        result = "failure",
                        "support lifecycle event publication failed"
                    );
                }
            }
        }

        let status = self
            .outbox
            .operational_status(Utc::now())
            .await
            .map_err(PublisherError::Outbox)?;
        let result = PublishBatchResult {
            batch_size,
            published_count,
            failed_count,
            exhausted_count,
            pending_count: status.pending_events(),
        };
        let duration_ms = started.elapsed().as_millis() as u64;
        if result.batch_size == 0 {
            debug!(
                publisher_instance_id = %self.instance_id,
                batch_size = result.batch_size,
                published_count = result.published_count,
                failed_count = result.failed_count,
                pending_count = result.pending_count,
                duration_ms,
                operation = "support.outbox.publish_batch",
                "support outbox poll completed without eligible events"
            );
        } else {
            info!(
                publisher_instance_id = %self.instance_id,
                batch_size = result.batch_size,
                published_count = result.published_count,
                failed_count = result.failed_count,
                pending_count = result.pending_count,
                duration_ms,
                operation = "support.outbox.publish_batch",
                "support outbox batch completed"
            );
        }
        Ok(result)
    }

    async fn publish_one(
        &self,
        lifecycle_event: &SupportCaseLifecycleEvent,
    ) -> Result<bool, EventServiceError> {
        let already_exists = self
            .event_service
            .get_event(lifecycle_event.event_id())
            .await?
            .is_some();
        if !already_exists {
            self.event_service
                .persist_prepared_event(map_lifecycle_event(lifecycle_event))
                .await?;
        }
        Ok(already_exists)
    }

    /// Runs until the Tokio task is cancelled. A failed poll is logged and the
    /// next tick continues; publisher failures never panic the server.
    pub async fn run(self) {
        let mut ticker = interval(self.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = self.publish_batch().await {
                warn!(
                    publisher_instance_id = %self.instance_id,
                    error = %error,
                    operation = "support.outbox.publish_batch",
                    result = "failure",
                    "support outbox poll failed"
                );
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishBatchResult {
    pub batch_size: usize,
    pub published_count: usize,
    pub failed_count: usize,
    pub exhausted_count: usize,
    pub pending_count: u64,
}

#[derive(Debug)]
pub enum PublisherError {
    Outbox(SupportRepositoryError),
    InvalidClaimLease(chrono::OutOfRangeError),
}

impl fmt::Display for PublisherError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Outbox(_) => formatter.write_str("support outbox repository operation failed"),
            Self::InvalidClaimLease(_) => {
                formatter.write_str("support outbox claim lease is invalid")
            }
        }
    }
}

impl Error for PublisherError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Outbox(error) => Some(error),
            Self::InvalidClaimLease(error) => Some(error),
        }
    }
}

fn retry_backoff(attempt_count: u32) -> chrono::Duration {
    let exponent = attempt_count.saturating_sub(1).min(6);
    chrono::Duration::seconds((1_i64 << exponent).min(60))
}

fn event_error_category(error: &EventServiceError) -> &'static str {
    match error {
        EventServiceError::Validation(_) => "validation",
        EventServiceError::Repository(RepositoryError::Unavailable(_)) => "unavailable",
        EventServiceError::Repository(RepositoryError::Timeout(_)) => "timeout",
        EventServiceError::Repository(RepositoryError::Decode(_)) => "decode",
        EventServiceError::Repository(RepositoryError::Internal(_)) => "internal",
    }
}

fn map_lifecycle_event(event: &SupportCaseLifecycleEvent) -> PreparedContextEvent {
    let case_id = event.case_id().to_string();
    let previous_status = event.previous_status().map(status_str);
    let current_status = status_str(event.current_status());
    PreparedContextEvent {
        event_id: event.event_id(),
        event_time: event.occurred_at(),
        source: "support".to_owned(),
        event_type: event.event_type(),
        project_id: event.project_id().unwrap_or("unassigned").to_owned(),
        service: event.service().unwrap_or("unassigned").to_owned(),
        environment: event.environment().unwrap_or("unassigned").to_owned(),
        subject_type: "support_case".to_owned(),
        subject_id: case_id.clone(),
        title: event.title().to_owned(),
        content: lifecycle_content(event.kind(), previous_status, current_status),
        trace_id: None,
        correlation_id: Some(case_id.clone()),
        metadata: json!({
            "case_id": case_id,
            "case_type": case_type_str(event.case_type()),
            "priority": priority_str(event.priority()),
            "reporter_id": event.reporter_id(),
            "assignee_id": event.assignee_id(),
            "previous_status": previous_status,
            "current_status": current_status,
        }),
    }
}

fn lifecycle_content(
    kind: SupportCaseLifecycleKind,
    previous_status: Option<&str>,
    current_status: &str,
) -> String {
    match kind {
        SupportCaseLifecycleKind::Created => "Support case created".to_owned(),
        _ => format!(
            "Support case status changed from {} to {current_status}",
            previous_status.unwrap_or("unknown")
        ),
    }
}

fn case_type_str(value: CaseType) -> &'static str {
    match value {
        CaseType::Ticket => "ticket",
        CaseType::Incident => "incident",
        CaseType::Request => "request",
    }
}

fn priority_str(value: Priority) -> &'static str {
    match value {
        Priority::Low => "low",
        Priority::Medium => "medium",
        Priority::High => "high",
        Priority::Critical => "critical",
    }
}

fn status_str(value: CaseStatus) -> &'static str {
    match value {
        CaseStatus::Open => "open",
        CaseStatus::Investigating => "investigating",
        CaseStatus::Resolved => "resolved",
        CaseStatus::Closed => "closed",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use anisp_event::{ContextEvent, EventQuery, EventRepository};
    use anisp_support::{ClaimedSupportLifecycleEvent, CreateSupportCaseCommand, SupportCase};
    use async_trait::async_trait;

    use super::*;

    #[derive(Clone)]
    struct FakeOutboxEntry {
        event: SupportCaseLifecycleEvent,
        attempt_count: u32,
        claimed_at: Option<DateTime<Utc>>,
        claimed_by: Option<String>,
        next_attempt_at: DateTime<Utc>,
        published_at: Option<DateTime<Utc>>,
        exhausted_at: Option<DateTime<Utc>>,
    }

    struct FakeOutbox {
        entries: Mutex<Vec<FakeOutboxEntry>>,
    }

    #[async_trait]
    impl SupportOutboxOperationalReader for FakeOutbox {
        async fn operational_status(
            &self,
            now: DateTime<Utc>,
        ) -> Result<SupportOutboxOperationalStatus, SupportRepositoryError> {
            let entries = self.entries.lock().unwrap();
            let pending: Vec<_> = entries
                .iter()
                .filter(|entry| entry.published_at.is_none() && entry.exhausted_at.is_none())
                .collect();
            let oldest = pending.iter().map(|entry| entry.event.occurred_at()).min();
            Ok(SupportOutboxOperationalStatus::new(
                pending.len() as u64,
                oldest.map(|time| now.signed_duration_since(time).num_seconds().max(0) as u64),
                entries
                    .iter()
                    .filter(|entry| entry.published_at.is_none() && entry.exhausted_at.is_some())
                    .count() as u64,
                entries.iter().filter_map(|entry| entry.published_at).max(),
            ))
        }
    }

    #[async_trait]
    impl SupportOutboxRepository for FakeOutbox {
        async fn claim_pending(
            &self,
            limit: u32,
            instance_id: &str,
            claimed_at: DateTime<Utc>,
            lease_expired_before: DateTime<Utc>,
        ) -> Result<Vec<ClaimedSupportLifecycleEvent>, SupportRepositoryError> {
            let mut entries = self.entries.lock().unwrap();
            let mut claimed = Vec::new();
            for entry in entries
                .iter_mut()
                .filter(|entry| {
                    entry.published_at.is_none()
                        && entry.exhausted_at.is_none()
                        && entry.next_attempt_at <= claimed_at
                        && entry
                            .claimed_at
                            .is_none_or(|time| time <= lease_expired_before)
                })
                .take(limit as usize)
            {
                entry.claimed_at = Some(claimed_at);
                entry.claimed_by = Some(instance_id.to_owned());
                entry.attempt_count += 1;
                claimed.push(ClaimedSupportLifecycleEvent::new(
                    entry.event.clone(),
                    entry.attempt_count,
                ));
            }
            Ok(claimed)
        }

        async fn mark_published(
            &self,
            event_id: Uuid,
            instance_id: &str,
            published_at: DateTime<Utc>,
        ) -> Result<(), SupportRepositoryError> {
            let mut entries = self.entries.lock().unwrap();
            let entry = entries
                .iter_mut()
                .find(|entry| {
                    entry.event.event_id() == event_id
                        && entry.claimed_by.as_deref() == Some(instance_id)
                })
                .ok_or_else(SupportRepositoryError::conflict)?;
            entry.published_at = Some(published_at);
            entry.claimed_at = None;
            entry.claimed_by = None;
            Ok(())
        }

        async fn record_failure(
            &self,
            event_id: Uuid,
            instance_id: &str,
            failed_at: DateTime<Utc>,
            disposition: OutboxFailureDisposition,
        ) -> Result<(), SupportRepositoryError> {
            let mut entries = self.entries.lock().unwrap();
            let entry = entries
                .iter_mut()
                .find(|entry| {
                    entry.event.event_id() == event_id
                        && entry.claimed_by.as_deref() == Some(instance_id)
                })
                .ok_or_else(SupportRepositoryError::conflict)?;
            entry.claimed_at = None;
            entry.claimed_by = None;
            match disposition {
                OutboxFailureDisposition::RetryAt(time) => entry.next_attempt_at = time,
                OutboxFailureDisposition::Exhausted => entry.exhausted_at = Some(failed_at),
            }
            Ok(())
        }
    }

    struct FakeEventRepository {
        available: AtomicBool,
        events: Mutex<HashMap<Uuid, ContextEvent>>,
        inserts: AtomicUsize,
    }

    #[async_trait]
    impl EventRepository for FakeEventRepository {
        async fn insert(&self, event: &ContextEvent) -> Result<(), RepositoryError> {
            if !self.available.load(Ordering::SeqCst) {
                return Err(RepositoryError::unavailable());
            }
            self.inserts.fetch_add(1, Ordering::SeqCst);
            self.events
                .lock()
                .unwrap()
                .insert(event.event_id(), event.clone());
            Ok(())
        }

        async fn find_by_id(
            &self,
            event_id: Uuid,
        ) -> Result<Option<ContextEvent>, RepositoryError> {
            if !self.available.load(Ordering::SeqCst) {
                return Err(RepositoryError::unavailable());
            }
            Ok(self.events.lock().unwrap().get(&event_id).cloned())
        }

        async fn search(&self, _query: &EventQuery) -> Result<Vec<ContextEvent>, RepositoryError> {
            Ok(self.events.lock().unwrap().values().cloned().collect())
        }
    }

    fn lifecycle_event() -> SupportCaseLifecycleEvent {
        SupportCase::create(CreateSupportCaseCommand {
            case_type: CaseType::Incident,
            title: "xpa-finance unavailable".to_owned(),
            description: "must not be copied".to_owned(),
            reporter_id: "user-001".to_owned(),
            assignee_id: Some("agent-001".to_owned()),
            project_id: Some("xpa".to_owned()),
            service: Some("xpa-finance".to_owned()),
            environment: Some("prod".to_owned()),
            priority: Priority::High,
        })
        .unwrap()
        .created_event()
    }

    fn harness(
        available: bool,
        max_attempts: u32,
    ) -> (
        SupportOutboxPublisher,
        Arc<FakeOutbox>,
        Arc<FakeEventRepository>,
        DateTime<Utc>,
    ) {
        let event = lifecycle_event();
        let now = event.occurred_at();
        let outbox = Arc::new(FakeOutbox {
            entries: Mutex::new(vec![FakeOutboxEntry {
                event,
                attempt_count: 0,
                claimed_at: None,
                claimed_by: None,
                next_attempt_at: now,
                published_at: None,
                exhausted_at: None,
            }]),
        });
        let repository = Arc::new(FakeEventRepository {
            available: AtomicBool::new(available),
            events: Mutex::new(HashMap::new()),
            inserts: AtomicUsize::new(0),
        });
        let publisher = SupportOutboxPublisher::with_instance_id(
            outbox.clone(),
            EventService::new(repository.clone()),
            Duration::from_millis(10),
            10,
            Duration::from_secs(30),
            max_attempts,
            "publisher-a".to_owned(),
        );
        (publisher, outbox, repository, now)
    }

    #[tokio::test]
    async fn publisher_success_maps_marks_and_reports_status() {
        let (publisher, outbox, repository, now) = harness(true, 10);
        let result = publisher.publish_batch_at(now).await.unwrap();
        assert_eq!(result.published_count, 1);
        assert_eq!(result.pending_count, 0);
        let events = repository.events.lock().unwrap();
        let event = events.values().next().unwrap();
        assert_eq!(event.event_type(), "support.incident.created");
        assert!(!event.content().contains("must not be copied"));
        assert!(outbox.entries.lock().unwrap()[0].published_at.is_some());
    }

    #[tokio::test]
    async fn failure_uses_backoff_then_recovers() {
        let (publisher, outbox, repository, now) = harness(false, 10);
        let failed = publisher.publish_batch_at(now).await.unwrap();
        assert_eq!(failed.failed_count, 1);
        assert_eq!(
            outbox.entries.lock().unwrap()[0].next_attempt_at,
            now + chrono::Duration::seconds(1)
        );
        assert_eq!(publisher.publish_batch_at(now).await.unwrap().batch_size, 0);
        repository.available.store(true, Ordering::SeqCst);
        assert_eq!(
            publisher
                .publish_batch_at(now + chrono::Duration::seconds(1))
                .await
                .unwrap()
                .published_count,
            1
        );
    }

    #[tokio::test]
    async fn reaches_exhausted_and_stops_claiming() {
        let (publisher, outbox, _repository, now) = harness(false, 1);
        let result = publisher.publish_batch_at(now).await.unwrap();
        assert_eq!(result.exhausted_count, 1);
        assert_eq!(result.pending_count, 0);
        assert!(outbox.entries.lock().unwrap()[0].exhausted_at.is_some());
        assert_eq!(
            publisher
                .publish_batch_at(now + chrono::Duration::seconds(60))
                .await
                .unwrap()
                .batch_size,
            0
        );
    }

    #[tokio::test]
    async fn claim_is_exclusive_and_expired_lease_is_reclaimable() {
        let (_publisher, outbox, _repository, now) = harness(true, 10);
        assert_eq!(
            outbox
                .claim_pending(1, "a", now, now - chrono::Duration::seconds(30))
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            outbox
                .claim_pending(1, "b", now, now - chrono::Duration::seconds(30))
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            outbox
                .claim_pending(
                    1,
                    "b",
                    now + chrono::Duration::seconds(31),
                    now + chrono::Duration::seconds(1)
                )
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn two_publishers_create_one_logical_event() {
        let (publisher_a, outbox, repository, now) = harness(true, 10);
        let publisher_b = SupportOutboxPublisher::with_instance_id(
            outbox,
            EventService::new(repository.clone()),
            Duration::from_millis(10),
            10,
            Duration::from_secs(30),
            10,
            "publisher-b".to_owned(),
        );
        let (a, b) = tokio::join!(
            publisher_a.publish_batch_at(now),
            publisher_b.publish_batch_at(now)
        );
        assert_eq!(a.unwrap().published_count + b.unwrap().published_count, 1);
        assert_eq!(repository.events.lock().unwrap().len(), 1);
        assert_eq!(repository.inserts.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn backoff_is_exponential_and_capped_at_sixty_seconds() {
        assert_eq!(retry_backoff(1), chrono::Duration::seconds(1));
        assert_eq!(retry_backoff(2), chrono::Duration::seconds(2));
        assert_eq!(retry_backoff(3), chrono::Duration::seconds(4));
        assert_eq!(retry_backoff(10), chrono::Duration::seconds(60));
    }
}
