use async_trait::async_trait;
use uuid::Uuid;

use chrono::{DateTime, Utc};

use crate::{SupportCase, SupportCaseLifecycleEvent, SupportCaseQuery, SupportRepositoryError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedSupportLifecycleEvent {
    event: SupportCaseLifecycleEvent,
    attempt_count: u32,
}

impl ClaimedSupportLifecycleEvent {
    pub fn new(event: SupportCaseLifecycleEvent, attempt_count: u32) -> Self {
        Self {
            event,
            attempt_count,
        }
    }

    pub fn event(&self) -> &SupportCaseLifecycleEvent {
        &self.event
    }

    pub fn into_event(self) -> SupportCaseLifecycleEvent {
        self.event
    }

    pub fn attempt_count(&self) -> u32 {
        self.attempt_count
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxFailureDisposition {
    RetryAt(DateTime<Utc>),
    Exhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportOutboxOperationalStatus {
    pending_events: u64,
    oldest_pending_age_seconds: Option<u64>,
    exhausted_events: u64,
    last_publish_success_at: Option<DateTime<Utc>>,
}

impl SupportOutboxOperationalStatus {
    pub fn new(
        pending_events: u64,
        oldest_pending_age_seconds: Option<u64>,
        exhausted_events: u64,
        last_publish_success_at: Option<DateTime<Utc>>,
    ) -> Self {
        Self {
            pending_events,
            oldest_pending_age_seconds,
            exhausted_events,
            last_publish_success_at,
        }
    }

    pub fn pending_events(&self) -> u64 {
        self.pending_events
    }

    pub fn oldest_pending_age_seconds(&self) -> Option<u64> {
        self.oldest_pending_age_seconds
    }

    pub fn exhausted_events(&self) -> u64 {
        self.exhausted_events
    }

    pub fn last_publish_success_at(&self) -> Option<DateTime<Utc>> {
        self.last_publish_success_at
    }
}

/// Persistence port owned by the Support Domain.
#[async_trait]
pub trait SupportCaseRepository: Send + Sync {
    async fn insert(
        &self,
        support_case: &SupportCase,
        lifecycle_event: &SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError>;

    async fn update(
        &self,
        support_case: &SupportCase,
        lifecycle_event: &SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError>;

    async fn find_by_id(
        &self,
        case_id: Uuid,
    ) -> Result<Option<SupportCase>, SupportRepositoryError>;

    async fn search(
        &self,
        query: &SupportCaseQuery,
    ) -> Result<Vec<SupportCase>, SupportRepositoryError>;
}

#[async_trait]
pub trait SupportOutboxOperationalReader: Send + Sync {
    async fn operational_status(
        &self,
        now: DateTime<Utc>,
    ) -> Result<SupportOutboxOperationalStatus, SupportRepositoryError>;
}

#[async_trait]
pub trait SupportOutboxRepository: SupportOutboxOperationalReader + Send + Sync {
    async fn claim_pending(
        &self,
        limit: u32,
        instance_id: &str,
        claimed_at: DateTime<Utc>,
        lease_expired_before: DateTime<Utc>,
    ) -> Result<Vec<ClaimedSupportLifecycleEvent>, SupportRepositoryError>;

    async fn mark_published(
        &self,
        event_id: Uuid,
        instance_id: &str,
        published_at: DateTime<Utc>,
    ) -> Result<(), SupportRepositoryError>;

    async fn record_failure(
        &self,
        event_id: Uuid,
        instance_id: &str,
        failed_at: DateTime<Utc>,
        disposition: OutboxFailureDisposition,
    ) -> Result<(), SupportRepositoryError>;
}
