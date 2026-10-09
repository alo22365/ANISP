use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{GitCommitProjection, GitOutboxError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitIngestOutcome {
    Accepted(Uuid),
    AlreadyIngested(Uuid),
}
impl GitIngestOutcome {
    pub fn event_id(self) -> Uuid {
        match self {
            Self::Accepted(id) | Self::AlreadyIngested(id) => id,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClaimedGitEvent {
    pub event_id: Uuid,
    /// A corrupt payload is still a claimed work item: it must receive bounded
    /// retries/exhaustion instead of poisoning the whole batch indefinitely.
    pub projection: Result<GitCommitProjection, GitOutboxError>,
    /// Generation token prevents an expired owner acknowledging a newer claim.
    pub attempt_count: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum GitFailureDisposition {
    RetryAt(DateTime<Utc>),
    Exhausted,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GitOutboxStatus {
    pub pending_events: u64,
    pub exhausted_events: u64,
}

/// Application-owned, storage-neutral port. Insert must atomically arbitrate
/// identity conflicts; the initial read is only an optimization.
#[async_trait]
pub trait GitOutboxRepository: Send + Sync {
    async fn find_ingested_event_id(
        &self,
        repository_id: &str,
        commit_sha: &str,
    ) -> Result<Option<Uuid>, GitOutboxError>;
    async fn insert_if_absent(
        &self,
        projection: &GitCommitProjection,
    ) -> Result<GitIngestOutcome, GitOutboxError>;
    async fn claim_pending(
        &self,
        limit: u32,
        instance_id: Uuid,
        now: DateTime<Utc>,
        lease_expired_before: DateTime<Utc>,
        max_attempts: u32,
    ) -> Result<Vec<ClaimedGitEvent>, GitOutboxError>;
    async fn mark_published(
        &self,
        claim: &ClaimedGitEvent,
        instance_id: Uuid,
        published_at: DateTime<Utc>,
    ) -> Result<(), GitOutboxError>;
    async fn record_failure(
        &self,
        claim: &ClaimedGitEvent,
        instance_id: Uuid,
        disposition: GitFailureDisposition,
    ) -> Result<(), GitOutboxError>;
    async fn operational_status(&self) -> Result<GitOutboxStatus, GitOutboxError>;
}
