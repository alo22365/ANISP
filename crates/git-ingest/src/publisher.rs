use std::{
    error::Error,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use anisp_event::{EventService, EventServiceError, RepositoryError};
use chrono::{DateTime, Utc};
use tokio::time::{MissedTickBehavior, interval};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::{ClaimedGitEvent, GitFailureDisposition, GitOutboxError, GitOutboxRepository};

#[derive(Debug, Clone)]
pub struct GitPublisherOptions {
    batch_size: u32,
    poll_interval: Duration,
    claim_lease: chrono::Duration,
    max_attempts: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct InvalidGitPublisherOptions;
impl fmt::Display for InvalidGitPublisherOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid Git publisher options")
    }
}
impl Error for InvalidGitPublisherOptions {}

impl GitPublisherOptions {
    pub fn new(
        batch_size: u32,
        poll_interval: Duration,
        claim_lease: Duration,
        max_attempts: u32,
    ) -> Result<Self, InvalidGitPublisherOptions> {
        if !(1..=500).contains(&batch_size)
            || poll_interval.is_zero()
            || claim_lease.is_zero()
            || max_attempts == 0
            || max_attempts > i32::MAX as u32
        {
            return Err(InvalidGitPublisherOptions);
        }
        let claim_lease =
            chrono::Duration::from_std(claim_lease).map_err(|_| InvalidGitPublisherOptions)?;
        Ok(Self {
            batch_size,
            poll_interval,
            claim_lease,
            max_attempts,
        })
    }
}
impl Default for GitPublisherOptions {
    fn default() -> Self {
        Self {
            batch_size: 100,
            poll_interval: Duration::from_secs(1),
            claim_lease: chrono::Duration::seconds(30),
            max_attempts: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GitPublishBatchResult {
    pub batch_size: usize,
    pub published_count: usize,
    pub failed_count: usize,
    pub exhausted_count: usize,
    pub pending_count: u64,
}

#[derive(Clone)]
pub struct GitOutboxPublisher {
    outbox: Arc<dyn GitOutboxRepository>,
    events: EventService,
    options: GitPublisherOptions,
    instance_id: Uuid,
}

impl GitOutboxPublisher {
    pub fn new(
        outbox: Arc<dyn GitOutboxRepository>,
        events: EventService,
        options: GitPublisherOptions,
    ) -> Self {
        Self {
            outbox,
            events,
            options,
            instance_id: Uuid::now_v7(),
        }
    }
    pub fn instance_id(&self) -> Uuid {
        self.instance_id
    }
    pub async fn publish_batch(&self) -> Result<GitPublishBatchResult, GitOutboxError> {
        self.publish_batch_with_clock(Utc::now(), true).await
    }
    /// Explicit clock lets tests advance backoff and leases without sleeping.
    pub async fn publish_batch_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<GitPublishBatchResult, GitOutboxError> {
        self.publish_batch_with_clock(now, false).await
    }

    async fn publish_batch_with_clock(
        &self,
        now: DateTime<Utc>,
        realtime: bool,
    ) -> Result<GitPublishBatchResult, GitOutboxError> {
        let started = Instant::now();
        let expired_before = now
            .checked_sub_signed(self.options.claim_lease)
            .ok_or(GitOutboxError::Internal)?;
        let claims = self
            .outbox
            .claim_pending(
                self.options.batch_size,
                self.instance_id,
                now,
                expired_before,
                self.options.max_attempts,
            )
            .await?;
        let mut result = GitPublishBatchResult {
            batch_size: claims.len(),
            ..Default::default()
        };
        for claim in claims {
            let publication = self.publish_one(&claim).await;
            match publication {
                Ok(already_exists) => {
                    match self
                        .outbox
                        .mark_published(&claim, self.instance_id, Utc::now())
                        .await
                    {
                        Ok(()) => result.published_count += 1,
                        Err(GitOutboxError::Conflict) => {
                            result.failed_count += 1;
                            warn!(publisher_instance_id = %self.instance_id, event_id = %claim.event_id, error_category = "claim_lost", "Git publication acknowledgement lost ownership");
                            continue;
                        }
                        Err(error) => return Err(error),
                    }
                    info!(publisher_instance_id = %self.instance_id, event_id = %claim.event_id, attempt_count = claim.attempt_count, already_exists,
                        operation = "git.outbox.publish", result = "success", "Git event published");
                }
                Err(category) => {
                    result.failed_count += 1;
                    let exhausted = claim.attempt_count >= self.options.max_attempts;
                    // Real retries start at FAILURE time, not batch start. A
                    // slow request/earlier rows must not consume the backoff.
                    // Explicit test-clock calls deliberately keep time frozen.
                    let failed_at = if realtime { Utc::now() } else { now };
                    let disposition = if exhausted {
                        result.exhausted_count += 1;
                        GitFailureDisposition::Exhausted
                    } else {
                        GitFailureDisposition::RetryAt(
                            failed_at
                                .checked_add_signed(retry_backoff(claim.attempt_count))
                                .ok_or(GitOutboxError::Internal)?,
                        )
                    };
                    match self
                        .outbox
                        .record_failure(&claim, self.instance_id, disposition)
                        .await
                    {
                        Ok(()) | Err(GitOutboxError::Conflict) => {}
                        Err(error) => return Err(error),
                    }
                    warn!(publisher_instance_id = %self.instance_id, event_id = %claim.event_id, attempt_count = claim.attempt_count,
                        error_category = category, exhausted, operation = "git.outbox.publish", result = "failure", "Git publication failed");
                }
            }
        }
        result.pending_count = self.outbox.operational_status().await?.pending_events;
        let duration_ms = started.elapsed().as_millis() as u64;
        if result.batch_size == 0 {
            debug!(publisher_instance_id = %self.instance_id, batch_size = result.batch_size, published_count = result.published_count,
                failed_count = result.failed_count, pending_count = result.pending_count, duration_ms, operation = "git.outbox.publish_batch", "Git outbox poll completed");
        } else {
            info!(publisher_instance_id = %self.instance_id, batch_size = result.batch_size, published_count = result.published_count,
                failed_count = result.failed_count, pending_count = result.pending_count, duration_ms, operation = "git.outbox.publish_batch", "Git outbox batch completed");
        }
        Ok(result)
    }

    async fn publish_one(&self, claim: &ClaimedGitEvent) -> Result<bool, &'static str> {
        let projection = claim.projection.as_ref().map_err(|_| "decode")?;
        if projection.event_id() != claim.event_id {
            return Err("decode");
        }
        let exists = self
            .events
            .get_event(claim.event_id)
            .await
            .map_err(event_error_category)?
            .is_some();
        if !exists {
            self.events
                .persist_prepared_event(projection.prepared_event())
                .await
                .map_err(event_error_category)?;
        }
        Ok(exists)
    }

    /// Polls ONLY the PostgreSQL outbox. Never scans Git repositories.
    pub async fn run(self) {
        let mut ticker = interval(self.options.poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if let Err(error) = self.publish_batch().await {
                warn!(publisher_instance_id = %self.instance_id, error_category = %error,
                    operation = "git.outbox.publish_batch", result = "failure", "Git outbox poll failed");
            }
        }
    }
}

pub fn retry_backoff(attempt_count: u32) -> chrono::Duration {
    chrono::Duration::seconds((1_i64 << attempt_count.saturating_sub(1).min(6)).min(60))
}
fn event_error_category(error: EventServiceError) -> &'static str {
    match error {
        EventServiceError::Validation(_) => "validation",
        EventServiceError::Repository(RepositoryError::Unavailable(_)) => "unavailable",
        EventServiceError::Repository(RepositoryError::Timeout(_)) => "timeout",
        EventServiceError::Repository(RepositoryError::Decode(_)) => "decode",
        EventServiceError::Repository(RepositoryError::Internal(_)) => "internal",
    }
}
