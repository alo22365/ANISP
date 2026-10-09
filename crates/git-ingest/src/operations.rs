//! Aggregate operational visibility, not an ingestion or delivery-control API.
use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::GitOutboxError;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GitOutboxOperationalStatus {
    /// Unpublished, non-exhausted rows, including claimed/backoff rows.
    pub pending_events: u64,
    /// Retained unpublished rows no longer automatically retried.
    pub exhausted_events: u64,
    /// Pending rows with an owner and an unexpired configured lease.
    pub active_claims: u64,
    /// Age since ingestion/created_at, not the commit's historical timestamp.
    pub oldest_pending_age_seconds: Option<u64>,
    pub last_publish_success_at: Option<DateTime<Utc>>,
}

/// Storage-independent, aggregate-only read port. No payload or claim detail
/// crosses this boundary. Published rows remain the ingestion identity ledger.
#[async_trait]
pub trait GitOutboxOperationalReader: Send + Sync {
    async fn read_operational_status(
        &self,
        now: DateTime<Utc>,
        lease_expired_before: DateTime<Utc>,
    ) -> Result<GitOutboxOperationalStatus, GitOutboxError>;
}

#[derive(Clone)]
pub struct GitOutboxStatusService {
    reader: Arc<dyn GitOutboxOperationalReader>,
    claim_lease: chrono::Duration,
}

impl GitOutboxStatusService {
    pub fn new(
        reader: Arc<dyn GitOutboxOperationalReader>,
        claim_lease: Duration,
    ) -> Result<Self, GitOutboxError> {
        if claim_lease.is_zero() {
            return Err(GitOutboxError::Internal);
        }
        let claim_lease =
            chrono::Duration::from_std(claim_lease).map_err(|_| GitOutboxError::Internal)?;
        Ok(Self {
            reader,
            claim_lease,
        })
    }

    pub async fn status(&self) -> Result<GitOutboxOperationalStatus, GitOutboxError> {
        self.status_at(Utc::now()).await
    }

    /// Explicit clock for deterministic tests; not an HTTP input.
    pub async fn status_at(
        &self,
        now: DateTime<Utc>,
    ) -> Result<GitOutboxOperationalStatus, GitOutboxError> {
        let expired = now
            .checked_sub_signed(self.claim_lease)
            .ok_or(GitOutboxError::Internal)?;
        self.reader.read_operational_status(now, expired).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Reader;
    #[async_trait]
    impl GitOutboxOperationalReader for Reader {
        async fn read_operational_status(
            &self,
            now: DateTime<Utc>,
            expired: DateTime<Utc>,
        ) -> Result<GitOutboxOperationalStatus, GitOutboxError> {
            assert_eq!(now - expired, chrono::Duration::seconds(30));
            Ok(GitOutboxOperationalStatus::default())
        }
    }
    #[tokio::test]
    async fn service_passes_the_configured_lease_and_rejects_invalid_clocks() {
        let service =
            GitOutboxStatusService::new(Arc::new(Reader), Duration::from_secs(30)).unwrap();
        assert_eq!(
            service.status_at(Utc::now()).await.unwrap(),
            Default::default()
        );
        assert!(matches!(
            service.status_at(DateTime::<Utc>::MIN_UTC).await,
            Err(GitOutboxError::Internal)
        ));
        assert!(GitOutboxStatusService::new(Arc::new(Reader), Duration::ZERO).is_err());
        assert!(GitOutboxStatusService::new(Arc::new(Reader), Duration::MAX).is_err());
    }
}
