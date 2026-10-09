use crate::GitReaderError;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// Narrow projection-freshness port. Counts ALL unpublished Git intents for
/// the exact project/service and half-open commit-time window, including
/// exhausted/claimed/backoff rows. Does not expose delivery internals.
#[async_trait]
pub trait GitContextSyncReader: Send + Sync {
    async fn pending_count_for_window(
        &self,
        project_id: &str,
        service: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<u64, GitReaderError>;
}
