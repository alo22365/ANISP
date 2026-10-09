use std::sync::Arc;

use anisp_git_context::ObservedGitCommit;
use tracing::info;
use uuid::Uuid;

use crate::{GitCommitProjection, GitIngestError, GitIngestOutcome, GitOutboxRepository};

#[derive(Clone)]
pub struct GitIngestService {
    outbox: Arc<dyn GitOutboxRepository>,
}
impl GitIngestService {
    pub fn new(outbox: Arc<dyn GitOutboxRepository>) -> Self {
        Self { outbox }
    }
    pub async fn ingest(
        &self,
        observed: ObservedGitCommit,
    ) -> Result<GitIngestOutcome, GitIngestError> {
        let repository_id = observed.repository().repository_id();
        let commit_sha = observed.commit().commit_sha().as_str();
        let outcome = if let Some(id) = self
            .outbox
            .find_ingested_event_id(repository_id, commit_sha)
            .await?
        {
            GitIngestOutcome::AlreadyIngested(id)
        } else {
            let projection = GitCommitProjection::from_observation(&observed, Uuid::now_v7())?;
            // Unique constraint, not this optimistic read, arbitrates concurrent
            // ingestion. A losing generated UUID is never persisted/published.
            self.outbox.insert_if_absent(&projection).await?
        };
        info!(event_id = %outcome.event_id(), repository_id, commit_sha,
            operation = "git.commit.ingest", result = match outcome { GitIngestOutcome::Accepted(_) => "accepted", GitIngestOutcome::AlreadyIngested(_) => "already_ingested" },
            "Git observation ingestion completed");
        Ok(outcome)
    }
}
