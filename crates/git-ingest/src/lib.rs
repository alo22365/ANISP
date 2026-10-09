//! Git ingestion application and delivery ports. No database, Git provider,
//! HTTP adapter, or repository polling belongs here.

mod error;
mod operations;
mod projection;
mod publisher;
mod repository;
mod service;

pub use error::{GitIngestError, GitOutboxError, GitProjectionError};
pub use operations::{
    GitOutboxOperationalReader, GitOutboxOperationalStatus, GitOutboxStatusService,
};
pub use projection::GitCommitProjection;
pub use publisher::{
    GitOutboxPublisher, GitPublishBatchResult, GitPublisherOptions, InvalidGitPublisherOptions,
    retry_backoff,
};
pub use repository::{
    ClaimedGitEvent, GitFailureDisposition, GitIngestOutcome, GitOutboxRepository, GitOutboxStatus,
};
pub use service::GitIngestService;

#[cfg(test)]
mod tests;
