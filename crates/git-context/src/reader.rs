use crate::{CommitSha, GitCommitQuery, GitCommitView};
use async_trait::async_trait;

/// Storage-independent categories; native storage errors never leave adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitReaderError {
    Unavailable,
    Timeout,
    Decode,
    Internal,
}
impl std::fmt::Display for GitReaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unavailable => "Git reader unavailable",
            Self::Timeout => "Git reader timed out",
            Self::Decode => "invalid persisted Git context",
            Self::Internal => "Git reader internal error",
        })
    }
}
impl std::error::Error for GitReaderError {}

#[async_trait]
pub trait GitCommitReader: Send + Sync {
    async fn find_by_identity(
        &self,
        repository_id: &str,
        commit_sha: &CommitSha,
    ) -> Result<Option<GitCommitView>, GitReaderError>;
    async fn search(&self, query: &GitCommitQuery) -> Result<Vec<GitCommitView>, GitReaderError>;
}
