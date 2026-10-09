use crate::{
    CommitSha, GitCommitQuery, GitCommitReader, GitCommitView, GitQueryError, GitReaderError,
};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitContextQueryError {
    InvalidQuery(GitQueryError),
    Reader(GitReaderError),
}
impl std::fmt::Display for GitContextQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidQuery(e) => e.fmt(f),
            Self::Reader(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for GitContextQueryError {}
impl From<GitReaderError> for GitContextQueryError {
    fn from(e: GitReaderError) -> Self {
        Self::Reader(e)
    }
}

#[derive(Clone)]
pub struct GitContextQueryService {
    reader: Arc<dyn GitCommitReader>,
}
impl GitContextQueryService {
    pub fn new(reader: Arc<dyn GitCommitReader>) -> Self {
        Self { reader }
    }
    pub async fn get_commit(
        &self,
        repository_id: &str,
        commit_sha: &str,
    ) -> Result<Option<GitCommitView>, GitContextQueryError> {
        if repository_id.trim().is_empty() {
            return Err(GitContextQueryError::InvalidQuery(
                GitQueryError::InvalidIdentity,
            ));
        }
        let sha = CommitSha::new(commit_sha)
            .map_err(|_| GitContextQueryError::InvalidQuery(GitQueryError::InvalidIdentity))?;
        Ok(self.reader.find_by_identity(repository_id, &sha).await?)
    }
    pub async fn search_commits(
        &self,
        query: &GitCommitQuery,
    ) -> Result<Vec<GitCommitView>, GitContextQueryError> {
        Ok(self.reader.search(query).await?)
    }
}
