use crate::{CommitSha, GitCommit, GitCommitInput, GitFileChange, GitReaderError};
use chrono::{DateTime, Utc};
use uuid::Uuid;

/// Read model, independent of HTTP DTOs and database rows. Only a validated
/// constructor can create a view. File counts describe the full commit even
/// when only a prefix of its changed paths was projected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommitView {
    event_id: Uuid,
    repository_name: String,
    project_id: String,
    service: String,
    branch: Option<String>,
    commit: GitCommit,
    observed_at: DateTime<Utc>,
    changed_file_count: u64,
    additions: Option<u64>,
    deletions: Option<u64>,
    changes_truncated: bool,
}
pub struct GitCommitViewInput {
    pub event_id: Uuid,
    pub repository_name: String,
    pub project_id: String,
    pub service: String,
    pub branch: Option<String>,
    pub commit: GitCommitInput,
    pub observed_at: DateTime<Utc>,
    pub changed_file_count: u64,
    pub additions: Option<u64>,
    pub deletions: Option<u64>,
    pub changes_truncated: bool,
}
impl GitCommitView {
    pub fn new(input: GitCommitViewInput) -> Result<Self, GitReaderError> {
        let included = input.commit.changes.len() as u64;
        if input.repository_name.trim().is_empty()
            || input.project_id.trim().is_empty()
            || input.service.trim().is_empty()
            || included > input.changed_file_count
            || input.changes_truncated != (included < input.changed_file_count)
        {
            return Err(GitReaderError::Decode);
        }
        let commit = GitCommit::new(input.commit).map_err(|_| GitReaderError::Decode)?;
        Ok(Self {
            event_id: input.event_id,
            repository_name: input.repository_name,
            project_id: input.project_id,
            service: input.service,
            branch: input.branch,
            commit,
            observed_at: input.observed_at,
            changed_file_count: input.changed_file_count,
            additions: input.additions,
            deletions: input.deletions,
            changes_truncated: input.changes_truncated,
        })
    }
    pub fn event_id(&self) -> Uuid {
        self.event_id
    }
    pub fn repository_id(&self) -> &str {
        self.commit.repository_id()
    }
    pub fn repository_name(&self) -> &str {
        &self.repository_name
    }
    pub fn project_id(&self) -> &str {
        &self.project_id
    }
    pub fn service(&self) -> &str {
        &self.service
    }
    pub fn branch(&self) -> Option<&str> {
        self.branch.as_deref()
    }
    pub fn commit_sha(&self) -> &CommitSha {
        self.commit.commit_sha()
    }
    pub fn parent_shas(&self) -> &[CommitSha] {
        self.commit.parent_shas()
    }
    pub fn author_name(&self) -> &str {
        self.commit.author_name()
    }
    pub fn author_email(&self) -> Option<&str> {
        self.commit.author_email()
    }
    pub fn message(&self) -> &str {
        self.commit.message()
    }
    pub fn committed_at(&self) -> DateTime<Utc> {
        self.commit.committed_at()
    }
    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
    pub fn changed_file_count(&self) -> u64 {
        self.changed_file_count
    }
    pub fn additions(&self) -> Option<u64> {
        self.additions
    }
    pub fn deletions(&self) -> Option<u64> {
        self.deletions
    }
    pub fn changes_truncated(&self) -> bool {
        self.changes_truncated
    }
    pub fn changed_files(&self) -> &[GitFileChange] {
        self.commit.changes()
    }
}
