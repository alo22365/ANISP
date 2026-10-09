use std::{
    collections::{BinaryHeap, HashSet},
    path::{Path, PathBuf},
};

use anisp_git_context::{CommitSha, GitCommit, GitCommitInput, GitRepository, ObservedGitCommit};
use chrono::{DateTime, Utc};
use git2::{BranchType, ErrorCode, Oid, Reference, Repository, RepositoryOpenFlags};

use crate::{CollectorError, diff::collect_changes};

pub const MAX_RECENT_COMMITS: usize = 500;

/// Synchronous, bounded, on-demand collection from one explicitly opened local
/// repository. Use a blocking worker when calling from an asynchronous runtime.
///
/// V1 uses stable libgit2 SHA-1 support. The Domain also accepts SHA-256, but
/// those identifiers return `UnsupportedObjectFormat` here rather than being
/// truncated or treated as malformed SHA-1.
pub struct LocalGitCollector {
    repository: GitRepository,
    local_path: PathBuf,
    native: Repository,
}

impl LocalGitCollector {
    /// Accepts a worktree root, its Git directory, or a bare repository. Does
    /// not search parent directories or fetch a configured remote.
    pub fn new(
        repository: GitRepository,
        local_path: impl AsRef<Path>,
    ) -> Result<Self, CollectorError> {
        let local_path = local_path
            .as_ref()
            .canonicalize()
            .map_err(|_| CollectorError::RepositoryUnavailable)?;
        let native = Repository::open_ext(
            &local_path,
            RepositoryOpenFlags::NO_SEARCH,
            std::iter::empty::<&Path>(),
        )
        .map_err(|_| CollectorError::RepositoryUnavailable)?;

        Ok(Self {
            repository,
            local_path,
            native,
        })
    }

    pub fn repository(&self) -> &GitRepository {
        &self.repository
    }

    pub fn local_path(&self) -> &Path {
        &self.local_path
    }

    /// `branch` records the observation path, not exclusive commit membership.
    /// Explicit branches must be local names (or `refs/heads/<name>`). With no
    /// branch, resolve HEAD; detached HEAD produces an observation with None.
    pub fn collect_commit(
        &self,
        commit_sha: &str,
        branch: Option<&str>,
    ) -> Result<ObservedGitCommit, CollectorError> {
        let sha = CommitSha::new(commit_sha).map_err(|_| CollectorError::InvalidCommitSha)?;
        if sha.as_str().len() == 64 {
            return Err(CollectorError::UnsupportedObjectFormat);
        }
        let oid = Oid::from_str(sha.as_str()).map_err(|_| CollectorError::InvalidCommitSha)?;
        let (_, branch) = self.resolve_observation(branch)?;
        self.collect_oid(oid, branch)
    }

    /// Bounded ancestry walk from the resolved tip, prioritizing the newest
    /// currently discovered parent. Includes side parents and deduplicates DAG
    /// visits. Results are sorted by timestamp descending, then SHA descending.
    ///
    /// Deliberately avoids time-sorted native revwalk, which may preload the
    /// entire history. Under clock skew this is not a global top-N timestamp
    /// search over all reachable ancestors: only the bounded frontier is read.
    pub fn collect_recent_commits(
        &self,
        branch: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObservedGitCommit>, CollectorError> {
        if !(1..=MAX_RECENT_COMMITS).contains(&limit) {
            return Err(CollectorError::InvalidLimit {
                limit,
                maximum: MAX_RECENT_COMMITS,
            });
        }
        let (tip, branch) = self.resolve_observation(branch)?;
        let mut frontier = BinaryHeap::new();
        let mut discovered = HashSet::from([tip]);
        let mut observations = Vec::with_capacity(limit);
        let tip_commit = self
            .native
            .find_commit(tip)
            .map_err(|_| CollectorError::ReadFailed)?;
        frontier.push((tip_commit.time().seconds(), tip));
        while let Some((_, oid)) = frontier.pop() {
            observations.push(self.collect_oid(oid, branch.clone())?);
            if observations.len() == limit {
                break;
            }
            let commit = self
                .native
                .find_commit(oid)
                .map_err(|_| CollectorError::ReadFailed)?;
            for parent in commit.parent_ids() {
                if discovered.insert(parent) {
                    let commit = self
                        .native
                        .find_commit(parent)
                        .map_err(|_| CollectorError::ReadFailed)?;
                    frontier.push((commit.time().seconds(), parent));
                }
            }
        }
        observations.sort_by(|left, right| {
            right
                .commit()
                .committed_at()
                .cmp(&left.commit().committed_at())
                .then_with(|| right.commit().commit_sha().cmp(left.commit().commit_sha()))
        });
        Ok(observations)
    }

    fn resolve_observation(
        &self,
        branch: Option<&str>,
    ) -> Result<(Oid, Option<String>), CollectorError> {
        if let Some(branch) = branch {
            let name = branch.strip_prefix("refs/heads/").unwrap_or(branch);
            if name.is_empty() || !Reference::is_valid_name(&format!("refs/heads/{name}")) {
                return Err(CollectorError::InvalidBranch);
            }
            let reference = self
                .native
                .find_branch(name, BranchType::Local)
                .map_err(|error| match error.code() {
                    ErrorCode::NotFound => CollectorError::BranchNotFound,
                    _ => CollectorError::ReadFailed,
                })?;
            let tip = reference
                .get()
                .peel_to_commit()
                .map_err(|_| CollectorError::ReadFailed)?;
            return Ok((tip.id(), Some(name.to_owned())));
        }

        let head = self
            .native
            .head()
            .map_err(|_| CollectorError::HeadUnavailable)?;
        let branch = if head.is_branch() {
            let name = head
                .name()
                .map_err(|_| CollectorError::InvalidEncoding { field: "branch" })?;
            Some(
                name.strip_prefix("refs/heads/")
                    .ok_or(CollectorError::InvalidBranch)?
                    .to_owned(),
            )
        } else {
            None
        };
        let tip = head
            .peel_to_commit()
            .map_err(|_| CollectorError::HeadUnavailable)?;
        Ok((tip.id(), branch))
    }

    fn collect_oid(
        &self,
        oid: Oid,
        branch: Option<String>,
    ) -> Result<ObservedGitCommit, CollectorError> {
        let observed_at = Utc::now();
        let commit = self
            .native
            .find_commit(oid)
            .map_err(|error| match error.code() {
                ErrorCode::NotFound => CollectorError::CommitNotFound,
                _ => CollectorError::ReadFailed,
            })?;
        let author = commit.author();
        let author_name = decode_utf8(author.name_bytes(), "author_name")?;
        let email = decode_utf8(author.email_bytes(), "author_email")?;
        let message = decode_utf8(commit.message_raw_bytes(), "message")?;
        // Git's timestamp is already a Unix UTC instant. The stored offset is
        // display information and must not be added a second time.
        let committed_at = DateTime::from_timestamp(commit.time().seconds(), 0)
            .ok_or(CollectorError::InvalidTimestamp)?;
        let changes = collect_changes(&self.native, &commit)?;
        let domain_commit = GitCommit::new(GitCommitInput {
            repository_id: self.repository.repository_id().to_owned(),
            commit_sha: commit.id().to_string(),
            parent_shas: commit.parent_ids().map(|oid| oid.to_string()).collect(),
            author_name,
            author_email: (!email.is_empty()).then_some(email),
            message,
            committed_at,
            changes,
        })?;
        ObservedGitCommit::new(self.repository.clone(), domain_commit, branch, observed_at)
            .map_err(Into::into)
    }
}

pub(crate) fn decode_utf8(bytes: &[u8], field: &'static str) -> Result<String, CollectorError> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| CollectorError::InvalidEncoding { field })
}
