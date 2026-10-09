use std::{fmt, str::FromStr};

use chrono::{DateTime, Utc};

use crate::{GitContextError, GitFileChange, GitRepository};

/// A full SHA-1 or SHA-256 identifier, validated and normalized to lowercase.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CommitSha(String);

impl CommitSha {
    pub fn new(value: impl Into<String>) -> Result<Self, GitContextError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(GitContextError::EmptyCommitSha);
        }
        if !matches!(value.len(), 40 | 64) || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(GitContextError::InvalidCommitSha);
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for CommitSha {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for CommitSha {
    type Err = GitContextError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for CommitSha {
    type Error = GitContextError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Raw metadata provided by a caller to the validating commit constructor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommitInput {
    pub repository_id: String,
    pub commit_sha: String,
    pub parent_shas: Vec<String>,
    pub author_name: String,
    pub author_email: Option<String>,
    pub message: String,
    pub committed_at: DateTime<Utc>,
    pub changes: Vec<GitFileChange>,
}

/// Immutable commit metadata with natural identity `(repository_id, commit_sha)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommit {
    repository_id: String,
    commit_sha: CommitSha,
    parent_shas: Vec<CommitSha>,
    author_name: String,
    author_email: Option<String>,
    message: String,
    committed_at: DateTime<Utc>,
    changes: Vec<GitFileChange>,
}

impl GitCommit {
    pub fn new(input: GitCommitInput) -> Result<Self, GitContextError> {
        if input.repository_id.trim().is_empty() {
            return Err(GitContextError::EmptyRepositoryId);
        }
        let commit_sha = CommitSha::new(input.commit_sha)?;
        let parent_shas = input
            .parent_shas
            .into_iter()
            .map(CommitSha::new)
            .collect::<Result<Vec<_>, _>>()?;
        if input.author_name.trim().is_empty() {
            return Err(GitContextError::EmptyAuthorName);
        }

        Ok(Self {
            repository_id: input.repository_id,
            commit_sha,
            parent_shas,
            author_name: input.author_name,
            author_email: input.author_email,
            message: input.message,
            committed_at: input.committed_at,
            changes: input.changes,
        })
    }

    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }

    pub fn commit_sha(&self) -> &CommitSha {
        &self.commit_sha
    }

    /// Metadata and observation time do not participate in commit identity.
    pub fn identity(&self) -> (&str, &CommitSha) {
        (self.repository_id(), self.commit_sha())
    }

    pub fn parent_shas(&self) -> &[CommitSha] {
        &self.parent_shas
    }

    pub fn author_name(&self) -> &str {
        &self.author_name
    }

    pub fn author_email(&self) -> Option<&str> {
        self.author_email.as_deref()
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Original commit timestamp; observation does not replace or truncate it.
    pub fn committed_at(&self) -> DateTime<Utc> {
        self.committed_at
    }

    pub fn changes(&self) -> &[GitFileChange] {
        &self.changes
    }
}

/// A commit observed in a repository, optionally on a particular branch.
///
/// `observed_at` is supplied by the observer independently of the commit's
/// timestamp. No clock ordering is imposed on these two facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservedGitCommit {
    repository: GitRepository,
    commit: GitCommit,
    branch: Option<String>,
    observed_at: DateTime<Utc>,
}

impl ObservedGitCommit {
    pub fn new(
        repository: GitRepository,
        commit: GitCommit,
        branch: Option<String>,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, GitContextError> {
        if repository.repository_id() != commit.repository_id() {
            return Err(GitContextError::RepositoryIdMismatch);
        }

        Ok(Self {
            repository,
            commit,
            branch,
            observed_at,
        })
    }

    pub fn repository(&self) -> &GitRepository {
        &self.repository
    }

    pub fn commit(&self) -> &GitCommit {
        &self.commit
    }

    pub fn branch(&self) -> Option<&str> {
        self.branch.as_deref()
    }

    pub fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
}

#[cfg(test)]
mod tests {
    use crate::ChangeType;

    use super::*;

    const SHA1: &str = "0123456789abcdef0123456789abcdef01234567";
    const SHA256: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn input() -> GitCommitInput {
        GitCommitInput {
            repository_id: "repo-xpa".to_owned(),
            commit_sha: SHA1.to_owned(),
            parent_shas: vec!["a".repeat(40), "b".repeat(40)],
            author_name: "Developer".to_owned(),
            author_email: Some("developer@example.com".to_owned()),
            message: "Fix finance calculation".to_owned(),
            committed_at: "2026-10-06T08:01:02.123456789Z".parse().unwrap(),
            changes: vec![
                GitFileChange::new(
                    "src/finance.rs",
                    None,
                    ChangeType::Modified,
                    Some(5),
                    Some(2),
                )
                .unwrap(),
            ],
        }
    }

    fn repository() -> GitRepository {
        GitRepository::new("repo-xpa", "finance", None, None, None).unwrap()
    }

    #[test]
    fn valid_sha1() {
        assert_eq!(CommitSha::new(SHA1).unwrap().as_str(), SHA1);
    }

    #[test]
    fn valid_sha256() {
        assert_eq!(SHA256.parse::<CommitSha>().unwrap().as_str(), SHA256);
    }

    #[test]
    fn invalid_sha_is_rejected() {
        for value in [
            "a".repeat(39),
            "a".repeat(41),
            "a".repeat(63),
            "a".repeat(65),
            "g".repeat(40),
            "é".repeat(20),
            format!(" {SHA1}"),
        ] {
            assert_eq!(
                CommitSha::new(value).unwrap_err(),
                GitContextError::InvalidCommitSha
            );
        }
    }

    #[test]
    fn empty_sha_is_rejected() {
        for value in ["", " \t\n"] {
            assert_eq!(
                CommitSha::new(value).unwrap_err(),
                GitContextError::EmptyCommitSha
            );
        }
    }

    #[test]
    fn uppercase_sha_normalizes_to_lowercase() {
        for expected in [SHA1, SHA256] {
            let sha = CommitSha::try_from(expected.to_ascii_uppercase()).unwrap();
            assert_eq!(sha.as_str(), expected);
            assert_eq!(sha.to_string(), expected);
        }
    }

    #[test]
    fn valid_commit_preserves_metadata_and_changed_files() {
        let expected = input();
        let commit = GitCommit::new(expected.clone()).unwrap();

        assert_eq!(commit.repository_id(), expected.repository_id);
        assert_eq!(commit.commit_sha().as_str(), expected.commit_sha);
        assert_eq!(commit.parent_shas().len(), 2);
        assert_eq!(commit.parent_shas()[0].as_str(), expected.parent_shas[0]);
        assert_eq!(commit.author_name(), expected.author_name);
        assert_eq!(commit.author_email(), expected.author_email.as_deref());
        assert_eq!(commit.message(), expected.message);
        assert_eq!(commit.committed_at(), expected.committed_at);
        assert_eq!(commit.changes(), expected.changes);
    }

    #[test]
    fn root_commit_can_have_empty_message_and_no_changes_or_email() {
        let commit = GitCommit::new(GitCommitInput {
            parent_shas: Vec::new(),
            message: String::new(),
            changes: Vec::new(),
            author_email: None,
            ..input()
        })
        .unwrap();

        assert!(commit.parent_shas().is_empty());
        assert_eq!(commit.message(), "");
        assert!(commit.changes().is_empty());
        assert_eq!(commit.author_email(), None);
    }

    #[test]
    fn empty_author_is_rejected() {
        for author_name in ["", " \t\n"] {
            assert_eq!(
                GitCommit::new(GitCommitInput {
                    author_name: author_name.to_owned(),
                    ..input()
                })
                .unwrap_err(),
                GitContextError::EmptyAuthorName
            );
        }
    }

    #[test]
    fn empty_commit_repository_id_is_rejected() {
        assert_eq!(
            GitCommit::new(GitCommitInput {
                repository_id: " \t".to_owned(),
                ..input()
            })
            .unwrap_err(),
            GitContextError::EmptyRepositoryId
        );
    }

    #[test]
    fn commit_validates_and_normalizes_all_parent_shas() {
        let commit = GitCommit::new(GitCommitInput {
            commit_sha: SHA256.to_ascii_uppercase(),
            parent_shas: vec![SHA256.to_ascii_uppercase()],
            ..input()
        })
        .unwrap();
        assert_eq!(commit.commit_sha().as_str(), SHA256);
        assert_eq!(commit.parent_shas()[0].as_str(), SHA256);

        for (sha, error) in [
            ("", GitContextError::EmptyCommitSha),
            ("abcd", GitContextError::InvalidCommitSha),
        ] {
            assert_eq!(
                GitCommit::new(GitCommitInput {
                    parent_shas: vec![SHA1.to_owned(), sha.to_owned()],
                    ..input()
                })
                .unwrap_err(),
                error
            );
        }
    }

    #[test]
    fn commit_identity_uses_repository_id_and_normalized_sha() {
        let original = GitCommit::new(input()).unwrap();
        let same_identity = GitCommit::new(GitCommitInput {
            commit_sha: SHA1.to_ascii_uppercase(),
            ..input()
        })
        .unwrap();
        let other_repository = GitCommit::new(GitCommitInput {
            repository_id: "other-repo".to_owned(),
            ..input()
        })
        .unwrap();

        assert_eq!(original.identity(), same_identity.identity());
        assert_ne!(original.identity(), other_repository.identity());
    }

    #[test]
    fn committed_and_observed_timestamps_keep_their_independent_semantics() {
        let commit = GitCommit::new(input()).unwrap();
        let committed_at = commit.committed_at();
        let observed_at = "2026-10-06T09:03:04.987654321Z".parse().unwrap();
        let observed =
            ObservedGitCommit::new(repository(), commit, Some("main".to_owned()), observed_at)
                .unwrap();

        assert_eq!(observed.commit().committed_at(), committed_at);
        assert_eq!(observed.observed_at(), observed_at);
        assert_ne!(observed.commit().committed_at(), observed.observed_at());
        assert_eq!(observed.branch(), Some("main"));
        assert_eq!(observed.repository().repository_id(), "repo-xpa");
    }

    #[test]
    fn observation_does_not_assume_commit_clock_precedes_observer_clock() {
        let commit = GitCommit::new(input()).unwrap();
        let observed_at = commit.committed_at() - chrono::Duration::seconds(1);
        let observed = ObservedGitCommit::new(repository(), commit, None, observed_at).unwrap();

        assert_eq!(observed.observed_at(), observed_at);
        assert_eq!(observed.branch(), None);
    }

    #[test]
    fn observation_rejects_mismatched_repository_identity() {
        let other_repository = GitRepository::new("other-repo", "other", None, None, None).unwrap();

        assert_eq!(
            ObservedGitCommit::new(
                other_repository,
                GitCommit::new(input()).unwrap(),
                None,
                Utc::now()
            )
            .unwrap_err(),
            GitContextError::RepositoryIdMismatch
        );
    }
}
