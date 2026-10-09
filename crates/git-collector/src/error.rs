use std::{error::Error, fmt};

use anisp_git_context::GitContextError;

/// Stable adapter categories; native errors and repository contents are not
/// embedded in messages or exposed as error sources.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CollectorError {
    RepositoryUnavailable,
    HeadUnavailable,
    BranchNotFound,
    InvalidBranch,
    CommitNotFound,
    InvalidCommitSha,
    UnsupportedObjectFormat,
    InvalidLimit { limit: usize, maximum: usize },
    InvalidEncoding { field: &'static str },
    InvalidTimestamp,
    UnsupportedChangeType,
    ReadFailed,
    InvalidDomainData(GitContextError),
}

impl fmt::Display for CollectorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RepositoryUnavailable => formatter.write_str("local repository is unavailable"),
            Self::HeadUnavailable => formatter.write_str("repository HEAD is unavailable"),
            Self::BranchNotFound => formatter.write_str("local branch was not found"),
            Self::InvalidBranch => formatter.write_str("invalid local branch name"),
            Self::CommitNotFound => formatter.write_str("commit was not found"),
            Self::InvalidCommitSha => formatter.write_str("invalid full commit SHA"),
            Self::UnsupportedObjectFormat => formatter.write_str("unsupported Git object format"),
            Self::InvalidLimit { limit, maximum } => {
                write!(
                    formatter,
                    "commit limit {limit} must be between 1 and {maximum}"
                )
            }
            Self::InvalidEncoding { field } => write!(formatter, "{field} is not valid UTF-8"),
            Self::InvalidTimestamp => formatter.write_str("commit timestamp is out of range"),
            Self::UnsupportedChangeType => formatter.write_str("unsupported Git file change"),
            Self::ReadFailed => formatter.write_str("failed to read local Git data"),
            Self::InvalidDomainData(error) => write!(formatter, "invalid Git domain data: {error}"),
        }
    }
}

impl Error for CollectorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidDomainData(error) => Some(error),
            _ => None,
        }
    }
}

impl From<GitContextError> for CollectorError {
    fn from(error: GitContextError) -> Self {
        Self::InvalidDomainData(error)
    }
}
