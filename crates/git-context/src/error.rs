use std::{error::Error, fmt};

/// Validation errors for Git context facts, independent of any provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitContextError {
    EmptyRepositoryId,
    EmptyRepositoryName,
    EmptyCommitSha,
    InvalidCommitSha,
    EmptyAuthorName,
    EmptyFilePath,
    RenameWithoutOldPath,
    RepositoryIdMismatch,
}

impl fmt::Display for GitContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyRepositoryId => "Git repository ID must not be empty",
            Self::EmptyRepositoryName => "Git repository name must not be empty",
            Self::EmptyCommitSha => "Git commit SHA must not be empty",
            Self::InvalidCommitSha => "Git commit SHA must contain 40 or 64 hexadecimal characters",
            Self::EmptyAuthorName => "Git commit author name must not be empty",
            Self::EmptyFilePath => "Git file path must not be empty",
            Self::RenameWithoutOldPath => "Git file rename requires a non-empty old path",
            Self::RepositoryIdMismatch => "Git commit and observation repository IDs must match",
        };
        formatter.write_str(message)
    }
}

impl Error for GitContextError {}
