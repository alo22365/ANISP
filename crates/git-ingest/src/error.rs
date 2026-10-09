use std::{error::Error, fmt};

use anisp_event::ValidationError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitOutboxError {
    Unavailable,
    Timeout,
    Decode,
    Internal,
    Conflict,
}

impl fmt::Display for GitOutboxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unavailable => "Git outbox is unavailable",
            Self::Timeout => "Git outbox request timed out",
            Self::Decode => "Git outbox returned invalid data",
            Self::Internal => "Git outbox operation failed",
            Self::Conflict => "Git outbox claim is no longer owned",
        })
    }
}
impl Error for GitOutboxError {}

#[derive(Debug)]
pub enum GitProjectionError {
    MetadataTooLarge,
    InvalidIdentity,
    InvalidMetadata,
    Serialization,
    EventValidation(ValidationError),
}
impl fmt::Display for GitProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MetadataTooLarge => {
                f.write_str("mandatory Git metadata exceeds the Event boundary")
            }
            Self::InvalidIdentity => f.write_str("invalid Git projection identity"),
            Self::InvalidMetadata => f.write_str("invalid Git projection metadata"),
            Self::Serialization => f.write_str("could not serialize Git metadata"),
            Self::EventValidation(error) => error.fmt(f),
        }
    }
}
impl Error for GitProjectionError {}

#[derive(Debug)]
pub enum GitIngestError {
    Projection(GitProjectionError),
    Repository(GitOutboxError),
}
impl fmt::Display for GitIngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Projection(error) => error.fmt(f),
            Self::Repository(error) => error.fmt(f),
        }
    }
}
impl Error for GitIngestError {}
impl From<GitProjectionError> for GitIngestError {
    fn from(error: GitProjectionError) -> Self {
        Self::Projection(error)
    }
}
impl From<GitOutboxError> for GitIngestError {
    fn from(error: GitOutboxError) -> Self {
        Self::Repository(error)
    }
}
