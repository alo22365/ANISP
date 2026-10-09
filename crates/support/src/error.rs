use std::{error::Error, fmt};

use crate::CaseStatus;

/// Domain validation failures returned while creating a support case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportCaseError {
    EmptyTitle,
    EmptyDescription,
    EmptyReporter,
    FieldTooLong {
        field: &'static str,
        max_bytes: usize,
    },
    InvalidTransition {
        from: CaseStatus,
        to: CaseStatus,
    },
}

impl fmt::Display for SupportCaseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyTitle => formatter.write_str("support case title must not be empty"),
            Self::EmptyDescription => {
                formatter.write_str("support case description must not be empty")
            }
            Self::EmptyReporter => {
                formatter.write_str("support case reporter_id must not be empty")
            }
            Self::FieldTooLong { field, max_bytes } => {
                write!(
                    formatter,
                    "support case {field} exceeds maximum length of {max_bytes} bytes"
                )
            }
            Self::InvalidTransition { from, to } => {
                write!(
                    formatter,
                    "invalid support case transition: {from:?} -> {to:?}"
                )
            }
        }
    }
}

impl Error for SupportCaseError {}

/// Storage-neutral failures exposed by a SupportCase repository adapter.
#[derive(Debug)]
pub enum SupportRepositoryError {
    Unavailable(Option<Box<dyn Error + Send + Sync>>),
    Timeout(Option<Box<dyn Error + Send + Sync>>),
    Conflict(Option<Box<dyn Error + Send + Sync>>),
    Decode(Option<Box<dyn Error + Send + Sync>>),
    Internal(Option<Box<dyn Error + Send + Sync>>),
}

impl SupportRepositoryError {
    pub fn unavailable() -> Self {
        Self::Unavailable(None)
    }

    pub fn unavailable_with_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Unavailable(Some(Box::new(error)))
    }

    pub fn timeout() -> Self {
        Self::Timeout(None)
    }

    pub fn timeout_with_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Timeout(Some(Box::new(error)))
    }

    pub fn conflict() -> Self {
        Self::Conflict(None)
    }

    pub fn conflict_with_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Conflict(Some(Box::new(error)))
    }

    pub fn decode() -> Self {
        Self::Decode(None)
    }

    pub fn decode_with_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Decode(Some(Box::new(error)))
    }

    pub fn internal() -> Self {
        Self::Internal(None)
    }

    pub fn internal_with_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Internal(Some(Box::new(error)))
    }
}

impl fmt::Display for SupportRepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(_) => formatter.write_str("support repository is unavailable"),
            Self::Timeout(_) => formatter.write_str("support repository timed out"),
            Self::Conflict(_) => formatter.write_str("support case update conflicted"),
            Self::Decode(_) => formatter.write_str("support repository returned invalid data"),
            Self::Internal(_) => formatter.write_str("support repository internal error"),
        }
    }
}

impl Error for SupportRepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Unavailable(source)
            | Self::Timeout(source)
            | Self::Conflict(source)
            | Self::Decode(source)
            | Self::Internal(source) => source
                .as_deref()
                .map(|source| source as &(dyn Error + 'static)),
        }
    }
}

#[derive(Debug)]
pub enum SupportServiceError {
    Validation(SupportCaseError),
    Repository(SupportRepositoryError),
}

impl fmt::Display for SupportServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(error) => error.fmt(formatter),
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl Error for SupportServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Validation(error) => Some(error),
            Self::Repository(error) => Some(error),
        }
    }
}

impl From<SupportCaseError> for SupportServiceError {
    fn from(error: SupportCaseError) -> Self {
        Self::Validation(error)
    }
}

impl From<SupportRepositoryError> for SupportServiceError {
    fn from(error: SupportRepositoryError) -> Self {
        Self::Repository(error)
    }
}
