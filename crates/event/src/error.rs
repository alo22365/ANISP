use std::{error::Error, fmt};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    field: &'static str,
    message: &'static str,
}

impl ValidationError {
    pub(crate) fn new(field: &'static str, message: &'static str) -> Self {
        Self { field, message }
    }

    pub fn field(&self) -> &'static str {
        self.field
    }

    pub fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.field, self.message)
    }
}

impl Error for ValidationError {}

#[derive(Debug)]
pub enum RepositoryError {
    Unavailable(Option<Box<dyn Error + Send + Sync>>),
    Timeout(Option<Box<dyn Error + Send + Sync>>),
    Decode(Box<dyn Error + Send + Sync>),
    Internal(Option<Box<dyn Error + Send + Sync>>),
}

impl RepositoryError {
    pub fn unavailable() -> Self {
        Self::Unavailable(None)
    }

    pub fn unavailable_with_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Unavailable(Some(Box::new(error)))
    }

    pub fn decode_with_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Decode(Box::new(error))
    }

    pub fn timeout() -> Self {
        Self::Timeout(None)
    }

    pub fn timeout_with_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Timeout(Some(Box::new(error)))
    }

    pub fn internal() -> Self {
        Self::Internal(None)
    }

    pub fn internal_with_source(error: impl Error + Send + Sync + 'static) -> Self {
        Self::Internal(Some(Box::new(error)))
    }

    pub fn is_decode(&self) -> bool {
        matches!(self, Self::Decode(_))
    }
}

impl fmt::Display for RepositoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(_) => formatter.write_str("event repository is unavailable"),
            Self::Timeout(_) => formatter.write_str("event repository timed out"),
            Self::Decode(_) => formatter.write_str("event repository returned invalid data"),
            Self::Internal(_) => formatter.write_str("event repository internal error"),
        }
    }
}

impl Error for RepositoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Unavailable(source) | Self::Timeout(source) | Self::Internal(source) => source
                .as_deref()
                .map(|source| source as &(dyn Error + 'static)),
            Self::Decode(source) => Some(source.as_ref()),
        }
    }
}

#[derive(Debug)]
pub enum EventServiceError {
    Validation(ValidationError),
    Repository(RepositoryError),
}

impl fmt::Display for EventServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Validation(error) => error.fmt(formatter),
            Self::Repository(error) => error.fmt(formatter),
        }
    }
}

impl Error for EventServiceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Validation(error) => Some(error),
            Self::Repository(error) => Some(error),
        }
    }
}

impl From<ValidationError> for EventServiceError {
    fn from(error: ValidationError) -> Self {
        Self::Validation(error)
    }
}

impl From<RepositoryError> for EventServiceError {
    fn from(error: RepositoryError) -> Self {
        Self::Repository(error)
    }
}
