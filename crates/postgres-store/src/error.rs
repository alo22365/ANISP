use std::{error::Error, fmt};

#[derive(Debug)]
pub struct StoreError {
    kind: StoreErrorKind,
}

impl StoreError {
    pub fn unavailable() -> Self {
        Self {
            kind: StoreErrorKind::Unavailable(None),
        }
    }

    pub fn timeout() -> Self {
        Self {
            kind: StoreErrorKind::Timeout(None),
        }
    }

    pub(crate) fn from_unavailable(source: sqlx::Error) -> Self {
        Self {
            kind: StoreErrorKind::Unavailable(Some(source)),
        }
    }

    pub(crate) fn from_timeout(source: tokio::time::error::Elapsed) -> Self {
        Self {
            kind: StoreErrorKind::Timeout(Some(source)),
        }
    }

    pub(crate) fn unexpected_ping_response() -> Self {
        Self {
            kind: StoreErrorKind::UnexpectedPingResponse,
        }
    }
}

#[derive(Debug)]
enum StoreErrorKind {
    Unavailable(Option<sqlx::Error>),
    Timeout(Option<tokio::time::error::Elapsed>),
    UnexpectedPingResponse,
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PostgreSQL is unavailable")
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            StoreErrorKind::Unavailable(source) => {
                source.as_ref().map(|source| source as &dyn Error)
            }
            StoreErrorKind::Timeout(source) => source.as_ref().map(|source| source as &dyn Error),
            StoreErrorKind::UnexpectedPingResponse => None,
        }
    }
}
