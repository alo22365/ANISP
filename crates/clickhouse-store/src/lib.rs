//! ClickHouse infrastructure adapter.
//!
//! Concrete clients, rows and SQL stay private to this infrastructure crate.

mod event_repository;
mod git_reader;

pub use event_repository::ClickHouseEventRepository;
pub use git_reader::ClickHouseGitCommitReader;

use std::{error::Error, fmt, future::Future, pin::Pin, time::Duration};

use anisp_config::ClickHouseConfig;
use clickhouse::Client;
use tokio::time::timeout;

#[derive(Clone)]
pub struct ClickHouseStore {
    client: Client,
    request_timeout: Duration,
}

impl ClickHouseStore {
    pub fn new(config: &ClickHouseConfig) -> Self {
        let client = Client::default()
            .with_url(config.url())
            .with_database(config.database())
            .with_user(config.user())
            .with_password(config.password());

        Self {
            client,
            request_timeout: Duration::from_millis(config.request_timeout_ms()),
        }
    }

    /// Executes a real lightweight query against ClickHouse.
    pub async fn ping(&self) -> Result<(), StoreError> {
        let value = timeout(
            self.request_timeout,
            self.client.query("SELECT 1").fetch_one::<u8>(),
        )
        .await
        .map_err(StoreError::from_timeout)?
        .map_err(StoreError::from_client)?;

        if value != 1 {
            return Err(StoreError::unexpected_ping_response());
        }

        Ok(())
    }

    #[cfg(test)]
    fn from_client(client: Client) -> Self {
        Self {
            client,
            request_timeout: Duration::from_secs(3),
        }
    }
}

impl fmt::Debug for ClickHouseStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClickHouseStore")
            .finish_non_exhaustive()
    }
}

pub type PingFuture<'a> = Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + 'a>>;

/// Narrow interface consumed by the HTTP readiness layer.
pub trait ClickHouseReadiness: Send + Sync {
    fn ping(&self) -> PingFuture<'_>;
}

impl ClickHouseReadiness for ClickHouseStore {
    fn ping(&self) -> PingFuture<'_> {
        Box::pin(async move { ClickHouseStore::ping(self).await })
    }
}

#[derive(Debug)]
pub struct StoreError {
    kind: StoreErrorKind,
}

impl StoreError {
    /// Creates an opaque unavailable error for alternate readiness adapters.
    pub fn unavailable() -> Self {
        Self {
            kind: StoreErrorKind::Unavailable,
        }
    }

    pub fn timeout() -> Self {
        Self {
            kind: StoreErrorKind::Timeout(None),
        }
    }

    fn from_timeout(source: tokio::time::error::Elapsed) -> Self {
        Self {
            kind: StoreErrorKind::Timeout(Some(source)),
        }
    }

    fn from_client(source: clickhouse::error::Error) -> Self {
        Self {
            kind: StoreErrorKind::Client(source),
        }
    }

    fn unexpected_ping_response() -> Self {
        Self {
            kind: StoreErrorKind::UnexpectedPingResponse,
        }
    }
}

#[derive(Debug)]
enum StoreErrorKind {
    Client(clickhouse::error::Error),
    Timeout(Option<tokio::time::error::Elapsed>),
    UnexpectedPingResponse,
    Unavailable,
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClickHouse is unavailable")
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            StoreErrorKind::Client(source) => Some(source),
            StoreErrorKind::Timeout(source) => source.as_ref().map(|source| source as &dyn Error),
            StoreErrorKind::UnexpectedPingResponse | StoreErrorKind::Unavailable => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error as _;

    use clickhouse::{Client, test};

    use super::*;

    #[tokio::test]
    async fn ping_accepts_select_one_response() {
        let mock = test::Mock::new();
        mock.add(test::handlers::provide([1_u8]));
        let store = ClickHouseStore::from_client(Client::default().with_url(mock.url()));

        store.ping().await.unwrap();
    }

    #[tokio::test]
    async fn ping_hides_client_error_behind_store_error() {
        let mock = test::Mock::new();
        mock.add(test::handlers::failure(test::status::SERVICE_UNAVAILABLE));
        let store = ClickHouseStore::from_client(Client::default().with_url(mock.url()));

        let error = store.ping().await.unwrap_err();

        assert_eq!(error.to_string(), "ClickHouse is unavailable");
        assert!(error.source().is_some());
    }

    #[test]
    fn unexpected_ping_response_is_not_exposed() {
        let error = StoreError::unexpected_ping_response();

        assert_eq!(error.to_string(), "ClickHouse is unavailable");
        assert!(matches!(error.kind, StoreErrorKind::UnexpectedPingResponse));
    }
}
