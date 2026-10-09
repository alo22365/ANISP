//! PostgreSQL infrastructure adapters.
//!
//! SQLx and PostgreSQL client types remain inside this crate. The support
//! domain only sees its own repository trait and storage-neutral errors.

mod error;
mod git_outbox_repository;
mod outbox_repository;
mod support_repository;

pub use error::StoreError;
pub use git_outbox_repository::PostgresGitOutboxRepository;
pub use support_repository::PostgresSupportCaseRepository;

use std::{fmt, future::Future, pin::Pin, time::Duration};

use anisp_config::PostgresConfig;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::time::timeout;

#[derive(Clone)]
pub struct PostgresStore {
    pub(crate) pool: PgPool,
    pub(crate) request_timeout: Duration,
}

impl PostgresStore {
    pub async fn connect(config: &PostgresConfig) -> Result<Self, StoreError> {
        let request_timeout = Duration::from_millis(config.request_timeout_ms());
        let pool = timeout(
            request_timeout,
            PgPoolOptions::new()
                .max_connections(config.max_connections())
                .acquire_timeout(request_timeout)
                .connect(config.url()),
        )
        .await
        .map_err(StoreError::from_timeout)?
        .map_err(StoreError::from_unavailable)?;

        Ok(Self {
            pool,
            request_timeout,
        })
    }

    /// Executes a lightweight query through the configured pool.
    pub async fn ping(&self) -> Result<(), StoreError> {
        let value = timeout(
            self.request_timeout,
            sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&self.pool),
        )
        .await
        .map_err(StoreError::from_timeout)?
        .map_err(StoreError::from_unavailable)?;

        if value != 1 {
            return Err(StoreError::unexpected_ping_response());
        }

        Ok(())
    }
}

pub type PostgresPingFuture<'a> = Pin<Box<dyn Future<Output = Result<(), StoreError>> + Send + 'a>>;

/// Narrow interface used by the server readiness endpoint.
pub trait PostgresReadiness: Send + Sync {
    fn ping(&self) -> PostgresPingFuture<'_>;
}

impl PostgresReadiness for PostgresStore {
    fn ping(&self) -> PostgresPingFuture<'_> {
        Box::pin(async move { PostgresStore::ping(self).await })
    }
}

impl fmt::Debug for PostgresStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresStore")
            .finish_non_exhaustive()
    }
}
