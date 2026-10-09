use async_trait::async_trait;

use uuid::Uuid;

use crate::{ContextEvent, EventQuery, RepositoryError};

/// Persistence boundary owned by the event domain.
#[async_trait]
pub trait EventRepository: Send + Sync {
    async fn insert(&self, event: &ContextEvent) -> Result<(), RepositoryError>;
    async fn find_by_id(&self, event_id: Uuid) -> Result<Option<ContextEvent>, RepositoryError>;
    async fn search(&self, query: &EventQuery) -> Result<Vec<ContextEvent>, RepositoryError>;
}
