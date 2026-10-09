use std::error::Error;

use std::sync::Arc;
use std::time::Duration;

use anisp_clickhouse_store::{
    ClickHouseEventRepository, ClickHouseGitCommitReader, ClickHouseStore,
};
use anisp_config::AppConfig;
use anisp_context::SupportCaseContextService;
use anisp_event::EventService;
use anisp_git_context::GitContextQueryService;
use anisp_git_ingest::{GitOutboxPublisher, GitOutboxStatusService, GitPublisherOptions};
use anisp_postgres_store::{
    PostgresGitOutboxRepository, PostgresStore, PostgresSupportCaseRepository,
};
use anisp_server::{AppState, build_app};
use anisp_support::SupportService;
use anisp_support_outbox::{SupportOutboxPublisher, SupportOutboxStatusService};
use tokio::net::TcpListener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = AppConfig::from_env()?;
    init_tracing(config.observability.log_filter())?;

    let clickhouse = ClickHouseStore::new(&config.clickhouse);
    let event_repository = Arc::new(ClickHouseEventRepository::new(&clickhouse));
    let event_service = EventService::new(event_repository);
    let git_context_query_service =
        GitContextQueryService::new(Arc::new(ClickHouseGitCommitReader::new(&clickhouse)));

    let postgres = PostgresStore::connect(&config.postgres).await?;
    let git_outbox_repository = Arc::new(PostgresGitOutboxRepository::new(&postgres));
    let git_outbox_status_service = GitOutboxStatusService::new(
        git_outbox_repository.clone(),
        Duration::from_millis(config.git_outbox.claim_lease_ms()),
    )?;
    let support_repository = Arc::new(PostgresSupportCaseRepository::new(&postgres));
    let support_service = SupportService::new(support_repository.clone());
    let support_context_service = SupportCaseContextService::new(
        support_service.clone(),
        event_service.clone(),
        support_repository.clone(),
        git_context_query_service.clone(),
        git_outbox_repository.clone(),
        config.context_git.clone(),
    );
    let support_outbox_status_service = SupportOutboxStatusService::new(support_repository.clone());
    let publisher = SupportOutboxPublisher::new(
        support_repository,
        event_service.clone(),
        Duration::from_millis(config.support_outbox.poll_interval_ms()),
        config.support_outbox.batch_size(),
        Duration::from_millis(config.support_outbox.claim_lease_ms()),
        config.support_outbox.max_attempts(),
    );
    let _publisher_task = tokio::spawn(publisher.run());
    let git_publisher = GitOutboxPublisher::new(
        git_outbox_repository,
        event_service.clone(),
        GitPublisherOptions::new(
            config.git_outbox.batch_size(),
            Duration::from_millis(config.git_outbox.poll_interval_ms()),
            Duration::from_millis(config.git_outbox.claim_lease_ms()),
            config.git_outbox.max_attempts(),
        )?,
    );
    let _git_publisher_task = tokio::spawn(git_publisher.run());

    let address = config.server.socket_addr();
    let listener = TcpListener::bind(address).await?;
    info!(%address, "HTTP server listening");

    let state = AppState::new(
        config,
        clickhouse,
        postgres,
        event_service,
        support_service,
        support_context_service,
        support_outbox_status_service,
        git_context_query_service,
        git_outbox_status_service,
    );
    axum::serve(listener, build_app(state)).await?;

    Ok(())
}

fn init_tracing(log_filter: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let filter = EnvFilter::try_new(log_filter)?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()?;
    Ok(())
}
