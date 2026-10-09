use std::sync::Arc;

use anisp_clickhouse_store::{ClickHouseReadiness, ClickHouseStore};
use anisp_config::AppConfig;
use anisp_context::SupportCaseContextService;
use anisp_event::EventService;
use anisp_git_context::GitContextQueryService;
use anisp_git_ingest::GitOutboxStatusService;
use anisp_postgres_store::{PostgresReadiness, PostgresStore};
use anisp_support::SupportService;
use anisp_support_outbox::SupportOutboxStatusService;

/// Dependencies shared by all request handlers.
///
/// Future services and stores should be added here and exposed through narrow
/// interfaces. Handlers must not create or connect to databases directly.
#[derive(Clone)]
pub struct AppState {
    config: Arc<AppConfig>,
    clickhouse: Arc<dyn ClickHouseReadiness>,
    postgres: Arc<dyn PostgresReadiness>,
    event_service: EventService,
    support_service: SupportService,
    support_context_service: SupportCaseContextService,
    support_outbox_status_service: SupportOutboxStatusService,
    git_context_query_service: GitContextQueryService,
    git_outbox_status_service: GitOutboxStatusService,
}

impl AppState {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: AppConfig,
        clickhouse: ClickHouseStore,
        postgres: PostgresStore,
        event_service: EventService,
        support_service: SupportService,
        support_context_service: SupportCaseContextService,
        support_outbox_status_service: SupportOutboxStatusService,
        git_context_query_service: GitContextQueryService,
        git_outbox_status_service: GitOutboxStatusService,
    ) -> Self {
        Self::with_dependencies(
            config,
            clickhouse,
            postgres,
            event_service,
            support_service,
            support_context_service,
            support_outbox_status_service,
            git_context_query_service,
            git_outbox_status_service,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn with_dependencies<C, P>(
        config: AppConfig,
        clickhouse: C,
        postgres: P,
        event_service: EventService,
        support_service: SupportService,
        support_context_service: SupportCaseContextService,
        support_outbox_status_service: SupportOutboxStatusService,
        git_context_query_service: GitContextQueryService,
        git_outbox_status_service: GitOutboxStatusService,
    ) -> Self
    where
        C: ClickHouseReadiness + 'static,
        P: PostgresReadiness + 'static,
    {
        Self {
            config: Arc::new(config),
            clickhouse: Arc::new(clickhouse),
            postgres: Arc::new(postgres),
            event_service,
            support_service,
            support_context_service,
            support_outbox_status_service,
            git_context_query_service,
            git_outbox_status_service,
        }
    }

    pub fn config(&self) -> &AppConfig {
        self.config.as_ref()
    }

    pub fn clickhouse(&self) -> &(dyn ClickHouseReadiness + 'static) {
        self.clickhouse.as_ref()
    }

    pub fn postgres(&self) -> &(dyn PostgresReadiness + 'static) {
        self.postgres.as_ref()
    }

    pub fn event_service(&self) -> &EventService {
        &self.event_service
    }

    pub fn support_service(&self) -> &SupportService {
        &self.support_service
    }

    pub fn support_context_service(&self) -> &SupportCaseContextService {
        &self.support_context_service
    }

    pub fn support_outbox_status_service(&self) -> &SupportOutboxStatusService {
        &self.support_outbox_status_service
    }

    pub fn git_context_query_service(&self) -> &GitContextQueryService {
        &self.git_context_query_service
    }

    pub fn git_outbox_status_service(&self) -> &GitOutboxStatusService {
        &self.git_outbox_status_service
    }
}
