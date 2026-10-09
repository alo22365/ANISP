#![allow(dead_code)]
use anisp_clickhouse_store::{ClickHouseReadiness, PingFuture};
use anisp_config::AppConfig;
use anisp_context::{SupportCaseContextService, SupportContextSyncReader};
use anisp_event::{ContextEvent, EventQuery, EventRepository, EventService, RepositoryError};
use anisp_git_context::{
    CommitSha, GitCommitInput, GitCommitQuery, GitCommitReader, GitCommitView, GitCommitViewInput,
    GitContextQueryService, GitContextSyncReader, GitReaderError,
};
use anisp_git_ingest::{
    GitOutboxError, GitOutboxOperationalReader, GitOutboxOperationalStatus, GitOutboxStatusService,
};
use anisp_postgres_store::{PostgresPingFuture, PostgresReadiness};
use anisp_server::AppState;
use anisp_support::{
    SupportCase, SupportCaseLifecycleEvent, SupportCaseQuery, SupportCaseRepository,
    SupportOutboxOperationalReader, SupportOutboxOperationalStatus, SupportRepositoryError,
    SupportService,
};
use anisp_support_outbox::SupportOutboxStatusService;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::{future, sync::Arc};
use uuid::Uuid;

pub struct FakeGitReader {
    pub rows: Vec<GitCommitView>,
    pub error: Option<GitReaderError>,
}
#[async_trait]
impl GitCommitReader for FakeGitReader {
    async fn find_by_identity(
        &self,
        repository_id: &str,
        sha: &CommitSha,
    ) -> Result<Option<GitCommitView>, GitReaderError> {
        if let Some(e) = self.error {
            return Err(e);
        }
        Ok(self
            .rows
            .iter()
            .find(|v| v.repository_id() == repository_id && v.commit_sha() == sha)
            .cloned())
    }
    async fn search(&self, query: &GitCommitQuery) -> Result<Vec<GitCommitView>, GitReaderError> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let f = query.filters();
        let mut rows = self
            .rows
            .iter()
            .filter(|v| {
                f.repository_id
                    .as_ref()
                    .is_none_or(|s| s == v.repository_id())
                    && f.project_id.as_ref().is_none_or(|s| s == v.project_id())
                    && f.service.as_ref().is_none_or(|s| s == v.service())
                    && f.branch
                        .as_ref()
                        .is_none_or(|s| Some(s.as_str()) == v.branch())
                    && query.from().is_none_or(|t| v.committed_at() >= t)
                    && query.to().is_none_or(|t| v.committed_at() < t)
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(|v| std::cmp::Reverse((v.committed_at(), v.event_id())));
        rows.truncate(usize::from(query.limit()));
        Ok(rows)
    }
}
pub fn empty_git_service() -> GitContextQueryService {
    GitContextQueryService::new(Arc::new(FakeGitReader {
        rows: vec![],
        error: None,
    }))
}

pub struct FakeGitSyncReader {
    pub pending: u64,
    pub error: Option<GitReaderError>,
}
#[async_trait]
impl GitContextSyncReader for FakeGitSyncReader {
    async fn pending_count_for_window(
        &self,
        _: &str,
        _: &str,
        _: DateTime<Utc>,
        _: DateTime<Utc>,
    ) -> Result<u64, GitReaderError> {
        self.error.map_or(Ok(self.pending), Err)
    }
}
pub fn empty_git_sync_reader() -> Arc<dyn GitContextSyncReader> {
    Arc::new(FakeGitSyncReader {
        pending: 0,
        error: None,
    })
}

pub struct FakeGitOperationalReader {
    pub status: GitOutboxOperationalStatus,
    pub error: Option<GitOutboxError>,
}
#[async_trait]
impl GitOutboxOperationalReader for FakeGitOperationalReader {
    async fn read_operational_status(
        &self,
        _: DateTime<Utc>,
        _: DateTime<Utc>,
    ) -> Result<GitOutboxOperationalStatus, GitOutboxError> {
        self.error.map_or(Ok(self.status), Err)
    }
}
pub fn empty_git_status_service() -> GitOutboxStatusService {
    GitOutboxStatusService::new(
        Arc::new(FakeGitOperationalReader {
            status: Default::default(),
            error: None,
        }),
        std::time::Duration::from_secs(30),
    )
    .unwrap()
}
pub fn view(
    repo: &str,
    digit: char,
    seconds: i64,
    project: &str,
    service: &str,
    branch: Option<&str>,
) -> GitCommitView {
    GitCommitView::new(GitCommitViewInput {
        event_id: Uuid::now_v7(),
        repository_name: format!("name-{repo}"),
        project_id: project.into(),
        service: service.into(),
        branch: branch.map(str::to_owned),
        commit: GitCommitInput {
            repository_id: repo.into(),
            commit_sha: digit.to_string().repeat(40),
            parent_shas: vec!["d".repeat(40)],
            author_name: "Author".into(),
            author_email: Some("author@example.com".into()),
            message: "Commit message".into(),
            committed_at: DateTime::from_timestamp(seconds, 0).unwrap(),
            changes: vec![],
        },
        observed_at: Utc::now(),
        changed_file_count: 0,
        additions: Some(0),
        deletions: Some(0),
        changes_truncated: false,
    })
    .unwrap()
}

struct Noop;
impl ClickHouseReadiness for Noop {
    fn ping(&self) -> PingFuture<'_> {
        Box::pin(future::ready(Ok(())))
    }
}
impl PostgresReadiness for Noop {
    fn ping(&self) -> PostgresPingFuture<'_> {
        Box::pin(future::ready(Ok(())))
    }
}
#[async_trait]
impl EventRepository for Noop {
    async fn insert(&self, _: &ContextEvent) -> Result<(), RepositoryError> {
        Ok(())
    }
    async fn find_by_id(&self, _: Uuid) -> Result<Option<ContextEvent>, RepositoryError> {
        Ok(None)
    }
    async fn search(&self, _: &EventQuery) -> Result<Vec<ContextEvent>, RepositoryError> {
        Ok(vec![])
    }
}
#[async_trait]
impl SupportCaseRepository for Noop {
    async fn insert(
        &self,
        _: &SupportCase,
        _: &SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError> {
        Ok(())
    }
    async fn update(
        &self,
        _: &SupportCase,
        _: &SupportCaseLifecycleEvent,
    ) -> Result<(), SupportRepositoryError> {
        Ok(())
    }
    async fn find_by_id(&self, _: Uuid) -> Result<Option<SupportCase>, SupportRepositoryError> {
        Ok(None)
    }
    async fn search(
        &self,
        _: &SupportCaseQuery,
    ) -> Result<Vec<SupportCase>, SupportRepositoryError> {
        Ok(vec![])
    }
}
#[async_trait]
impl SupportContextSyncReader for Noop {
    async fn pending_count_for_case(&self, _: Uuid) -> Result<u64, SupportRepositoryError> {
        Ok(0)
    }
}
#[async_trait]
impl SupportOutboxOperationalReader for Noop {
    async fn operational_status(
        &self,
        _: DateTime<Utc>,
    ) -> Result<SupportOutboxOperationalStatus, SupportRepositoryError> {
        Ok(SupportOutboxOperationalStatus::new(0, None, 0, None))
    }
}
pub fn app(service: GitContextQueryService) -> axum::Router {
    app_with_git_operations(service, empty_git_status_service())
}
pub fn app_with_git_operations(
    service: GitContextQueryService,
    operations: GitOutboxStatusService,
) -> axum::Router {
    let events = EventService::new(Arc::new(Noop));
    let support = SupportService::new(Arc::new(Noop));
    let context = SupportCaseContextService::new(
        support.clone(),
        events.clone(),
        Arc::new(Noop),
        service.clone(),
        empty_git_sync_reader(),
        Default::default(),
    );
    anisp_server::build_app(AppState::with_dependencies(
        AppConfig::default(),
        Noop,
        Noop,
        events,
        support,
        context,
        SupportOutboxStatusService::new(Arc::new(Noop)),
        service,
        operations,
    ))
}
