//! Narrow application composition: case, lifecycle history and time/label-
//! associated published Git context. No causality or relevance judgment.

mod git;
pub use git::{GitContext, GitContextStatus, GitContextWindow};

use std::{error::Error, fmt, sync::Arc};

use anisp_config::ContextGitConfig;
use anisp_event::{ContextEvent, EventFilters, EventQuery, EventService, EventServiceError};
use anisp_git_context::{
    GitContextQueryError, GitContextQueryService, GitContextSyncReader, GitReaderError,
};
use anisp_support::{SupportCase, SupportRepositoryError, SupportService, SupportServiceError};
use async_trait::async_trait;
use uuid::Uuid;

#[async_trait]
pub trait SupportContextSyncReader: Send + Sync {
    async fn pending_count_for_case(&self, case_id: Uuid) -> Result<u64, SupportRepositoryError>;
}

#[derive(Clone)]
pub struct SupportCaseContextService {
    support_service: SupportService,
    event_service: EventService,
    sync_reader: Arc<dyn SupportContextSyncReader>,
    git_service: GitContextQueryService,
    git_sync_reader: Arc<dyn GitContextSyncReader>,
    git_config: ContextGitConfig,
}

impl SupportCaseContextService {
    pub fn new(
        support_service: SupportService,
        event_service: EventService,
        sync_reader: Arc<dyn SupportContextSyncReader>,
        git_service: GitContextQueryService,
        git_sync_reader: Arc<dyn GitContextSyncReader>,
        git_config: ContextGitConfig,
    ) -> Self {
        Self {
            support_service,
            event_service,
            sync_reader,
            git_service,
            git_sync_reader,
            git_config,
        }
    }

    pub async fn get_context(
        &self,
        case_id: Uuid,
    ) -> Result<Option<SupportCaseContext>, SupportCaseContextError> {
        let Some(support_case) = self.support_service.get_case(case_id).await? else {
            return Ok(None);
        };

        // Read pending state before ClickHouse so a publisher completing
        // between the two reads cannot be reported as synced with a stale
        // timeline. A request that races with publication stays conservatively
        // pending until its next read.
        let pending_events = self
            .sync_reader
            .pending_count_for_case(case_id)
            .await
            .map_err(SupportCaseContextError::Sync)?;

        let query = EventQuery::new(
            EventFilters {
                source: Some("support".to_owned()),
                subject_type: Some("support_case".to_owned()),
                subject_id: Some(case_id.to_string()),
                ..EventFilters::default()
            },
            None,
            None,
            Some(500),
        )
        .expect("fixed support context query is valid");
        let mut events = self.event_service.search_events(&query).await?;
        events.sort_by(|left, right| {
            left.event_time()
                .cmp(&right.event_time())
                .then_with(|| left.event_id().cmp(&right.event_id()))
        });

        let history_sync = HistorySync {
            status: if pending_events == 0 {
                HistorySyncStatus::Synced
            } else {
                HistorySyncStatus::Pending
            },
            pending_events,
        };
        let git_context = self.get_git_context(&support_case).await?;

        Ok(Some(SupportCaseContext {
            support_case,
            events,
            history_sync,
            git_context,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistorySyncStatus {
    Synced,
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistorySync {
    status: HistorySyncStatus,
    pending_events: u64,
}

impl HistorySync {
    pub fn status(&self) -> HistorySyncStatus {
        self.status
    }

    pub fn pending_events(&self) -> u64 {
        self.pending_events
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SupportCaseContext {
    support_case: SupportCase,
    events: Vec<ContextEvent>,
    history_sync: HistorySync,
    git_context: GitContext,
}

impl SupportCaseContext {
    pub fn support_case(&self) -> &SupportCase {
        &self.support_case
    }

    pub fn events(&self) -> &[ContextEvent] {
        &self.events
    }

    pub fn history_sync(&self) -> &HistorySync {
        &self.history_sync
    }

    pub fn git_context(&self) -> &GitContext {
        &self.git_context
    }

    pub fn into_parts(self) -> (SupportCase, Vec<ContextEvent>, HistorySync, GitContext) {
        (
            self.support_case,
            self.events,
            self.history_sync,
            self.git_context,
        )
    }
}

#[derive(Debug)]
pub enum SupportCaseContextError {
    Support(SupportServiceError),
    Event(EventServiceError),
    Sync(SupportRepositoryError),
    Git(GitContextQueryError),
    GitSync(GitReaderError),
    InvalidGitWindow,
}

impl fmt::Display for SupportCaseContextError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Support(error) => error.fmt(formatter),
            Self::Event(error) => error.fmt(formatter),
            Self::Sync(error) => error.fmt(formatter),
            Self::Git(error) => error.fmt(formatter),
            Self::GitSync(error) => error.fmt(formatter),
            Self::InvalidGitWindow => formatter.write_str("invalid persisted case context window"),
        }
    }
}

impl Error for SupportCaseContextError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Support(error) => Some(error),
            Self::Event(error) => Some(error),
            Self::Sync(error) => Some(error),
            Self::Git(error) => Some(error),
            Self::GitSync(error) => Some(error),
            Self::InvalidGitWindow => None,
        }
    }
}

impl From<SupportServiceError> for SupportCaseContextError {
    fn from(error: SupportServiceError) -> Self {
        Self::Support(error)
    }
}

impl From<EventServiceError> for SupportCaseContextError {
    fn from(error: EventServiceError) -> Self {
        Self::Event(error)
    }
}

#[cfg(test)]
mod git_tests;

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use anisp_event::{EventRepository, RepositoryError, StoredContextEvent};
    use anisp_git_context::{CommitSha, GitCommitQuery, GitCommitReader, GitCommitView};
    use anisp_support::{
        CaseType, CreateSupportCaseCommand, Priority, SupportCaseLifecycleEvent, SupportCaseQuery,
        SupportCaseRepository,
    };
    use chrono::{Duration, TimeZone, Utc};
    use serde_json::json;

    use super::*;

    struct FakeSupportRepository {
        support_case: Option<SupportCase>,
        fail: bool,
    }

    #[async_trait]
    impl SupportCaseRepository for FakeSupportRepository {
        async fn insert(
            &self,
            _support_case: &SupportCase,
            _event: &SupportCaseLifecycleEvent,
        ) -> Result<(), SupportRepositoryError> {
            unreachable!()
        }

        async fn update(
            &self,
            _support_case: &SupportCase,
            _event: &SupportCaseLifecycleEvent,
        ) -> Result<(), SupportRepositoryError> {
            unreachable!()
        }

        async fn find_by_id(
            &self,
            case_id: Uuid,
        ) -> Result<Option<SupportCase>, SupportRepositoryError> {
            if self.fail {
                return Err(SupportRepositoryError::unavailable());
            }
            Ok(self
                .support_case
                .as_ref()
                .filter(|support_case| support_case.case_id() == case_id)
                .cloned())
        }

        async fn search(
            &self,
            _query: &SupportCaseQuery,
        ) -> Result<Vec<SupportCase>, SupportRepositoryError> {
            unreachable!()
        }
    }

    struct FakeEventRepository {
        events: Mutex<Vec<ContextEvent>>,
        fail: bool,
    }

    #[async_trait]
    impl EventRepository for FakeEventRepository {
        async fn insert(&self, _event: &ContextEvent) -> Result<(), RepositoryError> {
            unreachable!()
        }

        async fn find_by_id(
            &self,
            _event_id: Uuid,
        ) -> Result<Option<ContextEvent>, RepositoryError> {
            unreachable!()
        }

        async fn search(&self, query: &EventQuery) -> Result<Vec<ContextEvent>, RepositoryError> {
            if self.fail {
                return Err(RepositoryError::unavailable());
            }
            let filters = query.filters();
            Ok(self
                .events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| {
                    filters
                        .source
                        .as_deref()
                        .is_none_or(|value| event.source() == value)
                        && filters
                            .subject_type
                            .as_deref()
                            .is_none_or(|value| event.subject_type() == value)
                        && filters
                            .subject_id
                            .as_deref()
                            .is_none_or(|value| event.subject_id() == value)
                })
                .cloned()
                .collect())
        }
    }

    struct FakeSyncReader {
        pending: u64,
        fail: bool,
    }

    #[async_trait]
    impl SupportContextSyncReader for FakeSyncReader {
        async fn pending_count_for_case(
            &self,
            _case_id: Uuid,
        ) -> Result<u64, SupportRepositoryError> {
            if self.fail {
                Err(SupportRepositoryError::unavailable())
            } else {
                Ok(self.pending)
            }
        }
    }

    pub(super) fn support_case() -> SupportCase {
        SupportCase::create(CreateSupportCaseCommand {
            case_type: CaseType::Incident,
            title: "xpa unavailable".to_owned(),
            description: "production outage".to_owned(),
            reporter_id: "user-001".to_owned(),
            assignee_id: None,
            project_id: Some("xpa".to_owned()),
            service: Some("xpa-finance".to_owned()),
            environment: Some("prod".to_owned()),
            priority: Priority::High,
        })
        .unwrap()
    }

    fn event(case_id: Uuid, offset_seconds: i64, action: &str) -> ContextEvent {
        let time = Utc.with_ymd_and_hms(2026, 10, 6, 10, 0, 0).unwrap()
            + Duration::seconds(offset_seconds);
        ContextEvent::from_stored(StoredContextEvent {
            event_id: Uuid::now_v7(),
            event_time: time,
            ingest_time: time,
            source: "support".to_owned(),
            event_type: format!("support.incident.{action}"),
            project_id: "xpa".to_owned(),
            service: "xpa-finance".to_owned(),
            environment: "prod".to_owned(),
            subject_type: "support_case".to_owned(),
            subject_id: case_id.to_string(),
            title: "xpa unavailable".to_owned(),
            content: "lifecycle".to_owned(),
            trace_id: None,
            correlation_id: Some(case_id.to_string()),
            metadata: json!({}),
        })
        .unwrap()
    }

    struct EmptyGitReader;
    #[async_trait]
    impl GitCommitReader for EmptyGitReader {
        async fn find_by_identity(
            &self,
            _: &str,
            _: &CommitSha,
        ) -> Result<Option<GitCommitView>, GitReaderError> {
            Ok(None)
        }
        async fn search(&self, _: &GitCommitQuery) -> Result<Vec<GitCommitView>, GitReaderError> {
            Ok(vec![])
        }
    }
    #[async_trait]
    impl GitContextSyncReader for EmptyGitReader {
        async fn pending_count_for_window(
            &self,
            _: &str,
            _: &str,
            _: chrono::DateTime<Utc>,
            _: chrono::DateTime<Utc>,
        ) -> Result<u64, GitReaderError> {
            Ok(0)
        }
    }

    pub(super) fn service(
        support_case: Option<SupportCase>,
        support_fail: bool,
        events: Vec<ContextEvent>,
        event_fail: bool,
        pending: u64,
        sync_fail: bool,
    ) -> SupportCaseContextService {
        SupportCaseContextService::new(
            SupportService::new(Arc::new(FakeSupportRepository {
                support_case,
                fail: support_fail,
            })),
            EventService::new(Arc::new(FakeEventRepository {
                events: Mutex::new(events),
                fail: event_fail,
            })),
            Arc::new(FakeSyncReader {
                pending,
                fail: sync_fail,
            }),
            GitContextQueryService::new(Arc::new(EmptyGitReader)),
            Arc::new(EmptyGitReader),
            ContextGitConfig::default(),
        )
    }

    #[tokio::test]
    async fn existing_context_is_oldest_first_and_synced() {
        let support_case = support_case();
        let case_id = support_case.case_id();
        let context = service(
            Some(support_case),
            false,
            vec![event(case_id, 3, "closed"), event(case_id, 0, "created")],
            false,
            0,
            false,
        )
        .get_context(case_id)
        .await
        .unwrap()
        .unwrap();

        assert_eq!(context.events()[0].event_type(), "support.incident.created");
        assert_eq!(context.events()[1].event_type(), "support.incident.closed");
        assert_eq!(context.history_sync().status(), HistorySyncStatus::Synced);
        assert_eq!(context.history_sync().pending_events(), 0);
    }

    #[tokio::test]
    async fn pending_state_is_returned_without_hiding_available_history() {
        let support_case = support_case();
        let case_id = support_case.case_id();
        let context = service(Some(support_case), false, vec![], false, 2, false)
            .get_context(case_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(context.history_sync().status(), HistorySyncStatus::Pending);
        assert_eq!(context.history_sync().pending_events(), 2);
    }

    #[tokio::test]
    async fn missing_case_returns_none() {
        let result = service(None, false, vec![], false, 0, false)
            .get_context(Uuid::now_v7())
            .await
            .unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn postgres_or_clickhouse_failure_is_not_silently_degraded() {
        let support_case = support_case();
        let case_id = support_case.case_id();
        assert!(matches!(
            service(Some(support_case.clone()), true, vec![], false, 0, false)
                .get_context(case_id)
                .await,
            Err(SupportCaseContextError::Support(_))
        ));
        assert!(matches!(
            service(Some(support_case.clone()), false, vec![], true, 0, false)
                .get_context(case_id)
                .await,
            Err(SupportCaseContextError::Event(_))
        ));
        assert!(matches!(
            service(Some(support_case), false, vec![], false, 0, true)
                .get_context(case_id)
                .await,
            Err(SupportCaseContextError::Sync(_))
        ));
    }
}
