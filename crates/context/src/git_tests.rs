use super::*;
use anisp_git_context::{
    ChangeType, CommitSha, GitCommitInput, GitCommitQuery, GitCommitReader, GitCommitView,
    GitCommitViewInput, GitFileChange,
};
use anisp_support::StoredSupportCase;
use chrono::{DateTime, Duration, Utc};
use std::sync::{Arc, Mutex};

type SyncCall = (String, String, DateTime<Utc>, DateTime<Utc>);
struct FakeGitReader {
    commits: Vec<GitCommitView>,
    queries: Mutex<Vec<GitCommitQuery>>,
    error: Option<GitReaderError>,
}
#[async_trait]
impl GitCommitReader for FakeGitReader {
    async fn find_by_identity(
        &self,
        _: &str,
        _: &CommitSha,
    ) -> Result<Option<GitCommitView>, GitReaderError> {
        unreachable!()
    }
    async fn search(&self, query: &GitCommitQuery) -> Result<Vec<GitCommitView>, GitReaderError> {
        self.queries.lock().unwrap().push(query.clone());
        if let Some(e) = self.error {
            return Err(e);
        }
        let f = query.filters();
        let mut rows = self
            .commits
            .iter()
            .filter(|v| {
                f.project_id.as_deref().is_none_or(|p| p == v.project_id())
                    && f.service.as_deref().is_none_or(|s| s == v.service())
                    && query.from().is_none_or(|from| v.committed_at() >= from)
                    && query.to().is_none_or(|to| v.committed_at() < to)
            })
            .cloned()
            .collect::<Vec<_>>();
        rows.sort_by_key(|v| std::cmp::Reverse((v.committed_at(), v.event_id())));
        rows.truncate(usize::from(query.limit()));
        Ok(rows)
    }
}
struct FakeGitSync {
    pending: u64,
    error: Option<GitReaderError>,
    calls: Mutex<Vec<SyncCall>>,
}
#[async_trait]
impl GitContextSyncReader for FakeGitSync {
    async fn pending_count_for_window(
        &self,
        p: &str,
        s: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<u64, GitReaderError> {
        self.calls
            .lock()
            .unwrap()
            .push((p.into(), s.into(), from, to));
        self.error.map_or(Ok(self.pending), Err)
    }
}
fn case(project: Option<&str>, service: Option<&str>, to: DateTime<Utc>) -> SupportCase {
    let c = super::tests::support_case();
    SupportCase::rehydrate(StoredSupportCase {
        case_id: c.case_id(),
        case_type: c.case_type(),
        title: c.title().into(),
        description: c.description().into(),
        reporter_id: c.reporter_id().into(),
        assignee_id: None,
        project_id: project.map(str::to_owned),
        service: service.map(str::to_owned),
        environment: Some("prod".into()),
        priority: c.priority(),
        status: c.status(),
        created_at: to,
        updated_at: to + Duration::hours(3),
        resolved_at: None,
    })
    .unwrap()
}
fn anchor() -> DateTime<Utc> {
    "2026-10-07T12:00:00.123Z".parse().unwrap()
}
fn commit(repo: &str, sha: char, time: DateTime<Utc>) -> GitCommitView {
    GitCommitView::new(GitCommitViewInput {
        event_id: Uuid::now_v7(),
        repository_name: format!("name-{repo}"),
        project_id: "xpa".into(),
        service: "xpa-finance".into(),
        branch: Some("main".into()),
        commit: GitCommitInput {
            repository_id: repo.into(),
            commit_sha: sha.to_string().repeat(40),
            parent_shas: vec!["f".repeat(40)],
            author_name: "Author".into(),
            author_email: None,
            message: "message".into(),
            committed_at: time,
            changes: vec![],
        },
        observed_at: time + Duration::days(2),
        changed_file_count: 0,
        additions: Some(0),
        deletions: Some(0),
        changes_truncated: false,
    })
    .unwrap()
}
fn composer(
    c: SupportCase,
    commits: Vec<GitCommitView>,
    pending: u64,
    read_error: Option<GitReaderError>,
    sync_error: Option<GitReaderError>,
    config: ContextGitConfig,
) -> (
    SupportCaseContextService,
    Arc<FakeGitReader>,
    Arc<FakeGitSync>,
) {
    let mut s = super::tests::service(Some(c), false, vec![], false, 0, false);
    let reader = Arc::new(FakeGitReader {
        commits,
        queries: Mutex::default(),
        error: read_error,
    });
    let sync = Arc::new(FakeGitSync {
        pending,
        error: sync_error,
        calls: Mutex::default(),
    });
    s.git_service = GitContextQueryService::new(reader.clone());
    s.git_sync_reader = sync.clone();
    s.git_config = config;
    (s, reader, sync)
}
#[tokio::test]
async fn mapped_multiple_repositories_oldest_first_and_fixed_window() {
    let c = case(Some("xpa"), Some("xpa-finance"), anchor());
    let id = c.case_id();
    let commits = vec![
        commit("repo-a", 'c', anchor() - Duration::hours(1)),
        commit("repo-a", 'a', anchor() - Duration::hours(3)),
        commit("repo-b", 'b', anchor() - Duration::hours(2)),
    ];
    let (s, reader, sync) = composer(c, commits, 0, None, None, Default::default());
    for _ in 0..2 {
        let ctx = s.get_context(id).await.unwrap().unwrap();
        let g = ctx.git_context();
        assert_eq!(g.status(), GitContextStatus::Synced);
        assert_eq!(g.pending_events(), 0);
        assert_eq!(
            g.commits()
                .iter()
                .map(|v| v.commit_sha().as_str())
                .collect::<Vec<_>>(),
            vec!["a".repeat(40), "b".repeat(40), "c".repeat(40)]
        );
        assert_eq!(g.commits()[1].repository_id(), "repo-b");
        assert_eq!(g.window().unwrap().from(), anchor() - Duration::hours(24));
        assert_eq!(g.window().unwrap().to(), anchor());
        assert_eq!(ctx.history_sync().status(), HistorySyncStatus::Synced);
    }
    let q = reader.queries.lock().unwrap();
    assert_eq!(q[0], q[1]);
    assert_eq!(q[0].limit(), 50);
    assert_eq!(q[0].filters().project_id.as_deref(), Some("xpa"));
    assert_eq!(q[0].filters().service.as_deref(), Some("xpa-finance"));
    assert!(q[0].filters().repository_id.is_none() && q[0].filters().branch.is_none());
    assert_eq!(
        sync.calls.lock().unwrap()[0],
        (
            "xpa".into(),
            "xpa-finance".into(),
            anchor() - Duration::hours(24),
            anchor()
        )
    );
}
#[tokio::test]
async fn half_open_window_excludes_old_equal_upper_and_future_commits() {
    let from = anchor() - Duration::hours(24);
    let c = case(Some("xpa"), Some("xpa-finance"), anchor());
    let id = c.case_id();
    let rows = vec![
        commit("a", 'a', from - Duration::milliseconds(1)),
        commit("a", 'b', from),
        commit("a", 'c', anchor() - Duration::milliseconds(1)),
        commit("a", 'd', anchor()),
        commit("a", 'e', anchor() + Duration::hours(1)),
    ];
    let (s, _, _) = composer(c, rows, 0, None, None, Default::default());
    let ctx = s.get_context(id).await.unwrap().unwrap();
    assert_eq!(
        ctx.git_context()
            .commits()
            .iter()
            .map(|v| v.commit_sha().as_str())
            .collect::<Vec<_>>(),
        vec!["b".repeat(40), "c".repeat(40)]
    );
}
#[tokio::test]
async fn raw_microsecond_case_window_matches_millisecond_projection() {
    let to = anchor() + Duration::microseconds(456);
    let from = to - Duration::hours(24);
    let c = case(Some("xpa"), Some("xpa-finance"), to);
    let id = c.case_id();
    let rows = vec![
        commit("a", 'a', anchor() - Duration::hours(24)),
        commit(
            "a",
            'b',
            anchor() - Duration::hours(24) + Duration::milliseconds(1),
        ),
        commit("a", 'c', anchor()),
        commit("a", 'd', anchor() + Duration::milliseconds(1)),
    ];
    let (s, _, sync) = composer(c, rows, 0, None, None, Default::default());
    let ctx = s.get_context(id).await.unwrap().unwrap();
    assert_eq!(ctx.git_context().commits().len(), 2);
    assert_eq!(
        ctx.git_context().commits()[0].commit_sha().as_str(),
        "b".repeat(40)
    );
    assert_eq!(ctx.git_context().window().unwrap().to(), to);
    assert_eq!(sync.calls.lock().unwrap()[0].2, from);
}
#[tokio::test]
async fn mapped_empty_is_synced_not_unmapped() {
    let c = case(Some("xpa"), Some("xpa-finance"), anchor());
    let id = c.case_id();
    let (s, reader, sync) = composer(c, vec![], 0, None, None, Default::default());
    let ctx = s.get_context(id).await.unwrap().unwrap();
    assert_eq!(ctx.git_context().status(), GitContextStatus::Synced);
    assert!(ctx.git_context().commits().is_empty());
    assert!(ctx.git_context().window().is_some());
    assert_eq!(reader.queries.lock().unwrap().len(), 1);
    assert_eq!(sync.calls.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn missing_or_blank_mapping_never_queries_global_git() {
    for (p, s) in [
        (None, Some("xpa-finance")),
        (Some("xpa"), None),
        (None, None),
        (Some(""), Some("xpa-finance")),
        (Some("xpa"), Some(" ")),
    ] {
        let c = case(p, s, anchor());
        let id = c.case_id();
        let (s, reader, sync) = composer(
            c,
            vec![],
            9,
            Some(GitReaderError::Unavailable),
            Some(GitReaderError::Unavailable),
            Default::default(),
        );
        let ctx = s.get_context(id).await.unwrap().unwrap();
        let g = ctx.git_context();
        assert_eq!(g.status(), GitContextStatus::Unmapped);
        assert_eq!(g.pending_events(), 0);
        assert!(g.commits().is_empty() && g.window().is_none());
        assert!(reader.queries.lock().unwrap().is_empty() && sync.calls.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn pending_git_is_independent_of_support_history_sync() {
    let c = case(Some("xpa"), Some("xpa-finance"), anchor());
    let id = c.case_id();
    let (s, _, _) = composer(
        c,
        vec![commit("repo", 'a', anchor() - Duration::hours(1))],
        2,
        None,
        None,
        Default::default(),
    );
    let ctx = s.get_context(id).await.unwrap().unwrap();
    assert_eq!(ctx.git_context().status(), GitContextStatus::Pending);
    assert_eq!(ctx.git_context().pending_events(), 2);
    assert_eq!(ctx.git_context().commits().len(), 1);
    assert_eq!(ctx.history_sync().status(), HistorySyncStatus::Synced);
}
#[tokio::test]
async fn recent_set_is_bounded_then_sorted_ascending() {
    let c = case(Some("xpa"), Some("xpa-finance"), anchor());
    let id = c.case_id();
    let rows = vec![
        commit("repo", 'a', anchor() - Duration::hours(3)),
        commit("repo", 'b', anchor() - Duration::hours(2)),
        commit("repo", 'c', anchor() - Duration::hours(1)),
    ];
    let (s, _, _) = composer(c, rows, 0, None, None, ContextGitConfig::new(2, 1).unwrap());
    let ctx = s.get_context(id).await.unwrap().unwrap();
    let g = ctx.git_context();
    assert_eq!(g.commits().len(), 1);
    assert_eq!(g.commits()[0].commit_sha().as_str(), "c".repeat(40));
    assert_eq!(g.window().unwrap().from(), anchor() - Duration::hours(2));
}
#[tokio::test]
async fn git_reader_and_sync_failures_are_not_silently_empty() {
    for error in [
        GitReaderError::Unavailable,
        GitReaderError::Timeout,
        GitReaderError::Decode,
        GitReaderError::Internal,
    ] {
        let c = case(Some("xpa"), Some("xpa-finance"), anchor());
        let id = c.case_id();
        let (s, _, _) = composer(c.clone(), vec![], 0, Some(error), None, Default::default());
        assert!(
            matches!(s.get_context(id).await,Err(SupportCaseContextError::Git(GitContextQueryError::Reader(e))) if e==error)
        );
        let (s, reader, _) = composer(c, vec![], 0, None, Some(error), Default::default());
        assert!(
            matches!(s.get_context(id).await,Err(SupportCaseContextError::GitSync(e)) if e==error)
        );
        assert!(reader.queries.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn truncated_count_and_binary_unknown_stats_survive_composition() {
    let c = case(Some("xpa"), Some("xpa-finance"), anchor());
    let id = c.case_id();
    let row = GitCommitView::new(GitCommitViewInput {
        event_id: Uuid::now_v7(),
        repository_name: "repo".into(),
        project_id: "xpa".into(),
        service: "xpa-finance".into(),
        branch: None,
        commit: GitCommitInput {
            repository_id: "repo".into(),
            commit_sha: "a".repeat(40),
            parent_shas: vec![],
            author_name: "Author".into(),
            author_email: None,
            message: "binary change".into(),
            committed_at: anchor() - Duration::hours(1),
            changes: vec![
                GitFileChange::new("binary.bin", None, ChangeType::Added, None, None).unwrap(),
            ],
        },
        observed_at: anchor(),
        changed_file_count: 1000,
        additions: None,
        deletions: None,
        changes_truncated: true,
    })
    .unwrap();
    let (s, _, _) = composer(c, vec![row.clone()], 0, None, None, Default::default());
    let ctx = s.get_context(id).await.unwrap().unwrap();
    assert_eq!(ctx.git_context().commits(), &[row]);
    let v = &ctx.git_context().commits()[0];
    assert_eq!(v.changed_file_count(), 1000);
    assert!(v.changes_truncated());
    assert_eq!(v.changed_files()[0].additions(), None);
}
#[tokio::test]
async fn impossible_persisted_window_returns_error_without_panic() {
    let c = case(Some("xpa"), Some("xpa-finance"), DateTime::<Utc>::MIN_UTC);
    let id = c.case_id();
    let (s, _, _) = composer(c, vec![], 0, None, None, Default::default());
    assert!(matches!(
        s.get_context(id).await,
        Err(SupportCaseContextError::InvalidGitWindow)
    ));
}
