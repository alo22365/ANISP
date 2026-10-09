use anisp_git_context::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

fn time(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap()
}
fn view() -> GitCommitView {
    GitCommitView::new(GitCommitViewInput {
        event_id: Uuid::now_v7(),
        repository_name: "local".into(),
        project_id: "xpa".into(),
        service: "finance".into(),
        branch: Some("main".into()),
        commit: GitCommitInput {
            repository_id: "repo-a".into(),
            commit_sha: "a".repeat(40),
            parent_shas: vec![],
            author_name: "Author".into(),
            author_email: None,
            message: String::new(),
            committed_at: time(1000),
            changes: vec![],
        },
        observed_at: time(2000),
        changed_file_count: 0,
        additions: Some(0),
        deletions: Some(0),
        changes_truncated: false,
    })
    .unwrap()
}
#[test]
fn query_default_max_and_invalid_limits() {
    assert_eq!(GitCommitQuery::default().limit(), 100);
    assert_eq!(
        GitCommitQuery::new(Default::default(), None, None, Some(500))
            .unwrap()
            .limit(),
        500
    );
    for limit in [0, 501, u32::MAX] {
        assert_eq!(
            GitCommitQuery::new(Default::default(), None, None, Some(limit)).unwrap_err(),
            GitQueryError::InvalidLimit
        );
    }
}
#[test]
fn query_range_and_empty_filter_validation() {
    assert!(GitCommitQuery::new(Default::default(), Some(time(2)), Some(time(1)), None).is_err());
    assert!(GitCommitQuery::new(Default::default(), Some(time(1)), Some(time(1)), None).is_ok());
    for f in [
        GitCommitFilters {
            repository_id: Some(" ".into()),
            ..Default::default()
        },
        GitCommitFilters {
            project_id: Some("".into()),
            ..Default::default()
        },
        GitCommitFilters {
            service: Some("".into()),
            ..Default::default()
        },
        GitCommitFilters {
            branch: Some("".into()),
            ..Default::default()
        },
    ] {
        assert_eq!(
            GitCommitQuery::new(f, None, None, None).unwrap_err(),
            GitQueryError::EmptyFilter
        );
    }
}
#[test]
fn query_submillisecond_bounds_preserve_half_open_semantics() {
    let from = DateTime::from_timestamp(0, 1_000_001).unwrap();
    let q = GitCommitQuery::new(Default::default(), Some(from), Some(from), None).unwrap();
    assert_eq!(q.from(), Some(time(2)));
    assert_eq!(q.from(), q.to());
    assert_eq!(
        GitCommitQuery::new(
            Default::default(),
            Some(DateTime::<Utc>::MAX_UTC),
            None,
            None
        )
        .unwrap_err(),
        GitQueryError::InvalidTimeRange
    );
}
struct FakeReader {
    error: Option<GitReaderError>,
    queries: Mutex<Vec<GitCommitQuery>>,
}
#[async_trait]
impl GitCommitReader for FakeReader {
    async fn find_by_identity(
        &self,
        repo: &str,
        sha: &CommitSha,
    ) -> Result<Option<GitCommitView>, GitReaderError> {
        if let Some(e) = self.error {
            return Err(e);
        }
        Ok((repo == "repo-a" && sha.as_str() == "a".repeat(40)).then(view))
    }
    async fn search(&self, q: &GitCommitQuery) -> Result<Vec<GitCommitView>, GitReaderError> {
        if let Some(e) = self.error {
            return Err(e);
        }
        self.queries.lock().unwrap().push(q.clone());
        Ok(vec![])
    }
}
fn reader(error: Option<GitReaderError>) -> Arc<FakeReader> {
    Arc::new(FakeReader {
        error,
        queries: Mutex::default(),
    })
}
#[tokio::test]
async fn detail_existing_missing_normalization_and_invalid_sha() {
    let service = GitContextQueryService::new(reader(None));
    let v = service
        .get_commit("repo-a", &"A".repeat(40))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(v.commit_sha().as_str(), "a".repeat(40));
    assert_eq!(v.message(), "");
    assert_eq!(v.observed_at(), time(2000));
    assert_eq!(v.committed_at(), time(1000));
    assert!(
        service
            .get_commit("repo-b", &"a".repeat(40))
            .await
            .unwrap()
            .is_none()
    );
    for (id, sha) in [("repo-a", "bad"), (" ", &"a".repeat(40))] {
        assert!(matches!(
            service.get_commit(id, sha).await,
            Err(GitContextQueryError::InvalidQuery(_))
        ));
    }
}
#[tokio::test]
async fn search_empty_forwards_all_validated_filters() {
    let r = reader(None);
    let service = GitContextQueryService::new(r.clone());
    let query = GitCommitQuery::new(
        GitCommitFilters {
            repository_id: Some("repo".into()),
            project_id: Some("xpa".into()),
            service: Some("finance".into()),
            branch: Some("main".into()),
        },
        Some(time(1)),
        Some(time(2)),
        Some(12),
    )
    .unwrap();
    assert!(service.search_commits(&query).await.unwrap().is_empty());
    assert_eq!(r.queries.lock().unwrap().as_slice(), &[query]);
}
#[tokio::test]
async fn reader_errors_preserve_categories_for_detail_and_list() {
    for e in [
        GitReaderError::Unavailable,
        GitReaderError::Timeout,
        GitReaderError::Decode,
        GitReaderError::Internal,
    ] {
        let s = GitContextQueryService::new(reader(Some(e)));
        assert_eq!(
            s.get_commit("repo", &"a".repeat(40)).await.unwrap_err(),
            GitContextQueryError::Reader(e)
        );
        assert_eq!(
            s.search_commits(&Default::default()).await.unwrap_err(),
            GitContextQueryError::Reader(e)
        );
    }
}
