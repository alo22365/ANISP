use crate::{SupportCaseContextError, SupportCaseContextService};
use anisp_git_context::{GitCommitFilters, GitCommitQuery, GitCommitView};
use anisp_support::SupportCase;
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitContextStatus {
    Synced,
    Pending,
    Unmapped,
}

/// Raw case-anchored window; the Git query preserves these exact [from,to)
/// semantics against millisecond stored commit times by ceiling its bounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitContextWindow {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}
impl GitContextWindow {
    pub fn from(&self) -> DateTime<Utc> {
        self.from
    }
    pub fn to(&self) -> DateTime<Utc> {
        self.to
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitContext {
    status: GitContextStatus,
    window: Option<GitContextWindow>,
    pending_events: u64,
    commits: Vec<GitCommitView>,
}
impl GitContext {
    pub fn status(&self) -> GitContextStatus {
        self.status
    }
    pub fn window(&self) -> Option<&GitContextWindow> {
        self.window.as_ref()
    }
    pub fn pending_events(&self) -> u64 {
        self.pending_events
    }
    pub fn commits(&self) -> &[GitCommitView] {
        &self.commits
    }
    pub fn into_commits(self) -> Vec<GitCommitView> {
        self.commits
    }
}
impl SupportCaseContextService {
    pub(super) async fn get_git_context(
        &self,
        support_case: &SupportCase,
    ) -> Result<GitContext, SupportCaseContextError> {
        let Some((project, service)) = support_case
            .project_id()
            .zip(support_case.service())
            .filter(|(p, s)| !p.trim().is_empty() && !s.trim().is_empty())
        else {
            return Ok(GitContext {
                status: GitContextStatus::Unmapped,
                window: None,
                pending_events: 0,
                commits: vec![],
            });
        };
        let to = support_case.created_at();
        let from = to
            .checked_sub_signed(Duration::hours(i64::from(self.git_config.lookback_hours())))
            .ok_or(SupportCaseContextError::InvalidGitWindow)?;
        let query = GitCommitQuery::new(
            GitCommitFilters {
                project_id: Some(project.into()),
                service: Some(service.into()),
                ..Default::default()
            },
            Some(from),
            Some(to),
            Some(self.git_config.max_commits()),
        )
        .map_err(|_| SupportCaseContextError::InvalidGitWindow)?;
        // Read pending before the published projection. A publication race can
        // conservatively return pending; no cross-database snapshot is promised.
        let pending = self
            .git_sync_reader
            .pending_count_for_window(project, service, from, to)
            .await
            .map_err(SupportCaseContextError::GitSync)?;
        let mut commits = self
            .git_service
            .search_commits(&query)
            .await
            .map_err(SupportCaseContextError::Git)?;
        // Select the most recent bounded set, then present it oldest-first only
        // in Case Context. Day4's standalone Git List stays newest-first.
        commits.sort_by_key(|v| (v.committed_at(), v.event_id()));
        Ok(GitContext {
            status: if pending == 0 {
                GitContextStatus::Synced
            } else {
                GitContextStatus::Pending
            },
            window: Some(GitContextWindow { from, to }),
            pending_events: pending,
            commits,
        })
    }
}
