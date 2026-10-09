use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GitCommitFilters {
    pub repository_id: Option<String>,
    pub project_id: Option<String>,
    pub service: Option<String>,
    /// First accepted observation, not current branch membership.
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GitQueryError {
    EmptyFilter,
    InvalidTimeRange,
    InvalidLimit,
    InvalidIdentity,
}
impl std::fmt::Display for GitQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::EmptyFilter => "Git query filter must not be empty",
            Self::InvalidTimeRange => "from must be before or equal to to",
            Self::InvalidLimit => "limit must be between 1 and 500",
            Self::InvalidIdentity => "invalid Git commit identity",
        })
    }
}
impl std::error::Error for GitQueryError {}

/// Validated bounded query over published commits, in [from, to).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitCommitQuery {
    filters: GitCommitFilters,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    limit: u16,
}
impl GitCommitQuery {
    pub fn new(
        filters: GitCommitFilters,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Self, GitQueryError> {
        if from.zip(to).is_some_and(|(a, b)| a > b) {
            return Err(GitQueryError::InvalidTimeRange);
        }
        for value in [
            &filters.repository_id,
            &filters.project_id,
            &filters.service,
            &filters.branch,
        ] {
            if value.as_ref().is_some_and(|s| s.trim().is_empty()) {
                return Err(GitQueryError::EmptyFilter);
            }
        }
        let limit = limit.unwrap_or(100);
        if !(1..=500).contains(&limit) {
            return Err(GitQueryError::InvalidLimit);
        }
        Ok(Self {
            filters,
            from: from.map(ceil_millis).transpose()?,
            to: to.map(ceil_millis).transpose()?,
            limit: limit as u16,
        })
    }
    pub fn filters(&self) -> &GitCommitFilters {
        &self.filters
    }
    pub fn from(&self) -> Option<DateTime<Utc>> {
        self.from
    }
    pub fn to(&self) -> Option<DateTime<Utc>> {
        self.to
    }
    pub fn limit(&self) -> u16 {
        self.limit
    }
}
impl Default for GitCommitQuery {
    fn default() -> Self {
        Self::new(GitCommitFilters::default(), None, None, None).expect("valid default")
    }
}
fn ceil_millis(time: DateTime<Utc>) -> Result<DateTime<Utc>, GitQueryError> {
    DateTime::from_timestamp_millis(
        time.timestamp_millis() + i64::from(time.timestamp_subsec_nanos() % 1_000_000 != 0),
    )
    .ok_or(GitQueryError::InvalidTimeRange)
}
