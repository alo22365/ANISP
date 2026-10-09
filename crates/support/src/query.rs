use std::{error::Error, fmt};

use chrono::{DateTime, Utc};

use crate::{CaseStatus, CaseType, Priority};

pub const DEFAULT_SUPPORT_CASE_LIMIT: u16 = 100;
pub const MAX_SUPPORT_CASE_LIMIT: u16 = 500;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SupportCaseFilters {
    pub case_type: Option<CaseType>,
    pub status: Option<CaseStatus>,
    pub priority: Option<Priority>,
    pub reporter_id: Option<String>,
    pub assignee_id: Option<String>,
    pub project_id: Option<String>,
    pub service: Option<String>,
    pub environment: Option<String>,
}

/// Validated SupportCase query. The created-at interval is [from, to).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportCaseQuery {
    filters: SupportCaseFilters,
    created_from: Option<DateTime<Utc>>,
    created_to: Option<DateTime<Utc>>,
    limit: u16,
}

impl SupportCaseQuery {
    pub fn new(
        filters: SupportCaseFilters,
        created_from: Option<DateTime<Utc>>,
        created_to: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Self, SupportCaseQueryError> {
        if created_from
            .zip(created_to)
            .is_some_and(|(from, to)| from > to)
        {
            return Err(SupportCaseQueryError::new(
                "created_from",
                "must be before or equal to created_to",
            ));
        }

        let limit = limit.unwrap_or(u32::from(DEFAULT_SUPPORT_CASE_LIMIT));
        if limit == 0 || limit > u32::from(MAX_SUPPORT_CASE_LIMIT) {
            return Err(SupportCaseQueryError::new(
                "limit",
                "must be between 1 and 500",
            ));
        }

        for (field, value) in [
            ("reporter_id", &filters.reporter_id),
            ("assignee_id", &filters.assignee_id),
            ("project_id", &filters.project_id),
            ("service", &filters.service),
            ("environment", &filters.environment),
        ] {
            if value.as_ref().is_some_and(|value| value.trim().is_empty()) {
                return Err(SupportCaseQueryError::new(field, "must not be empty"));
            }
        }

        Ok(Self {
            filters,
            created_from,
            created_to,
            limit: limit as u16,
        })
    }

    pub fn filters(&self) -> &SupportCaseFilters {
        &self.filters
    }

    pub fn created_from(&self) -> Option<DateTime<Utc>> {
        self.created_from
    }

    pub fn created_to(&self) -> Option<DateTime<Utc>> {
        self.created_to
    }

    pub fn limit(&self) -> u16 {
        self.limit
    }
}

impl Default for SupportCaseQuery {
    fn default() -> Self {
        Self::new(SupportCaseFilters::default(), None, None, None)
            .expect("default support case query is valid")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SupportCaseQueryError {
    field: &'static str,
    message: &'static str,
}

impl SupportCaseQueryError {
    fn new(field: &'static str, message: &'static str) -> Self {
        Self { field, message }
    }

    pub fn field(&self) -> &'static str {
        self.field
    }

    pub fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for SupportCaseQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.field, self.message)
    }
}

impl Error for SupportCaseQueryError {}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;

    #[test]
    fn default_and_maximum_limits_are_valid() {
        assert_eq!(SupportCaseQuery::default().limit(), 100);
        assert_eq!(
            SupportCaseQuery::new(SupportCaseFilters::default(), None, None, Some(500))
                .unwrap()
                .limit(),
            500
        );
        assert_eq!(
            SupportCaseQuery::new(SupportCaseFilters::default(), None, None, Some(501))
                .unwrap_err()
                .field(),
            "limit"
        );
    }

    #[test]
    fn reversed_created_range_is_rejected() {
        let from = DateTime::parse_from_rfc3339("2026-10-06T10:00:01Z")
            .unwrap()
            .to_utc();
        let to = DateTime::parse_from_rfc3339("2026-10-06T10:00:00Z")
            .unwrap()
            .to_utc();
        assert_eq!(
            SupportCaseQuery::new(SupportCaseFilters::default(), Some(from), Some(to), None,)
                .unwrap_err()
                .field(),
            "created_from"
        );
    }

    #[test]
    fn blank_string_filter_is_rejected() {
        let filters = SupportCaseFilters {
            reporter_id: Some("  ".to_owned()),
            ..SupportCaseFilters::default()
        };
        assert_eq!(
            SupportCaseQuery::new(filters, None, None, None)
                .unwrap_err()
                .field(),
            "reporter_id"
        );
    }
}
