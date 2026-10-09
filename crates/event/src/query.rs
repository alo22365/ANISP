use chrono::{DateTime, Utc};

use crate::ValidationError;

pub const DEFAULT_LIMIT: u16 = 100;
pub const MAX_LIMIT: u16 = 500;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventFilters {
    pub project_id: Option<String>,
    pub service: Option<String>,
    pub environment: Option<String>,
    pub source: Option<String>,
    pub event_type: Option<String>,
    pub subject_type: Option<String>,
    pub subject_id: Option<String>,
}

/// Validated query. The interval is [from, to), compared against event_time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventQuery {
    filters: EventFilters,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    limit: u16,
}

impl EventQuery {
    pub fn new(
        filters: EventFilters,
        from: Option<DateTime<Utc>>,
        to: Option<DateTime<Utc>>,
        limit: Option<u32>,
    ) -> Result<Self, ValidationError> {
        if from.zip(to).is_some_and(|(from, to)| from > to) {
            return Err(ValidationError::new(
                "from",
                "must be before or equal to to",
            ));
        }

        let limit = limit.unwrap_or(u32::from(DEFAULT_LIMIT));
        if limit == 0 || limit > u32::from(MAX_LIMIT) {
            return Err(ValidationError::new("limit", "must be between 1 and 500"));
        }

        for (field, value) in [
            ("project_id", &filters.project_id),
            ("service", &filters.service),
            ("environment", &filters.environment),
            ("source", &filters.source),
            ("event_type", &filters.event_type),
            ("subject_type", &filters.subject_type),
            ("subject_id", &filters.subject_id),
        ] {
            if value.as_ref().is_some_and(|value| value.trim().is_empty()) {
                return Err(ValidationError::new(field, "must not be empty"));
            }
        }

        Ok(Self {
            filters,
            // Stored DateTime64(3) values are millisecond aligned. Ceiling the
            // bounds retains exact [from, to) semantics for sub-ms inputs.
            from: from.map(ceil_to_millis),
            to: to.map(ceil_to_millis),
            limit: limit as u16,
        })
    }

    pub fn filters(&self) -> &EventFilters {
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

impl Default for EventQuery {
    fn default() -> Self {
        Self::new(EventFilters::default(), None, None, None).expect("default query is valid")
    }
}

fn ceil_to_millis(time: DateTime<Utc>) -> DateTime<Utc> {
    let milliseconds = time.timestamp_millis();
    let extra = i64::from(time.timestamp_subsec_nanos() % 1_000_000 != 0);
    DateTime::from_timestamp_millis(milliseconds + extra).expect("valid datetime remains in range")
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;

    #[test]
    fn default_limit_is_100_and_maximum_is_500() {
        assert_eq!(EventQuery::default().limit(), 100);
        assert_eq!(
            EventQuery::new(EventFilters::default(), None, None, Some(500))
                .unwrap()
                .limit(),
            500
        );
        assert_eq!(
            EventQuery::new(EventFilters::default(), None, None, Some(501))
                .unwrap_err()
                .field(),
            "limit"
        );
    }

    #[test]
    fn rejects_reversed_range() {
        let from = DateTime::parse_from_rfc3339("2026-09-21T10:00:01Z")
            .unwrap()
            .to_utc();
        let to = DateTime::parse_from_rfc3339("2026-09-21T10:00:00Z")
            .unwrap()
            .to_utc();
        assert_eq!(
            EventQuery::new(EventFilters::default(), Some(from), Some(to), None)
                .unwrap_err()
                .field(),
            "from"
        );
    }

    #[test]
    fn submillisecond_bounds_round_up() {
        let from = DateTime::parse_from_rfc3339("2026-09-21T10:00:00.123001Z")
            .unwrap()
            .to_utc();
        let query = EventQuery::new(EventFilters::default(), Some(from), Some(from), None).unwrap();
        assert_eq!(
            query.from().unwrap().to_rfc3339(),
            "2026-09-21T10:00:00.124+00:00"
        );
        assert_eq!(query.to(), query.from());
    }
}
