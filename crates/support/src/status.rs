/// Lifecycle state of a support case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseStatus {
    Open,
    Investigating,
    Resolved,
    Closed,
}
