/// Classification of an IT support case.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseType {
    Ticket,
    Incident,
    Request,
}
