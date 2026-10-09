//! Provider- and storage-independent Git context facts.
//!
//! Repository identities are supplied by the caller. A commit's natural
//! identity is its repository ID and normalized full SHA; no UUID is generated.
//! Commit and observation timestamps are supplied independently and preserved.
//! Only metadata, changed paths, and optional line counts are modeled here.

mod change;
mod commit;
mod error;
mod query;
mod reader;
mod repository;
mod service;
mod sync;
mod view;

pub use change::{ChangeType, GitFileChange};
pub use commit::{CommitSha, GitCommit, GitCommitInput, ObservedGitCommit};
pub use error::GitContextError;
pub use query::{GitCommitFilters, GitCommitQuery, GitQueryError};
pub use reader::{GitCommitReader, GitReaderError};
pub use repository::GitRepository;
pub use service::{GitContextQueryError, GitContextQueryService};
pub use sync::GitContextSyncReader;
pub use view::{GitCommitView, GitCommitViewInput};
