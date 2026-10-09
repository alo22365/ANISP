use anisp_event::{MAX_EVENT_METADATA_BYTES, PreparedContextEvent};
use anisp_git_context::{ChangeType, CommitSha, GitFileChange, ObservedGitCommit};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::GitProjectionError;

/// Immutable, validated snapshot of the FIRST accepted observation. Stored as
/// application payload, independent of PostgreSQL/ClickHouse row types.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GitCommitProjection {
    repository_id: String,
    commit_sha: String,
    event_id: Uuid,
    event_time: DateTime<Utc>,
    project_id: String,
    service: String,
    title: String,
    content: String,
    metadata: Value,
}

impl GitCommitProjection {
    pub fn from_observation(
        observed: &ObservedGitCommit,
        event_id: Uuid,
    ) -> Result<Self, GitProjectionError> {
        let commit = observed.commit();
        let repository = observed.repository();
        let fallback = format!("Git commit {}", &commit.commit_sha().as_str()[..12]);
        let title = commit
            .message()
            .lines()
            .next()
            .filter(|line| !line.trim().is_empty())
            .unwrap_or(&fallback)
            .to_owned();
        let content = if commit.message().trim().is_empty() {
            fallback
        } else {
            commit.message().to_owned()
        };
        let changes = commit.changes();
        // Unknown per-file statistics make the corresponding aggregate unknown,
        // rather than presenting a partial sum as a complete total.
        let additions = changes
            .iter()
            .try_fold(0_u64, |sum, file| sum.checked_add(file.additions()?));
        let deletions = changes
            .iter()
            .try_fold(0_u64, |sum, file| sum.checked_add(file.deletions()?));
        let mut metadata = json!({
            "repository_id": repository.repository_id(),
            "repository_name": repository.name(),
            "branch": observed.branch(),
            "commit_sha": commit.commit_sha().as_str(),
            "parent_shas": commit.parent_shas().iter().map(|sha| sha.as_str()).collect::<Vec<_>>(),
            "author_name": commit.author_name(),
            "author_email": commit.author_email(),
            "observed_at": observed.observed_at(),
            "changed_file_count": changes.len(),
            "additions": additions,
            "deletions": deletions,
            "changed_files": [],
            "changes_truncated": !changes.is_empty(),
        });
        let mut used = serde_json::to_vec(&metadata)
            .map_err(|_| GitProjectionError::Serialization)?
            .len();
        if used > MAX_EVENT_METADATA_BYTES {
            return Err(GitProjectionError::MetadataTooLarge);
        }
        let mut files = Vec::new();
        for change in changes {
            let file = json!({
                "path": change.path(),
                "old_path": change.old_path(),
                "change_type": match change.change_type() {
                    ChangeType::Added => "added", ChangeType::Modified => "modified",
                    ChangeType::Deleted => "deleted", ChangeType::Renamed => "renamed",
                },
                "additions": change.additions(),
                "deletions": change.deletions(),
            });
            let bytes = serde_json::to_vec(&file)
                .map_err(|_| GitProjectionError::Serialization)?
                .len();
            let extra = bytes + usize::from(!files.is_empty());
            // Reserve one extra byte for false (five bytes) vs true (four).
            if used + extra + 1 > MAX_EVENT_METADATA_BYTES {
                break;
            }
            used += extra;
            files.push(file);
        }
        metadata["changes_truncated"] = json!(files.len() < changes.len());
        metadata["changed_files"] = Value::Array(files);
        let projection = Self {
            repository_id: repository.repository_id().to_owned(),
            commit_sha: commit.commit_sha().as_str().to_owned(),
            event_id,
            event_time: DateTime::from_timestamp_millis(commit.committed_at().timestamp_millis())
                .ok_or(GitProjectionError::InvalidIdentity)?,
            project_id: repository.project_id().unwrap_or("unassigned").to_owned(),
            service: repository.service().unwrap_or("unassigned").to_owned(),
            title,
            content,
            metadata,
        };
        projection.validate()?;
        Ok(projection)
    }

    /// Rechecks persisted payload before delivery; no malformed data fallback.
    pub fn validate(&self) -> Result<(), GitProjectionError> {
        let sha =
            CommitSha::new(&self.commit_sha).map_err(|_| GitProjectionError::InvalidIdentity)?;
        if self.repository_id.trim().is_empty()
            || sha.as_str() != self.commit_sha
            || self.event_id.get_version_num() != 7
            || self.metadata.get("repository_id").and_then(Value::as_str)
                != Some(self.repository_id.as_str())
            || self.metadata.get("commit_sha").and_then(Value::as_str)
                != Some(self.commit_sha.as_str())
        {
            return Err(GitProjectionError::InvalidIdentity);
        }
        validate_metadata(&self.metadata)?;
        self.prepared_event()
            .validate()
            .map_err(GitProjectionError::EventValidation)
    }

    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }
    pub fn commit_sha(&self) -> &str {
        &self.commit_sha
    }
    pub fn event_id(&self) -> Uuid {
        self.event_id
    }
    pub fn event_time(&self) -> DateTime<Utc> {
        self.event_time
    }
    pub fn metadata(&self) -> &Value {
        &self.metadata
    }
    pub fn prepared_event(&self) -> PreparedContextEvent {
        PreparedContextEvent {
            event_id: self.event_id,
            event_time: self.event_time,
            source: "git".to_owned(),
            event_type: "git.commit.created".to_owned(),
            project_id: self.project_id.clone(),
            service: self.service.clone(),
            environment: "unassigned".to_owned(),
            subject_type: "commit".to_owned(),
            subject_id: self.commit_sha.clone(),
            title: self.title.clone(),
            content: self.content.clone(),
            trace_id: None,
            correlation_id: None,
            metadata: self.metadata.clone(),
        }
    }
}

fn validate_metadata(metadata: &Value) -> Result<(), GitProjectionError> {
    let invalid = || GitProjectionError::InvalidMetadata;
    for key in ["repository_name", "author_name"] {
        if metadata
            .get(key)
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
        {
            return Err(invalid());
        }
    }
    for key in ["branch", "author_email"] {
        let value = metadata.get(key).ok_or_else(invalid)?;
        if !value.is_null() && !value.is_string() {
            return Err(invalid());
        }
    }
    for key in ["additions", "deletions"] {
        let value = metadata.get(key).ok_or_else(invalid)?;
        if !value.is_null() && value.as_u64().is_none() {
            return Err(invalid());
        }
    }
    let parents = metadata
        .get("parent_shas")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?;
    for parent in parents {
        let value = parent.as_str().ok_or_else(invalid)?;
        if CommitSha::new(value).map_err(|_| invalid())?.as_str() != value {
            return Err(invalid());
        }
    }
    let _: DateTime<Utc> =
        serde_json::from_value(metadata.get("observed_at").ok_or_else(invalid)?.clone())
            .map_err(|_| invalid())?;
    let total = metadata
        .get("changed_file_count")
        .and_then(Value::as_u64)
        .ok_or_else(invalid)?;
    let files = metadata
        .get("changed_files")
        .and_then(Value::as_array)
        .ok_or_else(invalid)?;
    if files.len() as u64 > total
        || metadata.get("changes_truncated").and_then(Value::as_bool)
            != Some((files.len() as u64) < total)
    {
        return Err(invalid());
    }
    for file in files {
        let path = file
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(invalid)?;
        let old_path = file.get("old_path").ok_or_else(invalid)?;
        let old_path = if old_path.is_null() {
            None
        } else {
            Some(old_path.as_str().ok_or_else(invalid)?.to_owned())
        };
        let change_type = match file.get("change_type").and_then(Value::as_str) {
            Some("added") => ChangeType::Added,
            Some("modified") => ChangeType::Modified,
            Some("deleted") => ChangeType::Deleted,
            Some("renamed") => ChangeType::Renamed,
            _ => return Err(invalid()),
        };
        let count = |key| -> Result<Option<u64>, GitProjectionError> {
            let value = file.get(key).ok_or_else(invalid)?;
            if value.is_null() {
                Ok(None)
            } else {
                value.as_u64().map(Some).ok_or_else(invalid)
            }
        };
        GitFileChange::new(
            path,
            old_path,
            change_type,
            count("additions")?,
            count("deletions")?,
        )
        .map_err(|_| invalid())?;
    }
    Ok(())
}
