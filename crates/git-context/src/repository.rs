use crate::GitContextError;

/// A repository's caller-supplied identity and optional context labels.
///
/// The ID is independent of the display name and remote URL: repositories
/// without a remote still have an identity. Remote syntax is provider-owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRepository {
    repository_id: String,
    name: String,
    remote_url: Option<String>,
    project_id: Option<String>,
    service: Option<String>,
}

impl GitRepository {
    pub fn new(
        repository_id: impl Into<String>,
        name: impl Into<String>,
        remote_url: Option<String>,
        project_id: Option<String>,
        service: Option<String>,
    ) -> Result<Self, GitContextError> {
        let repository_id = repository_id.into();
        let name = name.into();
        if repository_id.trim().is_empty() {
            return Err(GitContextError::EmptyRepositoryId);
        }
        if name.trim().is_empty() {
            return Err(GitContextError::EmptyRepositoryName);
        }

        Ok(Self {
            repository_id,
            name,
            remote_url,
            project_id,
            service,
        })
    }

    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn remote_url(&self) -> Option<&str> {
        self.remote_url.as_deref()
    }

    pub fn project_id(&self) -> Option<&str> {
        self.project_id.as_deref()
    }

    pub fn service(&self) -> Option<&str> {
        self.service.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_repository_preserves_identity_and_context() {
        let repository = GitRepository::new(
            "repo-xpa",
            "xpa-finance",
            Some("git@example.com:xpa/finance.git".to_owned()),
            Some("xpa".to_owned()),
            Some("xpa-finance".to_owned()),
        )
        .unwrap();

        assert_eq!(repository.repository_id(), "repo-xpa");
        assert_eq!(repository.name(), "xpa-finance");
        assert_eq!(
            repository.remote_url(),
            Some("git@example.com:xpa/finance.git")
        );
        assert_eq!(repository.project_id(), Some("xpa"));
        assert_eq!(repository.service(), Some("xpa-finance"));
    }

    #[test]
    fn repository_does_not_require_a_remote_or_context_labels() {
        let repository = GitRepository::new("local-repo", "local", None, None, None).unwrap();

        assert_eq!(repository.repository_id(), "local-repo");
        assert_eq!(repository.remote_url(), None);
        assert_eq!(repository.project_id(), None);
        assert_eq!(repository.service(), None);
    }

    #[test]
    fn invalid_repository_id_is_rejected() {
        for value in ["", "  \t\n"] {
            assert_eq!(
                GitRepository::new(value, "finance", None, None, None).unwrap_err(),
                GitContextError::EmptyRepositoryId
            );
        }
    }

    #[test]
    fn invalid_repository_name_is_rejected() {
        for value in ["", "  \t\n"] {
            assert_eq!(
                GitRepository::new("repo-xpa", value, None, None, None).unwrap_err(),
                GitContextError::EmptyRepositoryName
            );
        }
    }
}
