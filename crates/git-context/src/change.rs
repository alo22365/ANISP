use crate::GitContextError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeType {
    Added,
    Modified,
    Deleted,
    Renamed,
}

/// Changed paths and optional non-negative line counts, without a diff or source.
///
/// A missing count means unavailable (for example, a binary file), not zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitFileChange {
    path: String,
    old_path: Option<String>,
    change_type: ChangeType,
    additions: Option<u64>,
    deletions: Option<u64>,
}

impl GitFileChange {
    pub fn new(
        path: impl Into<String>,
        old_path: Option<String>,
        change_type: ChangeType,
        additions: Option<u64>,
        deletions: Option<u64>,
    ) -> Result<Self, GitContextError> {
        let path = path.into();
        if path.trim().is_empty() {
            return Err(GitContextError::EmptyFilePath);
        }
        if change_type == ChangeType::Renamed
            && old_path
                .as_deref()
                .is_none_or(|path| path.trim().is_empty())
        {
            return Err(GitContextError::RenameWithoutOldPath);
        }

        Ok(Self {
            path,
            old_path,
            change_type,
            additions,
            deletions,
        })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn old_path(&self) -> Option<&str> {
        self.old_path.as_deref()
    }

    pub fn change_type(&self) -> ChangeType {
        self.change_type
    }

    pub fn additions(&self) -> Option<u64> {
        self.additions
    }

    pub fn deletions(&self) -> Option<u64> {
        self.deletions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn added_file() {
        let change =
            GitFileChange::new("src/new.rs", None, ChangeType::Added, Some(12), Some(0)).unwrap();

        assert_eq!(change.path(), "src/new.rs");
        assert_eq!(change.old_path(), None);
        assert_eq!(change.change_type(), ChangeType::Added);
        assert_eq!(change.additions(), Some(12));
        assert_eq!(change.deletions(), Some(0));
    }

    #[test]
    fn modified_file() {
        let change =
            GitFileChange::new("src/lib.rs", None, ChangeType::Modified, Some(7), Some(3)).unwrap();

        assert_eq!(change.change_type(), ChangeType::Modified);
        assert_eq!(change.additions(), Some(7));
        assert_eq!(change.deletions(), Some(3));
    }

    #[test]
    fn deleted_file() {
        let change =
            GitFileChange::new("src/old.rs", None, ChangeType::Deleted, Some(0), Some(12)).unwrap();

        assert_eq!(change.path(), "src/old.rs");
        assert_eq!(change.change_type(), ChangeType::Deleted);
        assert_eq!(change.additions(), Some(0));
        assert_eq!(change.deletions(), Some(12));
    }

    #[test]
    fn renamed_file_preserves_both_paths() {
        let change = GitFileChange::new(
            "src/new name.rs",
            Some("src/old name.rs".to_owned()),
            ChangeType::Renamed,
            None,
            None,
        )
        .unwrap();

        assert_eq!(change.path(), "src/new name.rs");
        assert_eq!(change.old_path(), Some("src/old name.rs"));
        assert_eq!(change.change_type(), ChangeType::Renamed);
    }

    #[test]
    fn rename_without_non_empty_old_path_is_rejected() {
        for old_path in [None, Some(String::new()), Some(" \t".to_owned())] {
            assert_eq!(
                GitFileChange::new("src/new.rs", old_path, ChangeType::Renamed, None, None)
                    .unwrap_err(),
                GitContextError::RenameWithoutOldPath
            );
        }
    }

    #[test]
    fn empty_file_path_is_rejected_for_every_change_type() {
        for change_type in [
            ChangeType::Added,
            ChangeType::Modified,
            ChangeType::Deleted,
            ChangeType::Renamed,
        ] {
            for path in ["", "  \t\n"] {
                assert_eq!(
                    GitFileChange::new(path, Some("old.rs".to_owned()), change_type, None, None)
                        .unwrap_err(),
                    GitContextError::EmptyFilePath
                );
            }
        }
    }

    #[test]
    fn unavailable_counts_are_distinct_from_zero() {
        let binary =
            GitFileChange::new("image.png", None, ChangeType::Modified, None, None).unwrap();
        let empty =
            GitFileChange::new("empty.txt", None, ChangeType::Added, Some(0), Some(0)).unwrap();

        assert_eq!(binary.additions(), None);
        assert_eq!(binary.deletions(), None);
        assert_eq!(empty.additions(), Some(0));
        assert_eq!(empty.deletions(), Some(0));
    }
}
