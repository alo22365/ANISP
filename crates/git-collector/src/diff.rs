use std::cell::RefCell;

use anisp_git_context::{ChangeType, GitFileChange};
use git2::{Commit, Delta, DiffDelta, DiffFindOptions, DiffOptions, FileMode, Repository};

use crate::{CollectorError, collector::decode_utf8};

struct PendingChange {
    path: String,
    old_path: Option<String>,
    change_type: ChangeType,
    count_lines: bool,
    counts: Option<(u64, u64)>,
}

impl PendingChange {
    fn from_delta(delta: DiffDelta<'_>) -> Result<Self, CollectorError> {
        let change_type = match delta.status() {
            Delta::Added => ChangeType::Added,
            Delta::Modified | Delta::Typechange => ChangeType::Modified,
            Delta::Deleted => ChangeType::Deleted,
            Delta::Renamed => ChangeType::Renamed,
            _ => return Err(CollectorError::UnsupportedChangeType),
        };
        let old = delta.old_file();
        let new = delta.new_file();
        let file = if change_type == ChangeType::Deleted {
            &old
        } else {
            &new
        };
        let path = decode_utf8(
            file.path_bytes().ok_or(CollectorError::ReadFailed)?,
            "file_path",
        )?;
        let old_path = if change_type == ChangeType::Renamed {
            Some(decode_utf8(
                old.path_bytes().ok_or(CollectorError::ReadFailed)?,
                "old_file_path",
            )?)
        } else {
            None
        };
        let non_text_mode = [old.mode(), new.mode()]
            .into_iter()
            .any(|mode| matches!(mode, FileMode::Commit | FileMode::Tree));
        let text = !non_text_mode
            && !old.is_binary()
            && !new.is_binary()
            && (old.is_not_binary() || new.is_not_binary());

        Ok(Self {
            path,
            old_path,
            change_type,
            count_lines: !non_text_mode,
            counts: text.then_some((0, 0)),
        })
    }

    fn into_domain(self) -> Result<GitFileChange, CollectorError> {
        GitFileChange::new(
            self.path,
            self.old_path,
            self.change_type,
            self.counts.map(|counts| counts.0),
            self.counts.map(|counts| counts.1),
        )
        .map_err(Into::into)
    }
}

pub(crate) fn collect_changes(
    repository: &Repository,
    commit: &Commit<'_>,
) -> Result<Vec<GitFileChange>, CollectorError> {
    let tree = commit.tree().map_err(|_| CollectorError::ReadFailed)?;
    let parent_tree = if commit.parent_count() == 0 {
        None
    } else {
        Some(
            commit
                .parent(0)
                .and_then(|parent| parent.tree())
                .map_err(|_| CollectorError::ReadFailed)?,
        )
    };
    let mut options = DiffOptions::new();
    options.include_typechange(true);
    let mut diff = repository
        .diff_tree_to_tree(parent_tree.as_ref(), Some(&tree), Some(&mut options))
        .map_err(|_| CollectorError::ReadFailed)?;
    let mut find = DiffFindOptions::new();
    // Explicitly enable renames, independent of the repository's diff config.
    find.renames(true);
    diff.find_similar(Some(&mut find))
        .map_err(|_| CollectorError::ReadFailed)?;

    let changes = RefCell::new(Vec::<PendingChange>::new());
    let mapping_error = RefCell::new(None);
    let mut file = |delta: DiffDelta<'_>, _: f32| match PendingChange::from_delta(delta) {
        Ok(change) => {
            changes.borrow_mut().push(change);
            true
        }
        Err(error) => {
            *mapping_error.borrow_mut() = Some(error);
            false
        }
    };
    let mut binary = |_: DiffDelta<'_>, _: git2::DiffBinary<'_>| {
        if let Some(change) = changes.borrow_mut().last_mut() {
            change.count_lines = false;
            change.counts = None;
        }
        // Never read the binary data carried by this notification.
        true
    };
    let mut hunk = |_: DiffDelta<'_>, _: git2::DiffHunk<'_>| {
        if let Some(change) = changes.borrow_mut().last_mut() {
            if change.count_lines {
                change.counts.get_or_insert((0, 0));
            }
        }
        true
    };
    let mut line = |_: DiffDelta<'_>, _: Option<git2::DiffHunk<'_>>, line: git2::DiffLine<'_>| {
        if let Some(change) = changes
            .borrow_mut()
            .last_mut()
            .filter(|change| change.count_lines)
        {
            let counts = change.counts.get_or_insert((0, 0));
            match line.origin() {
                '+' => counts.0 += 1,
                '-' => counts.1 += 1,
                _ => {}
            }
        }
        // Count origins only; never copy line content or build a patch.
        true
    };
    let result = diff.foreach(
        &mut file,
        Some(&mut binary),
        Some(&mut hunk),
        Some(&mut line),
    );
    if let Some(error) = mapping_error.into_inner() {
        return Err(error);
    }
    result.map_err(|_| CollectorError::ReadFailed)?;
    changes
        .into_inner()
        .into_iter()
        .map(PendingChange::into_domain)
        .collect()
}
