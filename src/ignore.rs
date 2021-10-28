use std::path::Path;

use ignore::gitignore::{Gitignore, GitignoreBuilder};
use tracing::debug;

use crate::pathutil;

#[derive(Debug)]
pub struct Ignore {
    /// global git ignore
    global_ignore: Gitignore,
    local_ignore: Gitignore,
    ignore_hidden: bool,
}

impl Ignore {
    pub fn build(root: &Path, ignore_hidden: bool) -> anyhow::Result<Self> {
        let (global_ignore, err) = GitignoreBuilder::new("").build_global();
        if let Some(e) = err {
            debug!(error = %e, "global gitignore failed")
        }

        let mut ignore_builder = GitignoreBuilder::new(root);
        if let Some(e) = ignore_builder.add(".gitignore") {
            debug!(error = %e, ".gitignore failed");
        }
        if let Some(e) = ignore_builder.add(".ignore") {
            debug!(error = %e, ".ignore failed");
        }
        ignore_builder
            .add_line(None, ".git/**/*.lock")
            .expect("invalid rule");
        let local_ignore = ignore_builder.build()?;

        Ok(Self {
            global_ignore,
            local_ignore,
            ignore_hidden,
        })
    }

    pub fn should_skip_path(&self, path: &Path) -> bool {
        Self::should_ignore_skip_path(&self.local_ignore, path)
            .or_else(|| Self::should_ignore_skip_path(&self.global_ignore, path))
            .unwrap_or_else(|| {
                if self.ignore_hidden {
                    Self::is_hidden_or_any_parents(path)
                } else {
                    false
                }
            })
    }

    fn is_hidden_or_any_parents(mut path: &Path) -> bool {
        if pathutil::is_hidden(path) {
            true
        } else {
            while let Some(parent) = path.parent() {
                if pathutil::is_hidden(parent) {
                    return true;
                }
                path = parent;
            }
            false
        }
    }

    fn should_ignore_skip_path(gi: &Gitignore, path: &Path) -> Option<bool> {
        match gi.matched_path_or_any_parents(path, path.is_dir()) {
            ignore::Match::None => None,
            ignore::Match::Ignore(_) => Some(true),
            ignore::Match::Whitelist(_) => Some(false),
        }
    }
}
