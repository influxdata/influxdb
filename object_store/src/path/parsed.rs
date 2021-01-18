use super::{ObjectStorePath, PathPart, PathRepresentation, DELIMITER};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub(crate) struct DirsAndFileName {
    pub(crate) directories: Vec<PathPart>,
    pub(crate) file_name: Option<PathPart>,
}

impl DirsAndFileName {
    pub(crate) fn prefix_matches(&self, prefix: &Self) -> bool {
        let diff = itertools::diff_with(
            self.directories.iter(),
            prefix.directories.iter(),
            |a, b| a == b,
        );

        use itertools::Diff;
        match diff {
            None => match (self.file_name.as_ref(), prefix.file_name.as_ref()) {
                (Some(self_file), Some(prefix_file)) => {
                    self_file.encoded().starts_with(prefix_file.encoded())
                }
                (Some(_self_file), None) => true,
                (None, Some(_prefix_file)) => false,
                (None, None) => true,
            },
            Some(Diff::Shorter(_, mut remaining_self)) => {
                let next_dir = remaining_self
                    .next()
                    .expect("must have at least one mismatch to be in this case");
                match prefix.file_name.as_ref() {
                    Some(prefix_file) => next_dir.encoded().starts_with(prefix_file.encoded()),
                    None => true,
                }
            }
            Some(Diff::FirstMismatch(_, mut remaining_self, mut remaining_prefix)) => {
                let first_prefix = remaining_prefix
                    .next()
                    .expect("must have at least one mismatch to be in this case");

                // There must not be any other remaining parts in the prefix
                remaining_prefix.next().is_none()
                // and the next item in self must start with the last item in the prefix
                    && remaining_self
                        .next()
                        .expect("must be at least one value")
                        .encoded()
                        .starts_with(first_prefix.encoded())
            }
            _ => false,
        }
    }

    /// Returns all directory and file name `PathParts` in `self` after the
    /// specified `prefix`. Ignores any `file_name` part of `prefix`.
    /// Returns `None` if `self` dosen't start with `prefix`.
    pub(crate) fn parts_after_prefix(&self, prefix: &Self) -> Option<Vec<PathPart>> {
        let mut dirs_iter = self.directories.iter();
        let mut prefix_dirs_iter = prefix.directories.iter();

        let mut parts = vec![];

        for dir in &mut dirs_iter {
            let pre = prefix_dirs_iter.next();

            match pre {
                None => {
                    parts.push(dir.to_owned());
                    break;
                }
                Some(p) if p == dir => continue,
                Some(_) => return None,
            }
        }

        parts.extend(dirs_iter.cloned());

        if let Some(file_name) = &self.file_name {
            parts.push(file_name.to_owned());
        }

        Some(parts)
    }

    /// Add a part to the end of the path's directories, encoding any restricted
    /// characters.
    pub(crate) fn push_dir(&mut self, part: impl Into<String>) {
        let part = part.into();
        self.directories.push((&*part).into());
    }

    /// Push a bunch of parts as directories in one go.
    pub(crate) fn push_all_dirs<'a>(&mut self, parts: impl AsRef<[&'a str]>) {
        self.directories
            .extend(parts.as_ref().iter().map(|&v| v.into()));
    }

    /// Add a `PathPart` to the end of the path's directories.
    pub(crate) fn push_part_as_dir(&mut self, part: &PathPart) {
        self.directories.push(part.to_owned());
    }
}

impl From<PathRepresentation> for DirsAndFileName {
    fn from(path_rep: PathRepresentation) -> Self {
        match path_rep {
            PathRepresentation::RawCloud(path) => {
                let mut parts: Vec<PathPart> = path
                    .split_terminator(DELIMITER)
                    .map(|s| PathPart(s.to_string()))
                    .collect();
                let maybe_file_name = match parts.pop() {
                    Some(file) if file.encoded().contains('.') => Some(file),
                    Some(dir) => {
                        parts.push(dir);
                        None
                    }
                    None => None,
                };
                Self {
                    directories: parts,
                    file_name: maybe_file_name,
                }
            }
            PathRepresentation::RawPathBuf(path) => {
                let mut parts: Vec<PathPart> = path
                    .iter()
                    .flat_map(|s| s.to_os_string().into_string().map(PathPart))
                    .collect();

                let maybe_file_name = match parts.pop() {
                    Some(file)
                        if !file.encoded().starts_with('.')
                            && (file.encoded().ends_with(".json")
                                || file.encoded().ends_with(".parquet")
                                || file.encoded().ends_with(".segment")) =>
                    {
                        Some(file)
                    }
                    Some(dir) => {
                        parts.push(dir);
                        None
                    }
                    None => None,
                };
                Self {
                    directories: parts,
                    file_name: maybe_file_name,
                }
            }
            PathRepresentation::Parts(dirs_and_file_name) => dirs_and_file_name,
        }
    }
}

impl From<&'_ ObjectStorePath> for DirsAndFileName {
    fn from(other: &'_ ObjectStorePath) -> Self {
        other.clone().into()
    }
}

impl From<ObjectStorePath> for DirsAndFileName {
    fn from(other: ObjectStorePath) -> Self {
        other.inner.into()
    }
}
