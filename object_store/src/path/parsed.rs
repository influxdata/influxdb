use super::{ObjectStorePath, PathPart, DELIMITER};

use std::fmt;

use itertools::Itertools;

/// A path stored as a collection of 0 or more directories and 0 or 1 file name
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default, Hash)]
pub struct DirsAndFileName {
    /// Directory hierarchy.
    pub directories: Vec<PathPart>,

    /// Filename, if set.
    pub file_name: Option<PathPart>,
}

impl ObjectStorePath for DirsAndFileName {
    fn set_file_name(&mut self, part: impl Into<String>) {
        let part = part.into();
        self.file_name = Some((&*part).into());
    }

    fn push_dir(&mut self, part: impl Into<String>) {
        let part = part.into();
        self.directories.push((&*part).into());
    }

    fn push_all_dirs<'a>(&mut self, parts: impl AsRef<[&'a str]>) {
        self.directories
            .extend(parts.as_ref().iter().map(|&v| v.into()));
    }

    fn to_raw(&self) -> String {
        format!("{}", self)
    }
}

impl fmt::Display for DirsAndFileName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        let mut s = self
            .directories
            .iter()
            .map(PathPart::encoded)
            .join(DELIMITER);

        if !s.is_empty() {
            s.push_str(DELIMITER);
        }
        if let Some(file_name) = &self.file_name {
            s.push_str(file_name.encoded());
        }

        write!(f, "{}", s)
    }
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
            // directories match perfectly, now we need to look at the filenames
            None => match (self.file_name.as_ref(), prefix.file_name.as_ref()) {
                // both `self` and the prefix have a filename, they have to match
                (Some(self_file), Some(prefix_file)) => self_file == prefix_file,

                // `self` has a file name but the prefix doesn't, that's a match
                (Some(_self_file), None) => true,

                // `self` doesn't have a file name but the prefix has, so it's not really a prefix
                (None, Some(_prefix_file)) => false,

                // both `self` and the prefix have no filename, perfect match
                (None, None) => true,
            },

            // the prefix directories are a prefix of `self.directories` (or in other words "prefix is shorter than
            // self"), it's a prefix if the prefix doesn't have a file (because we don't wanna match a filename w/ a
            // directory element)
            Some(Diff::Shorter(_, _remaining_self)) => prefix.file_name.is_none(),

            // no real match
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

    /// Add a `PathPart` to the end of the path's directories.
    pub fn push_part_as_dir(&mut self, part: &PathPart) {
        self.directories.push(part.to_owned());
    }

    /// Remove the file name, if any.
    pub(crate) fn unset_file_name(&mut self) {
        self.file_name = None;
    }
}

/// Short-cut macro to create [`DirsAndFileName`] instances.
///
/// # Example
/// ```
/// use object_store::parsed_path;
///
/// // empty path
/// parsed_path!();
///
/// // filename only
/// parsed_path!("test.txt");
///
/// // directories only
/// parsed_path!(["path", "to"]);
///
/// // filename + directories
/// parsed_path!(["path", "to"], "test.txt");
/// ```
#[macro_export]
macro_rules! parsed_path {
    ([$($dir:expr),*], $file:expr) => {
        $crate::path::parsed::DirsAndFileName {
            directories: vec![$($crate::path::parts::PathPart::from($dir)),*],
            file_name: Some($crate::path::parts::PathPart::from($file)),
        }
    };
    ([$($dir:expr),*]) => {
        $crate::path::parsed::DirsAndFileName {
            directories: vec![$($crate::path::parts::PathPart::from($dir)),*],
            file_name: None,
        }
    };
    ($file:expr) => {
        parsed_path!([], $file)
    };
    () => {
        parsed_path!([])
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parts_after_prefix_behavior() {
        let mut existing_path = DirsAndFileName::default();
        existing_path.push_all_dirs(&["apple", "bear", "cow", "dog"]);
        existing_path.file_name = Some("egg.json".into());

        // Prefix with one directory
        let mut prefix = DirsAndFileName::default();
        prefix.push_dir("apple");
        let expected_parts: Vec<PathPart> = vec!["bear", "cow", "dog", "egg.json"]
            .into_iter()
            .map(Into::into)
            .collect();
        let parts = existing_path.parts_after_prefix(&prefix).unwrap();
        assert_eq!(parts, expected_parts);

        // Prefix with two directories
        let mut prefix = DirsAndFileName::default();
        prefix.push_all_dirs(&["apple", "bear"]);
        let expected_parts: Vec<PathPart> = vec!["cow", "dog", "egg.json"]
            .into_iter()
            .map(Into::into)
            .collect();
        let parts = existing_path.parts_after_prefix(&prefix).unwrap();
        assert_eq!(parts, expected_parts);

        // Not a prefix
        let mut prefix = DirsAndFileName::default();
        prefix.push_dir("cow");
        assert!(existing_path.parts_after_prefix(&prefix).is_none());

        // Prefix with a partial directory
        let mut prefix = DirsAndFileName::default();
        prefix.push_dir("ap");
        assert!(existing_path.parts_after_prefix(&prefix).is_none());

        // Prefix matches but there aren't any parts after it
        let mut existing_path = DirsAndFileName::default();
        existing_path.push_all_dirs(&["apple", "bear", "cow", "dog"]);
        let prefix = existing_path.clone();
        let parts = existing_path.parts_after_prefix(&prefix).unwrap();
        assert!(parts.is_empty());
    }

    #[test]
    fn prefix_matches() {
        let mut haystack = DirsAndFileName::default();
        haystack.push_all_dirs(&["foo/bar", "baz%2Ftest", "something"]);

        // self starts with self
        assert!(
            haystack.prefix_matches(&haystack),
            "{:?} should have started with {:?}",
            haystack,
            haystack
        );

        // a longer prefix doesn't match
        let mut needle = haystack.clone();
        needle.push_dir("longer now");
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} shouldn't have started with {:?}",
            haystack,
            needle
        );

        // one dir prefix matches
        let mut needle = DirsAndFileName::default();
        needle.push_dir("foo/bar");
        assert!(
            haystack.prefix_matches(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );

        // two dir prefix matches
        needle.push_dir("baz%2Ftest");
        assert!(
            haystack.prefix_matches(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );

        // partial dir prefix doesn't match
        let mut needle = DirsAndFileName::default();
        needle.push_dir("f");
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // one dir and one partial dir doesn't match
        let mut needle = DirsAndFileName::default();
        needle.push_all_dirs(&["foo/bar", "baz"]);
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );
    }

    #[test]
    fn prefix_matches_with_file_name() {
        let mut haystack = DirsAndFileName::default();
        haystack.push_all_dirs(&["foo/bar", "baz%2Ftest", "something"]);

        let mut needle = haystack.clone();

        // All directories match and file name is a prefix
        haystack.set_file_name("foo.segment");
        needle.set_file_name("foo");

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // All directories match but file name is not a prefix
        needle.set_file_name("e");

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // Not all directories match; file name is a prefix of the next directory; this
        // does not match
        let mut needle = DirsAndFileName::default();
        needle.push_all_dirs(&["foo/bar", "baz%2Ftest"]);
        needle.set_file_name("s");

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // Not all directories match; file name is NOT a prefix of the next directory;
        // no match
        needle.set_file_name("p");

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );
    }

    #[test]
    fn test_macro() {
        let actual = parsed_path!(["foo", "bar"], "baz");
        let expected = DirsAndFileName {
            directories: vec![PathPart::from("foo"), PathPart::from("bar")],
            file_name: Some(PathPart::from("baz")),
        };
        assert_eq!(actual, expected);

        let actual = parsed_path!([], "foo");
        let expected = DirsAndFileName {
            directories: vec![],
            file_name: Some(PathPart::from("foo")),
        };
        assert_eq!(actual, expected);

        let actual = parsed_path!("foo");
        let expected = DirsAndFileName {
            directories: vec![],
            file_name: Some(PathPart::from("foo")),
        };
        assert_eq!(actual, expected);

        let actual = parsed_path!(["foo", "bar"]);
        let expected = DirsAndFileName {
            directories: vec![PathPart::from("foo"), PathPart::from("bar")],
            file_name: None,
        };
        assert_eq!(actual, expected);

        let actual = parsed_path!([]);
        let expected = DirsAndFileName {
            directories: vec![],
            file_name: None,
        };
        assert_eq!(actual, expected);

        let actual = parsed_path!();
        let expected = DirsAndFileName {
            directories: vec![],
            file_name: None,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_display() {
        let path = parsed_path!(["foo", "bar"], "baz");
        assert_eq!(format!("{}", path), "foo/bar/baz");
    }
}
