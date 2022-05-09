//! Path abstraction for Object Storage

use itertools::Itertools;
use std::fmt::Formatter;

/// The delimiter to separate object namespaces, creating a directory structure.
pub const DELIMITER: &str = "/";

mod parts;

pub(crate) use parts::PathPart;

/// An object storage location suitable for passing to cloud storage APIs such
/// as AWS, GCS, and Azure.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct Path {
    /// The raw path with no leading or trailing delimiters
    raw: String,
}

impl Path {
    /// Create a new [`Path`] from a string
    pub fn from_raw(path: impl AsRef<str>) -> Self {
        let path = path.as_ref();
        Self::from_iter(path.split(DELIMITER))
    }

    /// Returns the raw encoded path in object storage
    pub fn to_raw(&self) -> &str {
        &self.raw
    }

    /// Returns the [`PathPart`] of this [`Path`]
    pub fn parts(&self) -> impl Iterator<Item = PathPart<'_>> {
        match self.raw.is_empty() {
            true => itertools::Either::Left(std::iter::empty()),
            false => itertools::Either::Right(
                self.raw
                    .split(DELIMITER)
                    .map(|s| PathPart { raw: s.into() }),
            ),
        }
    }

    pub(crate) fn prefix_match(
        &self,
        prefix: &Self,
    ) -> Option<impl Iterator<Item = PathPart<'_>> + '_> {
        let diff = itertools::diff_with(
            self.raw.split(DELIMITER),
            prefix.raw.split(DELIMITER),
            |a, b| a == b,
        );

        match diff {
            // Both were equal
            None => Some(itertools::Either::Left(std::iter::empty())),
            // Mismatch or prefix was longer => None
            Some(itertools::Diff::FirstMismatch(_, _, _) | itertools::Diff::Longer(_, _)) => None,
            // Match with remaining
            Some(itertools::Diff::Shorter(_, back)) => Some(itertools::Either::Right(
                back.map(|s| PathPart { raw: s.into() }),
            )),
        }
    }

    pub(crate) fn prefix_matches(&self, prefix: &Self) -> bool {
        self.prefix_match(prefix).is_some()
    }

    /// Creates a new child of this [`Path`]
    pub fn child<'a>(&self, child: impl Into<PathPart<'a>>) -> Self {
        let raw = match self.raw.is_empty() {
            true => format!("{}", child.into().raw),
            false => format!("{}{}{}", self.raw, DELIMITER, child.into().raw),
        };

        Self { raw }
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.raw.fmt(f)
    }
}

impl<'a, I> FromIterator<I> for Path
where
    I: Into<PathPart<'a>>,
{
    fn from_iter<T: IntoIterator<Item = I>>(iter: T) -> Self {
        let raw = T::into_iter(iter)
            .map(|s| s.into())
            .filter(|s| !s.raw.is_empty())
            .map(|s| s.raw)
            .join(DELIMITER);

        Self { raw }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_prefix_with_trailing_delimiter() {
        // Use case: files exist in object storage named `foo/bar.json` and
        // `foo_test.json`. A search for the prefix `foo/` should return
        // `foo/bar.json` but not `foo_test.json'.
        let prefix = Path::from_iter(["test"]);

        let converted = prefix.to_raw();
        assert_eq!(converted, "test");
    }

    #[test]
    fn push_encodes() {
        let location = Path::from_iter(["foo/bar", "baz%2Ftest"]);
        let converted = location.to_raw();
        assert_eq!(converted, "foo%2Fbar/baz%252Ftest");
    }

    #[test]
    fn convert_raw_before_partial_eq() {
        // dir and file_name
        let cloud = Path::from_raw("test_dir/test_file.json");
        let built = Path::from_iter(["test_dir", "test_file.json"]);

        assert_eq!(built, cloud);

        // dir and file_name w/o dot
        let cloud = Path::from_raw("test_dir/test_file");
        let built = Path::from_iter(["test_dir", "test_file"]);

        assert_eq!(built, cloud);

        // dir, no file
        let cloud = Path::from_raw("test_dir/");
        let built = Path::from_iter(["test_dir"]);
        assert_eq!(built, cloud);

        // file_name, no dir
        let cloud = Path::from_raw("test_file.json");
        let built = Path::from_iter(["test_file.json"]);
        assert_eq!(built, cloud);

        // empty
        let cloud = Path::from_raw("");
        let built = Path::from_iter(["", ""]);

        assert_eq!(built, cloud);
    }

    #[test]
    fn parts_after_prefix_behavior() {
        let existing_path = Path::from_raw("apple/bear/cow/dog/egg.json");

        // Prefix with one directory
        let prefix = Path::from_raw("apple");
        let expected_parts: Vec<PathPart<'_>> = vec!["bear", "cow", "dog", "egg.json"]
            .into_iter()
            .map(Into::into)
            .collect();
        let parts: Vec<_> = existing_path.prefix_match(&prefix).unwrap().collect();
        assert_eq!(parts, expected_parts);

        // Prefix with two directories
        let prefix = Path::from_raw("apple/bear");
        let expected_parts: Vec<PathPart<'_>> = vec!["cow", "dog", "egg.json"]
            .into_iter()
            .map(Into::into)
            .collect();
        let parts: Vec<_> = existing_path.prefix_match(&prefix).unwrap().collect();
        assert_eq!(parts, expected_parts);

        // Not a prefix
        let prefix = Path::from_raw("cow");
        assert!(existing_path.prefix_match(&prefix).is_none());

        // Prefix with a partial directory
        let prefix = Path::from_raw("ap");
        assert!(existing_path.prefix_match(&prefix).is_none());

        // Prefix matches but there aren't any parts after it
        let existing_path = Path::from_raw("apple/bear/cow/dog");

        let prefix = existing_path.clone();
        assert_eq!(existing_path.prefix_match(&prefix).unwrap().count(), 0);
    }

    #[test]
    fn prefix_matches() {
        let haystack = Path::from_iter(["foo/bar", "baz%2Ftest", "something"]);
        let needle = haystack.clone();
        // self starts with self
        assert!(
            haystack.prefix_matches(&haystack),
            "{:?} should have started with {:?}",
            haystack,
            haystack
        );

        // a longer prefix doesn't match
        let needle = needle.child("longer now");
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} shouldn't have started with {:?}",
            haystack,
            needle
        );

        // one dir prefix matches
        let needle = Path::from_iter(["foo/bar"]);
        assert!(
            haystack.prefix_matches(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );

        // two dir prefix matches
        let needle = needle.child("baz%2Ftest");
        assert!(
            haystack.prefix_matches(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );

        // partial dir prefix doesn't match
        let needle = Path::from_iter(["f"]);
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // one dir and one partial dir doesn't match
        let needle = Path::from_iter(["foo/bar", "baz"]);
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );
    }

    #[test]
    fn prefix_matches_with_file_name() {
        let haystack = Path::from_iter(["foo/bar", "baz%2Ftest", "something", "foo.segment"]);

        // All directories match and file name is a prefix
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "something", "foo"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // All directories match but file name is not a prefix
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "something", "e"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // Not all directories match; file name is a prefix of the next directory; this
        // does not match
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "s"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // Not all directories match; file name is NOT a prefix of the next directory;
        // no match
        let needle = Path::from_iter(["foo/bar", "baz%2Ftest", "p"]);

        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );
    }
}
