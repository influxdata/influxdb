//! This module contains code for abstracting object locations that work
//! across different backing implementations and platforms.

use itertools::Itertools;
use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};
use std::path::PathBuf;

/// Universal interface for handling paths and locations for objects and
/// directories in the object store.
///
/// It allows IOx to be completely decoupled from the underlying object store
/// implementations.
///
/// Deliberately does not implement `Display` or `ToString`! Use one of the
/// converters.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct ObjectStorePath {
    parts: Vec<PathPart>,
}

impl ObjectStorePath {
    /// For use when receiving a path from an object store API directly, not
    /// when building a path. Assumes DELIMITER is the separator.
    ///
    /// TODO: Improve performance by implementing a CoW-type model to delay
    /// parsing until needed TODO: This should only be available to cloud
    /// storage
    pub fn from_cloud_unchecked(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            parts: path
                .split_terminator(DELIMITER)
                .map(|s| PathPart(s.to_string()))
                .collect(),
        }
    }

    /// For use when receiving a path from a filesystem directly, not
    /// when building a path. Uses the standard library's path splitting
    /// implementation to separate into parts.
    pub fn from_path_buf_unchecked(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self {
            parts: path
                .iter()
                .flat_map(|s| s.to_os_string().into_string().map(PathPart))
                .collect(),
        }
    }

    /// Add a part to the end of the path, encoding any restricted characters.
    pub fn push(&mut self, part: impl Into<String>) {
        let part = part.into();
        self.parts.push((&*part).into());
    }

    /// Add a `PathPart` to the end of the path. Infallible because the
    /// `PathPart` should already have been checked for restricted
    /// characters.
    pub fn push_part(&mut self, part: &PathPart) {
        self.parts.push(part.to_owned());
    }

    /// Add the parts of `ObjectStorePath` to the end of the path. Notably does
    /// *not* behave as `PathBuf::push` does: no existing part of `self`
    /// will be replaced as part of this call.
    pub fn push_path(&mut self, path: &Self) {
        self.parts.extend_from_slice(&path.parts);
    }

    /// Push a bunch of parts in one go.
    pub fn push_all<'a>(&mut self, parts: impl AsRef<[&'a str]>) {
        self.parts.extend(parts.as_ref().iter().map(|&v| v.into()));
    }

    /// Return the component parts of the path.
    pub fn as_parts(&self) -> &[PathPart] {
        self.parts.as_ref()
    }

    /// Pops a part from the path and returns it, or `None` if it's empty.
    pub fn pop(&mut self) -> Option<&PathPart> {
        unimplemented!()
    }

    /// Determines whether `prefix` is a prefix of `self`.
    pub fn starts_with(&self, prefix: &Self) -> bool {
        let diff = itertools::diff_with(self.parts.iter(), prefix.parts.iter(), |a, b| a == b);
        match diff {
            None => true,
            Some(itertools::Diff::Shorter(..)) => true,
            Some(itertools::Diff::FirstMismatch(_, mut remaining_self, mut remaining_prefix)) => {
                let first_prefix = remaining_prefix.next().expect("must be at least one value");

                // there must not be any other remaining parts in the prefix
                remaining_prefix.next().is_none()
                // and the next item in self must start with the last item in the prefix
                    && remaining_self
                        .next()
                        .expect("must be at least one value")
                        .0
                        .starts_with(&first_prefix.0)
            }
            _ => false,
        }
    }

    /// Returns delimiter-separated parts contained in `self` after `prefix`.
    pub fn parts_after_prefix(&self, _prefix: &Self) -> &[PathPart] {
        unimplemented!()
    }
}

// TODO: I made these structs rather than functions because I could see
// `convert` being part of a trait, possibly, but that seemed a bit overly
// complex for now.

/// Converts `ObjectStorePath`s to `String`s that are appropriate for use as
/// locations in cloud storage.
#[derive(Debug, Clone, Copy)]
pub struct CloudConverter {}

impl CloudConverter {
    /// Creates a cloud storage location by joining this `ObjectStorePath`'s
    /// parts with `DELIMITER`
    pub fn convert(object_store_path: &ObjectStorePath) -> String {
        object_store_path.parts.iter().map(|p| &p.0).join(DELIMITER)
    }
}

/// Converts `ObjectStorePath`s to `String`s that are appropriate for use as
/// locations in filesystem storage.
#[derive(Debug, Clone, Copy)]
pub struct FileConverter {}

impl FileConverter {
    /// Creates a filesystem `PathBuf` location by using the standard library's
    /// `PathBuf` building implementation appropriate for the current
    /// platform.
    pub fn convert(object_store_path: &ObjectStorePath) -> PathBuf {
        object_store_path.parts.iter().map(|p| &p.0).collect()
    }
}

/// The delimiter to separate object namespaces, creating a directory structure.
pub const DELIMITER: &str = "/";
// percent_encode's API needs this as a byte
const DELIMITER_BYTE: u8 = DELIMITER.as_bytes()[0];

/// The PathPart type exists to validate the directory/file names that form part
/// of a path.
///
/// A PathPart instance is guaranteed to contain no `/` characters as it can
/// only be constructed by going through the `try_from` impl.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub struct PathPart(String);

/// Characters we want to encode.
const INVALID: &AsciiSet = &CONTROLS
    // The delimiter we are reserving for internal hierarchy
    .add(DELIMITER_BYTE)
    // Characters AWS recommends avoiding for object keys
    // https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
    .add(b'\\')
    .add(b'{')
    // TODO: Non-printable ASCII characters (128–255 decimal characters)
    .add(b'^')
    .add(b'}')
    .add(b'%')
    .add(b'`')
    .add(b']')
    .add(b'"') // " <-- my editor is confused about double quotes within single quotes
    .add(b'>')
    .add(b'[')
    .add(b'~')
    .add(b'<')
    .add(b'#')
    .add(b'|')
    // Characters Google Cloud Storage recommends avoiding for object names
    // https://cloud.google.com/storage/docs/naming-objects
    .add(b'\r')
    .add(b'\n')
    .add(b'*')
    .add(b'?');

impl From<&str> for PathPart {
    fn from(v: &str) -> Self {
        match v {
            // We don't want to encode `.` generally, but we do want to disallow parts of paths
            // to be equal to `.` or `..` to prevent file system traversal shenanigans.
            "." => Self(String::from("%2E")),
            ".." => Self(String::from("%2E%2E")),
            other => Self(percent_encode(other.as_bytes(), INVALID).to_string()),
        }
    }
}

impl std::fmt::Display for PathPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        percent_decode_str(&self.0)
            .decode_utf8()
            .expect("Valid UTF-8 that came from String")
            .fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_part_delimiter_gets_encoded() {
        let part: PathPart = "foo/bar".into();
        assert_eq!(part, PathPart(String::from("foo%2Fbar")));
    }

    #[test]
    fn path_part_gets_decoded_for_display() {
        let part: PathPart = "foo/bar".into();
        assert_eq!(part.to_string(), "foo/bar");
    }

    #[test]
    fn path_part_given_already_encoded_string() {
        let part: PathPart = "foo%2Fbar".into();
        assert_eq!(part, PathPart(String::from("foo%252Fbar")));
        assert_eq!(part.to_string(), "foo%2Fbar");
    }

    #[test]
    fn path_part_cant_be_one_dot() {
        let part: PathPart = ".".into();
        assert_eq!(part, PathPart(String::from("%2E")));
        assert_eq!(part.to_string(), ".");
    }

    #[test]
    fn path_part_cant_be_two_dots() {
        let part: PathPart = "..".into();
        assert_eq!(part, PathPart(String::from("%2E%2E")));
        assert_eq!(part.to_string(), "..");
    }

    // Invariants to maintain/document/test:
    //
    // - always ends in DELIMITER if it's a directory. If it's the end object, it
    //   should have some sort of file extension like .parquet, .json, or .segment
    // - does not contain unencoded DELIMITER
    // - for file paths: does not escape root dir
    // - for object storage: looks like directories
    // - Paths that come from object stores directly don't need to be
    //   parsed/validated
    // - Within a process, the same backing store will always be used
    //

    #[test]
    fn cloud_prefix_no_trailing_delimiter_or_filename() {
        // Use case: a file named `test_file.json` exists in object storage and it
        // should be returned for a search on prefix `test`, so the prefix path
        // should not get a trailing delimiter automatically added
        let mut prefix = ObjectStorePath::default();
        prefix.push("test");

        let converted = CloudConverter::convert(&prefix);
        assert_eq!(converted, "test");
    }

    #[test]
    fn cloud_prefix_with_trailing_delimiter() {
        // Use case: files exist in object storage named `foo/bar.json` and
        // `foo_test.json`. A search for the prefix `foo/` should return
        // `foo/bar.json` but not `foo_test.json'.
        let mut prefix = ObjectStorePath::default();
        prefix.push_all(&["test", ""]);

        let converted = CloudConverter::convert(&prefix);
        assert_eq!(converted, "test/");
    }

    #[test]
    fn push_encodes() {
        let mut location = ObjectStorePath::default();
        location.push("foo/bar");
        location.push("baz%2Ftest");

        let converted = CloudConverter::convert(&location);
        assert_eq!(converted, "foo%2Fbar/baz%252Ftest");
    }

    #[test]
    fn push_all_encodes() {
        let mut location = ObjectStorePath::default();
        location.push_all(&["foo/bar", "baz%2Ftest"]);

        let converted = CloudConverter::convert(&location);
        assert_eq!(converted, "foo%2Fbar/baz%252Ftest");
    }

    #[test]
    fn starts_with_parts() {
        let mut haystack = ObjectStorePath::default();
        haystack.push_all(&["foo/bar", "baz%2Ftest", "something"]);

        assert!(
            haystack.starts_with(&haystack),
            "{:?} should have started with {:?}",
            haystack,
            haystack
        );

        let mut needle = haystack.clone();
        needle.push("longer now");
        assert!(
            !haystack.starts_with(&needle),
            "{:?} shouldn't have started with {:?}",
            haystack,
            needle
        );

        let mut needle = ObjectStorePath::default();
        needle.push("foo/bar");
        assert!(
            haystack.starts_with(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );
        needle.push("baz%2Ftest");
        assert!(
            haystack.starts_with(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );

        let mut needle = ObjectStorePath::default();
        needle.push("f");
        assert!(
            haystack.starts_with(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );
        needle.push("oo/bar");
        assert!(
            !haystack.starts_with(&needle),
            "{:?} shouldn't have started with {:?}",
            haystack,
            needle
        );

        let mut needle = ObjectStorePath::default();
        needle.push_all(&["foo/bar", "baz"]);
        assert!(
            haystack.starts_with(&needle),
            "{:?} should have started with {:?}",
            haystack,
            needle
        );
    }
}
