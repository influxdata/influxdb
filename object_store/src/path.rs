//! This module contains code for abstracting object locations that work
//! across different backing implementations and platforms.

use itertools::Itertools;
use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};
use std::{mem, path::PathBuf};

/// Universal interface for handling paths and locations for objects and
/// directories in the object store.
///
/// It allows IOx to be completely decoupled from the underlying object store
/// implementations.
///
/// Deliberately does not implement `Display` or `ToString`! Use one of the
/// converters.
#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct ObjectStorePath {
    inner: PathRepresentation,
}

impl ObjectStorePath {
    /// For use when receiving a path from an object store API directly, not
    /// when building a path. Assumes DELIMITER is the separator.
    ///
    /// TODO: This should only be available to cloud storage
    pub fn from_cloud_unchecked(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            inner: PathRepresentation::RawCloud(path),
        }
    }

    /// For use when receiving a path from a filesystem directly, not
    /// when building a path. Uses the standard library's path splitting
    /// implementation to separate into parts.
    ///
    /// TODO: This should only be available to file storage
    pub fn from_path_buf_unchecked(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        Self {
            inner: PathRepresentation::RawPathBuf(path),
        }
    }

    /// Add a part to the end of the path, encoding any restricted characters.
    pub fn push_dir(&mut self, part: impl Into<String>) {
        self.inner = mem::take(&mut self.inner).push_dir(part);
    }

    /// Add a `PathPart` to the end of the path.
    pub fn push_part_as_dir(&mut self, part: &PathPart) {
        self.inner = mem::take(&mut self.inner).push_part_as_dir(part);
    }

    /// Set the file name of this path
    pub fn set_file_name(&mut self, part: impl Into<String>) {
        self.inner = mem::take(&mut self.inner).set_file_name(part);
    }

    /// Add the parts of `ObjectStorePath` to the end of the path. Notably does
    /// *not* behave as `PathBuf::push` does: there is no way to replace the
    /// root. If `self` has a file name, that will be removed, then the
    /// directories of `path` will be appended, then any file name of `path`
    /// will be assigned to `self`.
    pub fn push_path(&mut self, path: &Self) {
        self.inner = mem::take(&mut self.inner).push_path(path)
    }

    /// Push a bunch of parts as directories in one go.
    pub fn push_all_dirs<'a>(&mut self, parts: impl AsRef<[&'a str]>) {
        self.inner = mem::take(&mut self.inner).push_all_dirs(parts);
    }

    /// Pops a part from the path and returns it, or `None` if it's empty.
    pub fn pop(&mut self) -> Option<&PathPart> {
        unimplemented!()
    }

    /// Returns true if the directories in `prefix` are the same as the starting
    /// directories of `self`. Does not use filenames; does not match
    /// partial directory names.
    pub fn prefix_matches(&self, prefix: &Self) -> bool {
        use PathRepresentation::*;
        match (&self.inner, &prefix.inner) {
            (Parts(self_parts), Parts(other_parts)) => self_parts.prefix_matches(&other_parts),
            (Parts(self_parts), _) => {
                let prefix_parts: DirsAndFileName = prefix.into();
                self_parts.prefix_matches(&prefix_parts)
            }
            (_, Parts(prefix_parts)) => {
                let self_parts: DirsAndFileName = self.into();
                self_parts.prefix_matches(&prefix_parts)
            }
            _ => {
                let self_parts: DirsAndFileName = self.into();
                let prefix_parts: DirsAndFileName = prefix.into();
                self_parts.prefix_matches(&prefix_parts)
            }
        }
    }

    /// Returns delimiter-separated parts contained in `self` after `prefix`.
    pub fn parts_after_prefix(&self, _prefix: &Self) -> &[PathPart] {
        unimplemented!()
    }
}

impl From<&'_ DirsAndFileName> for ObjectStorePath {
    fn from(other: &'_ DirsAndFileName) -> Self {
        other.clone().into()
    }
}

impl From<DirsAndFileName> for ObjectStorePath {
    fn from(other: DirsAndFileName) -> Self {
        Self {
            inner: PathRepresentation::Parts(other),
        }
    }
}

#[derive(Clone, Eq, Debug)]
enum PathRepresentation {
    RawCloud(String),
    RawPathBuf(PathBuf),
    Parts(DirsAndFileName),
}

impl Default for PathRepresentation {
    fn default() -> Self {
        Self::Parts(DirsAndFileName::default())
    }
}

impl PathRepresentation {
    /// Add a part to the end of the path's directories, encoding any restricted
    /// characters.
    fn push_dir(self, part: impl Into<String>) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.push_dir(part);
        Self::Parts(dirs_and_file_name)
    }

    /// Push a bunch of parts as directories in one go.
    fn push_all_dirs<'a>(self, parts: impl AsRef<[&'a str]>) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name
            .directories
            .extend(parts.as_ref().iter().map(|&v| v.into()));

        Self::Parts(dirs_and_file_name)
    }

    /// Add a `PathPart` to the end of the path's directories.
    fn push_part_as_dir(self, part: &PathPart) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.push_part_as_dir(part);
        Self::Parts(dirs_and_file_name)
    }

    /// Add the parts of `ObjectStorePath` to the end of the path. Notably does
    /// *not* behave as `PathBuf::push` does: there is no way to replace the
    /// root. If `self` has a file name, that will be removed, then the
    /// directories of `path` will be appended, then any file name of `path`
    /// will be assigned to `self`.
    fn push_path(self, path: &ObjectStorePath) -> Self {
        let DirsAndFileName {
            directories: path_dirs,
            file_name: path_file_name,
        } = path.inner.to_owned().into();
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.directories.extend(path_dirs);
        dirs_and_file_name.file_name = path_file_name;

        Self::Parts(dirs_and_file_name)
    }

    /// Set the file name of this path
    fn set_file_name(self, part: impl Into<String>) -> Self {
        let part = part.into();
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.file_name = Some((&*part).into());
        Self::Parts(dirs_and_file_name)
    }
}

impl PartialEq for PathRepresentation {
    fn eq(&self, other: &Self) -> bool {
        use PathRepresentation::*;
        match (self, other) {
            (Parts(self_parts), Parts(other_parts)) => self_parts == other_parts,
            (Parts(self_parts), _) => {
                let other_parts: DirsAndFileName = other.to_owned().into();
                *self_parts == other_parts
            }
            (_, Parts(other_parts)) => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                self_parts == *other_parts
            }
            _ => {
                let self_parts: DirsAndFileName = self.to_owned().into();
                let other_parts: DirsAndFileName = other.to_owned().into();
                self_parts == other_parts
            }
        }
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Default)]
pub(crate) struct DirsAndFileName {
    directories: Vec<PathPart>,
    file_name: Option<PathPart>,
}

impl DirsAndFileName {
    pub(crate) fn prefix_matches(&self, prefix: &Self) -> bool {
        let diff = itertools::diff_with(
            self.directories.iter(),
            prefix.directories.iter(),
            |a, b| a == b,
        );
        matches!(diff, None | Some(itertools::Diff::Shorter(..)))
    }

    pub(crate) fn parts_after_prefix(&self, _prefix: &Self) -> &[PathPart] {
        unimplemented!()
    }

    /// Add a part to the end of the path's directories, encoding any restricted
    /// characters.
    fn push_dir(&mut self, part: impl Into<String>) {
        let part = part.into();
        self.directories.push((&*part).into());
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
                let mut parts: Vec<_> = path
                    .split_terminator(DELIMITER)
                    .map(|s| PathPart(s.to_string()))
                    .collect();
                let maybe_file_name = match parts.pop() {
                    Some(file) if file.0.contains('.') => Some(file),
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
                let mut parts: Vec<_> = path
                    .iter()
                    .flat_map(|s| s.to_os_string().into_string().map(PathPart))
                    .collect();

                let maybe_file_name = match parts.pop() {
                    Some(file) if file.0.contains('.') => Some(file),
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
        match &object_store_path.inner {
            PathRepresentation::RawCloud(path) => path.to_owned(),
            PathRepresentation::RawPathBuf(_path) => {
                todo!("convert");
            }
            PathRepresentation::Parts(dirs_and_file_name) => {
                let mut path = dirs_and_file_name
                    .directories
                    .iter()
                    .map(|p| &p.0)
                    .join(DELIMITER);

                if !path.is_empty() {
                    path.push_str(DELIMITER);
                }
                if let Some(file_name) = &dirs_and_file_name.file_name {
                    path.push_str(&file_name.0);
                }
                path
            }
        }
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
        match &object_store_path.inner {
            PathRepresentation::RawCloud(_path) => {
                todo!("convert");
            }
            PathRepresentation::RawPathBuf(path) => path.to_owned(),
            PathRepresentation::Parts(dirs_and_file_name) => {
                let mut path: PathBuf = dirs_and_file_name
                    .directories
                    .iter()
                    .map(|p| &p.0)
                    .collect();
                if let Some(file_name) = &dirs_and_file_name.file_name {
                    path.set_file_name(&file_name.0);
                }
                path
            }
        }
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
    fn cloud_prefix_no_trailing_delimiter_or_file_name() {
        // Use case: a file named `test_file.json` exists in object storage and it
        // should be returned for a search on prefix `test`, so the prefix path
        // should not get a trailing delimiter automatically added
        let mut prefix = ObjectStorePath::default();
        prefix.set_file_name("test");

        let converted = CloudConverter::convert(&prefix);
        assert_eq!(converted, "test");
    }

    #[test]
    fn cloud_prefix_with_trailing_delimiter() {
        // Use case: files exist in object storage named `foo/bar.json` and
        // `foo_test.json`. A search for the prefix `foo/` should return
        // `foo/bar.json` but not `foo_test.json'.
        let mut prefix = ObjectStorePath::default();
        prefix.push_dir("test");

        let converted = CloudConverter::convert(&prefix);
        assert_eq!(converted, "test/");
    }

    #[test]
    fn push_encodes() {
        let mut location = ObjectStorePath::default();
        location.push_dir("foo/bar");
        location.push_dir("baz%2Ftest");

        let converted = CloudConverter::convert(&location);
        assert_eq!(converted, "foo%2Fbar/baz%252Ftest/");
    }

    #[test]
    fn push_all_encodes() {
        let mut location = ObjectStorePath::default();
        location.push_all_dirs(&["foo/bar", "baz%2Ftest"]);

        let converted = CloudConverter::convert(&location);
        assert_eq!(converted, "foo%2Fbar/baz%252Ftest/");
    }

    #[test]
    fn prefix_matches_full_dirs() {
        let mut haystack = ObjectStorePath::default();
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
        let mut needle = ObjectStorePath::default();
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

        // partial dir prefix does NOT match, this may be surprising!
        let mut needle = ObjectStorePath::default();
        needle.push_dir("f");
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );

        // one dir and one partial dir does NOT match, this may be surprising!
        let mut needle = ObjectStorePath::default();
        needle.push_all_dirs(&["foo/bar", "baz"]);
        assert!(
            !haystack.prefix_matches(&needle),
            "{:?} should not have started with {:?}",
            haystack,
            needle
        );
    }

    #[test]
    fn convert_raw_before_partial_eq() {
        // dir and file_name
        let cloud = ObjectStorePath::from_cloud_unchecked("test_dir/test_file.json");
        let mut built = ObjectStorePath::default();
        built.push_dir("test_dir");
        built.set_file_name("test_file.json");

        assert_eq!(built, cloud);

        // dir, no file_name
        let cloud = ObjectStorePath::from_cloud_unchecked("test_dir");
        let mut built = ObjectStorePath::default();
        built.push_dir("test_dir");

        assert_eq!(built, cloud);

        // file_name, no dir
        let cloud = ObjectStorePath::from_cloud_unchecked("test_file.json");
        let mut built = ObjectStorePath::default();
        built.set_file_name("test_file.json");

        assert_eq!(built, cloud);

        // empty
        let cloud = ObjectStorePath::from_cloud_unchecked("");
        let built = ObjectStorePath::default();

        assert_eq!(built, cloud);
    }

    #[test]
    fn path_rep_conversions() {
        // dir and file name
        let cloud = PathRepresentation::RawCloud("foo/bar/blah.json".into());
        let cloud_parts: DirsAndFileName = cloud.into();

        let path_buf = PathRepresentation::RawPathBuf("foo/bar/blah.json".into());
        let path_buf_parts: DirsAndFileName = path_buf.into();

        let mut expected_parts = DirsAndFileName::default();
        expected_parts.push_dir("foo");
        expected_parts.push_dir("bar");
        expected_parts.file_name = Some("blah.json".into());

        assert_eq!(cloud_parts, expected_parts);
        assert_eq!(path_buf_parts, expected_parts);

        // dir, no file name
        let cloud = PathRepresentation::RawCloud("foo/bar".into());
        let cloud_parts: DirsAndFileName = cloud.into();

        let path_buf = PathRepresentation::RawPathBuf("foo/bar".into());
        let path_buf_parts: DirsAndFileName = path_buf.into();

        expected_parts.file_name = None;

        assert_eq!(cloud_parts, expected_parts);
        assert_eq!(path_buf_parts, expected_parts);

        // no dir, file name
        let cloud = PathRepresentation::RawCloud("blah.json".into());
        let cloud_parts: DirsAndFileName = cloud.into();

        let path_buf = PathRepresentation::RawPathBuf("blah.json".into());
        let path_buf_parts: DirsAndFileName = path_buf.into();

        assert!(cloud_parts.directories.is_empty());
        assert_eq!(cloud_parts.file_name.unwrap().0, "blah.json");

        assert!(path_buf_parts.directories.is_empty());
        assert_eq!(path_buf_parts.file_name.unwrap().0, "blah.json");

        // empty
        let cloud = PathRepresentation::RawCloud("".into());
        let cloud_parts: DirsAndFileName = cloud.into();

        let path_buf = PathRepresentation::RawPathBuf("".into());
        let path_buf_parts: DirsAndFileName = path_buf.into();

        assert!(cloud_parts.directories.is_empty());
        assert!(cloud_parts.file_name.is_none());

        assert!(path_buf_parts.directories.is_empty());
        assert!(path_buf_parts.file_name.is_none());
    }
}
