//! This module contains code for abstracting object locations that work
//! across different backing implementations and platforms.

use itertools::Itertools;
use percent_encoding::{percent_encode, AsciiSet, CONTROLS};
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
        // Turn T into a slice of str, validate each one, and collect() it into a
        // Vec<String>
        let parts = parts.as_ref().iter().map(|&v| v.into()).collect::<Vec<_>>();

        // Push them to the internal path
        self.parts.extend(parts);
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
    pub fn starts_with(&self, _prefix: &Self) -> bool {
        unimplemented!()
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
// percent_encode's API needs this as a byte... is there a const conversion for
// this?
const DELIMITER_BYTE: u8 = b'/';

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
    .add(b'"')
    .add(b'>')
    .add(b'[')
    .add(b'~')
    .add(b'<')
    .add(b'#')
    .add(b'|');

impl From<&str> for PathPart {
    fn from(v: &str) -> Self {
        Self(percent_encode(v.as_bytes(), INVALID).to_string())
    }
}

impl std::fmt::Display for PathPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_part_delimiter_gets_encoded() {
        let part: PathPart = "foo/bar".into();
        assert_eq!(part, PathPart(String::from("foo%2Fbar")))
    }
}
