use super::{DirsAndFileName, ObjectStorePath, PathPart, DELIMITER};

use std::{fmt, mem};

use itertools::Itertools;

/// An object storage location suitable for passing to cloud storage APIs such
/// as AWS, GCS, and Azure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CloudPath {
    inner: CloudPathRepresentation,
}

impl ObjectStorePath for CloudPath {
    fn set_file_name(&mut self, part: impl Into<String>) {
        self.inner = mem::take(&mut self.inner).set_file_name(part);
    }

    fn push_dir(&mut self, part: impl Into<String>) {
        self.inner = mem::take(&mut self.inner).push_dir(part);
    }

    fn push_all_dirs<'a>(&mut self, parts: impl AsRef<[&'a str]>) {
        self.inner = mem::take(&mut self.inner).push_all_dirs(parts);
    }

    /// Creates a cloud storage location by joining this `CloudPath`'s
    /// parts with `DELIMITER`
    fn to_raw(&self) -> String {
        use CloudPathRepresentation::*;

        match &self.inner {
            Raw(path) => path.to_owned(),
            Parsed(dirs_and_file_name) => {
                let mut path = dirs_and_file_name
                    .directories
                    .iter()
                    .map(PathPart::encoded)
                    .join(DELIMITER);

                if !path.is_empty() {
                    path.push_str(DELIMITER);
                }

                if let Some(file_name) = &dirs_and_file_name.file_name {
                    path.push_str(file_name.encoded());
                }
                path
            }
        }
    }
}

impl CloudPath {
    /// Creates a cloud storage location from a string received from a cloud
    /// storage API without parsing or allocating unless other methods are
    /// called on this instance that need it
    pub fn raw(path: impl Into<String>) -> Self {
        let path = path.into();
        Self {
            inner: CloudPathRepresentation::Raw(path),
        }
    }

    // Only being used by some features
    #[allow(dead_code)]
    pub(crate) fn is_dir(&self) -> bool {
        match &self.inner {
            CloudPathRepresentation::Raw(s) => s.ends_with(DELIMITER),
            CloudPathRepresentation::Parsed(p) => p.file_name.is_none(),
        }
    }
}

impl fmt::Display for CloudPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.to_raw())
    }
}

impl From<CloudPath> for DirsAndFileName {
    fn from(cloud_path: CloudPath) -> Self {
        cloud_path.inner.into()
    }
}

impl From<DirsAndFileName> for CloudPath {
    fn from(dirs_and_file_name: DirsAndFileName) -> Self {
        Self {
            inner: CloudPathRepresentation::Parsed(dirs_and_file_name),
        }
    }
}

#[derive(Debug, Clone, Eq)]
enum CloudPathRepresentation {
    Raw(String),
    Parsed(DirsAndFileName),
}

impl Default for CloudPathRepresentation {
    fn default() -> Self {
        Self::Parsed(DirsAndFileName::default())
    }
}

impl PartialEq for CloudPathRepresentation {
    fn eq(&self, other: &Self) -> bool {
        use CloudPathRepresentation::*;
        match (self, other) {
            (Parsed(self_parts), Parsed(other_parts)) => self_parts == other_parts,
            (Parsed(self_parts), _) => {
                let other_parts: DirsAndFileName = other.to_owned().into();
                *self_parts == other_parts
            }
            (_, Parsed(other_parts)) => {
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

impl CloudPathRepresentation {
    fn push_dir(self, part: impl Into<String>) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.push_dir(part);
        Self::Parsed(dirs_and_file_name)
    }

    fn push_all_dirs<'a>(self, parts: impl AsRef<[&'a str]>) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.push_all_dirs(parts);
        Self::Parsed(dirs_and_file_name)
    }

    fn set_file_name(self, part: impl Into<String>) -> Self {
        let mut dirs_and_file_name: DirsAndFileName = self.into();

        dirs_and_file_name.set_file_name(part);
        Self::Parsed(dirs_and_file_name)
    }
}

impl From<CloudPathRepresentation> for DirsAndFileName {
    fn from(cloud_path_rep: CloudPathRepresentation) -> Self {
        use CloudPathRepresentation::*;

        match cloud_path_rep {
            Raw(path) => {
                let mut parts: Vec<PathPart> = path
                    .split_terminator(DELIMITER)
                    .map(|s| PathPart(s.to_string()))
                    .collect();
                let maybe_file_name = if path.ends_with(DELIMITER) {
                    None
                } else {
                    parts.pop()
                };
                Self {
                    directories: parts,
                    file_name: maybe_file_name,
                }
            }
            Parsed(dirs_and_file_name) => dirs_and_file_name,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_prefix_no_trailing_delimiter_or_file_name() {
        // Use case: a file named `test_file.json` exists in object storage and it
        // should be returned for a search on prefix `test`, so the prefix path
        // should not get a trailing delimiter automatically added
        let mut prefix = CloudPath::default();
        prefix.set_file_name("test");

        let converted = prefix.to_raw();
        assert_eq!(converted, "test");
    }

    #[test]
    fn cloud_prefix_with_trailing_delimiter() {
        // Use case: files exist in object storage named `foo/bar.json` and
        // `foo_test.json`. A search for the prefix `foo/` should return
        // `foo/bar.json` but not `foo_test.json'.
        let mut prefix = CloudPath::default();
        prefix.push_dir("test");

        let converted = prefix.to_raw();
        assert_eq!(converted, "test/");
    }

    #[test]
    fn push_encodes() {
        let mut location = CloudPath::default();
        location.push_dir("foo/bar");
        location.push_dir("baz%2Ftest");

        let converted = location.to_raw();
        assert_eq!(converted, "foo%2Fbar/baz%252Ftest/");
    }

    #[test]
    fn push_all_encodes() {
        let mut location = CloudPath::default();
        location.push_all_dirs(&["foo/bar", "baz%2Ftest"]);

        let converted = location.to_raw();
        assert_eq!(converted, "foo%2Fbar/baz%252Ftest/");
    }

    #[test]
    fn convert_raw_before_partial_eq() {
        // dir and file_name
        let cloud = CloudPath::raw("test_dir/test_file.json");
        let mut built = CloudPath::default();
        built.push_dir("test_dir");
        built.set_file_name("test_file.json");

        assert_eq!(built, cloud);

        // dir and file_name w/o dot
        let cloud = CloudPath::raw("test_dir/test_file");
        let mut built = CloudPath::default();
        built.push_dir("test_dir");
        built.set_file_name("test_file");

        assert_eq!(built, cloud);

        // dir, no file
        let cloud = CloudPath::raw("test_dir/");
        let mut built = CloudPath::default();
        built.push_dir("test_dir");

        assert_eq!(built, cloud);

        // file_name, no dir
        let cloud = CloudPath::raw("test_file.json");
        let mut built = CloudPath::default();
        built.set_file_name("test_file.json");

        assert_eq!(built, cloud);

        // empty
        let cloud = CloudPath::raw("");
        let built = CloudPath::default();

        assert_eq!(built, cloud);
    }

    #[test]
    fn conversions() {
        // dir and file name
        let cloud = CloudPath::raw("foo/bar/blah.json");
        let cloud_parts: DirsAndFileName = cloud.into();

        let mut expected_parts = DirsAndFileName::default();
        expected_parts.push_dir("foo");
        expected_parts.push_dir("bar");
        expected_parts.file_name = Some("blah.json".into());

        assert_eq!(cloud_parts, expected_parts);

        // dir, filename w/o dot
        let cloud = CloudPath::raw("foo/bar/blah");
        let cloud_parts: DirsAndFileName = cloud.into();

        expected_parts.file_name = Some("blah".into());

        assert_eq!(cloud_parts, expected_parts);

        // dir, no file name
        let cloud = CloudPath::raw("foo/bar/");
        let cloud_parts: DirsAndFileName = cloud.into();

        expected_parts.file_name = None;

        assert_eq!(cloud_parts, expected_parts);

        // no dir, file name
        let cloud = CloudPath::raw("blah.json");
        let cloud_parts: DirsAndFileName = cloud.into();

        assert!(cloud_parts.directories.is_empty());
        assert_eq!(cloud_parts.file_name.unwrap().encoded(), "blah.json");

        // empty
        let cloud = CloudPath::raw("");
        let cloud_parts: DirsAndFileName = cloud.into();

        assert!(cloud_parts.directories.is_empty());
        assert!(cloud_parts.file_name.is_none());
    }

    #[test]
    fn test_path_display() {
        let cloudpath = CloudPath::raw("/foo/bar/bbb.json");
        assert_eq!(format!("{}", cloudpath), "/foo/bar/bbb.json");
    }
}
