use super::{ObjectStorePath, PathPart, PathRepresentation, DELIMITER};

use itertools::Itertools;

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
