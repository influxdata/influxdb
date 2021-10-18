//! This module contains code for abstracting object locations that work
//! across different backing implementations and platforms.

use std::fmt;

/// Paths that came from or are to be used in cloud-based object storage
pub mod cloud;
use cloud::CloudPath;

/// Paths that come from or are to be used in file-based object storage
pub mod file;
use file::FilePath;

/// Maximally processed storage-independent paths.
pub mod parsed;
use parsed::DirsAndFileName;

/// Parts for parsed paths.
pub mod parts;
use parts::PathPart;

/// The delimiter to separate object namespaces, creating a directory structure.
pub const DELIMITER: &str = "/";

/// Universal interface for handling paths and locations for objects and
/// directories in the object store.
///
/// It allows IOx to be completely decoupled from the underlying object store
/// implementations.
pub trait ObjectStorePath:
    std::fmt::Debug + Clone + PartialEq + Eq + Send + Sync + 'static
{
    /// Set the file name of this path
    fn set_file_name(&mut self, part: impl Into<String>);

    /// Add a part to the end of the path's directories, encoding any restricted
    /// characters.
    fn push_dir(&mut self, part: impl Into<String>);

    /// Push a bunch of parts as directories in one go.
    fn push_all_dirs<'a>(&mut self, parts: impl AsRef<[&'a str]>);

    /// Return a string representation relative to the root of the object storage that can be
    /// serialized for later deserialization.
    fn to_raw(&self) -> String;
}

/// Defines which object stores use which path logic.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Path {
    /// Amazon storage
    AmazonS3(CloudPath),
    /// Local file system storage
    File(FilePath),
    /// GCP storage
    GoogleCloudStorage(CloudPath),
    /// In memory storage for testing
    InMemory(DirsAndFileName),
    /// Microsoft Azure Blob storage
    MicrosoftAzure(CloudPath),
}

impl Path {
    /// Temp function until we support non-local files
    /// Return true if this file i located on the local filesystem
    pub fn local_file(&self) -> bool {
        matches!(self, Self::File(_))
    }
}

impl ObjectStorePath for Path {
    fn set_file_name(&mut self, part: impl Into<String>) {
        match self {
            Self::AmazonS3(path) => path.set_file_name(part),
            Self::File(path) => path.set_file_name(part),
            Self::GoogleCloudStorage(path) => path.set_file_name(part),
            Self::InMemory(path) => path.set_file_name(part),
            Self::MicrosoftAzure(path) => path.set_file_name(part),
        }
    }

    fn push_dir(&mut self, part: impl Into<String>) {
        match self {
            Self::AmazonS3(path) => path.push_dir(part),
            Self::File(path) => path.push_dir(part),
            Self::GoogleCloudStorage(path) => path.push_dir(part),
            Self::InMemory(path) => path.push_dir(part),
            Self::MicrosoftAzure(path) => path.push_dir(part),
        }
    }

    fn push_all_dirs<'a>(&mut self, parts: impl AsRef<[&'a str]>) {
        match self {
            Self::AmazonS3(path) => path.push_all_dirs(parts),
            Self::File(path) => path.push_all_dirs(parts),
            Self::GoogleCloudStorage(path) => path.push_all_dirs(parts),
            Self::InMemory(path) => path.push_all_dirs(parts),
            Self::MicrosoftAzure(path) => path.push_all_dirs(parts),
        }
    }

    fn to_raw(&self) -> String {
        match self {
            Self::AmazonS3(path) => path.to_raw(),
            Self::File(path) => path.to_raw(),
            Self::GoogleCloudStorage(path) => path.to_raw(),
            Self::InMemory(path) => path.to_raw(),
            Self::MicrosoftAzure(path) => path.to_raw(),
        }
    }
}

impl From<&Self> for Path {
    fn from(path: &Self) -> Self {
        path.clone()
    }
}

impl From<Path> for DirsAndFileName {
    fn from(path: Path) -> Self {
        match path {
            Path::AmazonS3(path) => path.into(),
            Path::File(path) => path.into(),
            Path::GoogleCloudStorage(path) => path.into(),
            Path::InMemory(path) => path,
            Path::MicrosoftAzure(path) => path.into(),
        }
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        match self {
            Self::AmazonS3(path) => {
                write!(f, "s3:{}", path)
            }
            Self::File(path) => {
                write!(f, "file:{}", path)
            }
            Self::GoogleCloudStorage(path) => {
                write!(f, "gcs:{}", path)
            }
            Self::InMemory(path) => {
                write!(f, "mem:{}", path)
            }
            Self::MicrosoftAzure(path) => {
                write!(f, "azure:{}", path)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::parsed_path;

    #[test]
    fn test_display() {
        assert_eq!(
            format!("{}", Path::AmazonS3(CloudPath::raw("/foo/bar/baz.json"))),
            "s3:/foo/bar/baz.json"
        );
        assert_eq!(
            format!(
                "{}",
                Path::GoogleCloudStorage(CloudPath::raw("/foo/bar/baz.json"))
            ),
            "gcs:/foo/bar/baz.json"
        );
        assert_eq!(
            format!(
                "{}",
                Path::MicrosoftAzure(CloudPath::raw("/foo/bar/baz.json"))
            ),
            "azure:/foo/bar/baz.json"
        );

        assert_eq!(
            format!(
                "{}",
                Path::File(FilePath::raw(&PathBuf::from("/foo/bar/baz.json"), false))
            ),
            "file:/foo/bar/baz.json"
        );

        assert_eq!(
            format!(
                "{}",
                Path::InMemory(parsed_path!(["foo", "bar"], "baz.json"))
            ),
            "mem:foo/bar/baz.json"
        );
    }

    #[test]
    fn test_to_raw_string() {
        assert_eq!(
            Path::AmazonS3(CloudPath::raw("/foo/bar/baz.json")).to_raw(),
            "/foo/bar/baz.json"
        );
        assert_eq!(
            Path::GoogleCloudStorage(CloudPath::raw("/foo/bar/baz.json")).to_raw(),
            "/foo/bar/baz.json"
        );
        assert_eq!(
            Path::MicrosoftAzure(CloudPath::raw("/foo/bar/baz.json")).to_raw(),
            "/foo/bar/baz.json"
        );

        assert_eq!(
            Path::File(FilePath::raw(&PathBuf::from("/foo/bar/baz.json"), false)).to_raw(),
            "/foo/bar/baz.json"
        );

        assert_eq!(
            Path::InMemory(parsed_path!(["foo", "bar"], "baz.json")).to_raw(),
            "foo/bar/baz.json"
        );

        assert_eq!(
            Path::InMemory(parsed_path!(["foo", "bar"])).to_raw(),
            "foo/bar/"
        );
    }
}
