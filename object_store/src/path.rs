//! This module contains code for abstracting object locations that work
//! across different backing implementations and platforms.

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
///
/// Deliberately does not implement `Display` or `ToString`!
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

    /// Like `std::path::Path::display, converts an `ObjectStorePath` to a
    /// `String` suitable for printing; not suitable for sending to
    /// APIs.
    fn display(&self) -> String;
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

    fn display(&self) -> String {
        match self {
            Self::AmazonS3(path) => path.display(),
            Self::File(path) => path.display(),
            Self::GoogleCloudStorage(path) => path.display(),
            Self::InMemory(path) => path.display(),
            Self::MicrosoftAzure(path) => path.display(),
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
