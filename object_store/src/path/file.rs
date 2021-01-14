use super::{ObjectStorePath, PathPart, PathRepresentation};

use std::path::PathBuf;

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
                    .map(PathPart::encoded)
                    .collect();
                if let Some(file_name) = &dirs_and_file_name.file_name {
                    path.push(file_name.encoded());
                }
                path
            }
        }
    }
}
