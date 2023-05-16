use std::fmt::Display;

use super::{ChangedFilesFilter, SavedParquetFileState};

use async_trait::async_trait;
use observability_deps::tracing::info;

#[derive(Debug, Default, Copy, Clone)]
pub struct LoggingChangedFiles {}

impl LoggingChangedFiles {
    pub fn new() -> Self {
        Self {}
    }
}

impl Display for LoggingChangedFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "logging_changed_files")
    }
}

#[async_trait]
impl ChangedFilesFilter for LoggingChangedFiles {
    async fn apply(&self, old: &SavedParquetFileState, new: &SavedParquetFileState) -> bool {
        if old.existing_files_modified(new) {
            let modified_ids_and_levels = old.modified_ids_and_levels(new);
            info!(?modified_ids_and_levels, "Concurrent modification detected");
        }

        false // we're ignoring the return value anyway for the moment
    }
}
