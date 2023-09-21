use std::{fmt::Display, sync::Arc};

use async_trait::async_trait;
use parquet_file::ParquetFilePath;
use uuid::Uuid;

use super::{Scratchpad, ScratchpadGen};

/// A scratchpad that ignores all inputs and outputs, for use in testing
#[derive(Debug, Default)]
pub struct NoopScratchpadGen;

impl NoopScratchpadGen {
    pub fn new() -> Self {
        Self
    }
}

impl Display for NoopScratchpadGen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "noop")
    }
}

impl ScratchpadGen for NoopScratchpadGen {
    fn pad(&self) -> Arc<dyn Scratchpad> {
        Arc::new(NoopScratchpad)
    }
}

#[derive(Debug)]
struct NoopScratchpad;

#[async_trait]
impl Scratchpad for NoopScratchpad {
    fn uuids(&self, files: &[ParquetFilePath]) -> Vec<Uuid> {
        files.iter().map(|f| f.objest_store_id()).collect()
    }

    async fn load_to_scratchpad(&self, files: &[ParquetFilePath]) -> Vec<Uuid> {
        files.iter().map(|f| f.objest_store_id()).collect()
    }

    async fn make_public(&self, files: &[ParquetFilePath]) -> Vec<Uuid> {
        files.iter().map(|f| f.objest_store_id()).collect()
    }

    async fn clean_from_scratchpad(&self, _files: &[ParquetFilePath]) {}

    async fn clean_written_from_scratchpad(&self, _files: &[ParquetFilePath]) {}

    async fn clean(&self) {}
}
