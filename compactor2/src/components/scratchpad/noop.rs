use std::fmt::Display;

use async_trait::async_trait;
use parquet_file::ParquetFilePath;
use uuid::Uuid;

use super::{Scratchpad, ScratchpadGen};

/// A scratchpad that ignores all inputs and outputs, for use in testing
#[derive(Debug, Default)]
pub struct NoopScratchpadGen;

impl NoopScratchpadGen {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Display for NoopScratchpadGen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "noop")
    }
}

impl ScratchpadGen for NoopScratchpadGen {
    fn pad(&self) -> Box<dyn Scratchpad> {
        Box::new(NoopScratchpad)
    }
}

#[derive(Debug)]
struct NoopScratchpad;

#[async_trait]
impl Scratchpad for NoopScratchpad {
    async fn load_to_scratchpad(&mut self, files: &[ParquetFilePath]) -> Vec<Uuid> {
        files.iter().map(|f| f.objest_store_id()).collect()
    }

    async fn make_public(&mut self, files: &[ParquetFilePath]) -> Vec<Uuid> {
        files.iter().map(|f| f.objest_store_id()).collect()
    }

    async fn clean_from_scratchpad(&mut self, _files: &[ParquetFilePath]) {}

    async fn clean(&mut self) {}
}
