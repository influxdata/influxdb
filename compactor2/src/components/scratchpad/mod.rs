use std::fmt::{Debug, Display};

use async_trait::async_trait;
use parquet_file::ParquetFilePath;
use uuid::Uuid;

// pub mod context;
pub mod prod;
mod util;

#[cfg(test)]
mod test_util;

pub trait ScratchpadGen: Debug + Display + Send + Sync {
    fn pad(&self) -> Box<dyn Scratchpad>;
}

#[async_trait]
pub trait Scratchpad: Debug + Send {
    async fn load_to_scratchpad(&mut self, files: &[ParquetFilePath]) -> Vec<Uuid>;
    async fn make_public(&mut self, files: &[ParquetFilePath]) -> Vec<Uuid>;
    async fn clean_from_scratchpad(&mut self, files: &[ParquetFilePath]);
    async fn clean(&mut self);
}
