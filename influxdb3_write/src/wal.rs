//! This is the implementation of the `Wal` that the buffer uses to make buffered data durable
//! on disk.

use std::path::PathBuf;
use std::sync::Arc;
use async_trait::async_trait;
use crate::{SegmentFile, SegmentId, Wal, WalSegmentReader, WalSegmentWriter};

#[derive(Debug)]
pub struct WalImpl {
    #[allow(dead_code)]
    path: PathBuf
}

impl WalImpl {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {path : path.into()}
    }
}

#[async_trait]
impl Wal for WalImpl {
    async fn open_segment_writer(&self, _segment_id: SegmentId) -> crate::Result<Arc<dyn WalSegmentWriter>> {
        todo!()
    }

    async fn open_segment_reader(&self, _segment_id: SegmentId) -> crate::Result<Arc<dyn WalSegmentReader>> {
        todo!()
    }

    async fn segment_files(&self) -> crate::Result<Vec<SegmentFile>> {
        todo!()
    }

    async fn drop_wal_segment(&self, _segment_id: SegmentId) -> crate::Result<()> {
        todo!()
    }
}