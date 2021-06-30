//! This module contains the code to unload chunks from the read buffer

use std::sync::Arc;

use lifecycle::LifecycleWriteGuard;
use observability_deps::tracing::debug;

use crate::db::{catalog::chunk::CatalogChunk, DbChunk};

use super::LockableCatalogChunk;

use super::error::Result;

pub fn unload_read_buffer_chunk(
    mut chunk: LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk<'_>>,
) -> Result<Arc<DbChunk>> {
    debug!(chunk=%chunk.addr(), "unloading chunk from read buffer");

    chunk.set_unload_from_read_buffer()?;

    debug!(chunk=%chunk.addr(), "chunk marked UNLOADED from read buffer");

    Ok(DbChunk::snapshot(&chunk))
}
