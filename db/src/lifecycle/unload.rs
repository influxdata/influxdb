//! This module contains the code to unload chunks from the read buffer

use super::{error::Result, LockableCatalogChunk};
use crate::{catalog::chunk::CatalogChunk, DbChunk};
use lifecycle::LifecycleWriteGuard;
use observability_deps::tracing::debug;
use std::sync::Arc;

pub fn unload_read_buffer_chunk(
    mut chunk: LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk>,
) -> Result<Arc<DbChunk>> {
    debug!(chunk=%chunk.addr(), "unloading chunk from read buffer");

    chunk.set_unloaded_from_read_buffer()?;

    debug!(chunk=%chunk.addr(), "chunk marked UNLOADED from read buffer");

    Ok(DbChunk::snapshot(&chunk))
}
