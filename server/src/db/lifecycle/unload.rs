//! This module contains the code to unload chunks from the read buffer

use snafu::ResultExt;
use std::sync::Arc;

use lifecycle::LifecycleWriteGuard;
use observability_deps::tracing::debug;

use crate::db::{catalog::chunk::CatalogChunk, DbChunk};

use super::LockableCatalogChunk;

use super::error::{LifecycleError, Result};

pub fn unload_read_buffer_impl(
    mut chunk: LifecycleWriteGuard<'_, CatalogChunk, LockableCatalogChunk<'_>>,
) -> Result<Arc<DbChunk>> {
    debug!(chunk=%chunk.addr(), "unloading chunk from read buffer");

    chunk
        .set_unload_from_read_buffer()
        .context(LifecycleError {})?;

    debug!(chunk=%chunk.addr(), "chunk marked UNLOADED from read buffer");

    Ok(DbChunk::snapshot(&chunk))
}
