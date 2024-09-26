//! Functions for persisting and loading data from the comapcted data layout.

use crate::{
    CompactionDetail, CompactionDetailPath, CompactionDetailRef, CompactionSummary,
    CompactionSummaryPath,
};
use bytes::Bytes;
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, error, warn};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompactedDataPersistenceError {
    #[error("Error serializing compaction detail: {0}")]
    SerializationError(#[from] serde_json::Error),
}

/// Result type for functions in this module.
pub type Result<T, E = CompactedDataPersistenceError> = std::result::Result<T, E>;

pub async fn persist_compaction_detail(
    compactor_id: &str,
    db_name: Arc<str>,
    table_name: Arc<str>,
    compaction_detail: &CompactionDetail,
    object_store: Arc<dyn ObjectStore>,
) -> Result<CompactionDetailRef> {
    let path = CompactionDetailPath::new(
        compactor_id,
        db_name.as_ref(),
        table_name.as_ref(),
        compaction_detail.sequence_number,
    );
    let data = serde_json::to_vec(compaction_detail)?;

    // loop until we persist it
    loop {
        match object_store.put(&path.0, data.clone().into()).await {
            Ok(_) => {
                debug!(
                    "Successfully wrote compaction detail to object store at path {}",
                    path.0
                );
                break;
            }
            Err(e) => {
                error!("Error writing compaction detail to object store: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    Ok(CompactionDetailRef {
        db_name,
        table_name,
        path,
    })
}

pub async fn get_compaction_detail(
    compaction_detail_path: CompactionDetailPath,
    object_store: Arc<dyn ObjectStore>,
) -> Option<CompactionDetail> {
    let bytes = get_bytes_at_path(&compaction_detail_path.0, object_store).await?;
    match serde_json::from_slice(&bytes) {
        Ok(detail) => {
            debug!(
                "Successfully deserialized compaction detail at path {}",
                compaction_detail_path.0
            );
            Some(detail)
        }
        Err(e) => {
            error!(
                "Error deserializing compaction detail at path {}: {}",
                compaction_detail_path.0, e
            );
            None
        }
    }
}

pub async fn persist_compaction_summary(
    compactor_id: &str,
    compaction_summary: &CompactionSummary,
    object_store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let path =
        CompactionSummaryPath::new(compactor_id, compaction_summary.compaction_sequence_number);
    let data = serde_json::to_vec(compaction_summary)?;

    // loop until we persist it
    loop {
        match object_store.put(&path.0, data.clone().into()).await {
            Ok(_) => {
                debug!(
                    "Successfully wrote compaction summary to object store at path {}",
                    path.0
                );
                break;
            }
            Err(e) => {
                error!("Error writing compaction summary to object store: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    Ok(())
}

/// Get the bytes at the given path from the object store. Will retry forever until it gets the bytes.
async fn get_bytes_at_path(path: &ObjPath, object_store: Arc<dyn ObjectStore>) -> Option<Bytes> {
    // loop until we get it
    loop {
        match object_store.get(path).await {
            Ok(get_result) => match get_result.bytes().await {
                Ok(bytes) => return Some(bytes),
                Err(e) => {
                    error!(
                        "Error reading file from object store: {}, retrying in 1 second",
                        e
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            },
            Err(object_store::Error::NotFound { .. }) => {
                warn!("File not found in object store: {}", path);
                return None;
            }
            Err(e) => {
                error!(
                    "Error reading file from object store: {}, retrying in 1 second",
                    e
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}
