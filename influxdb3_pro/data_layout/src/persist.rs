//! Functions for persisting and loading data from the comapcted data layout.

use crate::{
    CompactionDetail, CompactionDetailPath, CompactionSequenceNumber, CompactionSummary,
    CompactionSummaryPath, GenerationDetail, GenerationDetailPath, GenerationId,
};
use bytes::Bytes;
use futures_util::{stream::StreamExt, TryFutureExt};
use influxdb3_id::{DbId, TableId};
use object_store::path::Path as ObjPath;
use object_store::ObjectStore;
use observability_deps::tracing::{debug, error, warn};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompactedDataPersistenceError {
    #[error("serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("object store error: {0}")]
    ObjectStoreError(#[from] object_store::Error),
}

/// Result type for functions in this module.
pub type Result<T, E = CompactedDataPersistenceError> = std::result::Result<T, E>;

pub async fn persist_compaction_detail(
    compactor_id: Arc<str>,
    db_id: DbId,
    table_id: TableId,
    compaction_detail: &CompactionDetail,
    object_store: Arc<dyn ObjectStore>,
) -> Result<CompactionDetailPath> {
    let path = CompactionDetailPath::new(
        compactor_id,
        db_id,
        table_id,
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

    Ok(path)
}

pub async fn get_compaction_detail(
    compaction_detail_path: &CompactionDetailPath,
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

pub async fn persist_generation_detail(
    compactor_id: Arc<str>,
    generation_id: GenerationId,
    generation_detail: &GenerationDetail,
    object_store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let path = GenerationDetailPath::new(compactor_id, generation_id);
    let data = serde_json::to_vec(generation_detail)?;

    // loop until we persist it
    loop {
        match object_store.put(&path.0, data.clone().into()).await {
            Ok(_) => {
                debug!(
                    "Successfully wrote generation detail to object store at path {}",
                    path.0
                );
                break;
            }
            Err(e) => {
                error!("Error writing generation detail to object store: {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }

    Ok(())
}

pub async fn get_generation_detail(
    generation_detail_path: &GenerationDetailPath,
    object_store: Arc<dyn ObjectStore>,
) -> Option<GenerationDetail> {
    let bytes = get_bytes_at_path(&generation_detail_path.0, object_store).await?;
    match serde_json::from_slice(&bytes) {
        Ok(detail) => {
            debug!(
                "Successfully deserialized generation detail at path {}",
                generation_detail_path.0
            );
            Some(detail)
        }
        Err(e) => {
            error!(
                "Error deserializing generation detail at path {}: {}",
                generation_detail_path.0, e
            );
            None
        }
    }
}

pub async fn persist_compaction_summary(
    compactor_id: Arc<str>,
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

/// Load the latest compaction summary for the given compactor id. This does a LIST operation to
/// find the latest compaction summary.
pub async fn load_compaction_summary(
    compactor_id: Arc<str>,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Option<CompactionSummary>> {
    // do a list to find the latest compaction summary
    let compaction_summary_dir = ObjPath::from(format!("{compactor_id}/cs"));
    let Some(first_entry) = object_store
        .list(Some(&compaction_summary_dir))
        .next()
        .await
    else {
        return Ok(None);
    };
    let first_entry = first_entry?;

    let bytes = get_bytes_at_path(&first_entry.location, object_store)
        .await
        .expect("compaction summary in list should always be present");
    let compaction_summary: CompactionSummary = serde_json::from_slice(&bytes)?;

    Ok(Some(compaction_summary))
}

/// Load the compaction summary for the given sequence number.
pub async fn load_compaction_summary_for_sequence(
    compactor_id: Arc<str>,
    compaction_sequence_number: CompactionSequenceNumber,
    object_store: Arc<dyn ObjectStore>,
) -> Result<Option<CompactionSummary>> {
    let path = CompactionSummaryPath::new(compactor_id, compaction_sequence_number);

    match object_store.get(&path.0).await {
        Ok(get_result) => match get_result.bytes().await {
            Ok(bytes) => {
                let compaction_summary: CompactionSummary = serde_json::from_slice(&bytes)?;
                Ok(Some(compaction_summary))
            }
            Err(e) => {
                error!(error = %e, "Error reading compaction summary from object store");
                Err(CompactedDataPersistenceError::ObjectStoreError(e))
            }
        },
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => {
            error!(error = %e, "Error getting compaction summary from object store");
            Err(CompactedDataPersistenceError::ObjectStoreError(e))
        }
    }
}

/// Get the bytes at the given path from the object store. Will retry forever until it gets the bytes.
pub async fn get_bytes_at_path(
    path: &ObjPath,
    object_store: Arc<dyn ObjectStore>,
) -> Option<Bytes> {
    loop {
        let maybe_bytes = object_store
            .get(path)
            .and_then(|result| result.bytes())
            .await;

        match maybe_bytes {
            Ok(bytes) => return Some(bytes),
            Err(err) => {
                if let object_store::Error::NotFound { .. } = err {
                    warn!("File not found in object store: {}", path);
                    return None;
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::{memory::InMemory, path::Path, ObjectStore, PutPayload};

    use crate::persist::get_bytes_at_path;

    #[tokio::test]
    async fn test_get_bytes_at_path_not_found() {
        let res = get_bytes_at_path(&Path::from("/foo"), Arc::new(InMemory::new())).await;
        assert!(res.is_none());
    }

    #[tokio::test]
    async fn test_get_bytes_at_path_success() {
        let store = InMemory::new();
        let res = store.put(&Path::from("/foo"), PutPayload::new()).await;
        assert!(res.is_ok());

        let res = get_bytes_at_path(&Path::from("/foo"), Arc::new(store)).await;
        assert!(res.is_some());
    }
}
