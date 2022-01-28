//! Module to update the catalog during ingesting

use crate::{data::PersistingBatch, handler::IngestHandlerImpl};
use iox_catalog::interface::Tombstone;
use parquet_file::metadata::IoxMetadata;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub enum Error {
    #[snafu(display(
        "The Object Store ID of the Persisting Batch ({}) and its metadata ({}) are not matched",
        persisting_batch_id,
        metadata_id,
    ))]
    NoMatch {
        persisting_batch_id: Uuid,
        metadata_id: Uuid,
    },

    #[snafu(display("Error while removing a persisted batch from Data Buffer: {}", source))]
    RemoveBatch { source: crate::data::Error },
}

/// A specialized `Error` for Ingester's Catalog Update errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Update the catalog:
/// The given batch is already persisted, hence, its
/// metadata and applied tomstones should be added in the catalog,
/// and then the batch should be removed from the Ingester's memory
pub fn update_catalog_after_persisting(
    ingester: &IngestHandlerImpl,
    batch: &Arc<PersistingBatch>,
    _tombstones: &[Tombstone],
    metadata: &IoxMetadata,
) -> Result<()> {
    // verify if the given batch's id match the metadata's id
    if !metadata.match_object_store_id(batch.object_store_id) {
        return Err(Error::NoMatch {
            persisting_batch_id: batch.object_store_id,
            metadata_id: metadata.object_store_id,
        });
    }

    // Insert persisted information into Postgres
    // todo: turn this on when Paul has it implemented
    // ingester.iox_catalog.add_parquet_file_with_tombstones(metadata, tombstones);

    // Remove the batch from its memory
    let mut data_buffer = ingester.data.lock().expect("mutex poisoned");
    data_buffer
        .remove_persisting_batch(batch)
        .context(RemoveBatchSnafu {})?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        catalog_update::update_catalog_after_persisting,
        handler::IngestHandlerImpl,
        test_util::{make_persisting_batch, make_persisting_batch_with_meta},
    };
    use iox_catalog::{
        interface::{KafkaTopic, KafkaTopicId},
        mem::MemCatalog,
    };
    use object_store::ObjectStore;
    use std::sync::Arc;
    use uuid::Uuid;

    fn object_store() -> Arc<ObjectStore> {
        Arc::new(ObjectStore::new_in_memory())
    }

    #[tokio::test]
    async fn test_update_batch_removed() {
        // initialize an ingester server
        let mut ingester = IngestHandlerImpl::new(
            KafkaTopic {
                id: KafkaTopicId::new(1),
                name: "Test_KafkaTopic".to_string(),
            },
            vec![],
            Arc::new(MemCatalog::new()),
            object_store(),
        );

        // Make a persisting batch, tombstones and thier metadata after compaction
        let (batch, tombstones, meta) = make_persisting_batch_with_meta().await;

        // Request persiting
        ingester.add_persisting_batch(Arc::clone(&batch));

        // Update the catalog and remove the batch from the buffer
        update_catalog_after_persisting(&ingester, &batch, &tombstones, &meta).unwrap();

        // todo: verify that the catalog's parquet_files and tombstones have more data

        // Verfiy the persisting batch is already removed
        assert!(!ingester.is_persisting());
    }

    #[tokio::test]
    async fn test_update_nothing_removed() {
        // initialize an ingester server
        let mut ingester = IngestHandlerImpl::new(
            KafkaTopic {
                id: KafkaTopicId::new(1),
                name: "Test_KafkaTopic".to_string(),
            },
            vec![],
            Arc::new(MemCatalog::new()),
            object_store(),
        );

        // Make a persisting batch, tombstones and thier metadata after compaction
        let (batch, tombstones, meta) = make_persisting_batch_with_meta().await;

        // Request persiting
        ingester.add_persisting_batch(Arc::clone(&batch));

        // create different batch
        let other_batch =
            make_persisting_batch(1, 2, 3, "test_table", 4, Uuid::new_v4(), vec![], vec![]);

        // Update the catalog and remove the batch from the buffer
        let err = update_catalog_after_persisting(&ingester, &other_batch, &tombstones, &meta)
            .unwrap_err();
        assert!(err
            .to_string()
            .contains("The Object Store ID of the Persisting Batch"));

        // todo: verify that the catalog's parquet_files and tombstones are unchanged

        // Verfiy the persisting batch is still there
        assert!(ingester.is_persisting());
    }
}
