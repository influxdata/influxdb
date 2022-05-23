//! Persist compacted data to parquet files in object storage

use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use parquet_file::{
    metadata::{IoxMetadata, IoxParquetMetaData},
    storage::ParquetStorage,
};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Could not serialise and persist record batches {}", source))]
    Persist {
        source: parquet_file::storage::UploadError,
    },
}

/// A specialized `Error` for Ingester's persistence errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write the given data to the given location in the given object storage.
///
/// Returns the persisted file size (in bytes) and metadata if a file was created.
pub async fn persist(
    metadata: &IoxMetadata,
    record_batches: Vec<RecordBatch>,
    store: ParquetStorage,
) -> Result<Option<(usize, IoxParquetMetaData)>> {
    if record_batches.is_empty() {
        return Ok(None);
    }

    // TODO(4324): Yield the buffered RecordBatch instances as a stream as a
    // temporary measure until streaming compaction is complete.
    let stream = futures::stream::iter(record_batches).map(Ok);

    let (meta, file_size) = store.upload(stream, metadata).await.context(PersistSnafu)?;

    Ok(Some((file_size, meta)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId};
    use futures::{StreamExt, TryStreamExt};
    use iox_catalog::interface::INITIAL_COMPACTION_LEVEL;
    use iox_query::test::{raw_data, TestChunk};
    use iox_time::Time;
    use object_store::{memory::InMemory, DynObjectStore};
    use std::sync::Arc;
    use uuid::Uuid;

    fn now() -> Time {
        Time::from_timestamp(0, 0)
    }

    fn object_store() -> Arc<DynObjectStore> {
        Arc::new(InMemory::new())
    }

    #[tokio::test]
    async fn empty_list_writes_nothing() {
        let metadata = IoxMetadata {
            object_store_id: Uuid::new_v4(),
            creation_timestamp: now(),
            namespace_id: NamespaceId::new(1),
            namespace_name: "mydata".into(),
            sequencer_id: SequencerId::new(2),
            table_id: TableId::new(3),
            table_name: "temperature".into(),
            partition_id: PartitionId::new(4),
            partition_key: "somehour".into(),
            min_sequence_number: SequenceNumber::new(5),
            max_sequence_number: SequenceNumber::new(6),
            compaction_level: INITIAL_COMPACTION_LEVEL,
            sort_key: None,
        };
        let object_store = object_store();

        persist(
            &metadata,
            vec![],
            ParquetStorage::new(Arc::clone(&object_store)),
        )
        .await
        .unwrap();

        let mut list = object_store.list(None).await.unwrap();
        assert!(list.next().await.is_none());
    }

    #[tokio::test]
    async fn list_with_batches_writes_to_object_store() {
        let metadata = IoxMetadata {
            object_store_id: Uuid::new_v4(),
            creation_timestamp: now(),
            namespace_id: NamespaceId::new(1),
            namespace_name: "mydata".into(),
            sequencer_id: SequencerId::new(2),
            table_id: TableId::new(3),
            table_name: "temperature".into(),
            partition_id: PartitionId::new(4),
            partition_key: "somehour".into(),
            min_sequence_number: SequenceNumber::new(5),
            max_sequence_number: SequenceNumber::new(6),
            compaction_level: INITIAL_COMPACTION_LEVEL,
            sort_key: None,
        };

        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_id(1)
                .with_time_column() //_with_full_stats(
                .with_tag_column("tag1")
                .with_i64_field_column("field_int")
                .with_three_rows_of_data(),
        );
        let batches = raw_data(&[chunk1]).await;
        assert_eq!(batches.len(), 1);

        let object_store = object_store();

        persist(
            &metadata,
            batches,
            ParquetStorage::new(Arc::clone(&object_store)),
        )
        .await
        .unwrap();

        let list = object_store.list(None).await.unwrap();
        let obj_store_paths: Vec<_> = list.try_collect().await.unwrap();
        assert_eq!(obj_store_paths.len(), 1);
    }
}
