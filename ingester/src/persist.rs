//! Persist compacted data to parquet files in object storage

use arrow::record_batch::RecordBatch;
use parquet_file::{
    metadata::{IoxMetadata, IoxParquetMetaData},
    storage::ParquetStorage,
    ParquetFilePath,
};
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Error converting the parquet stream to bytes: {}", source))]
    ConvertingToBytes {
        source: parquet_file::storage::Error,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore {
        source: parquet_file::storage::Error,
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
    let schema = record_batches
        .first()
        .expect("record_batches.is_empty was just checked")
        .schema();

    let data = store
        .parquet_bytes(record_batches, schema, metadata)
        .await
        .context(ConvertingToBytesSnafu)?;

    if data.is_empty() {
        return Ok(None);
    }

    // extract metadata
    let data = Arc::new(data);
    let md = IoxParquetMetaData::from_file_bytes(Arc::clone(&data))
        .expect("cannot read parquet file metadata")
        .expect("no metadata in parquet file");
    let data = Arc::try_unwrap(data).expect("dangling reference to data");

    let file_size = data.len();

    let path = ParquetFilePath::new(
        metadata.namespace_id,
        metadata.table_id,
        metadata.sequencer_id,
        metadata.partition_id,
        metadata.object_store_id,
    );

    store
        .to_object_store(data, &path)
        .await
        .context(WritingToObjectStoreSnafu)?;

    Ok(Some((file_size, md)))
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
            time_of_first_write: now(),
            time_of_last_write: now(),
            min_sequence_number: SequenceNumber::new(5),
            max_sequence_number: SequenceNumber::new(6),
            row_count: 0,
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
            time_of_first_write: now(),
            time_of_last_write: now(),
            min_sequence_number: SequenceNumber::new(5),
            max_sequence_number: SequenceNumber::new(6),
            row_count: 3,
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
