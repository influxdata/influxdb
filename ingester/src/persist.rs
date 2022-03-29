//! Persist compacted data to parquet files in object storage

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use iox_object_store::ParquetFilePath;
use object_store::DynObjectStore;
use parquet_file::metadata::{IoxMetadata, IoxParquetMetaData};
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
    WritingToObjectStore { source: object_store::Error },
}

/// A specialized `Error` for Ingester's persistence errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write the given data to the given location in the given object storage.
///
/// Returns the persisted file size (in bytes) and metadata if a file was created.
pub async fn persist(
    metadata: &IoxMetadata,
    record_batches: Vec<RecordBatch>,
    object_store: &Arc<DynObjectStore>,
) -> Result<Option<(usize, IoxParquetMetaData)>> {
    if record_batches.is_empty() {
        return Ok(None);
    }
    let schema = record_batches
        .first()
        .expect("record_batches.is_empty was just checked")
        .schema();

    // Make a fake IOx object store to conform to the parquet file
    // interface, but note this isn't actually used to find parquet
    // paths to write to
    use iox_object_store::IoxObjectStore;
    let iox_object_store = Arc::new(IoxObjectStore::existing(
        Arc::clone(object_store),
        IoxObjectStore::root_path_for(&**object_store, uuid::Uuid::new_v4()),
    ));

    let data = parquet_file::storage::Storage::new(Arc::clone(&iox_object_store))
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
    let bytes = Bytes::from(data);

    let path = ParquetFilePath::new_new_gen(
        metadata.namespace_id,
        metadata.table_id,
        metadata.sequencer_id,
        metadata.partition_id,
        metadata.object_store_id,
    );

    iox_object_store
        .put_parquet_file(&path, bytes)
        .await
        .context(WritingToObjectStoreSnafu)?;

    Ok(Some((file_size, md)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types2::{NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId};
    use object_store::{ObjectStoreImpl, ObjectStoreTestConvenience};
    use query::test::{raw_data, TestChunk};
    use std::sync::Arc;
    use time::Time;
    use uuid::Uuid;

    fn now() -> Time {
        Time::from_timestamp(0, 0)
    }

    fn object_store() -> Arc<DynObjectStore> {
        Arc::new(ObjectStoreImpl::new_in_memory())
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
            sort_key: None,
        };
        let object_store = object_store();

        persist(&metadata, vec![], &object_store).await.unwrap();

        assert!(object_store.list_all().await.unwrap().is_empty());
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

        persist(&metadata, batches, &object_store).await.unwrap();

        let obj_store_paths = object_store.list_all().await.unwrap();
        assert_eq!(obj_store_paths.len(), 1);
    }
}
