//! Persist compacted data to parquet files in object storage

use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use object_store::{
    path::{ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use parquet_file::metadata::IoxMetadata;
use snafu::{ResultExt, Snafu};

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

/// Write the given data to the given location in the given object storage
pub async fn persist(
    metadata: &IoxMetadata,
    record_batches: Vec<RecordBatch>,
    object_store: &ObjectStore,
) -> Result<()> {
    if record_batches.is_empty() {
        return Ok(());
    }
    let schema = record_batches
        .first()
        .expect("record_batches.is_empty was just checked")
        .schema();

    let data = parquet_file::storage::Storage::parquet_bytes(record_batches, schema, metadata)
        .await
        .context(ConvertingToBytesSnafu)?;

    if data.is_empty() {
        return Ok(());
    }

    let bytes = Bytes::from(data);

    let path = parquet_file_object_store_path(metadata, object_store);

    object_store
        .put(&path, bytes)
        .await
        .context(WritingToObjectStoreSnafu)?;

    Ok(())
}

fn parquet_file_object_store_path(metadata: &IoxMetadata, object_store: &ObjectStore) -> Path {
    let mut path = object_store.new_path();

    path.push_all_dirs(&[
        metadata.namespace_id.to_string().as_str(),
        metadata.table_id.to_string().as_str(),
        metadata.sequencer_id.to_string().as_str(),
        metadata.partition_id.to_string().as_str(),
    ]);

    path.set_file_name(format!("{}.parquet", metadata.object_store_id));

    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, StreamExt, TryStreamExt};
    use iox_catalog::interface::{NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId};
    use query::test::{raw_data, TestChunk};
    use std::sync::Arc;
    use time::Time;
    use uuid::Uuid;

    fn now() -> Time {
        Time::from_timestamp(0, 0)
    }

    fn object_store() -> Arc<ObjectStore> {
        Arc::new(ObjectStore::new_in_memory())
    }

    async fn list_all(object_store: &ObjectStore) -> Result<Vec<Path>, object_store::Error> {
        object_store
            .list(None)
            .await?
            .map_ok(|v| stream::iter(v).map(Ok))
            .try_flatten()
            .try_collect()
            .await
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
        };
        let object_store = object_store();

        persist(&metadata, vec![], &object_store).await.unwrap();

        assert!(list_all(&object_store).await.unwrap().is_empty());
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

        let obj_store_paths = list_all(&object_store).await.unwrap();
        assert_eq!(obj_store_paths.len(), 1);
    }

    #[test]
    fn parquet_file_path_in_object_storage() {
        let object_store = object_store();
        let metadata = IoxMetadata {
            object_store_id: Uuid::new_v4(),
            creation_timestamp: now(),
            namespace_id: NamespaceId::new(1),
            namespace_name: "mydata".into(),
            sequencer_id: SequencerId::new(3),
            table_id: TableId::new(2),
            table_name: "temperature".into(),
            partition_id: PartitionId::new(4),
            partition_key: "somehour".into(),
            time_of_first_write: now(),
            time_of_last_write: now(),
            min_sequence_number: SequenceNumber::new(5),
            max_sequence_number: SequenceNumber::new(6),
        };

        let path = parquet_file_object_store_path(&metadata, &object_store);

        assert_eq!(
            path.to_raw(),
            format!("1/2/3/4/{}.parquet", metadata.object_store_id)
        );
    }
}
