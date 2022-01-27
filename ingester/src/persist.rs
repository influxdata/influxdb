//! Persist compacted data to parquet files in object storage

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use bytes::Bytes;
use iox_catalog::interface::{NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId};
use object_store::{
    path::{ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};
use parking_lot::Mutex;
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    file::{metadata::KeyValue, properties::WriterProperties, writer::TryClone},
};
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    io::{Cursor, Seek, SeekFrom, Write},
    sync::Arc,
};
use time::Time;
use uuid::Uuid;

/// IOx-specific metadata.
pub struct IoxMetadata {
    /// The uuid used as the location of the parquet file in the OS.
    /// This uuid will later be used as the catalog's ParquetFileId
    pub object_store_id: Uuid,

    /// Timestamp when this file was created.
    pub creation_timestamp: Time,

    /// namespace id of the data
    pub namespace_id: NamespaceId,

    /// namespace name of the data
    pub namespace: Arc<str>,

    /// sequencer id of the data
    pub sequencer_id: SequencerId,

    /// table id of the data
    pub table_id: TableId,

    /// table name of the data
    pub table_name: Arc<str>,

    /// partition id of the data
    pub partition_id: PartitionId,

    /// parittion key of the data
    pub partition_key: Arc<str>,

    /// Time of the first write of the data
    /// This is also the min value of the column `time`
    pub time_of_first_write: Time,

    /// Time of the last write of the data
    /// This is also the max value of the column `time`
    pub time_of_last_write: Time,

    /// sequence number of the first write
    pub min_sequence_number: SequenceNumber,

    /// sequence number of the last write
    pub max_sequence_number: SequenceNumber,
}

impl IoxMetadata {
    /// Convert to protobuf v3 message.
    fn to_protobuf(&self) -> std::result::Result<Vec<u8>, prost::EncodeError> {
        unimplemented!()
    }
}

/// File-level metadata key to store the IOx-specific data.
///
/// This will contain [`IoxMetadata`] serialized as base64-encoded [Protocol Buffers 3].
///
/// [Protocol Buffers 3]: https://developers.google.com/protocol-buffers/docs/proto3
pub const METADATA_KEY: &str = "IOX:metadata";

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Error converting the parquet stream to bytes: {}", source))]
    ConvertingToBytes { source: ParquetBytesError },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore { source: object_store::Error },
}

/// A specialized `Error` for Ingester's persistence errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write the given data to the given location in the given object storage
pub async fn persist(
    metadata: &IoxMetadata,
    record_batches: &[RecordBatch],
    object_store: &ObjectStore,
) -> Result<()> {
    if record_batches.is_empty() {
        return Ok(());
    }
    let schema = record_batches
        .first()
        .expect("record_batches.is_empty was just checked")
        .schema();

    let data = parquet_bytes(metadata, record_batches, schema)
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

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ParquetBytesError {
    #[snafu(display("Cannot encode metadata: {}", source))]
    MetadataEncodeFailure { source: prost::EncodeError },

    #[snafu(display("Error opening Parquet Writer: {}", source))]
    OpeningParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error writing Parquet to memory: {}", source))]
    WritingParquetToMemory {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error closing Parquet Writer: {}", source))]
    ClosingParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error converting to vec[u8]: Nothing else should have a reference here"))]
    WritingToMemWriter {},
}

/// Convert the given metadata and RecordBatches to parquet file bytes
async fn parquet_bytes(
    metadata: &IoxMetadata,
    record_batches: &[RecordBatch],
    schema: SchemaRef,
) -> Result<Vec<u8>, ParquetBytesError> {
    let metadata_bytes = metadata.to_protobuf().context(MetadataEncodeFailureSnafu)?;
    let props = writer_props(&metadata_bytes);

    let mem_writer = MemWriter::default();
    {
        let mut writer = ArrowWriter::try_new(mem_writer.clone(), schema, Some(props))
            .context(OpeningParquetWriterSnafu)?;
        for batch in record_batches {
            writer.write(batch).context(WritingParquetToMemorySnafu)?;
        }
        writer.close().context(ClosingParquetWriterSnafu)?;
    } // drop the reference to the MemWriter that the SerializedFileWriter has

    mem_writer.into_inner().context(WritingToMemWriterSnafu)
}

fn writer_props(metadata_bytes: &[u8]) -> WriterProperties {
    WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue {
            key: METADATA_KEY.to_string(),
            value: Some(base64::encode(&metadata_bytes)),
        }]))
        .set_compression(Compression::ZSTD)
        .build()
}

#[derive(Debug, Default, Clone)]
struct MemWriter {
    mem: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl MemWriter {
    /// Returns the inner buffer as long as there are no other references to the
    /// Arc.
    pub fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.mem)
            .ok()
            .map(|mutex| mutex.into_inner().into_inner())
    }
}

impl Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.mem.lock();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.mem.lock();
        inner.flush()
    }
}

impl Seek for MemWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.mem.lock();
        inner.seek(pos)
    }
}

impl TryClone for MemWriter {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            mem: Arc::clone(&self.mem),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{stream, StreamExt, TryStreamExt};
    use iox_catalog::interface::NamespaceId;
    use parquet::schema::types::ColumnPath;
    use query::test::{raw_data, TestChunk};

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
            namespace: "mydata".into(),
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

        persist(&metadata, &[], &object_store).await.unwrap();

        assert!(list_all(&object_store).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_with_batches_writes_to_object_store() {
        let metadata = IoxMetadata {
            object_store_id: Uuid::new_v4(),
            creation_timestamp: now(),
            namespace_id: NamespaceId::new(1),
            namespace: "mydata".into(),
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

        persist(&metadata, &batches, &object_store).await.unwrap();

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
            namespace: "mydata".into(),
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

    #[test]
    fn writer_props_have_compression() {
        // should be writing with compression
        let props = writer_props(&[]);

        // arbitrary column name to get default values
        let col_path: ColumnPath = "default".into();
        assert_eq!(props.compression(&col_path), Compression::ZSTD);
    }
}
