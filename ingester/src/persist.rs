//! Persist compacted data to parquet files in object storage

use arrow::{datatypes::SchemaRef, error::ArrowError};
use bytes::Bytes;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use iox_catalog::interface::{
    NamespaceId, PartitionId, SequenceNumber, SequencerId,
    TableId, Timestamp,
};
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
use uuid::Uuid;

/// IOx-specific metadata.
#[allow(missing_copy_implementations)]
pub struct IoxMetadata {}

impl IoxMetadata {
    pub(crate) fn to_protobuf(&self) -> std::result::Result<Vec<u8>, prost::EncodeError> {
        Ok(vec![])
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
    ConvertingToBytes { source: ParquetStreamToBytesError },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore { source: object_store::Error },
}

/// A specialized `Error` for Ingester's persistence errors
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Write the given data to the given location in the given object storage
pub async fn persist(
    namespace_id: NamespaceId,
    table_id: TableId,
    sequencer_id: SequencerId,
    partition_id: PartitionId,
    metadata: IoxMetadata,
    min_sequence_number: SequenceNumber,
    max_sequence_number: SequenceNumber,
    min_time: Timestamp,
    max_time: Timestamp,
    stream: SendableRecordBatchStream,
    object_store: &ObjectStore,
) -> Result<()> {
    let object_store_id = Uuid::new_v4();

    let schema = stream.schema();

    let data = parquet_stream_to_bytes(stream, schema, metadata)
        .await
        .context(ConvertingToBytesSnafu)?;

    // no data
    if data.is_empty() {
        return Ok(());
    }

    let bytes = Bytes::from(data);

    let path = parquet_file_object_store_path(
        object_store,
        namespace_id,
        table_id,
        sequencer_id,
        partition_id,
        object_store_id,
    );

    object_store
        .put(&path, bytes)
        .await
        .context(WritingToObjectStoreSnafu)?;

    Ok(())
}

fn parquet_file_object_store_path(
    object_store: &ObjectStore,
    namespace_id: NamespaceId,
    table_id: TableId,
    sequencer_id: SequencerId,
    partition_id: PartitionId,
    object_store_id: Uuid,
) -> Path {
    let mut path = object_store.new_path();

    path.push_all_dirs(&[
        namespace_id.to_string().as_str(),
        table_id.to_string().as_str(),
        sequencer_id.to_string().as_str(),
        partition_id.to_string().as_str(),
    ]);

    path.set_file_name(format!("{}.parquet", object_store_id));

    path
}

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ParquetStreamToBytesError {
    #[snafu(display("Cannot encode metadata: {}", source))]
    MetadataEncodeFailure { source: prost::EncodeError },

    #[snafu(display("Error opening Parquet Writer: {}", source))]
    OpeningParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error reading stream while creating snapshot: {}", source))]
    ReadingStream { source: ArrowError },

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

/// Convert the given stream of RecordBatches to bytes
async fn parquet_stream_to_bytes(
    mut stream: SendableRecordBatchStream,
    schema: SchemaRef,
    metadata: IoxMetadata,
) -> Result<Vec<u8>, ParquetStreamToBytesError> {
    let metadata_bytes = metadata.to_protobuf().context(MetadataEncodeFailureSnafu)?;

    let props = writer_props(&metadata_bytes);

    let mem_writer = MemWriter::default();
    {
        let mut writer = ArrowWriter::try_new(mem_writer.clone(), schema, Some(props))
            .context(OpeningParquetWriterSnafu)?;
        let mut no_stream_data = true;
        while let Some(batch) = stream.next().await {
            no_stream_data = false;
            let batch = batch.context(ReadingStreamSnafu)?;
            writer.write(&batch).context(WritingParquetToMemorySnafu)?;
        }
        if no_stream_data {
            return Ok(vec![]);
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
    use arrow::datatypes::Schema;
    use datafusion::physical_plan::{common::SizedRecordBatchStream, EmptyRecordBatchStream};
    use futures::{stream, TryStreamExt};
    use parquet::schema::types::ColumnPath;
    use query::test::{raw_data, TestChunk};

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
    async fn empty_stream_writes_nothing() {
        let namespace_id = NamespaceId::new(1);
        let table_id = TableId::new(2);
        let sequencer_id = SequencerId::new(3);
        let partition_id = PartitionId::new(4);
        let min_seq_num = SequenceNumber::new(5);
        let max_seq_num = SequenceNumber::new(6);

        let schema = Arc::new(Schema::empty());
        let stream = Box::pin(EmptyRecordBatchStream::new(schema));

        let object_store = object_store();

        persist(
            namespace_id,
            table_id,
            sequencer_id,
            partition_id,
            IoxMetadata {},
            min_seq_num,
            max_seq_num,
            Timestamp::new(7),
            Timestamp::new(8),
            stream,
            &object_store,
        )
        .await
        .unwrap();

        assert!(list_all(&object_store).await.unwrap().is_empty());
    }

    // TODO: SizedRecordBatchStream changed its API, need to figure out
    // #[tokio::test]
    // async fn stream_with_data_writes_to_object_store() {
    //     let namespace_id = NamespaceId::new(1);
    //     let table_id = TableId::new(2);
    //     let sequencer_id = SequencerId::new(3);
    //     let partition_id = PartitionId::new(4);
    //     let min_seq_num = SequenceNumber::new(5);
    //     let max_seq_num = SequenceNumber::new(6);
    //
    //     let chunk1 = Arc::new(
    //         TestChunk::new("t")
    //             .with_id(1)
    //             .with_time_column() //_with_full_stats(
    //             .with_tag_column("tag1")
    //             .with_i64_field_column("field_int")
    //             .with_three_rows_of_data(),
    //     );
    //     let batches: Vec<_> = raw_data(&[chunk1])
    //         .await
    //         .into_iter()
    //         .map(Arc::new)
    //         .collect();
    //     assert_eq!(batches.len(), 1);
    //     let schema = batches[0].schema();
    //     let stream = Box::pin(SizedRecordBatchStream::new(schema, batches));
    //
    //     let object_store = object_store();
    //
    //     persist(
    //         namespace_id,
    //         table_id,
    //         sequencer_id,
    //         partition_id,
    //         IoxMetadata {},
    //         min_seq_num,
    //         max_seq_num,
    //         Timestamp::new(7),
    //         Timestamp::new(8),
    //         stream,
    //         &object_store,
    //     )
    //     .await
    //     .unwrap();
    //
    //     let obj_store_paths = list_all(&object_store).await.unwrap();
    //     assert_eq!(obj_store_paths.len(), 1);
    // }

    #[test]
    fn parquet_file_path_in_object_storage() {
        let object_store = object_store();
        let namespace_id = NamespaceId::new(1);
        let table_id = TableId::new(2);
        let sequencer_id = SequencerId::new(3);
        let partition_id = PartitionId::new(4);
        let object_store_id = Uuid::new_v4();

        let path = parquet_file_object_store_path(
            &object_store,
            namespace_id,
            table_id,
            sequencer_id,
            partition_id,
            object_store_id,
        );

        assert_eq!(
            path.to_raw(),
            format!("1/2/3/4/{}.parquet", object_store_id)
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
