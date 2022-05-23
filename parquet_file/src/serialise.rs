//! Streaming [`RecordBatch`] / Parquet file encoder routines.

use std::{ops::DerefMut, sync::Arc};

use arrow::{error::ArrowError, record_batch::RecordBatch};
use futures::{pin_mut, Stream, StreamExt};
use parquet::{
    arrow::ArrowWriter,
    basic::Compression,
    errors::ParquetError,
    file::{
        metadata::KeyValue,
        properties::WriterProperties,
        writer::{InMemoryWriteableCursor, ParquetWriter},
    },
};
use thiserror::Error;

use crate::metadata::{IoxMetadata, METADATA_KEY};

/// [`RecordBatch`] to Parquet serialisation errors.
#[derive(Debug, Error)]
pub enum CodecError {
    /// The result stream contained no batches.
    #[error("no record batches to convert")]
    NoRecordBatches,

    /// The codec could not infer the schema for the stream as the first stream
    /// item contained an [`ArrowError`].
    #[error("failed to peek record stream schema")]
    SchemaPeek,

    /// An arrow error during the plan execution.
    #[error(transparent)]
    Arrow(#[from] ArrowError),

    /// Serialising the [`IoxMetadata`] to protobuf-encoded bytes failed.
    #[error("failed to serialise iox metadata: {0}")]
    MetadataSerialisation(#[from] prost::EncodeError),

    /// Writing the parquet file failed with the specified error.
    #[error("failed to build parquet file: {0}")]
    Writer(#[from] ParquetError),

    /// Attempting to clone a handle to the provided write sink failed.
    #[error("failed to obtain writer handle clone: {0}")]
    CloneSink(std::io::Error),
}

/// An IOx-specific, streaming [`RecordBatch`] to parquet file encoder.
///
/// This encoder discovers the schema from the first item in `batches`, and
/// encodes each [`RecordBatch`] one by one into `W`. All [`RecordBatch`]
/// yielded by the stream must be of the same schema, or this call will return
/// an error.
///
/// IOx metadata is encoded into the parquet file's metadata under the key
/// [`METADATA_KEY`], with a base64-wrapped, protobuf serialised
/// [`proto::IoxMetadata`] structure.
///
/// Returns the serialised [`FileMetaData`] for the encoded parquet file, from
/// which an [`IoxParquetMetaData`] can be derived.
///
/// [`proto::IoxMetadata`]: generated_types::influxdata::iox::ingester::v1
/// [`FileMetaData`]: parquet_format::FileMetaData
/// [`IoxParquetMetaData`]: crate::metadata::IoxParquetMetaData
pub async fn to_parquet<S, W, T>(
    batches: S,
    meta: &IoxMetadata,
    sink: W,
) -> Result<parquet_format::FileMetaData, CodecError>
where
    S: Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    W: DerefMut<Target = T> + Send,
    T: ParquetWriter + Send + 'static,
{
    // Let the caller pass a mutable ref to something that impls ParquetWriter
    // while still providing the necessary owned impl to the ArrowWriter.
    let sink = sink.deref().try_clone().map_err(CodecError::CloneSink)?;

    let stream = batches.peekable();
    pin_mut!(stream);

    // Peek into the stream and extract the schema from the first record batch.
    //
    // The ArrowWriter::write() call will return an error if any subsequent
    // batch does not match this schema, enforcing schema uniformity.
    let schema = stream
        .as_mut()
        .peek()
        .await
        .ok_or(CodecError::NoRecordBatches)?
        .as_ref()
        .ok()
        .map(|v| v.schema())
        .ok_or(CodecError::SchemaPeek)?;

    // Serialise the IoxMetadata to the protobuf bytes.
    let props = writer_props(meta)?;

    // Construct the arrow serialiser with the metadata as part of the parquet
    // file properties.
    let mut writer = ArrowWriter::try_new(sink, Arc::clone(&schema), Some(props))?;

    while let Some(maybe_batch) = stream.next().await {
        writer.write(&maybe_batch?)?;
    }

    writer.close().map_err(CodecError::from)
}

/// A helper function that calls [`to_parquet()`], serialising the parquet file
/// into an in-memory buffer and returning the resulting bytes.
pub async fn to_parquet_bytes<S>(
    batches: S,
    meta: &IoxMetadata,
) -> Result<(Vec<u8>, parquet_format::FileMetaData), CodecError>
where
    S: Stream<Item = Result<RecordBatch, ArrowError>> + Send,
{
    let mut w = InMemoryWriteableCursor::default();

    // Serialise the record batches into the in-memory buffer
    let meta = to_parquet(batches, meta, &mut w).await?;

    let mut bytes = w
        .into_inner()
        .expect("mem writer has outstanding reference");

    bytes.shrink_to_fit();

    Ok((bytes, meta))
}

/// Helper to construct [`WriterProperties`] for the [`ArrowWriter`],
/// serialising the given [`IoxMetadata`] and embedding it as a key=value
/// property keyed by [`METADATA_KEY`].
fn writer_props(meta: &IoxMetadata) -> Result<WriterProperties, prost::EncodeError> {
    let bytes = meta.to_protobuf()?;

    let builder = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue {
            key: METADATA_KEY.to_string(),
            value: Some(base64::encode(&bytes)),
        }]))
        .set_compression(Compression::ZSTD);

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringBuilder};
    use data_types::{NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId};
    use iox_time::Time;
    use parquet::{
        arrow::{ArrowReader, ParquetFileArrowReader},
        file::serialized_reader::{SerializedFileReader, SliceableCursor},
    };

    use crate::metadata::IoxParquetMetaData;

    use super::*;

    #[tokio::test]
    async fn test_encode_stream() {
        let meta = IoxMetadata {
            object_store_id: Default::default(),
            creation_timestamp: Time::from_timestamp_nanos(42),
            namespace_id: NamespaceId::new(1),
            namespace_name: "bananas".into(),
            sequencer_id: SequencerId::new(2),
            table_id: TableId::new(3),
            table_name: "platanos".into(),
            partition_id: PartitionId::new(4),
            partition_key: "potato".into(),
            time_of_first_write: Time::from_timestamp_nanos(4242),
            time_of_last_write: Time::from_timestamp_nanos(424242),
            min_sequence_number: SequenceNumber::new(10),
            max_sequence_number: SequenceNumber::new(11),
            row_count: 1000,
            compaction_level: 1,
            sort_key: None,
        };

        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let stream = futures::stream::iter([Ok(batch.clone())]);

        let (bytes, _file_meta) = to_parquet_bytes(stream, &meta)
            .await
            .expect("should serialise");

        // Read the metadata from the file bytes.
        //
        // This is quite wordy...
        let iox_parquet_meta = IoxParquetMetaData::from_file_bytes(Arc::new(bytes.clone()))
            .expect("should decode")
            .expect("should contain metadata")
            .decode()
            .expect("should decode IOx metadata")
            .read_iox_metadata_new()
            .expect("should read IOxMetadata");
        assert_eq!(iox_parquet_meta, meta);

        // Read the parquet file back to arrow records
        let cursor = SliceableCursor::new(bytes);
        let file_reader = SerializedFileReader::new(cursor).expect("should init reader");
        let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

        let mut record_batches = arrow_reader
            .get_record_reader(100)
            .expect("should read")
            .into_iter()
            .collect::<Vec<_>>();

        assert_eq!(record_batches.len(), 1);
        assert_eq!(
            record_batches.pop().unwrap().expect("should be OK batch"),
            batch
        );
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let mut builder = StringBuilder::new(strs.len());
        for s in strs {
            builder.append_value(s).expect("appending string");
        }
        Arc::new(builder.finish())
    }
}
