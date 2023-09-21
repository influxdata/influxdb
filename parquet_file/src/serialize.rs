//! Streaming [`RecordBatch`] / Parquet file encoder routines.
//!
//! [`RecordBatch`]: arrow::record_batch::RecordBatch

use std::{io::Write, sync::Arc};

use datafusion::{
    error::DataFusionError, execution::memory_pool::MemoryPool,
    physical_plan::SendableRecordBatchStream,
};
use datafusion_util::config::BATCH_SIZE;
use futures::{pin_mut, TryStreamExt};
use observability_deps::tracing::{debug, trace, warn};
use parquet::{
    basic::Compression,
    errors::ParquetError,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use thiserror::Error;

use crate::{
    metadata::{IoxMetadata, METADATA_KEY},
    writer::TrackedMemoryArrowWriter,
};

/// Parquet row group write size
pub const ROW_GROUP_WRITE_SIZE: usize = 1024 * 1024;

/// ensure read and write work well together
/// Skip clippy due to <https://github.com/rust-lang/rust-clippy/issues/8159>.
#[allow(clippy::assertions_on_constants)]
const _: () = assert!(ROW_GROUP_WRITE_SIZE % BATCH_SIZE == 0);

/// [`RecordBatch`] to Parquet serialisation errors.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
#[derive(Debug, Error)]
pub enum CodecError {
    /// The result stream contained no batches.
    #[error("no record batches to convert")]
    NoRecordBatches,

    /// The result stream contained at least one [`RecordBatch`] and all
    /// instances yielded by the stream contained 0 rows.
    ///
    /// This would result in an empty file being uploaded to object store.
    ///
    /// [`RecordBatch`]: arrow::record_batch::RecordBatch
    #[error("no rows to serialise")]
    NoRows,

    /// A DataFusion error during the plan execution.
    ///
    /// Of note: a ResourcesExhaused error likely means the buffer
    /// used for parquet data became too large.
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),

    /// Serialising the [`IoxMetadata`] to protobuf-encoded bytes failed.
    #[error("failed to serialize iox metadata: {0}")]
    MetadataSerialisation(#[from] prost::EncodeError),

    /// Writing the parquet file failed with the specified error.
    #[error("failed to build parquet file: {0}")]
    Writer(#[from] ParquetError),

    /// Attempting to clone a handle to the provided write sink failed.
    #[error("failed to obtain writer handle clone: {0}")]
    CloneSink(std::io::Error),
}

impl From<CodecError> for DataFusionError {
    fn from(value: CodecError) -> Self {
        match value {
            e @ (CodecError::NoRecordBatches
            | CodecError::NoRows
            | CodecError::MetadataSerialisation(_)
            | CodecError::CloneSink(_)) => Self::External(Box::new(e)),
            CodecError::Writer(e) => Self::ParquetError(e),
            CodecError::DataFusion(e) => e,
        }
    }
}

impl From<crate::writer::Error> for CodecError {
    fn from(value: crate::writer::Error) -> Self {
        match value {
            crate::writer::Error::Writer(e) => Self::Writer(e),
            crate::writer::Error::OutOfMemory(e) => Self::DataFusion(e),
        }
    }
}

/// An IOx-specific, streaming [`RecordBatch`] to parquet file encoder.
///
/// This encoder discovers the schema from the first item in `batches`, and
/// encodes each [`RecordBatch`] one by one into `W`. All [`RecordBatch`]
/// yielded by the stream must be of the same schema, or this call will return
/// an error.
///
/// IOx metadata is encoded into the parquet file's metadata under the key
/// [`METADATA_KEY`], with a base64-wrapped, protobuf serialized
/// [`proto::IoxMetadata`] structure.
///
/// Returns the serialized [`FileMetaData`] for the encoded parquet file, from
/// which an [`IoxParquetMetaData`] can be derived.
///
/// # Errors
///
/// If the stream yields a [`RecordBatch`] containing no rows, a warning is
/// logged and serialisation continues.
///
/// If [`to_parquet()`] observes at least one [`RecordBatch`], but 0 rows across
/// all [`RecordBatch`], then [`CodecError::NoRows`] is returned as no useful
/// data was serialised.
///
/// [`proto::IoxMetadata`]: generated_types::influxdata::iox::ingester::v1
/// [`FileMetaData`]: parquet::format::FileMetaData
/// [`IoxParquetMetaData`]: crate::metadata::IoxParquetMetaData
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub async fn to_parquet<W>(
    batches: SendableRecordBatchStream,
    meta: &IoxMetadata,
    pool: Arc<dyn MemoryPool>,
    sink: W,
) -> Result<parquet::format::FileMetaData, CodecError>
where
    W: Write + Send,
{
    // The ArrowWriter::write() call will return an error if any subsequent
    // batch does not match this schema, enforcing schema uniformity.
    let schema = batches.schema();

    let stream = batches;
    pin_mut!(stream);

    // Serialize the IoxMetadata to the protobuf bytes.
    let props = writer_props(meta)?;
    let write_batch_size = props.write_batch_size();
    let max_row_group_size = props.max_row_group_size();

    // Construct the arrow serializer with the metadata as part of the parquet
    // file properties.
    let mut writer = TrackedMemoryArrowWriter::try_new(sink, Arc::clone(&schema), props, pool)?;

    let mut num_batches = 0;
    while let Some(batch) = stream.try_next().await? {
        writer.write(batch)?;
        num_batches += 1;
    }

    let writer_meta = writer.close()?;
    if writer_meta.num_rows == 0 {
        // throw warning if all input batches are empty
        warn!("parquet serialisation encoded 0 rows");
        return Err(CodecError::NoRows);
    }

    debug!(num_batches,
           num_rows=writer_meta.num_rows,
           object_store_id=?meta.object_store_id,
           write_batch_size,
           max_row_group_size,
           "Created parquet file");

    Ok(writer_meta)
}

/// A helper function that calls [`to_parquet()`], serialising the parquet file
/// into an in-memory buffer and returning the resulting bytes.
pub async fn to_parquet_bytes(
    batches: SendableRecordBatchStream,
    meta: &IoxMetadata,
    pool: Arc<dyn MemoryPool>,
) -> Result<(Vec<u8>, parquet::format::FileMetaData), CodecError> {
    let mut bytes = vec![];

    debug!(
        ?meta,
        "IOxMetaData provided for serializing the data into the in-memory buffer"
    );

    // Serialize the record batches into the in-memory buffer
    let meta = to_parquet(batches, meta, pool, &mut bytes).await?;
    bytes.shrink_to_fit();

    trace!(?meta, "generated parquet file metadata");

    Ok((bytes, meta))
}

/// Helper to construct [`WriterProperties`] , serialising the given
/// [`IoxMetadata`] and embedding it as a key=value property keyed by
/// [`METADATA_KEY`].
fn writer_props(meta: &IoxMetadata) -> Result<WriterProperties, prost::EncodeError> {
    let builder = WriterProperties::builder()
        .set_key_value_metadata(Some(vec![KeyValue {
            key: METADATA_KEY.to_string(),
            value: Some(meta.to_base64()?),
        }]))
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(ROW_GROUP_WRITE_SIZE);

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::IoxParquetMetaData;
    use arrow::{
        array::{ArrayRef, StringArray},
        record_batch::RecordBatch,
    };
    use bytes::Bytes;
    use data_types::{CompactionLevel, NamespaceId, TableId};
    use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use datafusion_util::{unbounded_memory_pool, MemoryStream};
    use iox_time::Time;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_encode_stream() {
        let meta = IoxMetadata {
            object_store_id: Default::default(),
            creation_timestamp: Time::from_timestamp_nanos(42),
            namespace_id: NamespaceId::new(1),
            namespace_name: "bananas".into(),
            table_id: TableId::new(3),
            table_name: "platanos".into(),
            partition_key: "potato".into(),
            compaction_level: CompactionLevel::FileNonOverlapped,
            sort_key: None,
            max_l0_created_at: Time::from_timestamp_nanos(42),
        };

        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let stream = Box::pin(MemoryStream::new(vec![batch.clone()]));

        let (bytes, _file_meta) = to_parquet_bytes(stream, &meta, unbounded_memory_pool())
            .await
            .expect("should serialize");

        let bytes = Bytes::from(bytes);
        // Read the metadata from the file bytes.
        //
        // This is quite wordy...
        let iox_parquet_meta = IoxParquetMetaData::from_file_bytes(bytes.clone())
            .expect("should decode")
            .expect("should contain metadata")
            .decode()
            .expect("should decode IOx metadata")
            .read_iox_metadata_new()
            .expect("should read IOxMetadata");
        assert_eq!(iox_parquet_meta, meta);

        // Read the parquet file back to arrow records
        let arrow_reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .expect("should init builder")
            .with_batch_size(100)
            .build()
            .expect("should create reader");

        let mut record_batches = arrow_reader.into_iter().collect::<Vec<_>>();

        assert_eq!(record_batches.len(), 1);
        assert_eq!(
            record_batches.pop().unwrap().expect("should be OK batch"),
            batch
        );
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
        Arc::new(array)
    }
}
