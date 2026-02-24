//! Streaming [`RecordBatch`] / Parquet file encoder routines.
//!
//! [`RecordBatch`]: arrow::record_batch::RecordBatch

use std::{
    collections::{HashMap, HashSet},
    io::Write,
    num::NonZeroUsize,
    sync::Arc,
};

use datafusion::logical_expr::dml::InsertOp;
use datafusion::{
    config::{ParquetOptions, TableParquetOptions},
    datasource::{
        file_format::parquet::ParquetSink,
        listing::ListingTableUrl,
        physical_plan::{FileSink, FileSinkConfig},
    },
    error::DataFusionError,
    execution::{TaskContext, memory_pool::MemoryPool, runtime_env::RuntimeEnv},
    physical_plan::SendableRecordBatchStream,
};
use datafusion_util::config::{BATCH_SIZE, table_parquet_options};
use futures::{TryStreamExt, pin_mut};
use parquet::{
    arrow::ARROW_SCHEMA_META_KEY,
    basic::Compression,
    errors::ParquetError,
    file::{metadata::KeyValue, properties::WriterProperties},
};
use thiserror::Error;
use tracing::{debug, trace, warn};

use crate::{
    metadata::{IoxMetadata, METADATA_KEY},
    storage::ParquetUploadInput,
    writer::TrackedMemoryArrowWriter,
};

/// Parquet row group write size
pub const ROW_GROUP_WRITE_SIZE: usize = 1024 * 1024;

/// ensure read and write work well together
const _: () = assert!(ROW_GROUP_WRITE_SIZE.is_multiple_of(BATCH_SIZE));

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
    DataFusion(Box<DataFusionError>),

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
            CodecError::Writer(e) => Self::ParquetError(Box::new(e)),
            CodecError::DataFusion(e) => *e,
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

// Manual impl, see https://github.com/dtolnay/thiserror/issues/415
impl From<DataFusionError> for CodecError {
    fn from(e: DataFusionError) -> Self {
        Self::DataFusion(Box::new(e))
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
/// [`METADATA_KEY`]: crate::metadata::METADATA_KEY
/// [`FileMetaData`]: parquet::format::FileMetaData
/// [`IoxParquetMetaData`]: crate::metadata::IoxParquetMetaData
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
pub async fn to_parquet<W>(
    batches: SendableRecordBatchStream,
    meta: Vec<KeyValue>,
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
        warn!("parquet serialization encoded 0 rows");
        return Err(CodecError::NoRows);
    }

    debug!(
        num_batches,
        num_rows = writer_meta.num_rows,
        write_batch_size,
        max_row_group_size,
        "Created parquet file"
    );

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
    let meta = to_parquet(batches, meta.try_into()?, pool, &mut bytes).await?;
    bytes.shrink_to_fit();

    trace!(?meta, "generated parquet file metadata");

    Ok((bytes, meta))
}

/// Settings related to writing parquet files in parallel
///
/// Currently, this is a near-direct copy of the setting [used in the ParquetSink](https://github.com/apache/datafusion/blob/f8c623fe045d70a87eac8dc8620b74ff73be56d5/datafusion/core/src/datasource/file_format/parquet.rs#L803-L804).
///
/// How the two config settings interact with each other is based upon [this implementation](https://github.com/apache/datafusion/blob/f8c623fe045d70a87eac8dc8620b74ff73be56d5/datafusion/core/src/datasource/file_format/parquet.rs#L983-L1017),
/// where the separately spawned column writers have their outputs buffered to the row group serializers.
#[derive(Debug, Clone, Copy)]
pub struct ParallelParquetWriterOptions {
    /// How many row groups may be serialized at once.
    maximum_parallel_row_group_writers: usize,
    /// How many columns may be written in parallel, across all row groups.
    maximum_buffered_record_batches_per_stream: usize,
}

impl ParallelParquetWriterOptions {
    /// Create a new [`ParallelParquetWriterOptions`].
    pub fn new(
        num_row_group_writers: NonZeroUsize,
        num_column_writers_across_row_groups: NonZeroUsize,
    ) -> Self {
        Self {
            maximum_parallel_row_group_writers: num_row_group_writers.into(),
            maximum_buffered_record_batches_per_stream: num_column_writers_across_row_groups.into(),
        }
    }
}

impl Default for ParallelParquetWriterOptions {
    fn default() -> Self {
        Self {
            maximum_parallel_row_group_writers: 1,
            maximum_buffered_record_batches_per_stream: 2,
        }
    }
}

/// Performs a parallelized parquet encoding and upload to object_store.
///
/// Current implementation uses the [`ParquetSink`] from DataFusion,
/// and uploads on the same threadpool as encoding.
pub async fn to_parquet_upload(
    batches: SendableRecordBatchStream,
    meta: &IoxMetadata,
    upload_input: ParquetUploadInput,
    runtime: Arc<RuntimeEnv>,
    parallel_writer_options: ParallelParquetWriterOptions,
) -> Result<parquet::format::FileMetaData, CodecError> {
    let table_path = ListingTableUrl::parse(format!("file:///{}", upload_input.path()))?;
    let object_store_url = upload_input.object_store_url();

    // TODO: add fix upstream in the ParquetSink code.
    // Refer to <https://github.com/apache/datafusion/issues/11770>.
    let mut meta: Vec<KeyValue> = meta.try_into()?;
    arrow_util::parquet_meta::add_encoded_arrow_schema_to_metadata(batches.schema(), &mut meta);

    // parquet options
    let metadata_keys: HashSet<String> =
        HashSet::from_iter(meta.iter().map(|KeyValue { key, .. }| key.clone()));
    assert_eq!(
        metadata_keys.len(),
        2,
        "expected IoxMetadata to contain two KeyValues (iox meta and arrow meta)"
    );
    assert!(
        metadata_keys.contains(ARROW_SCHEMA_META_KEY),
        "expected to contain the arrow metadata key"
    );
    assert!(
        metadata_keys.contains(METADATA_KEY),
        "expected to contain the iox metadata key"
    );
    let parquet_options = parallel_parquet_options(parallel_writer_options, meta);

    // make sink
    let sink_config = FileSinkConfig {
        original_url: object_store_url.to_string(),
        object_store_url: object_store_url.clone(),
        file_group: Default::default(), // I believe this is unused in the ParquetSink path (is used for other DataSink impls).
        table_paths: vec![table_path], // Sink location used by the demuxer. Single location means no splitting; not a collection of outputs.
        output_schema: batches.schema(),
        table_partition_cols: vec![], // should be empty, since we want sink to be a single parquet
        insert_op: InsertOp::Overwrite, // always overwrite (we always write to a new file anyways)
        keep_partition_by_columns: false,
        file_extension: "parquet".into(),
    };
    let sink = ParquetSink::new(sink_config, parquet_options);

    // run sink write_all task
    let task_context = Arc::new(TaskContext::default().with_runtime(runtime));
    let num_rows = sink.write_all(batches, &task_context).await?;

    if num_rows == 0 {
        // throw warning if all input batches are empty
        warn!("parquet serialization encoded 0 rows");
        return Err(CodecError::NoRows);
    }

    // get file metadata
    let mut written_metas = sink.written().drain().collect::<Vec<_>>();
    assert_eq!(
        written_metas.len(),
        1,
        "should have written a single parquet file"
    );
    let writer_meta = written_metas.pop().unwrap().1;

    Ok(writer_meta)
}

/// Helper to contruct [`TableParquetOptions`] for parallelized writes.
///
/// The configuration used here should be the same as in [`writer_props`].
fn parallel_parquet_options(
    parallel_writer_options: ParallelParquetWriterOptions,
    meta: Vec<KeyValue>,
) -> TableParquetOptions {
    let default_options = table_parquet_options();

    let ParallelParquetWriterOptions {
        maximum_parallel_row_group_writers,
        maximum_buffered_record_batches_per_stream,
    } = parallel_writer_options;

    TableParquetOptions {
        global: ParquetOptions {
            // parallel options
            allow_single_file_parallelism: true,
            maximum_parallel_row_group_writers,
            maximum_buffered_record_batches_per_stream,

            // iox specific options (in line with `writer_props`)
            max_row_group_size: ROW_GROUP_WRITE_SIZE,
            compression: Some("zstd(1)".into()),

            // TODO: datafusion's ParquetOptions defaults need to be update
            // Refer to <https://github.com/apache/datafusion/issues/11367>
            data_page_row_count_limit: 20_000,
            column_index_truncate_length: Some(64),

            // TODO: enable view types in iox code
            // Refer to https://github.com/influxdata/influxdb_iox/issues/13093
            schema_force_view_types: false,

            ..default_options.global
        },
        key_value_metadata: meta.into_iter().fold(
            HashMap::new(),
            |mut acc, KeyValue { key, value }| {
                acc.insert(key, value);
                acc
            },
        ),
        ..default_options
    }
}

/// Helper to construct [`WriterProperties`] for a list of given [`KeyValue`] metadata values.
///
/// The configuration used here should be the same as in [`parallel_parquet_options`].
fn writer_props(meta: Vec<KeyValue>) -> Result<WriterProperties, prost::EncodeError> {
    let builder = WriterProperties::builder()
        .set_key_value_metadata(Some(meta))
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
    use data_types::{
        CompactionLevel, MaxL0CreatedAt, NamespaceId, ObjectStoreId, TableId, Timestamp,
    };
    use datafusion::{
        DATAFUSION_VERSION, common::file_options::parquet_writer::ParquetWriterOptions,
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder,
    };
    use datafusion_util::{MemoryStream, unbounded_memory_pool};
    use iox_time::Time;
    use parquet::schema::types::ColumnPath;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_encode_stream() {
        let meta = IoxMetadata {
            object_store_id: ObjectStoreId::new(),
            creation_timestamp: Time::from_timestamp_nanos(42),
            namespace_id: NamespaceId::new(1),
            namespace_name: "bananas".into(),
            table_id: TableId::new(3),
            table_name: "platanos".into(),
            partition_key: "potato".into(),
            compaction_level: CompactionLevel::FileNonOverlapped,
            sort_key: None,
            max_l0_created_at: MaxL0CreatedAt::Computed(Timestamp::new(42)),
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

    fn assert_writer_properties_are_eq(a: WriterProperties, b: WriterProperties) {
        let column_path = ColumnPath::new(vec!["foo".into()]);

        assert_eq!(
            a.data_page_size_limit(),
            b.data_page_size_limit(),
            "should have same data_page_size_limit"
        );
        assert_eq!(
            a.dictionary_page_size_limit(),
            b.dictionary_page_size_limit(),
            "should have same dictionary_page_size_limit"
        );
        assert_eq!(
            a.data_page_row_count_limit(),
            b.data_page_row_count_limit(),
            "should have same data_page_row_count_limit"
        );
        assert_eq!(
            a.write_batch_size(),
            b.write_batch_size(),
            "should have same write_batch_size"
        );
        assert_eq!(
            a.max_row_group_size(),
            b.max_row_group_size(),
            "should have same max_row_group_size"
        );
        assert_eq!(
            a.writer_version(),
            b.writer_version(),
            "should have same writer_version"
        );
        assert_eq!(
            a.key_value_metadata()
                .cloned()
                .map(|mut kv_vec| kv_vec.sort()),
            b.key_value_metadata()
                .cloned()
                .map(|mut kv_vec| kv_vec.sort()),
            "should have same key_value_metadata"
        );
        assert_eq!(
            a.sorting_columns(),
            b.sorting_columns(),
            "should have same sorting_columns"
        );
        assert_eq!(
            a.column_index_truncate_length(),
            b.column_index_truncate_length(),
            "should have same column_index_truncate_length"
        );
        assert_eq!(
            a.statistics_truncate_length(),
            b.statistics_truncate_length(),
            "should have same statistics_truncate_length"
        );
        assert_eq!(
            a.dictionary_data_page_encoding(),
            b.dictionary_data_page_encoding(),
            "should have same dictionary_data_page_encoding"
        );
        assert_eq!(
            a.dictionary_page_encoding(),
            b.dictionary_page_encoding(),
            "should have same dictionary_page_encoding"
        );
        assert_eq!(
            a.compression(&column_path),
            b.compression(&column_path),
            "should have same compression"
        );
        assert_eq!(
            a.dictionary_enabled(&column_path),
            b.dictionary_enabled(&column_path),
            "should have same dictionary_enabled"
        );
        assert_eq!(
            a.statistics_enabled(&column_path),
            b.statistics_enabled(&column_path),
            "should have same statistics_enabled"
        );
        assert_eq!(
            a.bloom_filter_properties(&column_path),
            b.bloom_filter_properties(&column_path),
            "should have same bloom_filter_properties"
        );
    }

    #[test]
    fn test_writer_properties_are_identical() {
        let kv_meta = vec![
            // the default behavior for WriterPropertiesBuilder requires ARROW_SCHEMA_META_KEY
            KeyValue {
                key: ARROW_SCHEMA_META_KEY.into(),
                value: None,
            },
            KeyValue {
                key: "iox-metadata-key".into(),
                value: None,
            },
        ];

        // use writer_props() for writer props
        let single_threaded_props = writer_props(kv_meta.clone()).unwrap();

        // use parallel_parquet_options() for writer props
        let set_for_single_threaded = ParallelParquetWriterOptions {
            maximum_parallel_row_group_writers: 1,
            maximum_buffered_record_batches_per_stream: 1,
        };
        let parallel_table_parquet_opts =
            parallel_parquet_options(set_for_single_threaded, kv_meta);
        let ParquetWriterOptions {
            writer_options: parallel_props,
        } = ParquetWriterOptions::try_from(&parallel_table_parquet_opts).unwrap();

        // will have different created_by (parquet-rs vs datafusion)
        assert_eq!(
            single_threaded_props.created_by(),
            parquet::file::properties::DEFAULT_CREATED_BY,
        );
        assert_eq!(
            parallel_props.created_by(),
            format!("datafusion version {DATAFUSION_VERSION}")
        );

        // assert they are the same
        assert_writer_properties_are_eq(single_threaded_props, parallel_props);
    }
}
