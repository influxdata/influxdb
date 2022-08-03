//! This module is responsible for writing the given data to the specified
//! object store and reading it back.

use crate::{
    metadata::{IoxMetadata, IoxParquetMetaData},
    serialize::{self, CodecError, ROW_GROUP_WRITE_SIZE},
    ParquetFilePath,
};
use arrow::{
    datatypes::{Field, Schema, SchemaRef},
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::{parquet::arrow::ProjectionMask, physical_plan::SendableRecordBatchStream};
use datafusion_util::{watch::WatchedTask, AdapterStream};
use futures::{Stream, TryStreamExt};
use object_store::{DynObjectStore, GetResult};
use observability_deps::tracing::*;
use parquet::{
    arrow::{ArrowReader, ParquetFileArrowReader},
    file::reader::SerializedFileReader,
};
use predicate::Predicate;
use schema::selection::{select_schema, Selection};
use std::{collections::HashMap, sync::Arc, time::Duration};
use thiserror::Error;
use tokio::io::AsyncReadExt;

/// Parquet row group read size
pub const ROW_GROUP_READ_SIZE: usize = 1024;

// ensure read and write work well together
// Skip clippy due to <https://github.com/rust-lang/rust-clippy/issues/8159>.
#[allow(clippy::assertions_on_constants)]
const _: () = assert!(ROW_GROUP_WRITE_SIZE % ROW_GROUP_READ_SIZE == 0);
/// Errors returned during a Parquet "put" operation, covering [`RecordBatch`]
/// pull from the provided stream, encoding, and finally uploading the bytes to
/// the object store.
#[derive(Debug, Error)]
pub enum UploadError {
    /// A codec failure during serialisation.
    #[error(transparent)]
    Serialise(#[from] CodecError),

    /// An error during Parquet metadata conversion when attempting to
    /// instantiate a valid [`IoxParquetMetaData`] instance.
    #[error("failed to construct IOx parquet metadata: {0}")]
    Metadata(crate::metadata::Error),

    /// Uploading the Parquet file to object store failed.
    #[error("failed to upload to object storage: {0}")]
    Upload(#[from] object_store::Error),
}

/// Errors during Parquet file download & scan.
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum ReadError {
    /// Error writing the bytes fetched from object store to the temporary
    /// parquet file on disk.
    #[error("i/o error writing downloaded parquet: {0}")]
    IO(#[from] std::io::Error),

    /// An error fetching Parquet file bytes from object store.
    #[error("failed to read data from object store: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// An error reading the downloaded Parquet file.
    #[error("invalid parquet file: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    /// Schema mismatch
    #[error("Schema mismatch (expected VS actual parquet file) for file '{path}': {source}")]
    SchemaMismatch {
        /// Path of the affected parquet file.
        path: object_store::path::Path,

        /// Source error
        source: ProjectionError,
    },
}

/// The [`ParquetStorage`] type encapsulates [`RecordBatch`] persistence to an
/// underlying [`ObjectStore`].
///
/// [`RecordBatch`] instances are serialized to Parquet files, with IOx specific
/// metadata ([`IoxParquetMetaData`]) attached.
///
/// Code that interacts with Parquet files in object storage should utilise this
/// type that encapsulates the storage & retrieval implementation.
///
/// [`ObjectStore`]: object_store::ObjectStore
#[derive(Debug, Clone)]
pub struct ParquetStorage {
    /// Underlying object store.
    object_store: Arc<DynObjectStore>,
}

impl ParquetStorage {
    /// Initialise a new [`ParquetStorage`] using `object_store` as the
    /// persistence layer.
    pub fn new(object_store: Arc<DynObjectStore>) -> Self {
        Self { object_store }
    }

    /// Push `batches`, a stream of [`RecordBatch`] instances, to object
    /// storage.
    ///
    /// # Retries
    ///
    /// This method retries forever in the presence of object store errors. All
    /// other errors are returned as they occur.
    pub async fn upload<S>(
        &self,
        batches: S,
        meta: &IoxMetadata,
    ) -> Result<(IoxParquetMetaData, usize), UploadError>
    where
        S: Stream<Item = Result<RecordBatch, ArrowError>> + Send,
    {
        // Stream the record batches into a parquet file.
        //
        // It would be nice to stream the encoded parquet to disk for this and
        // eliminate the buffering in memory, but the lack of a streaming object
        // store put negates any benefit of spilling to disk.
        //
        // This is not a huge concern, as the resulting parquet files are
        // currently smallish on average.
        let (data, parquet_file_meta) = serialize::to_parquet_bytes(batches, meta).await?;
        // TODO: remove this if after verifying the panic is thrown
        // correctly inside the serialize::to_parquet_bytes above
        if parquet_file_meta.row_groups.is_empty() {
            debug!(
                ?meta.partition_id, ?parquet_file_meta,
                "Created parquet_file_meta has no row groups which will introduce panic later when its statistics is read");
        }

        // Read the IOx-specific parquet metadata from the file metadata
        let parquet_meta =
            IoxParquetMetaData::try_from(parquet_file_meta).map_err(UploadError::Metadata)?;
        debug!(
            ?meta.partition_id,
            ?parquet_meta,
            "IoxParquetMetaData coverted from Row Group Metadata (aka FileMetaData)"
        );

        // Derive the correct object store path from the metadata.
        let path = ParquetFilePath::from(meta).object_store_path();

        let file_size = data.len();
        let data = Bytes::from(data);

        // Retry uploading the file endlessly.
        //
        // This is abort-able by the user by dropping the upload() future.
        //
        // Cloning `data` is a ref count inc, rather than a data copy.
        while let Err(e) = self.object_store.put(&path, data.clone()).await {
            error!(error=%e, ?meta, "failed to upload parquet file to object storage");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok((parquet_meta, file_size))
    }

    /// Pull the Parquet-encoded [`RecordBatch`] at the file path derived from
    /// the provided [`ParquetFilePath`].
    ///
    /// The `selection` projection is pushed down to the Parquet deserializer.
    ///
    /// This impl fetches the associated Parquet file bytes from object storage,
    /// temporarily persisting them to a local temp file to feed to the arrow
    /// reader.
    ///
    /// No caching is performed by `read_filter()`, and each call to
    /// `read_filter()` will re-download the parquet file unless the underlying
    /// object store impl caches the fetched bytes.
    pub fn read_filter(
        &self,
        _predicate: &Predicate,
        selection: Selection<'_>,
        schema: SchemaRef,
        path: &ParquetFilePath,
    ) -> Result<SendableRecordBatchStream, ReadError> {
        let path = path.object_store_path();
        trace!(path=?path, "fetching parquet data for filtered read");

        // Compute final (output) schema after selection
        let schema = select_schema(selection, &schema);

        let (tx, rx) = tokio::sync::mpsc::channel(2);

        // Run async dance here to make sure any error returned
        // `download_and_scan_parquet` is sent back to the reader and
        // not silently ignored
        let object_store = Arc::clone(&self.object_store);
        let schema_captured = Arc::clone(&schema);
        let tx_captured = tx.clone();
        let fut = async move {
            let download_result =
                download_and_scan_parquet(schema_captured, path, object_store, tx_captured.clone())
                    .await;

            // If there was an error returned from download_and_scan_parquet send it back to the receiver.
            if let Err(e) = download_result {
                warn!(error=%e, "Parquet download & scan failed");
                let e = ArrowError::ExternalError(Box::new(e));
                if let Err(e) = tx_captured.send(ArrowResult::Err(e)).await {
                    // if no one is listening, there is no one else to hear our screams
                    debug!(%e, "Error sending result of download function. Receiver is closed.");
                }
            }

            Ok(())
        };
        let handle = WatchedTask::new(fut, vec![tx], "download and scan parquet");

        // returned stream simply reads off the rx channel
        Ok(AdapterStream::adapt(schema, rx, handle))
    }

    /// Read all data from the parquet file.
    pub fn read_all(
        &self,
        schema: SchemaRef,
        path: &ParquetFilePath,
    ) -> Result<SendableRecordBatchStream, ReadError> {
        self.read_filter(&Predicate::default(), Selection::All, schema, path)
    }
}

/// Downloads the specified parquet file to a local temporary file
/// and push the [`RecordBatch`] contents over `tx`, projecting the specified
/// column indexes.
///
/// This call MAY download a parquet file from object storage, temporarily
/// spilling it to disk while it is processed.
async fn download_and_scan_parquet(
    expected_schema: SchemaRef,
    path: object_store::path::Path,
    object_store: Arc<DynObjectStore>,
    tx: tokio::sync::mpsc::Sender<ArrowResult<RecordBatch>>,
) -> Result<(), ReadError> {
    trace!(?path, "Start parquet download & scan");

    let read_stream = object_store.get(&path).await?;

    let data = match read_stream {
        GetResult::File(f, _) => {
            trace!(?path, "Using file directly");
            let mut f = tokio::fs::File::from_std(f);
            let l = f.metadata().await?.len();
            let mut buf = Vec::with_capacity(l as usize);
            f.read_to_end(&mut buf).await?;
            buf
        }
        GetResult::Stream(read_stream) => {
            let chunks: Vec<_> = read_stream.try_collect().await?;

            let mut buf = Vec::with_capacity(chunks.iter().map(|c| c.len()).sum::<usize>());
            for c in chunks {
                buf.extend(c);
            }

            buf
        }
    };

    // Size of each batch
    let file_reader = SerializedFileReader::new(Bytes::from(data))?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    // Check schema and calculate `file->expected` projections
    let file_schema = arrow_reader.get_schema()?;
    let (mask, reorder_projection) =
        match project_for_parquet_reader(&file_schema, &expected_schema) {
            Ok((mask, reorder_projection)) => (mask, reorder_projection),
            Err(e) => {
                return Err(ReadError::SchemaMismatch { path, source: e });
            }
        };

    let mask = ProjectionMask::roots(arrow_reader.parquet_schema(), mask);
    let record_batch_reader =
        arrow_reader.get_record_reader_by_columns(mask, ROW_GROUP_READ_SIZE)?;

    for batch in record_batch_reader {
        let batch = batch.map(|batch| {
            // project to fix column order
            let batch = batch
                .project(&reorder_projection)
                .expect("bug in projection calculation");

            // attach potential metadata
            RecordBatch::try_new(Arc::clone(&expected_schema), batch.columns().to_vec())
                .expect("bug in schema handling")
        });
        if tx.send(batch).await.is_err() {
            debug!("Receiver hung up - exiting");
            break;
        }
    }

    debug!(?path, "Completed parquet download & scan");

    Ok(())
}

/// Error during projecting parquet file data to an expected schema.
#[derive(Debug, Error)]
#[allow(clippy::large_enum_variant)]
pub enum ProjectionError {
    /// Unknown field.
    #[error("Unknown field: {0}")]
    UnknownField(String),

    /// Field type mismatch
    #[error("Type mismatch, expected {expected:?} but got {actual:?}")]
    FieldTypeMismatch {
        /// Expected field.
        expected: Field,

        /// Actual field.
        actual: Field,
    },
}

/// Calculate project for the parquet-rs reader.
///
/// Expects the schema that was extracted from the actual parquet file and the desired output schema.
///
/// Returns two masks:
///
/// 1. A mask that can be passed to the parquet reader. Since the parquet reader however ignores the mask order, the
///    resulting record batches will have the same order as in the file (i.e. NOT the order in the desired schema).
/// 2. A re-order mask that can be used to reorder the output batches to actually match the desired schema.
///
/// Will fail the desired schema contains a column that is unknown or the field types in the two schemas do not match.
fn project_for_parquet_reader(
    file_schema: &Schema,
    expected_schema: &Schema,
) -> Result<(Vec<usize>, Vec<usize>), ProjectionError> {
    let file_column_indices: HashMap<_, _> = file_schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .enumerate()
        .map(|(v, k)| (k, v))
        .collect();
    let mut mask = Vec::with_capacity(expected_schema.fields().len());
    for field in expected_schema.fields() {
        let file_idx = if let Some(idx) = file_column_indices.get(field.name().as_str()) {
            *idx
        } else {
            return Err(ProjectionError::UnknownField(field.name().clone()));
        };
        let file_field = file_schema.field(file_idx);
        if field != file_field {
            return Err(ProjectionError::FieldTypeMismatch {
                expected: field.clone(),
                actual: file_field.clone(),
            });
        }
        mask.push(file_idx);
    }

    // for some weird reason, the parquet-rs projection system only filters columns but ignores the mask order, so we
    // need to calculate a reorder projection
    // 1. remember for each mask element where it should go in the expected schema
    let mut mask_with_index: Vec<(usize, usize)> = mask.iter().copied().enumerate().collect();
    // 2. perform re-order as parquet-rs would do that (it just uses the mask and sorts it)
    mask_with_index.sort_by_key(|(_a, b)| *b);
    // 3. since we need to transform the re-ordered (i.e. messed up) view back into the mask, throw away the original
    //    mask, keep the expected schema position (added in step 1) and add the index within the parquet-rs output
    let mut mask_with_index: Vec<(usize, usize)> = mask_with_index
        .into_iter()
        .map(|(a, _b)| a)
        .enumerate()
        .collect();
    // 4. sort by the index within the expected schema (added in step 1, forwared in step 3)
    mask_with_index.sort_by_key(|(_a, b)| *b);
    // 5. just keep the index within the parquet-rs output (added in step 3)
    let reorder_projection: Vec<usize> = mask_with_index.into_iter().map(|(a, _b)| a).collect();

    Ok((mask, reorder_projection))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use data_types::{
        CompactionLevel, NamespaceId, PartitionId, SequenceNumber, SequencerId, TableId,
    };
    use datafusion::common::DataFusionError;
    use iox_time::Time;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_upload_metadata() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();

        // Serialize & upload the record batches.
        let (file_meta, _file_size) = upload(&store, &meta, batch.clone()).await;

        // Extract the various bits of metadata.
        let file_meta = file_meta.decode().expect("should decode parquet metadata");
        let got_iox_meta = file_meta
            .read_iox_metadata_new()
            .expect("should read IOx metadata from parquet meta");

        // Ensure the metadata in the file decodes to the same IOx metadata we
        // provided when uploading.
        assert_eq!(got_iox_meta, meta);
    }

    #[tokio::test]
    async fn test_simple_roundtrip() {
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();

        assert_roundtrip(batch.clone(), Selection::All, schema, batch).await;
    }

    #[tokio::test]
    async fn test_selection() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
            ("c", to_string_array(&["foo"])),
            ("d", to_int_array(&[2])),
        ])
        .unwrap();
        let schema = batch.schema();

        let expected_batch = RecordBatch::try_from_iter([
            ("d", to_int_array(&[2])),
            ("c", to_string_array(&["foo"])),
        ])
        .unwrap();
        assert_roundtrip(batch, Selection::Some(&["d", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_selection_unknown() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
        ])
        .unwrap();
        let schema = batch.schema();

        let expected_batch = RecordBatch::try_from_iter([("b", to_int_array(&[1]))]).unwrap();
        assert_roundtrip(batch, Selection::Some(&["b", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_file_has_different_column_order() {
        let file_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
        ])
        .unwrap();
        let schema_batch = RecordBatch::try_from_iter([
            ("b", to_int_array(&[1])),
            ("a", to_string_array(&["value"])),
        ])
        .unwrap();
        let schema = schema_batch.schema();
        assert_roundtrip(file_batch, Selection::All, schema, schema_batch).await;
    }

    #[tokio::test]
    async fn test_file_has_different_column_order_with_selection() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
            ("c", to_string_array(&["foo"])),
            ("d", to_int_array(&[2])),
        ])
        .unwrap();
        let schema_batch = RecordBatch::try_from_iter([
            ("b", to_int_array(&[1])),
            ("d", to_int_array(&[2])),
            ("c", to_string_array(&["foo"])),
            ("a", to_string_array(&["value"])),
        ])
        .unwrap();
        let schema = schema_batch.schema();

        let expected_batch = RecordBatch::try_from_iter([
            ("d", to_int_array(&[2])),
            ("c", to_string_array(&["foo"])),
        ])
        .unwrap();
        assert_roundtrip(batch, Selection::Some(&["d", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_different_types() {
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let other_batch = RecordBatch::try_from_iter([("a", to_int_array(&[1]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Schema mismatch (expected VS actual parquet file) for file '1/3/2/4/00000000-0000-0000-0000-000000000000.parquet': Type mismatch, expected Field { name: \"a\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None } but got Field { name: \"a\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_different_names() {
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let other_batch = RecordBatch::try_from_iter([("b", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Schema mismatch (expected VS actual parquet file) for file '1/3/2/4/00000000-0000-0000-0000-000000000000.parquet': Unknown field: a",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_unknown_column() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_string_array(&["value"])),
        ])
        .unwrap();
        let other_batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Schema mismatch (expected VS actual parquet file) for file '1/3/2/4/00000000-0000-0000-0000-000000000000.parquet': Unknown field: b",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_mem() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();

        // Serialize & upload the record batches.
        upload(&store, &meta, batch).await;

        // add metadata to reference schema
        let schema = Arc::new(
            schema
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
        );
        download(&store, &meta, Selection::All, schema)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_file() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();
        // add metadata to stored batch
        let batch = RecordBatch::try_new(
            Arc::new(
                schema
                    .as_ref()
                    .clone()
                    .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
            ),
            batch.columns().to_vec(),
        )
        .unwrap();

        // Serialize & upload the record batches.
        upload(&store, &meta, batch).await;

        download(&store, &meta, Selection::All, schema)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_schema_check_ignores_extra_column_in_file() {
        let file_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_string_array(&["value"])),
        ])
        .unwrap();
        let expected_batch =
            RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = expected_batch.schema();
        assert_roundtrip(file_batch, Selection::All, schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_schema_check_ignores_type_for_unselected_column() {
        let file_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_string_array(&["value"])),
        ])
        .unwrap();
        let schema_batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
        ])
        .unwrap();
        let schema = schema_batch.schema();
        let expected_batch =
            RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        assert_roundtrip(file_batch, Selection::Some(&["a"]), schema, expected_batch).await;
    }

    #[test]
    fn test_project_for_parquet_reader() {
        assert_eq!(
            run_project_for_parquet_reader(&[], &[]).unwrap(),
            (vec![], vec![]),
        );

        assert_eq!(
            run_project_for_parquet_reader(&[("a", ColType::Int), ("b", ColType::String)], &[])
                .unwrap(),
            (vec![], vec![]),
        );

        assert_eq!(
            run_project_for_parquet_reader(
                &[("a", ColType::Int), ("b", ColType::String)],
                &[("a", ColType::Int), ("b", ColType::String)]
            )
            .unwrap(),
            (vec![0, 1], vec![0, 1]),
        );

        assert_eq!(
            run_project_for_parquet_reader(
                &[("a", ColType::Int), ("b", ColType::String)],
                &[("b", ColType::String), ("a", ColType::Int)]
            )
            .unwrap(),
            (vec![1, 0], vec![1, 0]),
        );

        assert_eq!(
            run_project_for_parquet_reader(
                &[
                    ("a", ColType::Int),
                    ("b", ColType::String),
                    ("c", ColType::String)
                ],
                &[("c", ColType::String), ("a", ColType::Int)]
            )
            .unwrap(),
            (vec![2, 0], vec![1, 0]),
        );

        assert_eq!(
            run_project_for_parquet_reader(
                &[
                    ("a", ColType::Int),
                    ("b", ColType::String),
                    ("c", ColType::String),
                    ("d", ColType::Int)
                ],
                &[
                    ("c", ColType::String),
                    ("b", ColType::String),
                    ("d", ColType::Int)
                ]
            )
            .unwrap(),
            (vec![2, 1, 3], vec![1, 0, 2]),
        );

        assert_eq!(
            run_project_for_parquet_reader(
                &[
                    ("field_int", ColType::Int),
                    ("tag1", ColType::String),
                    ("tag2", ColType::String),
                    ("tag3", ColType::String),
                    ("time", ColType::Int),
                ],
                &[
                    ("tag1", ColType::String),
                    ("tag2", ColType::String),
                    ("tag3", ColType::String),
                    ("field_int", ColType::Int),
                    ("time", ColType::Int),
                ]
            )
            .unwrap(),
            (vec![1, 2, 3, 0, 4], vec![1, 2, 3, 0, 4]),
        );

        assert!(matches!(
            run_project_for_parquet_reader(
                &[("a", ColType::Int), ("b", ColType::String)],
                &[("a", ColType::Int), ("c", ColType::String)]
            )
            .unwrap_err(),
            ProjectionError::UnknownField(_),
        ));

        assert!(matches!(
            run_project_for_parquet_reader(
                &[("a", ColType::Int), ("b", ColType::String)],
                &[("a", ColType::Int), ("b", ColType::Int)]
            )
            .unwrap_err(),
            ProjectionError::FieldTypeMismatch { .. },
        ));
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
        Arc::new(array)
    }

    fn to_int_array(vals: &[i64]) -> ArrayRef {
        let array: Int64Array = vals.iter().map(|v| Some(*v)).collect();
        Arc::new(array)
    }

    fn meta() -> IoxMetadata {
        IoxMetadata {
            object_store_id: Default::default(),
            creation_timestamp: Time::from_timestamp_nanos(42),
            namespace_id: NamespaceId::new(1),
            namespace_name: "bananas".into(),
            sequencer_id: SequencerId::new(2),
            table_id: TableId::new(3),
            table_name: "platanos".into(),
            partition_id: PartitionId::new(4),
            partition_key: "potato".into(),
            max_sequence_number: SequenceNumber::new(11),
            compaction_level: CompactionLevel::FileNonOverlapped,
            sort_key: None,
        }
    }

    async fn upload(
        store: &ParquetStorage,
        meta: &IoxMetadata,
        batch: RecordBatch,
    ) -> (IoxParquetMetaData, usize) {
        let stream = futures::stream::iter([Ok(batch)]);
        store
            .upload(stream, meta)
            .await
            .expect("should serialize and store sucessfully")
    }

    async fn download<'a>(
        store: &ParquetStorage,
        meta: &IoxMetadata,
        selection: Selection<'_>,
        expected_schema: SchemaRef,
    ) -> Result<RecordBatch, DataFusionError> {
        let path: ParquetFilePath = meta.into();
        let rx = store
            .read_filter(&Predicate::default(), selection, expected_schema, &path)
            .expect("should read record batches from object store");
        let schema = rx.schema();
        datafusion::physical_plan::common::collect(rx)
            .await
            .map(|mut batches| {
                assert_eq!(batches.len(), 1);
                let batch = batches.remove(0);
                assert_eq!(batch.schema(), schema);
                batch
            })
    }

    async fn assert_roundtrip(
        upload_batch: RecordBatch,
        selection: Selection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        // Serialize & upload the record batches.
        let meta = meta();
        upload(&store, &meta, upload_batch).await;

        // And compare to the original input
        let actual_batch = download(&store, &meta, selection, expected_schema)
            .await
            .unwrap();
        assert_eq!(actual_batch, expected_batch);
    }

    async fn assert_schema_check_fail(
        persisted_batch: RecordBatch,
        expected_schema: SchemaRef,
        msg: &str,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store);

        let meta = meta();
        upload(&store, &meta, persisted_batch).await;

        let err = download(&store, &meta, Selection::All, expected_schema)
            .await
            .unwrap_err();

        // And compare to the original input
        if let DataFusionError::ArrowError(ArrowError::ExternalError(err)) = err {
            assert_eq!(&err.to_string(), msg,);
        } else {
            panic!("Wrong error type: {err}");
        }
    }

    enum ColType {
        Int,
        String,
    }

    fn build_schema(cols: &[(&str, ColType)]) -> Schema {
        let batch = RecordBatch::try_from_iter(
            cols.iter()
                .map(|(c, t)| {
                    let array = match t {
                        ColType::Int => to_int_array(&[1]),
                        ColType::String => to_string_array(&["foo"]),
                    };

                    (*c, array)
                })
                .chain(std::iter::once(("_not_empty", to_int_array(&[1])))),
        )
        .unwrap();
        let indices: Vec<_> = (0..cols.len()).collect();
        batch.schema().project(&indices).unwrap()
    }

    fn run_project_for_parquet_reader(
        cols_file: &[(&str, ColType)],
        cols_expected: &[(&str, ColType)],
    ) -> Result<(Vec<usize>, Vec<usize>), ProjectionError> {
        let file_schema = build_schema(cols_file);
        let expected_schema = build_schema(cols_expected);
        project_for_parquet_reader(&file_schema, &expected_schema)
    }
}
