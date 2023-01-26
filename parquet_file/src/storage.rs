//! This module is responsible for writing the given data to the specified
//! object store and reading it back.

use crate::{
    metadata::{IoxMetadata, IoxParquetMetaData},
    serialize::{self, CodecError},
    ParquetFilePath,
};
use arrow::{
    datatypes::{Field, SchemaRef},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use datafusion::{
    datasource::{listing::PartitionedFile, object_store::ObjectStoreUrl},
    error::DataFusionError,
    execution::context::TaskContext,
    physical_plan::{
        file_format::{FileScanConfig, ParquetExec},
        ExecutionPlan, SendableRecordBatchStream, Statistics,
    },
    prelude::SessionContext,
};
use datafusion_util::config::iox_session_config;
use object_store::{DynObjectStore, ObjectMeta};
use observability_deps::tracing::*;
use schema::Projection;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;

/// Errors returned during a Parquet "put" operation, covering [`RecordBatch`]
/// pull from the provided stream, encoding, and finally uploading the bytes to
/// the object store.
///
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
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

impl From<UploadError> for DataFusionError {
    fn from(value: UploadError) -> Self {
        match value {
            UploadError::Serialise(e) => {
                Self::Context(String::from("serialize"), Box::new(e.into()))
            }
            UploadError::Metadata(e) => Self::External(Box::new(e)),
            UploadError::Upload(e) => Self::ObjectStore(e),
        }
    }
}

/// ID for an object store hooked up into DataFusion.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct StorageId(&'static str);

impl From<&'static str> for StorageId {
    fn from(id: &'static str) -> Self {
        Self(id)
    }
}

impl AsRef<str> for StorageId {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl std::fmt::Display for StorageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Inputs required to build a [`ParquetExec`] for one or multiple files.
///
/// The files shall be grouped by [`object_store_url`](Self::object_store_url). For each each object store, you shall
/// create one [`ParquetExec`] and put each file into its own "file group".
///
/// [`ParquetExec`]: datafusion::physical_plan::file_format::ParquetExec
#[derive(Debug)]
pub struct ParquetExecInput {
    /// Store where the file is located.
    pub object_store_url: ObjectStoreUrl,

    /// Object metadata.
    pub object_meta: ObjectMeta,
}

impl ParquetExecInput {
    /// Read parquet file into [`RecordBatch`]es.
    ///
    /// This should only be used for testing purposes.
    pub async fn read_to_batches(
        &self,
        schema: SchemaRef,
        projection: Projection<'_>,
        session_ctx: &SessionContext,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        // Compute final (output) schema after selection
        let schema = Arc::new(
            projection
                .project_schema(&schema)
                .as_ref()
                .clone()
                .with_metadata(Default::default()),
        );

        let base_config = FileScanConfig {
            object_store_url: self.object_store_url.clone(),
            file_schema: schema,
            file_groups: vec![vec![PartitionedFile {
                object_meta: self.object_meta.clone(),
                partition_values: vec![],
                range: None,
                extensions: None,
            }]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            // Parquet files ARE actually sorted but we don't care here since we just construct a `collect` plan.
            output_ordering: None,
            infinite_source: false,
        };
        let exec = ParquetExec::new(base_config, None, None);
        let exec_schema = exec.schema();
        datafusion::physical_plan::collect(Arc::new(exec), session_ctx.task_ctx())
            .await
            .map(|batches| {
                for batch in &batches {
                    assert_eq!(batch.schema(), exec_schema);
                }
                batches
            })
    }
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
/// [`RecordBatch`]: arrow::record_batch::RecordBatch
#[derive(Debug, Clone)]
pub struct ParquetStorage {
    /// Underlying object store.
    object_store: Arc<DynObjectStore>,

    /// Storage ID to hook it into DataFusion.
    id: StorageId,
}

impl ParquetStorage {
    /// Initialise a new [`ParquetStorage`] using `object_store` as the
    /// persistence layer.
    pub fn new(object_store: Arc<DynObjectStore>, id: StorageId) -> Self {
        Self { object_store, id }
    }

    /// Get underlying object store.
    pub fn object_store(&self) -> &Arc<DynObjectStore> {
        &self.object_store
    }

    /// Get ID.
    pub fn id(&self) -> StorageId {
        self.id
    }

    /// Fake DataFusion context for testing that contains this store
    pub fn test_df_context(&self) -> SessionContext {
        // set up "fake" DataFusion session
        let object_store = Arc::clone(&self.object_store);
        let session_ctx = SessionContext::with_config(iox_session_config());
        let task_ctx = Arc::new(TaskContext::from(&session_ctx));
        task_ctx
            .runtime_env()
            .register_object_store("iox", self.id, object_store);

        session_ctx
    }

    /// Push `batches`, a stream of [`RecordBatch`] instances, to object
    /// storage.
    ///
    /// # Retries
    ///
    /// This method retries forever in the presence of object store errors. All
    /// other errors are returned as they occur.
    ///
    /// [`RecordBatch`]: arrow::record_batch::RecordBatch
    pub async fn upload(
        &self,
        batches: SendableRecordBatchStream,
        meta: &IoxMetadata,
    ) -> Result<(IoxParquetMetaData, usize), UploadError> {
        let start = Instant::now();

        // Stream the record batches into a parquet file.
        //
        // It would be nice to stream the encoded parquet to disk for this and
        // eliminate the buffering in memory, but the lack of a streaming object
        // store put negates any benefit of spilling to disk.
        //
        // This is not a huge concern, as the resulting parquet files are
        // currently smallish on average.
        let (data, parquet_file_meta) = serialize::to_parquet_bytes(batches, meta).await?;

        // Read the IOx-specific parquet metadata from the file metadata
        let parquet_meta =
            IoxParquetMetaData::try_from(parquet_file_meta).map_err(UploadError::Metadata)?;
        trace!(
            ?meta.partition_id,
            ?parquet_meta,
            "IoxParquetMetaData coverted from Row Group Metadata (aka FileMetaData)"
        );

        // Derive the correct object store path from the metadata.
        let path = ParquetFilePath::from(meta).object_store_path();

        let file_size = data.len();
        let data = Bytes::from(data);

        debug!(
            file_size,
            object_store_id=?meta.object_store_id,
            partition_id=?meta.partition_id,
            // includes the time to run the datafusion plan (that is the batches)
            total_time_to_create_parquet_bytes=?(Instant::now() - start),
            "Uploading parquet to object store"
        );

        // Retry uploading the file endlessly.
        //
        // This is abort-able by the user by dropping the upload() future.
        //
        // Cloning `data` is a ref count inc, rather than a data copy.
        let mut retried = false;
        while let Err(e) = self.object_store.put(&path, data.clone()).await {
            warn!(error=%e, ?meta, "failed to upload parquet file to object storage, retrying");
            tokio::time::sleep(Duration::from_secs(1)).await;
            retried = true;
        }

        if retried {
            info!(
                ?meta,
                "Succeeded uploading files to object storage on retry"
            );
        }

        Ok((parquet_meta, file_size))
    }

    /// Inputs for [`ParquetExec`].
    ///
    /// See [`ParquetExecInput`] for more information.
    ///
    /// [`ParquetExec`]: datafusion::physical_plan::file_format::ParquetExec
    pub fn parquet_exec_input(&self, path: &ParquetFilePath, file_size: usize) -> ParquetExecInput {
        ParquetExecInput {
            object_store_url: ObjectStoreUrl::parse(format!("iox://{}/", self.id))
                .expect("valid object store URL"),
            object_meta: ObjectMeta {
                location: path.object_store_path(),
                // we don't care about the "last modified" field
                last_modified: Default::default(),
                size: file_size,
            },
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        record_batch::RecordBatch,
    };
    use data_types::{CompactionLevel, NamespaceId, PartitionId, SequenceNumber, ShardId, TableId};
    use datafusion::common::DataFusionError;
    use datafusion_util::MemoryStream;
    use iox_time::Time;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_upload_metadata() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

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

        assert_roundtrip(batch.clone(), Projection::All, schema, batch).await;
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
        assert_roundtrip(batch, Projection::Some(&["d", "c"]), schema, expected_batch).await;
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
        assert_roundtrip(batch, Projection::Some(&["b", "c"]), schema, expected_batch).await;
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
        assert_roundtrip(file_batch, Projection::All, schema, schema_batch).await;
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
        assert_roundtrip(batch, Projection::Some(&["d", "c"]), schema, expected_batch).await;
    }

    #[tokio::test]
    async fn test_schema_check_fail_different_types() {
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let other_batch = RecordBatch::try_from_iter([("a", to_int_array(&[1]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Arrow error: External error: Execution error: Failed to map column projection for field a. Incompatible data types Int64 and Utf8",
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
            "Arrow error: Invalid argument error: Column 'a' is declared as non-nullable but contains null values",
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
            "Arrow error: Invalid argument error: Column 'b' is declared as non-nullable but contains null values",
        ).await;
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_mem() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        let meta = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();

        // Serialize & upload the record batches.
        let (_iox_md, file_size) = upload(&store, &meta, batch).await;

        // add metadata to reference schema
        let schema = Arc::new(
            schema
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
        );
        download(&store, &meta, Projection::All, schema, file_size)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_file() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

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
        let (_iox_md, file_size) = upload(&store, &meta, batch).await;

        download(&store, &meta, Projection::All, schema, file_size)
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
        assert_roundtrip(file_batch, Projection::All, schema, expected_batch).await;
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
        assert_roundtrip(file_batch, Projection::Some(&["a"]), schema, expected_batch).await;
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
            shard_id: ShardId::new(2),
            table_id: TableId::new(3),
            table_name: "platanos".into(),
            partition_id: PartitionId::new(4),
            partition_key: "potato".into(),
            max_sequence_number: SequenceNumber::new(11),
            compaction_level: CompactionLevel::FileNonOverlapped,
            sort_key: None,
            max_l0_created_at: Time::from_timestamp_nanos(42),
        }
    }

    async fn upload(
        store: &ParquetStorage,
        meta: &IoxMetadata,
        batch: RecordBatch,
    ) -> (IoxParquetMetaData, usize) {
        let stream = Box::pin(MemoryStream::new(vec![batch]));
        store
            .upload(stream, meta)
            .await
            .expect("should serialize and store sucessfully")
    }

    async fn download<'a>(
        store: &ParquetStorage,
        meta: &IoxMetadata,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        file_size: usize,
    ) -> Result<RecordBatch, DataFusionError> {
        let path: ParquetFilePath = meta.into();
        store
            .parquet_exec_input(&path, file_size)
            .read_to_batches(expected_schema, selection, &store.test_df_context())
            .await
            .map(|mut batches| {
                assert_eq!(batches.len(), 1);
                batches.remove(0)
            })
    }

    async fn assert_roundtrip(
        upload_batch: RecordBatch,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        // Serialize & upload the record batches.
        let meta = meta();
        let (_iox_md, file_size) = upload(&store, &meta, upload_batch).await;

        // And compare to the original input
        let actual_batch = download(&store, &meta, selection, expected_schema, file_size)
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

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        let meta = meta();
        let (_iox_md, file_size) = upload(&store, &meta, persisted_batch).await;

        let err = download(&store, &meta, Projection::All, expected_schema, file_size)
            .await
            .unwrap_err();

        // And compare to the original input
        assert_eq!(err.to_string(), msg);
    }
}
