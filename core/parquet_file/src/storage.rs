//! This module is responsible for writing the given data to the specified
//! object store and reading it back.

use crate::{
    ParquetFilePath,
    metadata::{IoxMetadata, IoxParquetMetaData},
    serialize::{self, CodecError, ParallelParquetWriterOptions},
};
use arrow::{
    datatypes::{Field, SchemaRef},
    record_batch::RecordBatch,
};
use bytes::Bytes;
use data_types::PartitionHashId;
use datafusion::{
    catalog::memory::DataSourceExec,
    datasource::{
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
        physical_plan::{FileScanConfigBuilder, ParquetSource},
    },
    error::DataFusionError,
    execution::runtime_env::RuntimeEnv,
    physical_plan::{ExecutionPlan, SendableRecordBatchStream},
    prelude::SessionContext,
};
use datafusion_util::config::{
    iox_session_config, register_iox_object_store, table_parquet_options,
};
use object_store::{DynObjectStore, ObjectMeta, PutPayload};
use schema::Projection;
use std::{
    fmt::Display,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tracing::*;

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

    /// Error in configuration.
    #[error("failed to properly configure parquet upload: {0}")]
    Config(#[from] DataFusionError),
}

impl From<UploadError> for DataFusionError {
    fn from(value: UploadError) -> Self {
        match value {
            UploadError::Serialise(e) => {
                Self::Context(String::from("serialize"), Box::new(e.into()))
            }
            UploadError::Metadata(e) => Self::External(Box::new(e)),
            UploadError::Upload(e) => Self::ObjectStore(Box::new(e)),
            UploadError::Config(e) => Self::External(e.into()),
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

/// Inputs required to build a [`DataSourceExec`] for one or multiple files.
///
/// The files shall be grouped by [`object_store_url`](Self::object_store_url). For each each object store, you shall
/// create one [`DataSourceExec`] and put each file into its own "file group".
///
/// [`DataSourceExec`]: datafusion::datasource::memory::DataSourceExec
#[derive(Debug, Clone)]
pub struct DataSourceExecInput {
    /// Store where the file is located.
    pub object_store_url: ObjectStoreUrl,

    /// The actual store referenced by [`object_store_url`](Self::object_store_url).
    pub object_store: Arc<DynObjectStore>,

    /// Object metadata.
    pub object_meta: ObjectMeta,
}

impl DataSourceExecInput {
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
        let schema = Arc::new(projection.project_schema(&schema).as_ref().clone());
        let file_scan_config = FileScanConfigBuilder::new(
            self.object_store_url.clone(),
            schema,
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file(PartitionedFile::from(self.object_meta.clone()))
        .build();
        let exec = DataSourceExec::from_data_source(file_scan_config);
        let exec_schema = exec.schema();
        datafusion::physical_plan::collect(exec, session_ctx.task_ctx())
            .await
            .inspect(|batches| {
                for batch in batches {
                    assert_eq!(batch.schema(), exec_schema);
                }
            })
    }
}

/// Inputs required for write upload.
///
/// Eventually these may be consumed separately when we
/// separate encoding from upload.
#[derive(Debug, Clone)]
pub struct ParquetUploadInput {
    // Store where the file is located.
    object_store_url: ObjectStoreUrl,

    // Path within the store.
    object_store_path: object_store::path::Path,

    /// The actual store referenced by [`object_store_url`](Self::object_store_url).
    pub object_store: Arc<DynObjectStore>,
}

impl ParquetUploadInput {
    fn try_new(
        partition_id: &PartitionHashId,
        meta: &IoxMetadata,
        id: &StorageId,
        object_store: Arc<DynObjectStore>,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            object_store_url: ObjectStoreUrl::parse(format!("iox://{id}/"))?,
            object_store_path: ParquetFilePath::from((partition_id, meta)).object_store_path(),
            object_store,
        })
    }

    pub(crate) fn path(&self) -> &object_store::path::Path {
        &self.object_store_path
    }

    pub(crate) fn object_store_url(&self) -> &ObjectStoreUrl {
        &self.object_store_url
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

    /// Parallelized parquet write settings.
    parquet_write_parallelization_settings: ParallelParquetWriterOptions,
}

impl Display for ParquetStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParquetStorage(id={:?}, object_store={}",
            self.id, self.object_store
        )
    }
}

impl ParquetStorage {
    /// Initialise a new [`ParquetStorage`] using `object_store` as the
    /// persistence layer.
    pub fn new(object_store: Arc<DynObjectStore>, id: StorageId) -> Self {
        Self {
            object_store,
            id,
            parquet_write_parallelization_settings: Default::default(),
        }
    }

    /// Provide settings for parallelized writes. Settings determine the
    /// amount of parallelization per row group and per column.
    ///
    /// # Panics
    ///
    /// This will panic if an invalid usize (not > 0) is used.
    pub fn with_parallel_write_settings(
        self,
        num_row_group_writers: usize,
        num_column_writers_across_row_groups: usize,
    ) -> Self {
        Self {
            parquet_write_parallelization_settings: ParallelParquetWriterOptions::new(
                NonZeroUsize::new(num_row_group_writers)
                    .expect("num_row_groups_in_parallel should be above zero"),
                NonZeroUsize::new(num_column_writers_across_row_groups)
                    .expect("num_columns_in_parallel should be above zero"),
            ),
            ..self
        }
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
        let session_ctx = SessionContext::new_with_config(iox_session_config());
        register_iox_object_store(session_ctx.runtime_env(), self.id, object_store);
        session_ctx
    }

    /// Push `batches`, a stream of [`RecordBatch`] instances, to object
    /// storage.
    ///
    /// Any buffering needed is registered with the pool provided by the [`RuntimeEnv`].
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
        partition_id: &PartitionHashId,
        meta: &IoxMetadata,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<(IoxParquetMetaData, u64), UploadError> {
        let start = Instant::now();

        // Stream the record batches into a parquet file.
        //
        // It would be nice to stream the encoded parquet to disk for this and
        // eliminate the buffering in memory, but the lack of a streaming object
        // store put negates any benefit of spilling to disk.
        //
        // This is not a huge concern, as the resulting parquet files are
        // currently smallish on average.
        let (data, parquet_file_meta) =
            serialize::to_parquet_bytes(batches, meta, Arc::clone(&runtime.memory_pool)).await?;
        let num_rows = parquet_file_meta.file_metadata().num_rows();

        // Read the IOx-specific parquet metadata from the file metadata
        let parquet_meta =
            IoxParquetMetaData::try_from(parquet_file_meta).map_err(UploadError::Metadata)?;
        trace!(
            ?parquet_meta,
            "IoxParquetMetaData converted from Row Group Metadata (aka FileMetaData)"
        );

        // Derive the correct object store path from the metadata.
        let path = ParquetFilePath::from((partition_id, meta)).object_store_path();

        let file_size = data.len() as u64;
        let data = Bytes::from(data);
        let playload = PutPayload::from_bytes(data);

        debug!(
            file_size,
            object_store_id=?meta.object_store_id,
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
        while let Err(e) = self.object_store.put(&path, playload.clone()).await {
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

        debug!(
            %num_rows,
            %file_size,
            %path,
            // includes the time to run the datafusion plan (that is the batches) & upload
            total_time_to_create_and_upload_parquet=?(Instant::now() - start),
            "Created and uploaded parquet file"
        );

        Ok((parquet_meta, file_size))
    }

    /// Push `batches`, a stream of [`RecordBatch`] instances, to object
    /// storage.
    ///
    /// The [`RuntimeEnv`] is utilized if parallelized writes are enabled.
    pub async fn parallel_upload(
        &self,
        batches: SendableRecordBatchStream,
        partition_id: &PartitionHashId,
        meta: &IoxMetadata,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<(IoxParquetMetaData, u64), UploadError> {
        let upload_input = ParquetUploadInput::try_new(
            partition_id,
            meta,
            &self.id(),
            Arc::clone(&self.object_store),
        )?;

        let start = Instant::now();

        let parquet_file_meta = serialize::to_parquet_upload(
            batches,
            meta,
            upload_input.clone(),
            runtime,
            self.parquet_write_parallelization_settings,
        )
        .await
        .map_err(|e| {
            warn!(error=%e, ?meta, "failed to parallel-upload parquet file to object storage");
            e
        })?;
        let num_rows = parquet_file_meta.file_metadata().num_rows();

        // Read the IOx-specific parquet metadata from the file metadata
        let parquet_meta =
            IoxParquetMetaData::try_from(parquet_file_meta).map_err(UploadError::Metadata)?;
        trace!(
            ?parquet_meta,
            "IoxParquetMetaData converted from Row Group Metadata (aka FileMetaData)"
        );

        let file_size = self.object_store.head(upload_input.path()).await?.size;

        debug!(
            %num_rows,
            %file_size,
            path=?upload_input.path(),
            // includes the time to run the datafusion plan (that is the batches) & upload
            total_time_to_create_and_upload_parquet=?(Instant::now() - start),
            "Created and uploaded parquet file (parallelized)"
        );

        Ok((parquet_meta, file_size))
    }

    /// Inputs for [`DataSourceExec`].
    ///
    /// See [`DataSourceExecInput`] for more information.
    ///
    /// [`DataSourceExec`]: datafusion::datasource::memory::DataSourceExec
    pub fn data_source_exec_input(
        &self,
        path: &ParquetFilePath,
        file_size: u64,
    ) -> DataSourceExecInput {
        DataSourceExecInput {
            object_store_url: ObjectStoreUrl::parse(format!("iox://{}/", self.id))
                .expect("valid object store URL"),
            object_store: Arc::clone(&self.object_store),
            object_meta: ObjectMeta {
                location: path.object_store_path(),
                // we don't care about the "last modified" field
                last_modified: Default::default(),
                size: file_size,
                e_tag: None,
                version: None,
            },
        }
    }
}

/// Error during projecting parquet file data to an expected schema.
#[derive(Debug, Error)]
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
        datatypes::{DataType, Schema},
        record_batch::RecordBatch,
    };
    use data_types::{
        CompactionLevel, MaxL0CreatedAt, NamespaceId, ObjectStoreId, PartitionHashId, TableId,
        Timestamp,
    };
    use datafusion::common::{DataFusionError, ScalarValue};
    use datafusion_util::{MemoryStream, config::BATCH_SIZE, unbounded_memory_pool};
    use iox_time::Time;
    use schema::SchemaBuilder;
    use tokio::runtime::{Handle, RuntimeFlavor};

    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    /// Test: perform and assert upload of metadata
    async fn test_upload_metadata(store: ParquetStorage, upload_type: UploadType) {
        let (transition_partition_id, meta) = meta();
        let mut batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"; 8192 * 2])),
            ("b", to_int_array(&[1; 8192 * 2])),
            ("c", to_string_array(&["foo"; 8192 * 2])),
            ("d", to_int_array(&[2; 8192 * 2])),
        ])
        .unwrap();
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 8192 * 2);

        // With schema crate `v3`, columns lacking metadata are accepted as
        // Arrow-native columns, so this assertion only holds without `v3`.
        #[cfg(not(feature = "v3"))]
        {
            use schema::{Error::InvalidInfluxColumnType, Schema as IoxSchema};

            let iox_schema = IoxSchema::try_from(batch.schema());
            assert!(
                matches!(iox_schema, Err(InvalidInfluxColumnType { .. })),
                "should not have iox schema, yet"
            );
        }

        // build & use iox_schema
        let iox_schema = SchemaBuilder::new()
            .influx_field("a", schema::InfluxFieldType::String)
            .influx_field("b", schema::InfluxFieldType::Integer)
            .influx_field("c", schema::InfluxFieldType::String)
            .influx_field("d", schema::InfluxFieldType::Integer)
            .build()
            .expect("valid iox schema");
        batch = batch
            .with_schema(iox_schema.clone().into())
            .expect("should update batch with iox schema");

        // Serialize & upload the record batches.
        let (encoded_file_meta, _file_size) = upload(
            &store,
            &transition_partition_id,
            &meta,
            batch.clone(),
            upload_type,
        )
        .await;

        // Extract the various bits of metadata.
        let file_meta = encoded_file_meta
            .decode()
            .expect("should decode parquet metadata");

        let got_iox_meta = file_meta
            .read_iox_metadata_new()
            .expect("should read IOx metadata from parquet meta");

        // Ensure the metadata in the file decodes to the same IOx metadata we
        // provided when uploading.
        assert_eq!(got_iox_meta, meta);

        // Ensure gets the IOx Schema from the parquet metadata
        assert!(
            matches!(file_meta.read_schema(), Ok(encoded_schema) if encoded_schema == iox_schema),
            "should have the parquet encoded schema match the schema provided to upload"
        );
    }

    #[tokio::test]
    async fn test_normal_upload_metadata() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        test_upload_metadata(store, UploadType::SingleThread).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 20)]
    async fn test_parallelized_upload_metadata() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        // test with parallel column writers
        let store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"))
            .with_parallel_write_settings(1, 6); // parallelized only column writing
        test_upload_metadata(store, UploadType::MultiThread).await;

        // test with parallel rowgroup writers
        let store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"))
            .with_parallel_write_settings(2, 1); // parallelized only rowgroup writing
        test_upload_metadata(store, UploadType::MultiThread).await;

        // test with both parallelized column and rowgroup writers
        let store = ParquetStorage::new(object_store, StorageId::from("iox"))
            .with_parallel_write_settings(2, 6); // parallelized both rowgroup & column writing
        test_upload_metadata(store, UploadType::MultiThread).await;
    }

    /// Test: perform and assert `upload()` or `parallel_upload()` while counting active tasks.
    async fn run_upload_and_count_max_active_tasks(
        store: ParquetStorage,
        batch: RecordBatch,
        upload_type: UploadType,
    ) -> Arc<AtomicUsize> {
        // prepare schema
        let schema = batch.schema();

        // background watcher, counting the num of active tasks
        // this uses a tokio stable API in RuntimeMetrics
        let counter = Arc::new(AtomicUsize::new(0));
        let captured_counter = Arc::clone(&counter);
        let counter_handle = Handle::current().spawn(async move {
            loop {
                let num_active = Handle::current().metrics().num_alive_tasks(); // tokio stable api
                captured_counter.fetch_max(num_active, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_nanos(1)).await;
            }
        });

        // Test: start upload on current thread
        assert_roundtrip_with_existing_store(
            store,
            batch.clone(),
            Projection::All,
            schema,
            batch,
            upload_type,
        )
        .await;
        counter_handle.abort();

        // provide counter for test assertions
        counter
    }

    /// Verify that using the ArrowWriter (our non-parallel writing
    /// implementation) uses a single thread, even when multiple threads
    /// are available in the tokio threadpool.
    ///
    /// The number of tasks is a proxy for the number of threads,
    /// since tokio distributes tasks across its threadpool.
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_normal_upload_is_single_threaded() {
        // confirm test runtime is multi-threaded
        assert_eq!(
            RuntimeFlavor::MultiThread,
            Handle::current().runtime_flavor()
        );

        // store with single threaded `upload()` -- vs `parallel_upload()`
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        // Test: single task, even when many columns
        let counter = run_upload_and_count_max_active_tasks(
            store.clone(),
            batch_with_many_cols_and_1_row(),
            UploadType::SingleThread,
        )
        .await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "should be only 1 tasks, instead found {:?}",
            counter.load(Ordering::SeqCst)
        );

        // Test: single task, even when many rows
        let counter = run_upload_and_count_max_active_tasks(
            store,
            batch_with_many_rows_and_1_col(),
            UploadType::SingleThread,
        )
        .await;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "should be only 1 tasks, instead found {:?}",
            counter.load(Ordering::SeqCst)
        );
    }

    /// Verify using datafusion writer is using more than a single task to
    /// encode parquet files. The number of tasks is used as a proxy for
    /// the speed of writing parquet files.
    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn test_parallel_parquet_sink_uses_multiple_tasks() {
        use tokio::runtime::{Handle, RuntimeFlavor};

        // confirm test runtime is multi-threaded
        assert_eq!(
            RuntimeFlavor::MultiThread,
            Handle::current().runtime_flavor()
        );

        // store with multi-threaded upload
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(object_store, StorageId::from("iox"))
            .with_parallel_write_settings(1, 1); // only 1 for each

        // Test: we should be using multiple tasks
        // w/ the many-cols-in-1-row
        let counter = run_upload_and_count_max_active_tasks(
            store.clone(),
            batch_with_many_cols_and_1_row(),
            UploadType::MultiThread,
        )
        .await;
        assert!(
            counter.load(Ordering::SeqCst) > 1,
            "should have >1 task, instead found {:?}",
            counter.load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_simple_roundtrip() {
        let batch = RecordBatch::try_from_iter([
            ("a", to_string_array(&["value"])),
            ("b", to_int_array(&[1])),
            ("c", to_string_array(&["foo"])),
            ("d", to_int_array(&[2])),
        ])
        .unwrap();
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 1);

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
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 1);
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
        assert_eq!(batch.num_columns(), 2);
        assert_eq!(batch.num_rows(), 1);
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
        assert_eq!(file_batch.num_columns(), 2);
        assert_eq!(file_batch.num_rows(), 1);

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
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.num_rows(), 1);

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
        let batch = RecordBatch::try_from_iter([("a", to_interval_array(&[123456]))]).unwrap();
        let other_batch = RecordBatch::try_from_iter([("a", to_int_array(&[123456]))]).unwrap();
        let schema = batch.schema();
        assert_schema_check_fail(
            other_batch,
            schema,
            "Error during planning: Cannot cast file schema field a of type Int64 to table schema field of type Interval(MonthDayNano)",
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

    /// This test, and the following test [`test_schema_check_ignore_additional_metadata_in_file`]
    /// demonstrate how the test helpers work. Specifically, during schema-check the test helpers
    /// ignore added metadata.
    ///
    /// | add metadata to expected_schema | add metadata to uploaded batches | upload_batch.schema == download_batch.schema |
    /// | ------------------------------- | -------------------------------- | -------------------------------------------- |
    /// |             y                   |              n                   |                     n                        |
    /// |             n                   |              y                   |                     n                        |
    /// |             n                   |              n                   |                     y                        |
    /// |             y                   |              y                   |                     y                        |
    ///
    /// In the test `test_schema_arrow_metadata_preserved` the metadata is added to both the uploaded batch & expected_schema.
    /// As a result, for that test the upload_batch.schema == download_batch.schema.
    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_mem() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        let (partition_id, meta) = meta();
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();
        let schema = batch.schema();

        // Serialize & upload the record batches.
        let (_iox_md, file_size) = upload(
            &store,
            &partition_id,
            &meta,
            batch.clone(),
            UploadType::SingleThread,
        )
        .await;

        // add metadata to expected_schema
        let expected_schema = Arc::new(
            schema
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(String::from("foo"), String::from("bar"))])),
        );

        // Test: download() test-helpers
        // can perform download with expected_schema having extra metadata
        let downloaded_batch = download(
            &store,
            &partition_id,
            &meta,
            Projection::All,
            expected_schema,
            file_size,
        )
        .await
        .unwrap(); // does not error

        // But the downloaded batch.schema will not be equal.
        assert_ne!(batch.schema(), downloaded_batch.schema());
        assert_eq!(batch.columns(), downloaded_batch.columns());
    }

    /// Refer to the description on the above test [`test_schema_check_ignore_additional_metadata_in_mem`].
    #[tokio::test]
    async fn test_schema_check_ignore_additional_metadata_in_file() {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        let (partition_id, meta) = meta();
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
        let (_iox_md, file_size) = upload(
            &store,
            &partition_id,
            &meta,
            batch.clone(),
            UploadType::SingleThread,
        )
        .await;

        // Test: download() test-helpers
        // can perform download with batch.schema having extra metadata
        let downloaded_batch = download(
            &store,
            &partition_id,
            &meta,
            Projection::All,
            schema,
            file_size,
        )
        .await
        .unwrap(); // does not error

        // But the downloaded batch.schema will not be equal.
        assert_ne!(batch.schema(), downloaded_batch.schema());
        assert_eq!(batch.columns(), downloaded_batch.columns());
    }

    #[tokio::test]
    async fn test_schema_arrow_metadata_preserved() {
        // Setup
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(object_store, StorageId::from("iox"));
        let (partition_id, meta) = meta();

        // Create a basic record batch:
        let batch = RecordBatch::try_from_iter([("a", to_string_array(&["value"]))]).unwrap();

        // Extend the created record batch with schema metadata:
        let schema = Arc::new(
            batch
                .schema()
                .as_ref()
                .clone()
                .with_metadata(HashMap::from([(
                    String::from("iox::measurement::name"),
                    String::from("foo"),
                )])),
        );

        // Recreate the batch with the added metadata in the schema:
        let upload_batch =
            RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec()).unwrap();

        // Serialize & upload the record batches
        let (_iox_md, file_size) = upload(
            &store,
            &partition_id,
            &meta,
            upload_batch.clone(),
            UploadType::SingleThread,
        )
        .await;

        // Test: download() test-helpers
        // can perform download with BOTH expected_schema & batch.schema having extra metadata
        let downloaded_batch = download(
            &store,
            &partition_id,
            &meta,
            Projection::All,
            schema,
            file_size,
        )
        .await
        .unwrap();

        // Check the uploaded and downloaded batches, including their schema, are equal.
        assert_eq!(upload_batch, downloaded_batch);
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

    fn batch_with_many_cols_and_1_row() -> RecordBatch {
        let mut batch = Vec::with_capacity(10_000);
        for field in 0..10_000 {
            batch.push((field.to_string(), to_string_array(&["value"])));
        }
        let batch = RecordBatch::try_from_iter(batch).unwrap();

        assert_eq!(batch.num_columns(), 10_000);
        assert_eq!(batch.num_rows(), 1);
        batch
    }

    fn batch_with_many_rows_and_1_col() -> RecordBatch {
        let iox_session_batch_size = BATCH_SIZE;

        let col_1 = to_int_array(&[2; 1024 * 8]);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(schema, vec![col_1]).expect("created new record batch");

        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), iox_session_batch_size);
        batch
    }

    fn to_string_array(strs: &[&str]) -> ArrayRef {
        let array: StringArray = strs.iter().map(|s| Some(*s)).collect();
        Arc::new(array)
    }

    /// Returns an IntervalMonthDayNano array with the given nanosecond values
    fn to_interval_array(vals: &[i64]) -> ArrayRef {
        ScalarValue::iter_to_array(vals.iter().map(|v| ScalarValue::new_interval_mdn(0, 0, *v)))
            .unwrap()
    }

    fn to_int_array(vals: &[i64]) -> ArrayRef {
        let array: Int64Array = vals.iter().map(|v| Some(*v)).collect();
        Arc::new(array)
    }

    fn meta() -> (PartitionHashId, IoxMetadata) {
        let table_id = TableId::new(3);
        let partition_key = "potato".into();
        (
            PartitionHashId::new(table_id, &partition_key),
            IoxMetadata {
                object_store_id: ObjectStoreId::new(),
                creation_timestamp: Time::from_timestamp_nanos(42),
                namespace_id: NamespaceId::new(1),
                namespace_name: "bananas".into(),
                table_id,
                table_name: "platanos".into(),
                partition_key,
                compaction_level: CompactionLevel::FileNonOverlapped,
                sort_key: None,
                max_l0_created_at: MaxL0CreatedAt::Computed(Timestamp::new(42)),
            },
        )
    }

    enum UploadType {
        SingleThread,
        MultiThread,
    }

    /// Perform (not assert): upload based upon [`UploadType`]
    async fn upload(
        store: &ParquetStorage,
        partition_id: &PartitionHashId,
        meta: &IoxMetadata,
        batch: RecordBatch,
        upload_type: UploadType,
    ) -> (IoxParquetMetaData, u64) {
        let stream = Box::pin(MemoryStream::new(vec![batch]));
        let runtime = Arc::new(RuntimeEnv {
            memory_pool: unbounded_memory_pool(),
            ..Default::default()
        });
        datafusion_util::config::register_iox_object_store(
            &runtime,
            store.id(),
            Arc::clone(store.object_store()),
        );

        let upload_res = match upload_type {
            UploadType::SingleThread => store.upload(stream, partition_id, meta, runtime).await,
            UploadType::MultiThread => {
                store
                    .parallel_upload(stream, partition_id, meta, runtime)
                    .await
            }
        };

        upload_res.expect("should serialize and store sucessfully")
    }

    /// Perform (not assert): download
    async fn download(
        store: &ParquetStorage,
        partition_id: &PartitionHashId,
        meta: &IoxMetadata,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        file_size: u64,
    ) -> Result<RecordBatch, DataFusionError> {
        let path: ParquetFilePath = (partition_id, meta).into();
        store
            .data_source_exec_input(&path, file_size)
            .read_to_batches(expected_schema, selection, &store.test_df_context())
            .await
            .map(|mut batches| {
                assert_eq!(batches.len(), 1);
                batches.remove(0)
            })
    }

    /// Assert: roundtrip for both single- and multi- threaded.
    async fn assert_roundtrip(
        upload_batch: RecordBatch,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        // test the single threaded path
        assert_roundtrip_single_thread(
            upload_batch.clone(),
            selection,
            Arc::clone(&expected_schema),
            expected_batch.clone(),
        )
        .await;
        // test the multi threaded path
        assert_roundtrip_multi_thread(upload_batch, selection, expected_schema, expected_batch)
            .await;
    }

    /// Assert: roundtrip for single-threaded.
    async fn assert_roundtrip_single_thread(
        upload_batch: RecordBatch,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());
        let store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"));

        assert_roundtrip_with_existing_store(
            store,
            upload_batch,
            selection,
            expected_schema,
            expected_batch,
            UploadType::SingleThread,
        )
        .await;
    }

    /// Assert: roundtrip for multi-threaded.
    async fn assert_roundtrip_multi_thread(
        upload_batch: RecordBatch,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        // test with parallel column writer
        let parallel_store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"))
            .with_parallel_write_settings(1, 6); // parallelized only column writing
        assert_roundtrip_with_existing_store(
            parallel_store,
            upload_batch.clone(),
            selection,
            Arc::clone(&expected_schema),
            expected_batch.clone(),
            UploadType::MultiThread,
        )
        .await;

        // test with parallel rowgroup writers
        let parallel_store = ParquetStorage::new(Arc::clone(&object_store), StorageId::from("iox"))
            .with_parallel_write_settings(6, 1); // parallelized only rowgroup writing
        assert_roundtrip_with_existing_store(
            parallel_store,
            upload_batch.clone(),
            selection,
            Arc::clone(&expected_schema),
            expected_batch.clone(),
            UploadType::MultiThread,
        )
        .await;

        // test with both parallelized column and rowgroup writers
        let parallel_store = ParquetStorage::new(object_store, StorageId::from("iox"))
            .with_parallel_write_settings(3, 3);
        assert_roundtrip_with_existing_store(
            parallel_store,
            upload_batch,
            selection,
            expected_schema,
            expected_batch,
            UploadType::MultiThread,
        )
        .await;
    }

    /// Assert: roundtrip using existing store, based upon [`UploadType`]
    async fn assert_roundtrip_with_existing_store(
        store: ParquetStorage,
        upload_batch: RecordBatch,
        selection: Projection<'_>,
        expected_schema: SchemaRef,
        expected_batch: RecordBatch,
        upload_type: UploadType,
    ) {
        // Serialize & upload the record batches.
        let (partition_id, meta) = meta();
        let (_iox_md, file_size) = upload(
            &store,
            &partition_id,
            &meta,
            upload_batch.clone(),
            upload_type,
        )
        .await;

        // And compare to the original input
        let actual_batch = download(
            &store,
            &partition_id,
            &meta,
            selection,
            Arc::clone(&expected_schema),
            file_size,
        )
        .await
        .unwrap();
        assert_eq!(actual_batch, expected_batch);
    }

    /// Assert: schema check failure for both single- and multi- threaded.
    async fn assert_schema_check_fail(
        persisted_batch: RecordBatch,
        expected_schema: SchemaRef,
        msg: &str,
    ) {
        let object_store: Arc<DynObjectStore> = Arc::new(object_store::memory::InMemory::default());

        let store = ParquetStorage::new(object_store, StorageId::from("iox"));

        let (partition_id, meta) = meta();

        // single-threaded
        let (_iox_md, file_size) = upload(
            &store,
            &partition_id,
            &meta,
            persisted_batch.clone(),
            UploadType::SingleThread,
        )
        .await;
        let err = download(
            &store,
            &partition_id,
            &meta,
            Projection::All,
            Arc::clone(&expected_schema),
            file_size,
        )
        .await
        .unwrap_err();
        // And compare to the original input
        assert_eq!(err.to_string(), msg);

        // multi-threaded
        let (_iox_md, file_size) = upload(
            &store,
            &partition_id,
            &meta,
            persisted_batch,
            UploadType::MultiThread,
        )
        .await;
        let err = download(
            &store,
            &partition_id,
            &meta,
            Projection::All,
            expected_schema,
            file_size,
        )
        .await
        .unwrap_err();
        // And compare to the original input
        assert_eq!(err.to_string(), msg);
    }
}
