//! Code that can convert between parquet files and line protocol

use datafusion::{
    arrow::datatypes::SchemaRef as ArrowSchemaRef,
    datasource::{
        file_format::{parquet::ParquetFormat, FileFormat},
        listing::PartitionedFile,
        object_store::ObjectStoreUrl,
    },
    execution::{
        context::{SessionState, TaskContext},
        runtime_env::RuntimeEnv,
    },
    physical_plan::{
        execute_stream,
        file_format::{FileScanConfig, ParquetExec},
        SendableRecordBatchStream, Statistics,
    },
    prelude::SessionContext,
};
use datafusion_util::config::iox_session_config;
use futures::{stream::BoxStream, StreamExt};
use object_store::{
    local::LocalFileSystem, path::Path as ObjectStorePath, ObjectMeta, ObjectStore,
};
use parquet_file::metadata::{IoxMetadata, METADATA_KEY};
use schema::Schema;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

mod batch;
use batch::convert_to_lines;
pub type Result<T = (), E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid path: {:?}: {}", path, source))]
    Path {
        path: PathBuf,
        source: object_store::path::Error,
    },

    #[snafu(display("Error listing: {:?}: {}", object_store_path, source))]
    ObjectStorePath {
        object_store_path: ObjectStorePath,
        source: object_store::Error,
    },

    #[snafu(display(
        "Can not find IOx metadata in parquet metadata. Could not find {}",
        METADATA_KEY
    ))]
    MissingMetadata {},

    #[snafu(display("Error reading IOx metadata: {}", source))]
    Metadata {
        source: parquet_file::metadata::Error,
    },

    #[snafu(display("Error inferring IOx schema: {}", source))]
    InferringSchema {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Error reading batch: {}", source))]
    ReadingBatch {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("Error reading IOx schema: {}", source))]
    Schema { source: schema::Error },

    #[snafu(display("Error in processing task: {}", source))]
    Task { source: tokio::task::JoinError },

    #[snafu(display("Error converting: {}", message))]
    Conversion { message: String },

    #[snafu(display("Error executing: {}", source))]
    ExecutingStream {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display("IO Error: {}", source))]
    IO { source: std::io::Error },
}

/// Converts a parquet file that was written by IOx from the local
/// file system path specified a stream of line protocol bytes
///
/// Each returned `Vec<u8>` is guarnteed to have complete line
/// protocol (aka lines are not split across the buffers)
pub async fn convert_file<P>(path: P) -> Result<BoxStream<'static, Result<Vec<u8>>>>
where
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let object_store_path =
        ObjectStorePath::from_filesystem_path(path).context(PathSnafu { path })?;

    // Fire up a parquet reader, read the batches, and then convert
    // them asynchronously in parallel

    let object_store = Arc::new(LocalFileSystem::new()) as Arc<dyn ObjectStore>;
    let object_store_url = ObjectStoreUrl::local_filesystem();

    let object_meta = object_store
        .head(&object_store_path)
        .await
        .context(ObjectStorePathSnafu { object_store_path })?;

    let reader = ParquetFileReader::try_new(object_store, object_store_url, object_meta).await?;

    // Determines the measurement name from the IOx metadata
    let schema = reader.schema();
    let encoded_meta = schema
        .metadata
        .get(METADATA_KEY)
        .context(MissingMetadataSnafu)?;

    let iox_meta = IoxMetadata::from_base64(encoded_meta.as_bytes()).context(MetadataSnafu)?;

    // Attempt to extract the IOx schema from the schema stored in the
    // parquet file. This schema is where information such as what
    // columns are tags and fields is stored
    let iox_schema: Schema = schema.try_into().context(SchemaSnafu)?;

    let iox_schema = Arc::new(iox_schema);

    let measurement_name = iox_meta.table_name;

    // now convert the record batches to line protocol, in parallel
    let stream = reader
        .read()
        .await?
        .map(move |batch| {
            let iox_schema = Arc::clone(&iox_schema);
            let measurement_name = Arc::clone(&measurement_name);
            tokio::task::spawn(async move {
                batch.context(ReadingBatchSnafu).and_then(|batch| {
                    convert_to_lines(&measurement_name, &iox_schema, &batch)
                        .map_err(|message| Error::Conversion { message })
                })
            })
        })
        // run some number of futures in parallel
        .buffered(num_cpus::get())
        // unwrap task result (check for panics)
        .map(|result| match result {
            Ok(res) => res,
            Err(source) => Err(Error::Task { source }),
        })
        .boxed();

    Ok(stream)
}

/// Handles the details of interacting with parquet libraries /
/// readers. Tries not to have any IOx specific logic
pub struct ParquetFileReader {
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    /// Name / path information of the object to read
    object_meta: ObjectMeta,

    /// Parquet file metadata
    schema: ArrowSchemaRef,

    /// DataFusion configuration, such as the target batchsize, etc
    session_ctx: SessionContext,
}

impl ParquetFileReader {
    /// Find and open the specified parquet file, and read its metadata / schema
    pub async fn try_new(
        object_store: Arc<dyn ObjectStore>,
        object_store_url: ObjectStoreUrl,
        object_meta: ObjectMeta,
    ) -> Result<Self, Error> {
        let runtime = Arc::new(RuntimeEnv::default());
        let session_config = iox_session_config();
        let session_state = SessionState::with_config_rt(session_config, runtime);

        // Keep metadata so we can find the measurement name
        let format = ParquetFormat::new().with_skip_metadata(Some(false));

        // Use datafusion parquet reader to read the metadata from the
        // file.
        let schema = format
            .infer_schema(&session_state, &object_store, &[object_meta.clone()])
            .await
            .context(InferringSchemaSnafu)?;

        let session_ctx = SessionContext::with_state(session_state);

        Ok(Self {
            object_store,
            object_store_url,
            object_meta,
            schema,
            session_ctx,
        })
    }

    // retrieves the Arrow schema for this file
    pub fn schema(&self) -> ArrowSchemaRef {
        Arc::clone(&self.schema)
    }

    /// read the parquet file as a stream
    pub async fn read(&self) -> Result<SendableRecordBatchStream, Error> {
        let base_config = FileScanConfig {
            object_store_url: self.object_store_url.clone(),
            file_schema: self.schema(),
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
            output_ordering: None,
            infinite_source: false,
        };

        // set up enough datafusion context to do the real read session
        let predicate = None;
        let metadata_size_hint = None;
        let exec = ParquetExec::new(base_config, predicate, metadata_size_hint);

        let object_store = Arc::clone(&self.object_store);
        let task_ctx = Arc::new(TaskContext::from(&self.session_ctx));
        task_ctx
            .runtime_env()
            .register_object_store("iox", "iox", object_store);

        execute_stream(Arc::new(exec), task_ctx).context(ExecutingStreamSnafu)
    }
}
