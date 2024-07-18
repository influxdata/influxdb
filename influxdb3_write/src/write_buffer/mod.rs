//! Implementation of an in-memory buffer for writes that persists data into a wal if it is configured.

pub(crate) mod buffer_segment;
mod flusher;
mod loader;
pub mod persisted_files;
mod persister;
mod segment_state;
mod table_buffer;
pub(crate) mod validator;

use crate::cache::ParquetCache;
use crate::catalog::{Catalog, DatabaseSchema, LastCacheDefinition};
use crate::chunk::ParquetChunk;
use crate::last_cache::{self, CreateCacheArguments, LastCacheProvider};
use crate::persister::PersisterImpl;
use crate::write_buffer::flusher::WriteBufferFlusher;
use crate::write_buffer::loader::load_starting_state;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::persister::{
    run_buffer_segment_persist_and_cleanup, run_buffer_size_check_and_persist,
};
use crate::write_buffer::segment_state::SegmentState;
use crate::write_buffer::validator::WriteValidator;
use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile, Persister,
    Precision, SegmentDuration, SequenceNumber, Wal, WalOp, WriteBuffer, WriteLineError,
};
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, ColumnType, NamespaceName, NamespaceNameError};
use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use influxdb_line_protocol::v3::SeriesValue;
use influxdb_line_protocol::FieldValue;
use iox_query::chunk_statistics::{create_chunk_statistics, NoColumnRanges};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore};
use observability_deps::tracing::{debug, error};
use parking_lot::{Mutex, RwLock};
use parquet_file::storage::ParquetExecInput;
use schema::Schema;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Error)]
pub enum Error {
    #[error("parsing for line protocol failed")]
    ParseError(WriteLineError),

    #[error("column type mismatch for column {name}: existing: {existing:?}, new: {new:?}")]
    ColumnTypeMismatch {
        name: String,
        existing: ColumnType,
        new: ColumnType,
    },

    #[error("catalog update erorr {0}")]
    CatalogUpdateError(#[from] crate::catalog::Error),

    #[error("error from wal: {0}")]
    WalError(#[from] crate::wal::Error),

    #[error("error from buffer segment: {0}")]
    BufferSegmentError(String),

    #[error("error from persister: {0}")]
    PersisterError(#[from] crate::persister::Error),

    #[error("corrupt load state: {0}")]
    CorruptLoadState(String),

    #[error("database name error: {0}")]
    DatabaseNameError(#[from] NamespaceNameError),

    #[error("walop in file {0} contained data for more than one segment, which is invalid")]
    WalOpForMultipleSegments(String),

    #[error("error from table buffer: {0}")]
    TableBufferError(#[from] table_buffer::Error),

    #[error("error in last cache: {0}")]
    LastCacheError(#[from] last_cache::Error),

    #[error("tried accessing database and table that do not exist")]
    DbDoesNotExist,

    #[error("tried accessing database and table that do not exist")]
    TableDoesNotExist,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteRequest<'a> {
    pub db_name: NamespaceName<'static>,
    pub line_protocol: &'a str,
    pub default_time: u64,
}

#[derive(Debug)]
pub struct WriteBufferImpl<W, T> {
    catalog: Arc<Catalog>,
    persister: Arc<PersisterImpl>,
    parquet_cache: Arc<ParquetCache>,
    segment_state: Arc<RwLock<SegmentState<T, W>>>,
    persisted_files: Arc<PersistedFiles>,
    wal: Option<Arc<W>>,
    write_buffer_flusher: WriteBufferFlusher,
    segment_duration: SegmentDuration,
    #[allow(dead_code)]
    time_provider: Arc<T>,
    #[allow(dead_code)]
    segment_persist_handle: Mutex<tokio::task::JoinHandle<()>>,
    #[allow(dead_code)]
    shutdown_segment_persist_tx: watch::Sender<()>,
    #[allow(dead_code)]
    buffer_check_handle: Mutex<tokio::task::JoinHandle<()>>,
    last_cache: Arc<LastCacheProvider>,
}

impl<W: Wal, T: TimeProvider> WriteBufferImpl<W, T> {
    pub async fn new(
        persister: Arc<PersisterImpl>,
        wal: Option<Arc<W>>,
        time_provider: Arc<T>,
        segment_duration: SegmentDuration,
        executor: Arc<iox_query::exec::Executor>,
        buffer_mem_limit_mb: usize,
    ) -> Result<Self> {
        let now = time_provider.now();
        let loaded_state =
            load_starting_state(Arc::clone(&persister), wal.clone(), now, segment_duration).await?;

        let segment_state = Arc::new(RwLock::new(SegmentState::new(
            segment_duration,
            loaded_state.last_segment_id,
            Arc::clone(&loaded_state.catalog),
            Arc::clone(&time_provider),
            loaded_state.open_segments,
            loaded_state.persisting_buffer_segments,
            wal.clone(),
        )));

        let last_cache = Arc::new(LastCacheProvider::new());
        let write_buffer_flusher =
            WriteBufferFlusher::new(Arc::clone(&segment_state), Arc::clone(&last_cache));

        let persisted_files = Arc::new(PersistedFiles::new_from_persisted_segments(
            loaded_state.persisted_segments,
        ));

        let segment_state_persister = Arc::clone(&segment_state);
        let persisted_files_persister = Arc::clone(&persisted_files);
        let time_provider_persister = Arc::clone(&time_provider);
        let wal_perister = wal.clone();
        let cloned_persister = Arc::clone(&persister);
        let cloned_executor = Arc::clone(&executor);

        let (shutdown_segment_persist_tx, shutdown_rx) = watch::channel(());
        let shutdown = shutdown_rx.clone();
        let segment_persist_handle = tokio::task::spawn(async move {
            run_buffer_segment_persist_and_cleanup(
                cloned_persister,
                segment_state_persister,
                persisted_files_persister,
                shutdown_rx,
                time_provider_persister,
                wal_perister,
                cloned_executor,
            )
            .await;
        });

        let segment_state_persister = Arc::clone(&segment_state);
        let cloned_persister = Arc::clone(&persister);
        let cloned_executor = Arc::clone(&executor);

        let buffer_check_handle = tokio::task::spawn(async move {
            run_buffer_size_check_and_persist(
                cloned_persister,
                segment_state_persister,
                shutdown,
                cloned_executor,
                buffer_mem_limit_mb,
            )
            .await;
        });

        Ok(Self {
            catalog: loaded_state.catalog,
            segment_state,
            parquet_cache: Arc::new(ParquetCache::new(&persister.mem_pool)),
            persister,
            wal,
            write_buffer_flusher,
            time_provider,
            segment_duration,
            segment_persist_handle: Mutex::new(segment_persist_handle),
            shutdown_segment_persist_tx,
            buffer_check_handle: Mutex::new(buffer_check_handle),
            last_cache,
            persisted_files,
        })
    }

    pub fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    pub fn persisted_files(&self) -> Arc<PersistedFiles> {
        Arc::clone(&self.persisted_files)
    }

    async fn write_lp(
        &self,
        db_name: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        debug!("write_lp to {} in writebuffer", db_name);

        let result = WriteValidator::initialize(db_name.clone(), self.catalog())?
            .v1_parse_lines_and_update_schema(lp, accept_partial)?
            .convert_lines_to_buffer(ingest_time, self.segment_duration, precision);

        self.write_buffer_flusher
            .write_to_open_segment(result.valid_segmented_data)
            .await?;

        Ok(BufferedWriteRequest {
            db_name,
            invalid_lines: result.errors,
            line_count: result.line_count,
            field_count: result.field_count,
            index_count: result.index_count,
        })
    }

    async fn write_lp_v3(
        &self,
        db_name: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        let result = WriteValidator::initialize(db_name.clone(), self.catalog())?
            .v3_parse_lines_and_update_schema(lp, accept_partial)?
            .convert_lines_to_buffer(ingest_time, self.segment_duration, precision);

        self.write_buffer_flusher
            .write_to_open_segment(result.valid_segmented_data)
            .await?;

        Ok(BufferedWriteRequest {
            db_name,
            invalid_lines: result.errors,
            line_count: result.line_count,
            field_count: result.field_count,
            index_count: result.index_count,
        })
    }

    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let db_schema = self
            .catalog
            .db_schema(database_name)
            .ok_or_else(|| DataFusionError::Execution(format!("db {} not found", database_name)))?;

        let table_schema = {
            let table = db_schema.tables.get(table_name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "table {} not found in db {}",
                    table_name, database_name
                ))
            })?;

            table.schema.clone()
        };

        let object_store_url = self.persister.object_store_url();

        let segment_state = self.segment_state.read();
        let mut chunks = segment_state.get_table_chunks(
            db_schema,
            table_name,
            filters,
            projection,
            object_store_url.clone(),
            self.persister.object_store(),
            ctx,
        )?;
        let parquet_files = self.persisted_files.get_files(database_name, table_name);

        let mut chunk_order = chunks.len() as i64;

        for parquet_file in parquet_files {
            let parquet_chunk = parquet_chunk_from_file(
                &parquet_file,
                &table_schema,
                object_store_url.clone(),
                Arc::clone(&self.persister.object_store()),
                chunk_order,
            );

            chunk_order += 1;

            chunks.push(Arc::new(parquet_chunk));
        }

        // Get any cached files and add them to the query
        // This is mostly the same as above, but we change the object store to
        // point to the in memory cache
        for parquet_file in self
            .parquet_cache
            .get_parquet_files(database_name, table_name)
        {
            let partition_key = data_types::PartitionKey::from(parquet_file.path.clone());
            let partition_id = data_types::partition::TransitionPartitionId::new(
                data_types::TableId::new(0),
                &partition_key,
            );

            let chunk_stats = create_chunk_statistics(
                Some(parquet_file.row_count as usize),
                &table_schema,
                Some(parquet_file.timestamp_min_max()),
                &NoColumnRanges,
            );

            let location = ObjPath::from(parquet_file.path.clone());

            let parquet_exec = ParquetExecInput {
                object_store_url: object_store_url.clone(),
                object_meta: ObjectMeta {
                    location,
                    last_modified: Default::default(),
                    size: parquet_file.size_bytes as usize,
                    e_tag: None,
                    version: None,
                },
                object_store: Arc::clone(&self.parquet_cache.object_store()),
            };

            let parquet_chunk = ParquetChunk {
                schema: table_schema.clone(),
                stats: Arc::new(chunk_stats),
                partition_id,
                sort_key: None,
                id: ChunkId::new(),
                chunk_order: ChunkOrder::new(chunk_order),
                parquet_exec,
            };

            chunk_order += 1;

            chunks.push(Arc::new(parquet_chunk));
        }

        Ok(chunks)
    }

    pub async fn cache_parquet(
        &self,
        db_name: &str,
        table_name: &str,
        min_time: i64,
        max_time: i64,
        records: SendableRecordBatchStream,
    ) -> Result<(), Error> {
        Ok(self
            .parquet_cache
            .persist_parquet_file(db_name, table_name, min_time, max_time, records, None)
            .await?)
    }

    pub async fn update_parquet(
        &self,
        db_name: &str,
        table_name: &str,
        min_time: i64,
        max_time: i64,
        path: ObjPath,
        records: SendableRecordBatchStream,
    ) -> Result<(), Error> {
        Ok(self
            .parquet_cache
            .persist_parquet_file(db_name, table_name, min_time, max_time, records, Some(path))
            .await?)
    }

    pub async fn remove_parquet(&self, path: ObjPath) -> Result<(), Error> {
        Ok(self.parquet_cache.remove_parquet_file(path).await?)
    }

    pub async fn purge_cache(&self) -> Result<(), Error> {
        Ok(self.parquet_cache.purge_cache().await?)
    }

    #[cfg(test)]
    fn get_table_record_batches(
        &self,
        datbase_name: &str,
        table_name: &str,
    ) -> Vec<arrow::record_batch::RecordBatch> {
        let db_schema = self.catalog.db_schema(datbase_name).unwrap();
        let table = db_schema.tables.get(table_name).unwrap();
        let schema = table.schema.clone();

        let segment_state = self.segment_state.read();
        segment_state.open_segments_table_record_batches(datbase_name, table_name, &schema)
    }
}

pub(crate) fn parquet_chunk_from_file(
    parquet_file: &ParquetFile,
    table_schema: &Schema,
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    chunk_order: i64,
) -> ParquetChunk {
    // TODO: update persisted segments to serialize their key to use here
    let partition_key = data_types::PartitionKey::from(parquet_file.path.clone());
    let partition_id = data_types::partition::TransitionPartitionId::new(
        data_types::TableId::new(0),
        &partition_key,
    );

    let chunk_stats = create_chunk_statistics(
        Some(parquet_file.row_count as usize),
        table_schema,
        Some(parquet_file.timestamp_min_max()),
        &NoColumnRanges,
    );

    let location = ObjPath::from(parquet_file.path.clone());

    let parquet_exec = ParquetExecInput {
        object_store_url,
        object_meta: ObjectMeta {
            location,
            last_modified: Default::default(),
            size: parquet_file.size_bytes as usize,
            e_tag: None,
            version: None,
        },
        object_store,
    };

    ParquetChunk {
        schema: table_schema.clone(),
        stats: Arc::new(chunk_stats),
        partition_id,
        sort_key: None,
        id: ChunkId::new(),
        chunk_order: ChunkOrder::new(chunk_order),
        parquet_exec,
    }
}

#[async_trait]
impl<W: Wal, T: TimeProvider> Bufferer for WriteBufferImpl<W, T> {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        self.write_lp(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    async fn write_lp_v3(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> Result<BufferedWriteRequest> {
        self.write_lp_v3(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    fn wal(&self) -> Option<Arc<impl Wal>> {
        self.wal.clone()
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.catalog()
    }
}

impl<W: Wal, T: TimeProvider> ChunkContainer for WriteBufferImpl<W, T> {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> crate::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.get_table_chunks(database_name, table_name, filters, projection, ctx)
    }
}

#[async_trait::async_trait]
impl<W: Wal, T: TimeProvider> LastCacheManager for WriteBufferImpl<W, T> {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        Arc::clone(&self.last_cache)
    }

    /// Create a new last-N-value cache in the specified database and table, along with the given
    /// parameters.
    ///
    /// Returns the name of the newly created cache, or `None` if a cache was not created, but the
    /// provided parameters match those of an existing cache.
    #[allow(clippy::too_many_arguments)]
    async fn create_last_cache(
        &self,
        db_name: &str,
        tbl_name: &str,
        cache_name: Option<&str>,
        count: Option<usize>,
        ttl: Option<Duration>,
        key_columns: Option<Vec<String>>,
        value_columns: Option<Vec<String>>,
    ) -> Result<Option<LastCacheDefinition>, Error> {
        let cache_name = cache_name.map(Into::into);
        let segment_id = self.segment_state.read().current_segment_id();
        let catalog = self.catalog();
        let sequence = catalog.sequence_number();
        let db_schema = catalog.db_schema(db_name).ok_or(Error::DbDoesNotExist)?;
        let schema = db_schema
            .get_table(tbl_name)
            .ok_or(Error::TableDoesNotExist)?
            .schema()
            .clone();
        if let Some(info) = self.last_cache.create_cache(CreateCacheArguments {
            db_name: db_name.to_string(),
            tbl_name: tbl_name.to_string(),
            schema,
            cache_name,
            count,
            ttl,
            key_columns,
            value_columns,
        })? {
            let mut db_schema = db_schema.as_ref().clone();
            let table = db_schema.get_table_mut(tbl_name).unwrap();
            table.add_last_cache(info.clone());
            // NOTE: if this fails then the cache will not persist beyond server restart.
            // It will probably be good to have the cache creation inserted to the WAL, which
            // should harden against a failure here:
            catalog.replace_database(sequence, Arc::new(db_schema))?;
            let inner_catalog = catalog.clone_inner();
            // Force persistence to the catalog, since we aren't going through the WAL:
            self.persister
                .persist_catalog(segment_id, Catalog::from_inner(inner_catalog))
                .await?;
            Ok(Some(info))
        } else {
            Ok(None)
        }
    }

    async fn delete_last_cache(
        &self,
        db_name: &str,
        tbl_name: &str,
        cache_name: &str,
    ) -> crate::Result<(), self::Error> {
        let segment_id = self.segment_state.read().current_segment_id();
        let catalog = self.catalog();
        let sequence = catalog.sequence_number();
        self.last_cache
            .delete_cache(db_name, tbl_name, cache_name)?;
        let mut db_schema = catalog
            .db_schema(db_name)
            .ok_or(Error::DbDoesNotExist)?
            .as_ref()
            .clone();
        let table = db_schema
            .get_table_mut(tbl_name)
            .ok_or(Error::TableDoesNotExist)?;
        table.remove_last_cache(cache_name);
        // NOTE: if this fails then the cache will be resurrected on server restart.
        // As for create, if this operation can go through the WAL, then that would protect
        // against this kind of failure.
        catalog.replace_database(sequence, Arc::new(db_schema))?;
        let inner_catalog = catalog.clone_inner();
        // Force persistence to the catalog, since we aren't going through the WAL:
        self.persister
            .persist_catalog(segment_id, Catalog::from_inner(inner_catalog))
            .await?;
        Ok(())
    }
}

impl<W: Wal, T: TimeProvider> WriteBuffer for WriteBufferImpl<W, T> {}

#[derive(Debug, Default)]
pub(crate) struct TableBatch {
    #[allow(dead_code)]
    pub(crate) name: String,
    pub(crate) rows: Vec<Row>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Row {
    pub(crate) time: i64,
    pub(crate) fields: Vec<Field>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Field {
    pub(crate) name: String,
    pub(crate) value: FieldData,
}

#[derive(Clone, Debug)]
pub(crate) enum FieldData {
    Timestamp(i64),
    Key(String),
    Tag(String),
    String(String),
    Integer(i64),
    UInteger(u64),
    Float(f64),
    Boolean(bool),
}

impl PartialEq for FieldData {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FieldData::Timestamp(a), FieldData::Timestamp(b)) => a == b,
            (FieldData::Tag(a), FieldData::Tag(b)) => a == b,
            (FieldData::Key(a), FieldData::Key(b)) => a == b,
            (FieldData::String(a), FieldData::String(b)) => a == b,
            (FieldData::Integer(a), FieldData::Integer(b)) => a == b,
            (FieldData::UInteger(a), FieldData::UInteger(b)) => a == b,
            (FieldData::Float(a), FieldData::Float(b)) => a == b,
            (FieldData::Boolean(a), FieldData::Boolean(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for FieldData {}

impl<'a> From<&SeriesValue<'a>> for FieldData {
    fn from(sk: &SeriesValue<'a>) -> Self {
        match sk {
            SeriesValue::String(s) => Self::Key(s.to_string()),
        }
    }
}

impl<'a> From<FieldValue<'a>> for FieldData {
    fn from(value: FieldValue<'a>) -> Self {
        match value {
            FieldValue::I64(v) => Self::Integer(v),
            FieldValue::U64(v) => Self::UInteger(v),
            FieldValue::F64(v) => Self::Float(v),
            FieldValue::String(v) => Self::String(v.to_string()),
            FieldValue::Boolean(v) => Self::Boolean(v),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ValidSegmentedData {
    pub(crate) database_name: NamespaceName<'static>,
    pub(crate) segment_start: Time,
    pub(crate) table_batches: HashMap<String, TableBatch>,
    pub(crate) wal_op: WalOp,
    /// The sequence number of the catalog before any updates were applied based on this write.
    pub(crate) starting_catalog_sequence_number: SequenceNumber,
}

#[derive(Debug, Default)]
pub(crate) struct TableBatchMap<'a> {
    pub(crate) lines: Vec<&'a str>,
    pub(crate) table_batches: HashMap<String, TableBatch>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persister::PersisterImpl;
    use crate::wal::WalImpl;
    use crate::{LpWriteOp, SegmentId, SequenceNumber, WalOpBatch};
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use datafusion_util::config::register_iox_object_store;
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time};
    use object_store::memory::InMemory;
    use object_store::ObjectStore;

    #[test]
    fn parse_lp_into_buffer() {
        let catalog = Arc::new(Catalog::new());
        let db_name = NamespaceName::new("foo").unwrap();
        let lp = "cpu,region=west user=23.2 100\nfoo f1=1i";
        WriteValidator::initialize(db_name, Arc::clone(&catalog))
            .unwrap()
            .v1_parse_lines_and_update_schema(lp, false)
            .unwrap()
            .convert_lines_to_buffer(
                Time::from_timestamp_nanos(0),
                SegmentDuration::new_5m(),
                Precision::Nanosecond,
            );

        let db = catalog.db_schema("foo").unwrap();

        assert_eq!(db.tables.len(), 2);
        assert_eq!(db.tables.get("cpu").unwrap().num_columns(), 3);
        assert_eq!(db.tables.get("foo").unwrap().num_columns(), 2);
    }

    #[tokio::test]
    async fn buffers_and_persists_to_wal() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = WalImpl::new(dir.clone()).unwrap();
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = SegmentDuration::new_5m();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            Some(Arc::new(wal)),
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();

        let summary = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1 10",
                Time::from_timestamp_nanos(123),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        assert_eq!(summary.line_count, 1);
        assert_eq!(summary.field_count, 1);
        assert_eq!(summary.index_count, 0);

        // ensure the data is in the buffer
        let actual = write_buffer.get_table_record_batches("foo", "cpu");
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &actual);

        // ensure the data is in the wal
        let wal = WalImpl::new(dir).unwrap();
        let mut reader = wal.open_segment_reader(SegmentId::new(1)).unwrap();
        let batch = reader.next_batch().unwrap().unwrap();
        let expected_batch = WalOpBatch {
            sequence_number: SequenceNumber::new(1),
            ops: vec![WalOp::LpWrite(LpWriteOp {
                db_name: "foo".to_string(),
                lp: "cpu bar=1 10".to_string(),
                default_time: 123,
                precision: Precision::Nanosecond,
            })],
        };
        assert_eq!(batch, expected_batch);

        // ensure we load state from the persister
        let write_buffer = WriteBufferImpl::new(
            persister,
            Some(Arc::new(wal)),
            time_provider,
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();
        let actual = write_buffer.get_table_record_batches("foo", "cpu");
        assert_batches_eq!(&expected, &actual);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returns_chunks_across_buffered_and_persisted_data() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Some(Arc::new(WalImpl::new(dir.clone()).unwrap()));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = SegmentDuration::new_5m();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            wal.clone(),
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();
        let session_context = IOxSessionContext::with_testing();
        let runtime_env = session_context.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1 10",
                Time::from_timestamp_nanos(123),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // advance the time and wait for it to persist
        time_provider.set(Time::from_timestamp(800, 0).unwrap());
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            if !write_buffer
                .persisted_files
                .get_files("foo", "cpu")
                .is_empty()
            {
                break;
            }
        }

        // nothing should be open at this point
        assert!(write_buffer
            .segment_state
            .read()
            .open_segment_times()
            .is_empty());

        // verify we get the persisted data
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // now write some into the next segment we're in and verify we get both buffer and persisted
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=2",
                Time::from_timestamp(900, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 2.0 | 1970-01-01T00:15:00Z           |",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // and now reload the buffer and verify that we get persisted and the buffer again
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            wal,
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // and now add to the buffer and verify that we still only get two chunks
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=3",
                Time::from_timestamp(950, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 2.0 | 1970-01-01T00:15:00Z           |",
            "| 3.0 | 1970-01-01T00:15:50Z           |",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sets_starting_catalog_number_on_new_segment() {
        let dir = test_helpers::tmp_dir().unwrap().into_path();
        let wal = Some(Arc::new(WalImpl::new(dir.clone()).unwrap()));
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store)));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let segment_duration = SegmentDuration::new_5m();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            wal.clone(),
            Arc::clone(&time_provider),
            segment_duration,
            crate::test_help::make_exec(),
            1000,
        )
        .await
        .unwrap();
        let starting_catalog_sequence_number = write_buffer.catalog().sequence_number();

        let session_context = IOxSessionContext::with_testing();
        let runtime_env = session_context.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));

        // write data into the buffer that will go into a new segment
        let new_segment_time = Time::from_timestamp(360, 0).unwrap();
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1",
                new_segment_time,
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 1.0 | 1970-01-01T00:06:00Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // get the segment for the new_segment_time and validate that it has the correct starting catalog sequence number
        let state = write_buffer.segment_state.read();
        let segment_start_time = SegmentDuration::new_5m().start_time(new_segment_time.timestamp());
        let segment = state.segment_for_time(segment_start_time).unwrap();
        assert_eq!(
            segment.starting_catalog_sequence_number(),
            starting_catalog_sequence_number
        );
    }

    async fn get_table_batches(
        write_buffer: &WriteBufferImpl<WalImpl, MockProvider>,
        database_name: &str,
        table_name: &str,
        ctx: &IOxSessionContext,
    ) -> Vec<RecordBatch> {
        let chunks = write_buffer
            .get_table_chunks(database_name, table_name, &[], None, &ctx.inner().state())
            .unwrap();
        let mut batches = vec![];
        for chunk in chunks {
            let chunk = chunk
                .data()
                .read_to_batches(chunk.schema(), ctx.inner())
                .await;
            batches.extend(chunk);
        }
        batches
    }
}
