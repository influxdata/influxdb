//! Implementation of an in-memory buffer for writes that persists data into a wal if it is configured.

mod metrics;
pub mod persisted_files;
#[allow(dead_code)]
pub mod plugins;
pub mod queryable_buffer;
mod table_buffer;
pub mod validator;

use crate::persister::Persister;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::queryable_buffer::QueryableBuffer;
use crate::write_buffer::validator::WriteValidator;
use crate::{chunk::ParquetChunk, write_buffer, DatabaseManager};
use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, MetaCacheManager,
    ParquetFile, PersistedSnapshot, Precision, WriteBuffer, WriteLineError,
};
use async_trait::async_trait;
use data_types::{
    ChunkId, ChunkOrder, ColumnType, NamespaceName, NamespaceNameError, PartitionHashId,
    PartitionId,
};
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use influxdb3_cache::last_cache::{self, LastCacheProvider};
use influxdb3_cache::meta_cache::{self, CreateMetaCacheArgs, MetaCacheProvider};
use influxdb3_cache::parquet_cache::ParquetCacheOracle;
use influxdb3_catalog::catalog;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_id::{ColumnId, DbId, TableId};
use influxdb3_wal::FieldDataType;
use influxdb3_wal::TableDefinition;
use influxdb3_wal::{
    object_store::WalObjectStore, DeleteDatabaseDefinition, PluginDefinition, PluginType,
    TriggerDefinition, TriggerSpecificationDefinition, WalContents,
};
use influxdb3_wal::{
    CatalogBatch, CatalogOp, LastCacheDefinition, LastCacheDelete, LastCacheSize,
    MetaCacheDefinition, MetaCacheDelete, Wal, WalConfig, WalFileNotifier, WalOp,
};
use influxdb3_wal::{CatalogOp::CreateLastCache, DeleteTableDefinition};
use influxdb3_wal::{DatabaseDefinition, FieldDefinition};
use iox_query::chunk_statistics::{create_chunk_statistics, NoColumnRanges};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use metrics::WriteMetrics;
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore};
use observability_deps::tracing::{debug, error};
use parquet_file::storage::ParquetExecInput;
use plugins::ProcessingEngineManager;
use queryable_buffer::QueryableBufferArgs;
use schema::Schema;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::watch::Receiver;

#[cfg(feature = "system-py")]
use {
    crate::write_buffer::plugins::PluginContext,
    influxdb3_catalog::catalog::Error::ProcessingEngineTriggerNotFound,
};

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
    CatalogUpdateError(#[from] influxdb3_catalog::catalog::Error),

    #[error("error from persister: {0}")]
    PersisterError(#[from] crate::persister::Error),

    #[error("corrupt load state: {0}")]
    CorruptLoadState(String),

    #[error("database name error: {0}")]
    DatabaseNameError(#[from] NamespaceNameError),

    #[error("error from table buffer: {0}")]
    TableBufferError(#[from] table_buffer::Error),

    #[error("error in last cache: {0}")]
    LastCacheError(#[from] last_cache::Error),

    #[error("database not found {db_name:?}")]
    DatabaseNotFound { db_name: String },

    #[error("table not found {table_name:?} in db {db_name:?}")]
    TableNotFound { db_name: String, table_name: String },

    // This error is exclusive to the table creation API
    #[error("table creation failed due to no fields")]
    EmptyFields,

    #[error("tried accessing database that does not exist")]
    DbDoesNotExist,

    #[error("tried creating database named '{0}' that already exists")]
    DatabaseExists(String),

    #[error("tried accessing table that do not exist")]
    TableDoesNotExist,

    #[error("tried accessing column with name ({0}) that does not exist")]
    ColumnDoesNotExist(String),

    #[error(
        "updating catalog on delete of last cache failed, you will need to delete the cache \
        again on server restart"
    )]
    DeleteLastCache(#[source] influxdb3_catalog::catalog::Error),

    #[error("error from wal: {0}")]
    WalError(#[from] influxdb3_wal::Error),

    #[error("cannot write to a read-only server")]
    NoWriteInReadOnly,

    #[error("error in metadata cache: {0}")]
    MetaCacheError(#[from] meta_cache::ProviderError),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteRequest<'a> {
    pub db_name: NamespaceName<'static>,
    pub line_protocol: &'a str,
    pub default_time: u64,
}

#[derive(Debug)]
pub struct WriteBufferImpl {
    catalog: Arc<Catalog>,
    persister: Arc<Persister>,
    // NOTE(trevor): the parquet cache interface may be used to register other cache
    // requests from the write buffer, e.g., during query...
    #[allow(dead_code)]
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    persisted_files: Arc<PersistedFiles>,
    buffer: Arc<QueryableBuffer>,
    wal_config: WalConfig,
    wal: Arc<dyn Wal>,
    time_provider: Arc<dyn TimeProvider>,
    metrics: WriteMetrics,
    meta_cache: Arc<MetaCacheProvider>,
    last_cache: Arc<LastCacheProvider>,
}

/// The maximum number of snapshots to load on start
pub const N_SNAPSHOTS_TO_LOAD_ON_START: usize = 1_000;

#[derive(Debug)]
pub struct WriteBufferImplArgs {
    pub persister: Arc<Persister>,
    pub catalog: Arc<Catalog>,
    pub last_cache: Arc<LastCacheProvider>,
    pub meta_cache: Arc<MetaCacheProvider>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub executor: Arc<iox_query::exec::Executor>,
    pub wal_config: WalConfig,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub metric_registry: Arc<Registry>,
}

impl WriteBufferImpl {
    pub async fn new(
        WriteBufferImplArgs {
            persister,
            catalog,
            last_cache,
            meta_cache,
            time_provider,
            executor,
            wal_config,
            parquet_cache,
            metric_registry,
        }: WriteBufferImplArgs,
    ) -> Result<Arc<Self>> {
        // load snapshots and replay the wal into the in memory buffer
        let persisted_snapshots = persister
            .load_snapshots(N_SNAPSHOTS_TO_LOAD_ON_START)
            .await?;
        let last_wal_sequence_number = persisted_snapshots
            .first()
            .map(|s| s.wal_file_sequence_number);
        let last_snapshot_sequence_number = persisted_snapshots
            .first()
            .map(|s| s.snapshot_sequence_number);
        // If we have any snapshots, set sequential IDs from the newest one.
        if let Some(first_snapshot) = persisted_snapshots.first() {
            first_snapshot.next_db_id.set_next_id();
            first_snapshot.next_table_id.set_next_id();
            first_snapshot.next_column_id.set_next_id();
            first_snapshot.next_file_id.set_next_id();
        }

        let persisted_files = Arc::new(PersistedFiles::new_from_persisted_snapshots(
            persisted_snapshots,
        ));
        let queryable_buffer = Arc::new(QueryableBuffer::new(QueryableBufferArgs {
            executor,
            catalog: Arc::clone(&catalog),
            persister: Arc::clone(&persister),
            last_cache_provider: Arc::clone(&last_cache),
            meta_cache_provider: Arc::clone(&meta_cache),
            persisted_files: Arc::clone(&persisted_files),
            parquet_cache: parquet_cache.clone(),
        }));

        // create the wal instance, which will replay into the queryable buffer and start
        // the background flush task.
        let wal = WalObjectStore::new(
            Arc::clone(&time_provider),
            persister.object_store(),
            persister.host_identifier_prefix(),
            Arc::clone(&queryable_buffer) as Arc<dyn WalFileNotifier>,
            wal_config,
            last_wal_sequence_number,
            last_snapshot_sequence_number,
        )
        .await?;

        let result = Arc::new(Self {
            catalog,
            parquet_cache,
            persister,
            wal_config,
            wal,
            time_provider,
            meta_cache,
            last_cache,
            persisted_files,
            buffer: queryable_buffer,
            metrics: WriteMetrics::new(&metric_registry),
        });
        let write_buffer: Arc<dyn WriteBuffer> = result.clone();
        let triggers = result.catalog().triggers();
        for (db_name, trigger_name) in triggers {
            result
                .run_trigger(Arc::clone(&write_buffer), &db_name, &trigger_name)
                .await?;
        }
        Ok(result)
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

        // validated lines will update the in-memory catalog, ensuring that all write operations
        // past this point will be infallible
        let result = WriteValidator::initialize(
            db_name.clone(),
            self.catalog(),
            ingest_time.timestamp_nanos(),
        )?
        .v1_parse_lines_and_update_schema(lp, accept_partial, ingest_time, precision)?
        .convert_lines_to_buffer(self.wal_config.gen1_duration);

        // if there were catalog updates, ensure they get persisted to the wal, so they're
        // replayed on restart
        let mut ops = Vec::with_capacity(2);
        if let Some(catalog_batch) = result.catalog_updates {
            ops.push(WalOp::Catalog(catalog_batch));
        }
        ops.push(WalOp::Write(result.valid_data));

        // write to the wal. Behind the scenes the ops get buffered in memory and once a second (or
        // whatever the configured wal flush interval is set to) the buffer is flushed and all the
        // data is persisted into a single wal file in the configured object store. Then the
        // contents are sent to the configured notifier, which in this case is the queryable buffer.
        // Thus, after this returns, the data is both durable and queryable.
        self.wal.write_ops(ops).await?;

        // record metrics for lines written, rejected, and bytes written
        self.metrics
            .record_lines(&db_name, result.line_count as u64);
        self.metrics
            .record_lines_rejected(&db_name, result.errors.len() as u64);
        self.metrics
            .record_bytes(&db_name, result.valid_bytes_count);

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
        // validated lines will update the in-memory catalog, ensuring that all write operations
        // past this point will be infallible
        let result = WriteValidator::initialize(
            db_name.clone(),
            self.catalog(),
            ingest_time.timestamp_nanos(),
        )?
        .v3_parse_lines_and_update_schema(lp, accept_partial, ingest_time, precision)?
        .convert_lines_to_buffer(self.wal_config.gen1_duration);

        // if there were catalog updates, ensure they get persisted to the wal, so they're
        // replayed on restart
        let mut ops = Vec::with_capacity(2);
        if let Some(catalog_batch) = result.catalog_updates {
            ops.push(WalOp::Catalog(catalog_batch));
        }
        ops.push(WalOp::Write(result.valid_data));

        // write to the wal. Behind the scenes the ops get buffered in memory and once a second (or
        // whatever the configured wal flush interval is set to) the buffer is flushed and all the
        // data is persisted into a single wal file in the configured object store. Then the
        // contents are sent to the configured notifier, which in this case is the queryable buffer.
        // Thus, after this returns, the data is both durable and queryable.
        self.wal.write_ops(ops).await?;

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
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let db_schema = self.catalog.db_schema(database_name).ok_or_else(|| {
            DataFusionError::Execution(format!("database {} not found", database_name))
        })?;

        let (table_id, table_schema) =
            db_schema.table_id_and_schema(table_name).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "table {} not found in db {}",
                    table_name, database_name
                ))
            })?;

        let mut chunks = self.buffer.get_table_chunks(
            Arc::clone(&db_schema),
            table_name,
            filters,
            projection,
            ctx,
        )?;

        let parquet_files = self.persisted_files.get_files(db_schema.id, table_id);

        let mut chunk_order = chunks.len() as i64;

        for parquet_file in parquet_files {
            let parquet_chunk = parquet_chunk_from_file(
                &parquet_file,
                &table_schema,
                self.persister.object_store_url().clone(),
                self.persister.object_store(),
                chunk_order,
            );

            chunk_order += 1;

            chunks.push(Arc::new(parquet_chunk));
        }

        Ok(chunks)
    }
}

pub fn parquet_chunk_from_file(
    parquet_file: &ParquetFile,
    table_schema: &Schema,
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    chunk_order: i64,
) -> ParquetChunk {
    let partition_key = data_types::PartitionKey::from(parquet_file.chunk_time.to_string());
    let partition_id = data_types::partition::TransitionPartitionId::from_parts(
        PartitionId::new(0),
        Some(PartitionHashId::new(
            data_types::TableId::new(0),
            &partition_key,
        )),
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
impl Bufferer for WriteBufferImpl {
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

    fn catalog(&self) -> Arc<Catalog> {
        self.catalog()
    }

    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        self.buffer.persisted_parquet_files(db_id, table_id)
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        self.buffer.persisted_snapshot_notify_rx()
    }
}

impl ChunkContainer for WriteBufferImpl {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> crate::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.get_table_chunks(database_name, table_name, filters, projection, ctx)
    }
}

#[async_trait::async_trait]
impl MetaCacheManager for WriteBufferImpl {
    fn meta_cache_provider(&self) -> Arc<MetaCacheProvider> {
        Arc::clone(&self.meta_cache)
    }

    async fn create_meta_cache(
        &self,
        db_schema: Arc<DatabaseSchema>,
        cache_name: Option<String>,
        args: CreateMetaCacheArgs,
    ) -> Result<Option<MetaCacheDefinition>, Error> {
        if let Some(new_cache_definition) = self
            .meta_cache
            .create_cache(db_schema.id, cache_name.map(Into::into), args)
            .map_err(Error::MetaCacheError)?
        {
            let catalog_op = CatalogOp::CreateMetaCache(new_cache_definition.clone());
            let catalog_batch = CatalogBatch {
                database_id: db_schema.id,
                database_name: db_schema.name.clone(),
                time_ns: self.time_provider.now().timestamp_nanos(),
                ops: vec![catalog_op],
            };
            if let Some(catalog_batch) = self.catalog.apply_catalog_batch(catalog_batch)? {
                self.wal
                    .write_ops(vec![WalOp::Catalog(catalog_batch)])
                    .await?;
            }
            Ok(Some(new_cache_definition))
        } else {
            Ok(None)
        }
    }

    async fn delete_meta_cache(
        &self,
        db_id: &DbId,
        tbl_id: &TableId,
        cache_name: &str,
    ) -> Result<(), Error> {
        let catalog = self.catalog();
        let db_schema = catalog.db_schema_by_id(db_id).expect("db should exist");
        self.meta_cache.delete_cache(db_id, tbl_id, cache_name)?;
        let catalog_batch = CatalogBatch {
            database_id: *db_id,
            database_name: Arc::clone(&db_schema.name),
            time_ns: self.time_provider.now().timestamp_nanos(),
            ops: vec![CatalogOp::DeleteMetaCache(MetaCacheDelete {
                table_name: db_schema.table_id_to_name(tbl_id).expect("table exists"),
                table_id: *tbl_id,
                cache_name: cache_name.into(),
            })],
        };
        if let Some(catalog_batch) = catalog.apply_catalog_batch(catalog_batch)? {
            self.wal
                .write_ops(vec![WalOp::Catalog(catalog_batch)])
                .await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl LastCacheManager for WriteBufferImpl {
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
        db_id: DbId,
        table_id: TableId,
        cache_name: Option<&str>,
        count: Option<usize>,
        ttl: Option<Duration>,
        key_columns: Option<Vec<ColumnId>>,
        value_columns: Option<Vec<ColumnId>>,
    ) -> Result<Option<LastCacheDefinition>, Error> {
        let cache_name = cache_name.map(Into::into);
        let catalog = self.catalog();
        let db_schema = catalog
            .db_schema_by_id(&db_id)
            .ok_or(Error::DbDoesNotExist)?;
        let table_def = db_schema
            .table_definition_by_id(&table_id)
            .ok_or(Error::TableDoesNotExist)?;
        let count = count
            .map(LastCacheSize::new)
            .transpose()
            .map_err(|_| last_cache::Error::InvalidCacheSize)?
            .unwrap_or_default();
        let ttl = ttl.map(Into::into).unwrap_or_default();
        let key_columns = key_columns.into();
        let value_columns = value_columns.into();

        if let Some(info) = self.last_cache.create_cache(
            db_id,
            cache_name,
            last_cache::CreateLastCacheArgs {
                table_def,
                count,
                ttl,
                key_columns,
                value_columns,
            },
        )? {
            let catalog_batch = CatalogBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_id: db_schema.id,
                database_name: Arc::clone(&db_schema.name),
                ops: vec![CreateLastCache(info.clone())],
            };
            if let Some(catalog_batch) = self.catalog.apply_catalog_batch(catalog_batch)? {
                self.wal
                    .write_ops(vec![WalOp::Catalog(catalog_batch)])
                    .await?;
            }

            Ok(Some(info))
        } else {
            Ok(None)
        }
    }

    async fn delete_last_cache(
        &self,
        db_id: DbId,
        tbl_id: TableId,
        cache_name: &str,
    ) -> crate::Result<(), Error> {
        let catalog = self.catalog();
        let db_schema = catalog.db_schema_by_id(&db_id).expect("db should exist");
        self.last_cache.delete_cache(db_id, tbl_id, cache_name)?;

        let catalog_batch = CatalogBatch {
            time_ns: self.time_provider.now().timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![CatalogOp::DeleteLastCache(LastCacheDelete {
                table_id: tbl_id,
                table_name: db_schema.table_id_to_name(&tbl_id).expect("table exists"),
                name: cache_name.into(),
            })],
        };

        // NOTE: if this fails then the cache will be gone from the running server, but will be
        // resurrected on server restart.
        if let Some(catalog_batch) = catalog.apply_catalog_batch(catalog_batch)? {
            self.wal
                .write_ops(vec![WalOp::Catalog(catalog_batch)])
                .await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl DatabaseManager for WriteBufferImpl {
    async fn create_database(&self, name: String) -> crate::Result<(), self::Error> {
        if self.catalog.db_name_to_id(&name).is_some() {
            return Err(self::Error::DatabaseExists(name.clone()));
        }
        // Create the Database
        let db_schema = self.catalog.db_or_create(&name)?;
        let db_id = db_schema.id;

        let catalog_batch = CatalogBatch {
            time_ns: self.time_provider.now().timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![CatalogOp::CreateDatabase(DatabaseDefinition {
                database_id: db_id,
                database_name: Arc::clone(&db_schema.name),
            })],
        };
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(catalog_batch)? {
            self.wal
                .write_ops(vec![WalOp::Catalog(catalog_batch)])
                .await?;
        }
        debug!(db_id = ?db_id, name = ?&db_schema.name, "successfully created database");
        Ok(())
    }

    async fn soft_delete_database(&self, name: String) -> crate::Result<(), self::Error> {
        let (db_id, db_schema) =
            self.catalog
                .db_id_and_schema(&name)
                .ok_or_else(|| self::Error::DatabaseNotFound {
                    db_name: name.to_owned(),
                })?;

        let deletion_time = self.time_provider.now();
        let catalog_batch = CatalogBatch {
            time_ns: deletion_time.timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![CatalogOp::DeleteDatabase(DeleteDatabaseDefinition {
                database_id: db_id,
                database_name: Arc::clone(&db_schema.name),
                deletion_time: deletion_time.timestamp_nanos(),
            })],
        };
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(catalog_batch)? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
            debug!(db_id = ?db_id, name = ?&db_schema.name, "successfully deleted database");
        }
        Ok(())
    }
    async fn create_table(
        &self,
        db: String,
        table: String,
        tags: Vec<String>,
        fields: Vec<(String, String)>,
    ) -> Result<(), self::Error> {
        if fields.is_empty() {
            return Err(self::Error::EmptyFields);
        }
        let (db_id, db_schema) =
            self.catalog
                .db_id_and_schema(&db)
                .ok_or_else(|| self::Error::DatabaseNotFound {
                    db_name: db.to_owned(),
                })?;

        let table_id = TableId::new();
        let table_name = table.into();
        let mut key = Vec::new();
        let field_definitions = {
            let mut field_definitions = Vec::new();
            for tag in tags {
                let id = ColumnId::new();
                key.push(id);
                field_definitions.push(FieldDefinition {
                    name: tag.into(),
                    id,
                    data_type: FieldDataType::Tag,
                });
            }

            for (name, ty) in fields {
                field_definitions.push(FieldDefinition {
                    name: name.into(),
                    id: ColumnId::new(),
                    data_type: match ty.as_str() {
                        "uint64" => FieldDataType::UInteger,
                        "float64" => FieldDataType::Float,
                        "int64" => FieldDataType::Integer,
                        "bool" => FieldDataType::Boolean,
                        "utf8" => FieldDataType::String,
                        _ => todo!(),
                    },
                });
            }

            field_definitions.push(FieldDefinition {
                name: "time".into(),
                id: ColumnId::new(),
                data_type: FieldDataType::Timestamp,
            });

            field_definitions
        };

        let catalog_table_def = TableDefinition {
            database_id: db_schema.id,
            database_name: Arc::clone(&db_schema.name),
            table_name: Arc::clone(&table_name),
            table_id,
            field_definitions,
            key,
        };

        let catalog_batch = CatalogBatch {
            time_ns: self.time_provider.now().timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![CatalogOp::CreateTable(catalog_table_def)],
        };
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(catalog_batch)? {
            self.wal
                .write_ops(vec![WalOp::Catalog(catalog_batch)])
                .await?;
        }

        debug!(
            db_name = ?db_schema.name,
            db_id = ?db_id,
            table_id = ?table_id,
            name = ?table_name,
            "successfully created table"
        );

        Ok(())
    }

    async fn soft_delete_table(
        &self,
        db_name: String,
        table_name: String,
    ) -> crate::Result<(), self::Error> {
        let (db_id, db_schema) = self.catalog.db_id_and_schema(&db_name).ok_or_else(|| {
            self::Error::DatabaseNotFound {
                db_name: db_name.to_owned(),
            }
        })?;

        let (table_id, table_defn) = db_schema
            .table_id_and_definition(table_name.as_str())
            .ok_or_else(|| self::Error::TableNotFound {
                db_name: db_name.to_owned(),
                table_name: table_name.to_owned(),
            })?;
        let deletion_time = self.time_provider.now();
        let catalog_batch = CatalogBatch {
            time_ns: deletion_time.timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![CatalogOp::DeleteTable(DeleteTableDefinition {
                database_id: db_id,
                database_name: Arc::clone(&db_schema.name),
                deletion_time: deletion_time.timestamp_nanos(),
                table_id,
                table_name: Arc::clone(&table_defn.table_name),
            })],
        };
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(catalog_batch)? {
            self.wal
                .write_ops(vec![WalOp::Catalog(catalog_batch)])
                .await?;
        }
        debug!(
            db_id = ?db_id,
            db_name = ?&db_schema.name,
            table_id = ?table_id,
            table_name = ?table_defn.table_name,
            "successfully deleted table"
        );
        Ok(())
    }
}

#[async_trait::async_trait]
impl ProcessingEngineManager for WriteBufferImpl {
    async fn insert_plugin(
        &self,
        db: &str,
        plugin_name: String,
        code: String,
        function_name: String,
        plugin_type: PluginType,
    ) -> crate::Result<(), Error> {
        let (db_id, db_schema) =
            self.catalog
                .db_id_and_schema(db)
                .ok_or_else(|| self::Error::DatabaseNotFound {
                    db_name: db.to_owned(),
                })?;

        let catalog_op = CatalogOp::CreatePlugin(PluginDefinition {
            plugin_name,
            code,
            function_name,
            plugin_type,
        });

        let creation_time = self.time_provider.now();
        let catalog_batch = CatalogBatch {
            time_ns: creation_time.timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![catalog_op],
        };
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(catalog_batch)? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
        }
        Ok(())
    }

    async fn insert_trigger(
        &self,
        db_name: &str,
        trigger_name: String,
        plugin_name: String,
        trigger_specification: TriggerSpecificationDefinition,
    ) -> crate::Result<(), Error> {
        let Some((db_id, db_schema)) = self.catalog.db_id_and_schema(db_name) else {
            return Err(Error::DatabaseNotFound {
                db_name: db_name.to_owned(),
            });
        };
        let plugin = db_schema
            .processing_engine_plugins
            .get(&plugin_name)
            .ok_or_else(|| catalog::Error::ProcessingEnginePluginNotFound {
                plugin_name: plugin_name.to_string(),
                database_name: db_schema.name.to_string(),
            })?;
        let catalog_op = CatalogOp::CreateTrigger(TriggerDefinition {
            trigger_name,
            plugin_name,
            plugin: plugin.clone(),
            trigger: trigger_specification,
        });
        let creation_time = self.time_provider.now();
        let catalog_batch = CatalogBatch {
            time_ns: creation_time.timestamp_nanos(),
            database_id: db_id,
            database_name: Arc::clone(&db_schema.name),
            ops: vec![catalog_op],
        };
        if let Some(catalog_batch) = self.catalog.apply_catalog_batch(catalog_batch)? {
            let wal_op = WalOp::Catalog(catalog_batch);
            self.wal.write_ops(vec![wal_op]).await?;
        }
        Ok(())
    }

    #[cfg_attr(not(feature = "system-py"), allow(unused))]
    async fn run_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        db_name: &str,
        trigger_name: &str,
    ) -> crate::Result<(), write_buffer::Error> {
        #[cfg(feature = "system-py")]
        {
            let (_db_id, db_schema) =
                self.catalog
                    .db_id_and_schema(db_name)
                    .ok_or_else(|| Error::DatabaseNotFound {
                        db_name: db_name.to_string(),
                    })?;
            let trigger = db_schema
                .processing_engine_triggers
                .get(trigger_name)
                .ok_or_else(|| ProcessingEngineTriggerNotFound {
                    database_name: db_name.to_string(),
                    trigger_name: trigger_name.to_string(),
                })?
                .clone();
            let trigger_rx = self.buffer.subscribe_to_plugin_events();
            let plugin_context = PluginContext {
                trigger_rx,
                write_buffer,
            };
            plugins::run_plugin(db_name.to_string(), trigger, plugin_context);
        }

        Ok(())
    }
}

#[derive(Clone)]
#[allow(unused)]
pub(crate) enum PluginEvent {
    WriteWalContents(Arc<WalContents>),
}

impl WriteBuffer for WriteBufferImpl {}

#[cfg(test)]
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use crate::paths::{CatalogFilePath, SnapshotInfoFilePath};
    use crate::persister::Persister;
    use crate::PersistedSnapshot;
    use arrow::record_batch::RecordBatch;
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use bytes::Bytes;
    use datafusion_util::config::register_iox_object_store;
    use futures_util::StreamExt;
    use influxdb3_cache::parquet_cache::test_cached_obj_store_and_oracle;
    use influxdb3_catalog::catalog::CatalogSequenceNumber;
    use influxdb3_id::{DbId, ParquetFileId};
    use influxdb3_test_helpers::object_store::RequestCountedObjectStore;
    use influxdb3_wal::{Gen1Duration, SnapshotSequenceNumber, WalFileSequenceNumber};
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time};
    use metric::{Attributes, Metric, U64Counter};
    use metrics::{
        WRITE_BYTES_METRIC_NAME, WRITE_LINES_METRIC_NAME, WRITE_LINES_REJECTED_METRIC_NAME,
    };
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, PutPayload};

    #[test]
    fn parse_lp_into_buffer() {
        let host_id = Arc::from("sample-host-id");
        let instance_id = Arc::from("sample-instance-id");
        let catalog = Arc::new(Catalog::new(host_id, instance_id));
        let db_name = NamespaceName::new("foo").unwrap();
        let lp = "cpu,region=west user=23.2 100\nfoo f1=1i";
        WriteValidator::initialize(db_name, Arc::clone(&catalog), 0)
            .unwrap()
            .v1_parse_lines_and_update_schema(
                lp,
                false,
                Time::from_timestamp_nanos(0),
                Precision::Nanosecond,
            )
            .unwrap()
            .convert_lines_to_buffer(Gen1Duration::new_5m());

        let db = catalog.db_schema_by_id(&DbId::from(0)).unwrap();

        assert_eq!(db.tables.len(), 2);
        // cpu table
        assert_eq!(db.tables.get(&TableId::from(0)).unwrap().num_columns(), 3);
        // foo table
        assert_eq!(db.tables.get(&TableId::from(1)).unwrap().num_columns(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_data_to_wal_and_is_queryable() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let (object_store, parquet_cache) = test_cached_obj_store_and_oracle(
            object_store,
            Arc::clone(&time_provider),
            Default::default(),
        );
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), "test_host"));
        let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _).unwrap();
        let meta_cache =
            MetaCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
                .unwrap();
        let write_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
            persister: Arc::clone(&persister),
            catalog,
            last_cache,
            meta_cache,
            time_provider: Arc::clone(&time_provider),
            executor: crate::test_help::make_exec(),
            wal_config: WalConfig::test_config(),
            parquet_cache: Some(Arc::clone(&parquet_cache)),
            metric_registry: Default::default(),
        })
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

        // do two more writes to trigger a snapshot
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=2 20",
                Time::from_timestamp_nanos(124),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=3 30",
                Time::from_timestamp_nanos(125),
                false,
                Precision::Nanosecond,
            )
            .await;

        // query the buffer and make sure we get the data back
        let expected = [
            "+-----+--------------------------------+",
            "| bar | time                           |",
            "+-----+--------------------------------+",
            "| 1.0 | 1970-01-01T00:00:00.000000010Z |",
            "| 2.0 | 1970-01-01T00:00:00.000000020Z |",
            "| 3.0 | 1970-01-01T00:00:00.000000030Z |",
            "+-----+--------------------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // now load a new buffer from object storage
        let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _).unwrap();
        let meta_cache =
            MetaCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
                .unwrap();
        let write_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog,
            last_cache,
            meta_cache,
            time_provider,
            executor: crate::test_help::make_exec(),
            wal_config: WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(50),
                snapshot_size: 100,
            },
            parquet_cache: Some(Arc::clone(&parquet_cache)),
            metric_registry: Default::default(),
        })
        .await
        .unwrap();

        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn last_cache_create_and_delete_is_durable() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _ctx, time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;
        let db_name = "db";
        let db_id = DbId::from(0);
        let tbl_name = "table";
        let tbl_id = TableId::from(0);
        let cache_name = "cache";
        // Write some data to the current segment and update the catalog:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},t1=a f1=true").as_str(),
            Time::from_timestamp(20, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();
        // Create a last cache:
        wbuf.create_last_cache(db_id, tbl_id, Some(cache_name), None, None, None, None)
            .await
            .unwrap();

        let reload = || async {
            let persister = Arc::clone(&wbuf.persister);
            let time_provider = Arc::clone(&time_provider);
            let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
            let last_cache =
                LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _).unwrap();
            let meta_cache = MetaCacheProvider::new_from_catalog(
                Arc::clone(&time_provider),
                Arc::clone(&catalog),
            )
            .unwrap();
            WriteBufferImpl::new(WriteBufferImplArgs {
                persister: Arc::clone(&wbuf.persister),
                catalog,
                last_cache,
                meta_cache,
                time_provider,
                executor: Arc::clone(&wbuf.buffer.executor),
                wal_config: WalConfig {
                    gen1_duration: Gen1Duration::new_1m(),
                    max_write_buffer_size: 100,
                    flush_interval: Duration::from_millis(10),
                    snapshot_size: 1,
                },
                parquet_cache: wbuf.parquet_cache.clone(),
                metric_registry: Default::default(),
            })
            .await
            .unwrap()
        };

        // load a new write buffer to ensure its durable
        let wbuf = reload().await;

        let catalog_json = catalog_to_json(&wbuf.catalog);
        insta::assert_json_snapshot!("catalog-immediately-after-last-cache-create",
            catalog_json,
            { ".instance_id" => "[uuid]" }
        );

        // Do another write that will update the state of the catalog, specifically, the table
        // that the last cache was created for, and add a new field to the table/cache `f2`:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},t1=a f1=false,f2=42i").as_str(),
            Time::from_timestamp(30, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // and do another replay and verification
        let wbuf = reload().await;

        let catalog_json = catalog_to_json(&wbuf.catalog);
        insta::assert_json_snapshot!(
           "catalog-after-last-cache-create-and-new-field",
           catalog_json,
           { ".instance_id" => "[uuid]" }
        );

        // write a new data point to fill the cache
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},t1=a f1=true,f2=53i").as_str(),
            Time::from_timestamp(40, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // Fetch record batches from the last cache directly:
        let expected = [
            "+----+------+----+----------------------+",
            "| t1 | f1   | f2 | time                 |",
            "+----+------+----+----------------------+",
            "| a  | true | 53 | 1970-01-01T00:00:40Z |",
            "+----+------+----+----------------------+",
        ];
        let actual = wbuf
            .last_cache_provider()
            .get_cache_record_batches(db_id, tbl_id, None)
            .unwrap()
            .unwrap();
        assert_batches_eq!(&expected, &actual);
        // Delete the last cache:
        wbuf.delete_last_cache(db_id, tbl_id, cache_name)
            .await
            .unwrap();

        // do another reload and verify it's gone
        reload().await;

        let catalog_json = catalog_to_json(&wbuf.catalog);
        insta::assert_json_snapshot!("catalog-immediately-after-last-cache-delete",
            catalog_json,
            { ".instance_id" => "[uuid]" }
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returns_chunks_across_parquet_and_buffered_data() {
        let (write_buffer, session_context, time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::new(InMemory::new()),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
        )
        .await;

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=1",
                Time::from_timestamp(10, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_sorted_eq!(&expected, &actual);

        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=2",
                Time::from_timestamp(65, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // trigger snapshot with a third write, creating parquet files
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=3 147000000000",
                Time::from_timestamp(147, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        // give the snapshot some time to persist in the background
        let mut ticks = 0;
        loop {
            ticks += 1;
            let persisted = write_buffer.persister.load_snapshots(1000).await.unwrap();
            if !persisted.is_empty() {
                assert_eq!(persisted.len(), 1);
                assert_eq!(persisted[0].min_time, 10000000000);
                assert_eq!(persisted[0].row_count, 2);
                break;
            } else if ticks > 10 {
                panic!("not persisting");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 3.0 | 1970-01-01T00:02:27Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // now validate that buffered data and parquet data are all returned
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=4",
                Time::from_timestamp(250, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 3.0 | 1970-01-01T00:02:27Z |",
            "| 4.0 | 1970-01-01T00:04:10Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_sorted_eq!(&expected, &actual);

        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_sorted_eq!(&expected, &actual);
        // and now replay in a new write buffer and attempt to write
        let catalog = Arc::new(
            write_buffer
                .persister
                .load_or_create_catalog()
                .await
                .unwrap(),
        );
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _).unwrap();
        let meta_cache =
            MetaCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
                .unwrap();
        let write_buffer = WriteBufferImpl::new(WriteBufferImplArgs {
            persister: Arc::clone(&write_buffer.persister),
            catalog,
            last_cache,
            meta_cache,
            time_provider: Arc::clone(&write_buffer.time_provider),
            executor: Arc::clone(&write_buffer.buffer.executor),
            wal_config: WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
            parquet_cache: write_buffer.parquet_cache.clone(),
            metric_registry: Default::default(),
        })
        .await
        .unwrap();
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(
            runtime_env,
            "influxdb3",
            write_buffer.persister.object_store(),
        );

        // verify the data is still there
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &ctx).await;
        assert_batches_sorted_eq!(&expected, &actual);

        // now write some new data
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=5",
                Time::from_timestamp(300, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        // and write more to force another snapshot
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu bar=6",
                Time::from_timestamp(330, 0).unwrap(),
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let expected = [
            "+-----+----------------------+",
            "| bar | time                 |",
            "+-----+----------------------+",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "| 3.0 | 1970-01-01T00:02:27Z |",
            "| 4.0 | 1970-01-01T00:04:10Z |",
            "| 5.0 | 1970-01-01T00:05:00Z |",
            "| 6.0 | 1970-01-01T00:05:30Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &ctx).await;
        assert_batches_sorted_eq!(&expected, &actual);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_snapshots_only_if_updated() {
        let (write_buffer, _ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::new(InMemory::new()),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(5),
                snapshot_size: 1,
            },
        )
        .await;

        let db_name = "foo";
        // do three writes to force a snapshot
        do_writes(
            db_name,
            write_buffer.as_ref(),
            &[
                TestWrite {
                    lp: "cpu bar=1",
                    time_seconds: 10,
                },
                TestWrite {
                    lp: "cpu bar=2",
                    time_seconds: 20,
                },
                TestWrite {
                    lp: "cpu bar=3",
                    time_seconds: 30,
                },
            ],
        )
        .await;

        verify_catalog_count(2, write_buffer.persister.object_store()).await;
        verify_snapshot_count(1, &write_buffer.persister).await;

        // only another two writes are needed to trigger a snapshot, because there is still one
        // WAL period left from before:
        do_writes(
            db_name,
            write_buffer.as_ref(),
            &[
                TestWrite {
                    lp: "cpu bar=4",
                    time_seconds: 40,
                },
                TestWrite {
                    lp: "cpu bar=5",
                    time_seconds: 50,
                },
            ],
        )
        .await;

        // verify the catalog didn't get persisted, but a snapshot did
        verify_catalog_count(2, write_buffer.persister.object_store()).await;
        verify_snapshot_count(2, &write_buffer.persister).await;

        // and finally, do two more, with a catalog update, forcing persistence
        do_writes(
            db_name,
            write_buffer.as_ref(),
            &[
                TestWrite {
                    lp: "cpu bar=6,asdf=true",
                    time_seconds: 60,
                },
                TestWrite {
                    lp: "cpu bar=7,asdf=true",
                    time_seconds: 70,
                },
            ],
        )
        .await;

        verify_catalog_count(3, write_buffer.persister.object_store()).await;
        verify_snapshot_count(3, &write_buffer.persister).await;
    }

    /// Check that when a WriteBuffer is initialized with existing snapshot files, that newly
    /// generated snapshot files use the next sequence number.
    #[tokio::test]
    async fn new_snapshots_use_correct_sequence() {
        // set up a local file system object store:
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());

        // create a snapshot file that will be loaded on initialization of the write buffer:
        // Set ParquetFileId to a non zero number for the snapshot
        ParquetFileId::from(500).set_next_id();
        let prev_snapshot_seq = SnapshotSequenceNumber::new(42);
        let prev_snapshot = PersistedSnapshot::new(
            "test_host".to_string(),
            prev_snapshot_seq,
            WalFileSequenceNumber::new(0),
            CatalogSequenceNumber::new(0),
        );
        let snapshot_json = serde_json::to_vec(&prev_snapshot).unwrap();
        // set ParquetFileId to be 0 so that we can make sure when it's loaded from the
        // snapshot that it becomes the expected number
        ParquetFileId::from(0).set_next_id();

        // put the snapshot file in object store:
        object_store
            .put(
                &SnapshotInfoFilePath::new("test_host", prev_snapshot_seq),
                PutPayload::from_bytes(Bytes::from(snapshot_json)),
            )
            .await
            .unwrap();

        // setup the write buffer:
        let (wbuf, _ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(5),
                snapshot_size: 1,
            },
        )
        .await;

        // Assert that loading the snapshots sets ParquetFileId to the correct id number
        assert_eq!(ParquetFileId::new().as_u64(), 500);

        // there should be one snapshot already, i.e., the one we created above:
        verify_snapshot_count(1, &wbuf.persister).await;
        // there is only one initial catalog so far:
        verify_catalog_count(1, object_store.clone()).await;

        // do three writes to force a new snapshot
        wbuf.write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu bar=1",
            Time::from_timestamp(10, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();
        wbuf.write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu bar=2",
            Time::from_timestamp(20, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();
        wbuf.write_lp(
            NamespaceName::new("foo").unwrap(),
            "cpu bar=3",
            Time::from_timestamp(30, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // Check that there are now 2 snapshots:
        verify_snapshot_count(2, &wbuf.persister).await;
        // Check that the next sequence number is used for the new snapshot:
        assert_eq!(
            prev_snapshot_seq.next(),
            wbuf.wal.last_snapshot_sequence_number().await
        );
        // There should be a catalog now, since the above writes updated the catalog
        verify_catalog_count(2, object_store.clone()).await;
        // Check the catalog sequence number in the latest snapshot is correct:
        let persisted_snapshot_bytes = object_store
            .get(&SnapshotInfoFilePath::new(
                "test_host",
                prev_snapshot_seq.next(),
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let persisted_snapshot =
            serde_json::from_slice::<PersistedSnapshot>(&persisted_snapshot_bytes).unwrap();
        // NOTE: it appears that writes which create a new db increment the catalog sequence twice.
        // This is likely due to the catalog sequence being incremented first for the db creation and
        // then again for the updates to the table written to. Hence the sequence number is 2 here.
        // If we manage to make it so that scenario only increments the catalog sequence once, then
        // this assertion may fail:
        assert_eq!(
            CatalogSequenceNumber::new(2),
            persisted_snapshot.catalog_sequence_number
        );
    }

    #[tokio::test]
    async fn next_id_is_correct_number() {
        // set up a local file system object store:
        let object_store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(test_helpers::tmp_dir().unwrap()).unwrap());

        let prev_snapshot_seq = SnapshotSequenceNumber::new(42);
        let mut prev_snapshot = PersistedSnapshot::new(
            "test_host".to_string(),
            prev_snapshot_seq,
            WalFileSequenceNumber::new(0),
            CatalogSequenceNumber::new(0),
        );

        assert_eq!(prev_snapshot.next_file_id.as_u64(), 0);
        assert_eq!(prev_snapshot.next_table_id.as_u32(), 0);
        assert_eq!(prev_snapshot.next_db_id.as_u32(), 0);

        for _ in 0..=5 {
            prev_snapshot.add_parquet_file(
                DbId::from(0),
                TableId::from(0),
                ParquetFile {
                    id: ParquetFileId::new(),
                    path: "file/path2".into(),
                    size_bytes: 20,
                    row_count: 1,
                    chunk_time: 1,
                    min_time: 0,
                    max_time: 1,
                },
            );
        }

        assert_eq!(prev_snapshot.databases.len(), 1);
        let files = prev_snapshot.databases[&DbId::from(0)].tables[&TableId::from(0)].clone();

        // Assert that all of the files are smaller than the next_file_id field
        // and that their index corresponds to the order they were added in
        assert_eq!(prev_snapshot.next_file_id.as_u64(), 6);
        assert_eq!(files.len(), 6);
        for (i, file) in files.iter().enumerate() {
            assert_ne!(file.id, ParquetFileId::from(6));
            assert!(file.id.as_u64() < 6);
            assert_eq!(file.id.as_u64(), i as u64);
        }

        let snapshot_json = serde_json::to_vec(&prev_snapshot).unwrap();

        // put the snapshot file in object store:
        object_store
            .put(
                &SnapshotInfoFilePath::new("test_host", prev_snapshot_seq),
                PutPayload::from_bytes(Bytes::from(snapshot_json)),
            )
            .await
            .unwrap();

        // setup the write buffer:
        let (_wbuf, _ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(5),
                snapshot_size: 1,
            },
        )
        .await;

        // Test that the next_file_id has been set properly
        assert_eq!(ParquetFileId::next_id().as_u64(), 6);
    }

    /// This is the reproducer for [#25277][see]
    ///
    /// [see]: https://github.com/influxdata/influxdb/issues/25277
    #[tokio::test]
    async fn writes_not_dropped_on_snapshot() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: influxdb3_wal::Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (mut wbuf, mut ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            wal_config,
        )
        .await;

        let db_name = "coffee_shop";
        let tbl_name = "menu";

        // do some writes to get a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso price=2.50"),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=americano price=3.00"),
                    time_seconds: 2,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=latte price=4.50"),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // wait for snapshot to be created:
        verify_snapshot_count(1, &wbuf.persister).await;
        // initialize twice
        for _i in 0..2 {
            (wbuf, ctx, _) = setup(
                Time::from_timestamp_nanos(0),
                Arc::clone(&obj_store),
                wal_config,
            )
            .await;
        }

        // Get the record batches from replayed buffer:
        let batches = get_table_batches(&wbuf, db_name, tbl_name, &ctx).await;
        assert_batches_sorted_eq!(
            [
                "+-----------+-------+----------------------+",
                "| name      | price | time                 |",
                "+-----------+-------+----------------------+",
                "| americano | 3.0   | 1970-01-01T00:00:02Z |",
                "| espresso  | 2.5   | 1970-01-01T00:00:01Z |",
                "| latte     | 4.5   | 1970-01-01T00:00:03Z |",
                "+-----------+-------+----------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn writes_not_dropped_on_larger_snapshot_size() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
        )
        .await;

        let db_name = "coffee_shop";
        let tbl_name = "menu";

        // Do six writes to trigger a snapshot
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso,type=drink price=2.50"),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=americano,type=drink price=3.00"),
                    time_seconds: 2,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=latte,type=drink price=4.50"),
                    time_seconds: 3,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=croissant,type=food price=5.50"),
                    time_seconds: 4,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=muffin,type=food price=4.50"),
                    time_seconds: 5,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=biscotto,type=food price=3.00"),
                    time_seconds: 6,
                },
            ],
        )
        .await;

        verify_snapshot_count(1, &wbuf.persister).await;

        // Drop the write buffer, and create a new one that replays:
        drop(wbuf);
        let (wbuf, ctx, _time_provider) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
        )
        .await;

        // Get the record batches from replyed buffer:
        let batches = get_table_batches(&wbuf, db_name, tbl_name, &ctx).await;
        assert_batches_sorted_eq!(
            [
                "+-----------+-------+----------------------+-------+",
                "| name      | price | time                 | type  |",
                "+-----------+-------+----------------------+-------+",
                "| americano | 3.0   | 1970-01-01T00:00:02Z | drink |",
                "| biscotto  | 3.0   | 1970-01-01T00:00:06Z | food  |",
                "| croissant | 5.5   | 1970-01-01T00:00:04Z | food  |",
                "| espresso  | 2.5   | 1970-01-01T00:00:01Z | drink |",
                "| latte     | 4.5   | 1970-01-01T00:00:03Z | drink |",
                "| muffin    | 4.5   | 1970-01-01T00:00:05Z | food  |",
                "+-----------+-------+----------------------+-------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn writes_not_dropped_with_future_writes() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;

        let db_name = "coffee_shop";
        let tbl_name = "menu";

        // do some writes to get a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso price=2.50"),
                    time_seconds: 1,
                },
                // This write is way out in the future, so as to be outside the normal
                // range for a snapshot:
                TestWrite {
                    lp: format!("{tbl_name},name=americano price=3.00"),
                    time_seconds: 20_000,
                },
                // This write will trigger the snapshot:
                TestWrite {
                    lp: format!("{tbl_name},name=latte price=4.50"),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // Wait for snapshot to be created:
        verify_snapshot_count(1, &wbuf.persister).await;

        // Now drop the write buffer, and create a new one that replays:
        drop(wbuf);
        let (wbuf, ctx, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;

        // Get the record batches from replayed buffer:
        let batches = get_table_batches(&wbuf, db_name, tbl_name, &ctx).await;
        assert_batches_sorted_eq!(
            [
                "+-----------+-------+----------------------+",
                "| name      | price | time                 |",
                "+-----------+-------+----------------------+",
                "| americano | 3.0   | 1970-01-01T05:33:20Z |",
                "| espresso  | 2.5   | 1970-01-01T00:00:01Z |",
                "| latte     | 4.5   | 1970-01-01T00:00:03Z |",
                "+-----------+-------+----------------------+",
            ],
            &batches
        );
    }

    #[tokio::test]
    async fn notifies_watchers_of_snapshot() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;

        let mut watcher = wbuf.watch_persisted_snapshots();
        watcher.mark_changed();

        let db_name = "coffee_shop";
        let tbl_name = "menu";

        // do some writes to get a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso price=2.50"),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=americano price=3.00"),
                    time_seconds: 2,
                },
                TestWrite {
                    lp: format!("{tbl_name},name=latte price=4.50"),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // wait for snapshot to be created:
        verify_snapshot_count(1, &wbuf.persister).await;
        watcher.changed().await.unwrap();
        let snapshot = watcher.borrow();
        assert!(snapshot.is_some(), "watcher should be notified of snapshot");
    }

    #[tokio::test]
    async fn test_db_id_is_persisted_and_updated() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;
        let db_name = "coffee_shop";
        let tbl_name = "menu";

        // do some writes to get a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!("{tbl_name},name=espresso price=2.50"),
                    time_seconds: 1,
                },
                // This write is way out in the future, so as to be outside the normal
                // range for a snapshot:
                TestWrite {
                    lp: format!("{tbl_name},name=americano price=3.00"),
                    time_seconds: 20_000,
                },
                // This write will trigger the snapshot:
                TestWrite {
                    lp: format!("{tbl_name},name=latte price=4.50"),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // Wait for snapshot to be created:
        verify_snapshot_count(1, &wbuf.persister).await;

        // Now drop the write buffer, and create a new one that replays:
        drop(wbuf);

        // Set DbId to a large number to make sure it is properly set on replay
        // and assert that it's what we expect it to be before we replay
        dbg!(DbId::next_id());
        DbId::from(10_000).set_next_id();
        assert_eq!(DbId::next_id().as_u32(), 10_000);
        dbg!(DbId::next_id());
        let (_wbuf, _, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;
        dbg!(DbId::next_id());

        assert_eq!(DbId::next_id().as_u32(), 1);
    }

    #[tokio::test]
    async fn test_parquet_cache() {
        // set up a write buffer using a TestObjectStore so we can spy on requests that get
        // through to the object store for parquet files:
        let test_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let obj_store: Arc<dyn ObjectStore> = Arc::clone(&test_store) as _;
        let (wbuf, ctx, _) = setup_cache_optional(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
            true,
        )
        .await;
        let db_name = "my_corp";
        let db_id = DbId::from(0);
        let tbl_name = "temp";
        let tbl_id = TableId::from(0);

        // make some writes to generate a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=36\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=29\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=33\n\
                        "
                    ),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=37\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=28\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=32\n\
                        "
                    ),
                    time_seconds: 2,
                },
                // This write will trigger the snapshot:
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=35\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=24\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=30\n\
                        "
                    ),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // Wait for snapshot to be created, once this is done, then the parquet has been persisted:
        verify_snapshot_count(1, &wbuf.persister).await;

        // get the path for the created parquet file:
        let persisted_files = wbuf.persisted_files().get_files(db_id, tbl_id);
        assert_eq!(1, persisted_files.len());
        let path = ObjPath::from(persisted_files[0].path.as_str());

        // check the number of requests to that path before making a query:
        // there should be one get request, made by the cache oracle:
        assert_eq!(1, test_store.get_request_count(&path));
        assert_eq!(0, test_store.get_opts_request_count(&path));
        assert_eq!(0, test_store.get_ranges_request_count(&path));
        assert_eq!(0, test_store.get_range_request_count(&path));
        assert_eq!(0, test_store.head_request_count(&path));

        let batches = get_table_batches(&wbuf, db_name, tbl_name, &ctx).await;
        assert_batches_sorted_eq!(
            [
                "+--------+---------+------+----------------------+-----------+",
                "| device | reading | room | time                 | warehouse |",
                "+--------+---------+------+----------------------+-----------+",
                "| 10001  | 35.0    | 01a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10001  | 37.0    | 01a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 24.0    | 01b  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10002  | 28.0    | 01b  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:01Z | us-east   |",
                "| 30003  | 30.0    | 02a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 30003  | 32.0    | 02a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:01Z | us-east   |",
                "+--------+---------+------+----------------------+-----------+",
            ],
            &batches
        );

        // counts should not change, since requests for this parquet file hit the cache:
        assert_eq!(1, test_store.get_request_count(&path));
        assert_eq!(0, test_store.get_opts_request_count(&path));
        assert_eq!(0, test_store.get_ranges_request_count(&path));
        assert_eq!(0, test_store.get_range_request_count(&path));
        assert_eq!(0, test_store.head_request_count(&path));
    }
    #[tokio::test]
    async fn test_no_parquet_cache() {
        // set up a write buffer using a TestObjectStore so we can spy on requests that get
        // through to the object store for parquet files:
        let test_store = Arc::new(RequestCountedObjectStore::new(Arc::new(InMemory::new())));
        let obj_store: Arc<dyn ObjectStore> = Arc::clone(&test_store) as _;
        let (wbuf, ctx, _) = setup_cache_optional(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                gen1_duration: Gen1Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
            false,
        )
        .await;
        let db_name = "my_corp";
        let db_id = DbId::from(0);
        let tbl_name = "temp";
        let tbl_id = TableId::from(0);

        // make some writes to generate a snapshot:
        do_writes(
            db_name,
            wbuf.as_ref(),
            &[
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=36\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=29\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=33\n\
                        "
                    ),
                    time_seconds: 1,
                },
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=37\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=28\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=32\n\
                        "
                    ),
                    time_seconds: 2,
                },
                // This write will trigger the snapshot:
                TestWrite {
                    lp: format!(
                        "\
                        {tbl_name},warehouse=us-east,room=01a,device=10001 reading=35\n\
                        {tbl_name},warehouse=us-east,room=01b,device=10002 reading=24\n\
                        {tbl_name},warehouse=us-east,room=02a,device=30003 reading=30\n\
                        "
                    ),
                    time_seconds: 3,
                },
            ],
        )
        .await;

        // Wait for snapshot to be created, once this is done, then the parquet has been persisted:
        verify_snapshot_count(1, &wbuf.persister).await;

        // get the path for the created parquet file:
        let persisted_files = wbuf.persisted_files().get_files(db_id, tbl_id);
        assert_eq!(1, persisted_files.len());
        let path = ObjPath::from(persisted_files[0].path.as_str());

        // check the number of requests to that path before making a query:
        // there should be no get or get_range requests since nothing has asked for this file yet:
        assert_eq!(0, test_store.get_request_count(&path));
        assert_eq!(0, test_store.get_opts_request_count(&path));
        assert_eq!(0, test_store.get_ranges_request_count(&path));
        assert_eq!(0, test_store.get_range_request_count(&path));
        assert_eq!(0, test_store.head_request_count(&path));

        let batches = get_table_batches(&wbuf, db_name, tbl_name, &ctx).await;
        assert_batches_sorted_eq!(
            [
                "+--------+---------+------+----------------------+-----------+",
                "| device | reading | room | time                 | warehouse |",
                "+--------+---------+------+----------------------+-----------+",
                "| 10001  | 35.0    | 01a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10001  | 36.0    | 01a  | 1970-01-01T00:00:01Z | us-east   |",
                "| 10001  | 37.0    | 01a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 24.0    | 01b  | 1970-01-01T00:00:03Z | us-east   |",
                "| 10002  | 28.0    | 01b  | 1970-01-01T00:00:02Z | us-east   |",
                "| 10002  | 29.0    | 01b  | 1970-01-01T00:00:01Z | us-east   |",
                "| 30003  | 30.0    | 02a  | 1970-01-01T00:00:03Z | us-east   |",
                "| 30003  | 32.0    | 02a  | 1970-01-01T00:00:02Z | us-east   |",
                "| 30003  | 33.0    | 02a  | 1970-01-01T00:00:01Z | us-east   |",
                "+--------+---------+------+----------------------+-----------+",
            ],
            &batches
        );

        // counts should change, since requests for this parquet file were made with no cache:
        assert_eq!(0, test_store.get_request_count(&path));
        assert_eq!(0, test_store.get_opts_request_count(&path));
        assert_eq!(1, test_store.get_ranges_request_count(&path));
        assert_eq!(2, test_store.get_range_request_count(&path));
        assert_eq!(0, test_store.head_request_count(&path));
    }

    #[tokio::test]
    async fn test_delete_database() {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (write_buffer, _, _) =
            setup_cache_optional(start_time, test_store, wal_config, false).await;
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let result = write_buffer.soft_delete_database("foo".to_string()).await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_table() {
        let start_time = Time::from_rfc3339("2024-11-14T11:00:00+00:00").unwrap();
        let test_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            gen1_duration: Gen1Duration::new_1m(),
            max_write_buffer_size: 100,
            flush_interval: Duration::from_millis(10),
            snapshot_size: 1,
        };
        let (write_buffer, _, _) =
            setup_cache_optional(start_time, test_store, wal_config, false).await;
        let _ = write_buffer
            .write_lp(
                NamespaceName::new("foo").unwrap(),
                "cpu,warehouse=us-east,room=01a,device=10001 reading=37\n",
                start_time,
                false,
                Precision::Nanosecond,
            )
            .await
            .unwrap();

        let result = write_buffer
            .soft_delete_table("foo".to_string(), "cpu".to_string())
            .await;

        assert!(result.is_ok());
    }

    #[test_log::test(tokio::test)]
    async fn write_metrics() {
        let object_store = Arc::new(InMemory::new());
        let (buf, metrics) = setup_with_metrics(
            Time::from_timestamp_nanos(0),
            object_store,
            WalConfig::test_config(),
        )
        .await;
        let lines_observer = metrics
            .get_instrument::<Metric<U64Counter>>(WRITE_LINES_METRIC_NAME)
            .unwrap();
        let lines_rejected_observer = metrics
            .get_instrument::<Metric<U64Counter>>(WRITE_LINES_REJECTED_METRIC_NAME)
            .unwrap();
        let bytes_observer = metrics
            .get_instrument::<Metric<U64Counter>>(WRITE_BYTES_METRIC_NAME)
            .unwrap();

        let db_1 = "foo";
        let db_2 = "bar";

        // do a write and check the metrics:
        let lp = "\
            cpu,region=us,host=a usage=10\n\
            cpu,region=eu,host=b usage=10\n\
            cpu,region=ca,host=c usage=10\n\
            ";
        do_writes(
            db_1,
            buf.as_ref(),
            &[TestWrite {
                lp,
                time_seconds: 1,
            }],
        )
        .await;
        assert_eq!(
            3,
            lines_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );
        assert_eq!(
            0,
            lines_rejected_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );
        let mut bytes: usize = lp.lines().map(|l| l.len()).sum();
        assert_eq!(
            bytes as u64,
            bytes_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );

        // do another write to that db and check again for updates:
        let lp = "\
            mem,region=us,host=a used=1,swap=4\n\
            mem,region=eu,host=b used=1,swap=4\n\
            mem,region=ca,host=c used=1,swap=4\n\
            ";
        do_writes(
            db_1,
            buf.as_ref(),
            &[TestWrite {
                lp,
                time_seconds: 1,
            }],
        )
        .await;
        assert_eq!(
            6,
            lines_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );
        assert_eq!(
            0,
            lines_rejected_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );
        bytes += lp.lines().map(|l| l.len()).sum::<usize>();
        assert_eq!(
            bytes as u64,
            bytes_observer
                .get_observer(&Attributes::from(&[("db", db_1)]))
                .unwrap()
                .fetch()
        );

        // now do a write that will only be partially accepted to ensure that
        // the metrics are only calculated for writes that get accepted:

        // the legume will not be accepted, because it contains a new tag,
        // so should not be included in metric calculations:
        let lp = "\
            produce,type=fruit,name=banana price=1.50\n\
            produce,type=fruit,name=papaya price=5.50\n\
            produce,type=vegetable,name=lettuce price=1.00\n\
            produce,type=fruit,name=lentils,family=legume price=2.00\n\
            ";
        do_writes_partial(
            db_2,
            buf.as_ref(),
            &[TestWrite {
                lp,
                time_seconds: 1,
            }],
        )
        .await;
        assert_eq!(
            3,
            lines_observer
                .get_observer(&Attributes::from(&[("db", db_2)]))
                .unwrap()
                .fetch()
        );
        assert_eq!(
            1,
            lines_rejected_observer
                .get_observer(&Attributes::from(&[("db", db_2)]))
                .unwrap()
                .fetch()
        );
        // only take first three (valid) lines to get expected bytes:
        let bytes: usize = lp.lines().take(3).map(|l| l.len()).sum();
        assert_eq!(
            bytes as u64,
            bytes_observer
                .get_observer(&Attributes::from(&[("db", db_2)]))
                .unwrap()
                .fetch()
        );
    }

    struct TestWrite<LP> {
        lp: LP,
        time_seconds: i64,
    }

    async fn do_writes<W: WriteBuffer, LP: AsRef<str>>(
        db: &'static str,
        buffer: &W,
        writes: &[TestWrite<LP>],
    ) {
        for w in writes {
            buffer
                .write_lp(
                    NamespaceName::new(db).unwrap(),
                    w.lp.as_ref(),
                    Time::from_timestamp_nanos(w.time_seconds * 1_000_000_000),
                    false,
                    Precision::Nanosecond,
                )
                .await
                .unwrap();
        }
    }

    async fn do_writes_partial<W: WriteBuffer, LP: AsRef<str>>(
        db: &'static str,
        buffer: &W,
        writes: &[TestWrite<LP>],
    ) {
        for w in writes {
            buffer
                .write_lp(
                    NamespaceName::new(db).unwrap(),
                    w.lp.as_ref(),
                    Time::from_timestamp_nanos(w.time_seconds * 1_000_000_000),
                    true,
                    Precision::Nanosecond,
                )
                .await
                .unwrap();
        }
    }

    async fn verify_catalog_count(n: usize, object_store: Arc<dyn ObjectStore>) {
        let mut checks = 0;
        loop {
            let mut list = object_store.list(Some(&CatalogFilePath::dir("test_host")));
            let mut catalogs = vec![];
            while let Some(c) = list.next().await {
                catalogs.push(c.unwrap());
            }

            if catalogs.len() > n {
                panic!("checking for {} catalogs but found {}", n, catalogs.len());
            } else if catalogs.len() == n && checks > 5 {
                // let enough checks happen to ensure extra catalog persists aren't running in the background
                break;
            } else {
                checks += 1;
                if checks > 10 {
                    panic!("not persisting catalogs");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    async fn verify_snapshot_count(n: usize, persister: &Arc<Persister>) {
        let mut checks = 0;
        loop {
            let persisted_snapshots = persister.load_snapshots(1000).await.unwrap();
            if persisted_snapshots.len() > n {
                panic!(
                    "checking for {} snapshots but found {}",
                    n,
                    persisted_snapshots.len()
                );
            } else if persisted_snapshots.len() == n && checks > 5 {
                // let enough checks happen to ensure extra snapshots aren't running ion the background
                break;
            } else {
                checks += 1;
                if checks > 10 {
                    panic!("not persisting snapshots");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    fn catalog_to_json(catalog: &Catalog) -> serde_json::Value {
        let bytes = serde_json::to_vec_pretty(catalog).unwrap();
        serde_json::from_slice::<serde_json::Value>(&bytes).expect("parse bytes as JSON")
    }

    async fn setup(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
    ) -> (
        Arc<WriteBufferImpl>,
        IOxSessionContext,
        Arc<dyn TimeProvider>,
    ) {
        let (buf, ctx, time_provider, _metrics) =
            setup_inner(start, object_store, wal_config, true).await;
        (buf, ctx, time_provider)
    }

    async fn setup_cache_optional(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
        use_cache: bool,
    ) -> (
        Arc<WriteBufferImpl>,
        IOxSessionContext,
        Arc<dyn TimeProvider>,
    ) {
        let (buf, ctx, time_provider, _metrics) =
            setup_inner(start, object_store, wal_config, use_cache).await;
        (buf, ctx, time_provider)
    }

    async fn setup_with_metrics(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
    ) -> (Arc<WriteBufferImpl>, Arc<Registry>) {
        let (buf, _ctx, _time_provider, metrics) =
            setup_inner(start, object_store, wal_config, false).await;
        (buf, metrics)
    }

    async fn setup_inner(
        start: Time,
        object_store: Arc<dyn ObjectStore>,
        wal_config: WalConfig,
        use_cache: bool,
    ) -> (
        Arc<WriteBufferImpl>,
        IOxSessionContext,
        Arc<dyn TimeProvider>,
        Arc<Registry>,
    ) {
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start));
        let metric_registry = Arc::new(Registry::new());
        let (object_store, parquet_cache) = if use_cache {
            let (object_store, parquet_cache) = test_cached_obj_store_and_oracle(
                object_store,
                Arc::clone(&time_provider),
                Default::default(),
            );
            (object_store, Some(parquet_cache))
        } else {
            (object_store, None)
        };
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), "test_host"));
        let catalog = Arc::new(persister.load_or_create_catalog().await.unwrap());
        let last_cache = LastCacheProvider::new_from_catalog(Arc::clone(&catalog) as _).unwrap();
        let meta_cache =
            MetaCacheProvider::new_from_catalog(Arc::clone(&time_provider), Arc::clone(&catalog))
                .unwrap();
        let wbuf = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog,
            last_cache,
            meta_cache,
            time_provider: Arc::clone(&time_provider),
            executor: crate::test_help::make_exec(),
            wal_config,
            parquet_cache,
            metric_registry: Arc::clone(&metric_registry),
        })
        .await
        .unwrap();
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));
        (wbuf, ctx, time_provider, metric_registry)
    }

    async fn get_table_batches(
        write_buffer: &WriteBufferImpl,
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
