//! Implementation of an in-memory buffer for writes that persists data into a wal if it is configured.

pub mod persisted_files;
pub mod queryable_buffer;
mod table_buffer;
pub(crate) mod validator;

use crate::cache::ParquetCache;
use crate::chunk::ParquetChunk;
use crate::last_cache::{self, CreateCacheArguments, LastCacheProvider};
use crate::persister::Persister;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::queryable_buffer::QueryableBuffer;
use crate::write_buffer::validator::WriteValidator;
use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile, Precision,
    WriteBuffer, WriteLineError, NEXT_FILE_ID,
};
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, ColumnType, NamespaceName, NamespaceNameError};
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use influxdb3_catalog::catalog::Catalog;
use influxdb3_wal::object_store::WalObjectStore;
use influxdb3_wal::CatalogOp::CreateLastCache;
use influxdb3_wal::{
    CatalogBatch, CatalogOp, LastCacheDefinition, LastCacheDelete, Wal, WalConfig, WalFileNotifier,
    WalOp,
};
use iox_query::chunk_statistics::{create_chunk_statistics, NoColumnRanges};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore};
use observability_deps::tracing::{debug, error};
use parquet_file::storage::ParquetExecInput;
use schema::Schema;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;

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

    #[error("tried accessing database and table that do not exist")]
    DbDoesNotExist,

    #[error("tried accessing database and table that do not exist")]
    TableDoesNotExist,

    #[error(
        "updating catalog on delete of last cache failed, you will need to delete the cache \
        again on server restart"
    )]
    DeleteLastCache(#[source] influxdb3_catalog::catalog::Error),

    #[error("error from wal: {0}")]
    WalError(#[from] influxdb3_wal::Error),

    #[error("cannot write to a read-only server")]
    NoWriteInReadOnly,
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
    parquet_cache: Arc<ParquetCache>,
    persisted_files: Arc<PersistedFiles>,
    buffer: Arc<QueryableBuffer>,
    wal_config: WalConfig,
    wal: Arc<dyn Wal>,
    time_provider: Arc<dyn TimeProvider>,
    last_cache: Arc<LastCacheProvider>,
}

const N_SNAPSHOTS_TO_LOAD_ON_START: usize = 1_000;

impl WriteBufferImpl {
    pub async fn new(
        persister: Arc<Persister>,
        catalog: Arc<Catalog>,
        last_cache: Arc<LastCacheProvider>,
        time_provider: Arc<dyn TimeProvider>,
        executor: Arc<iox_query::exec::Executor>,
        wal_config: WalConfig,
    ) -> Result<Self> {
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
        // Set the next file id to use when persisting ParquetFiles
        NEXT_FILE_ID.store(
            persisted_snapshots
                .first()
                .map(|s| s.next_file_id.as_u64())
                .unwrap_or(0),
            Ordering::SeqCst,
        );
        let persisted_files = Arc::new(PersistedFiles::new_from_persisted_snapshots(
            persisted_snapshots,
        ));
        let queryable_buffer = Arc::new(QueryableBuffer::new(
            executor,
            Arc::clone(&catalog),
            Arc::clone(&persister),
            Arc::clone(&last_cache),
            Arc::clone(&persisted_files),
        ));

        // create the wal instance, which will replay into the queryable buffer and start
        // the background flush task.
        let wal = WalObjectStore::new(
            persister.object_store(),
            persister.host_identifier_prefix(),
            Arc::clone(&queryable_buffer) as Arc<dyn WalFileNotifier>,
            wal_config,
            last_wal_sequence_number,
            last_snapshot_sequence_number,
        )
        .await?;

        Ok(Self {
            catalog,
            parquet_cache: Arc::new(ParquetCache::new(&persister.mem_pool)),
            persister,
            wal_config,
            wal,
            time_provider,
            last_cache,
            persisted_files,
            buffer: queryable_buffer,
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

        // validated lines will update the in-memory catalog, ensuring that all write operations
        // past this point will be infallible
        let result = WriteValidator::initialize(
            db_name.clone(),
            self.catalog(),
            ingest_time.timestamp_nanos(),
        )?
        .v1_parse_lines_and_update_schema(lp, accept_partial)?
        .convert_lines_to_buffer(ingest_time, self.wal_config.level_0_duration, precision);

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
        .v3_parse_lines_and_update_schema(lp, accept_partial)?
        .convert_lines_to_buffer(ingest_time, self.wal_config.level_0_duration, precision);

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

        let mut chunks = self.buffer.get_table_chunks(
            Arc::clone(&db_schema),
            table_name,
            filters,
            projection,
            ctx,
        )?;
        let parquet_files = self.persisted_files.get_files(database_name, table_name);

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
                object_store_url: self.persister.object_store_url().clone(),
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
}

pub(crate) fn parquet_chunk_from_file(
    parquet_file: &ParquetFile,
    table_schema: &Schema,
    object_store_url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
    chunk_order: i64,
) -> ParquetChunk {
    let partition_key = data_types::PartitionKey::from(parquet_file.chunk_time.to_string());
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

    fn parquet_files(&self, db_name: &str, table_name: &str) -> Vec<ParquetFile> {
        self.buffer.persisted_parquet_files(db_name, table_name)
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
        db_name: &str,
        tbl_name: &str,
        cache_name: Option<&str>,
        count: Option<usize>,
        ttl: Option<Duration>,
        key_columns: Option<Vec<String>>,
        value_columns: Option<Vec<String>>,
    ) -> Result<Option<LastCacheDefinition>, Error> {
        let cache_name = cache_name.map(Into::into);
        let catalog = self.catalog();
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
            self.catalog.add_last_cache(db_name, tbl_name, info.clone());
            let add_cache_catalog_batch = WalOp::Catalog(CatalogBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_name: Arc::clone(&db_schema.name),
                ops: vec![CreateLastCache(info.clone())],
            });
            self.wal.write_ops(vec![add_cache_catalog_batch]).await?;

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
        let catalog = self.catalog();
        self.last_cache
            .delete_cache(db_name, tbl_name, cache_name)?;
        catalog.delete_last_cache(db_name, tbl_name, cache_name);

        // NOTE: if this fails then the cache will be gone from the running server, but will be
        // resurrected on server restart.
        self.wal
            .write_ops(vec![WalOp::Catalog(CatalogBatch {
                time_ns: self.time_provider.now().timestamp_nanos(),
                database_name: db_name.into(),
                ops: vec![CatalogOp::DeleteLastCache(LastCacheDelete {
                    table: tbl_name.into(),
                    name: cache_name.into(),
                })],
            })])
            .await?;

        Ok(())
    }
}

impl WriteBuffer for WriteBufferImpl {}

#[cfg(test)]
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
    use influxdb3_catalog::catalog::SequenceNumber;
    use influxdb3_wal::{Level0Duration, SnapshotSequenceNumber, WalFileSequenceNumber};
    use iox_query::exec::IOxSessionContext;
    use iox_time::{MockProvider, Time};
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, PutPayload};

    #[test]
    fn parse_lp_into_buffer() {
        let catalog = Arc::new(Catalog::new());
        let db_name = NamespaceName::new("foo").unwrap();
        let lp = "cpu,region=west user=23.2 100\nfoo f1=1i";
        WriteValidator::initialize(db_name, Arc::clone(&catalog), 0)
            .unwrap()
            .v1_parse_lines_and_update_schema(lp, false)
            .unwrap()
            .convert_lines_to_buffer(
                Time::from_timestamp_nanos(0),
                Level0Duration::new_5m(),
                Precision::Nanosecond,
            );

        let db = catalog.db_schema("foo").unwrap();

        assert_eq!(db.tables.len(), 2);
        assert_eq!(db.tables.get("cpu").unwrap().num_columns(), 3);
        assert_eq!(db.tables.get("foo").unwrap().num_columns(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_data_to_wal_and_is_queryable() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), "test_host"));
        let (last_cache, catalog) = persister.load_last_cache_and_catalog().await.unwrap();
        let time_provider: Arc<dyn TimeProvider> =
            Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(catalog),
            Arc::new(last_cache),
            Arc::clone(&time_provider),
            crate::test_help::make_exec(),
            WalConfig::test_config(),
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
        let (last_cache, catalog) = persister.load_last_cache_and_catalog().await.unwrap();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(catalog),
            Arc::new(last_cache),
            Arc::clone(&time_provider),
            crate::test_help::make_exec(),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(50),
                snapshot_size: 100,
            },
        )
        .await
        .unwrap();

        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);
    }

    #[tokio::test]
    async fn last_cache_create_and_delete_is_durable() {
        let (wbuf, _ctx) = setup(
            Time::from_timestamp_nanos(0),
            Arc::new(InMemory::new()),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await;
        let db_name = "db";
        let tbl_name = "table";
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
        wbuf.create_last_cache(db_name, tbl_name, Some(cache_name), None, None, None, None)
            .await
            .unwrap();

        // load a new write buffer to ensure its durable
        let (last_cache, catalog) = wbuf.persister.load_last_cache_and_catalog().await.unwrap();
        let wbuf = WriteBufferImpl::new(
            Arc::clone(&wbuf.persister),
            Arc::new(catalog),
            Arc::new(last_cache),
            Arc::clone(&wbuf.time_provider),
            Arc::clone(&wbuf.buffer.executor),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await
        .unwrap();

        let catalog_json = catalog_to_json(&wbuf.catalog);
        insta::assert_json_snapshot!("catalog-immediately-after-last-cache-create", catalog_json);

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
        let (last_cache, catalog) = wbuf.persister.load_last_cache_and_catalog().await.unwrap();
        let wbuf = WriteBufferImpl::new(
            Arc::clone(&wbuf.persister),
            Arc::new(catalog),
            Arc::new(last_cache),
            Arc::clone(&wbuf.time_provider),
            Arc::clone(&wbuf.buffer.executor),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await
        .unwrap();

        let catalog_json = catalog_to_json(&wbuf.catalog);
        insta::assert_json_snapshot!(
            "catalog-after-last-cache-create-and-new-field",
            catalog_json
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
            .get_cache_record_batches(db_name, tbl_name, None, &[])
            .unwrap()
            .unwrap();
        assert_batches_eq!(&expected, &actual);
        // Delete the last cache:
        wbuf.delete_last_cache(db_name, tbl_name, cache_name)
            .await
            .unwrap();

        // do another reload and verify it's gone
        let (last_cache, catalog) = wbuf.persister.load_last_cache_and_catalog().await.unwrap();
        let wbuf = WriteBufferImpl::new(
            Arc::clone(&wbuf.persister),
            Arc::new(catalog),
            Arc::new(last_cache),
            Arc::clone(&wbuf.time_provider),
            Arc::clone(&wbuf.buffer.executor),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 1,
            },
        )
        .await
        .unwrap();
        let catalog_json = catalog_to_json(&wbuf.catalog);
        insta::assert_json_snapshot!("catalog-immediately-after-last-cache-delete", catalog_json);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returns_chunks_across_parquet_and_buffered_data() {
        let (write_buffer, session_context) = setup(
            Time::from_timestamp_nanos(0),
            Arc::new(InMemory::new()),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
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
        assert_batches_eq!(&expected, &actual);

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
        assert_batches_eq!(&expected, &actual);

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
        assert_batches_eq!(&expected, &actual);

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
        assert_batches_eq!(&expected, &actual);

        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // and now replay in a new write buffer and attempt to write
        let (last_cache, catalog) = write_buffer
            .persister
            .load_last_cache_and_catalog()
            .await
            .unwrap();
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&write_buffer.persister),
            Arc::new(catalog),
            Arc::new(last_cache),
            Arc::clone(&write_buffer.time_provider),
            Arc::clone(&write_buffer.buffer.executor),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(10),
                snapshot_size: 2,
            },
        )
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
        assert_batches_eq!(&expected, &actual);

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
        let (write_buffer, _ctx) = setup(
            Time::from_timestamp_nanos(0),
            Arc::new(InMemory::new()),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
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
            &write_buffer,
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

        verify_catalog_count(1, write_buffer.persister.object_store()).await;
        verify_snapshot_count(1, &write_buffer.persister).await;

        // only another two writes are needed to trigger a snapshot, because there is still one
        // WAL period left from before:
        do_writes(
            db_name,
            &write_buffer,
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
        verify_catalog_count(1, write_buffer.persister.object_store()).await;
        verify_snapshot_count(2, &write_buffer.persister).await;

        // and finally, do two more, with a catalog update, forcing persistence
        do_writes(
            db_name,
            &write_buffer,
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

        verify_catalog_count(2, write_buffer.persister.object_store()).await;
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
        // Set NEXT_FILE_ID to a non zero number for the snapshot
        NEXT_FILE_ID.store(500, Ordering::SeqCst);
        let prev_snapshot_seq = SnapshotSequenceNumber::new(42);
        let prev_snapshot = PersistedSnapshot::new(
            prev_snapshot_seq,
            WalFileSequenceNumber::new(0),
            SequenceNumber::new(0),
        );
        let snapshot_json = serde_json::to_vec(&prev_snapshot).unwrap();
        // set NEXT_FILE_ID to be 0 so that we can make sure when it's loaded from the
        // snapshot that it becomes the expected number
        NEXT_FILE_ID.store(0, Ordering::SeqCst);

        // put the snapshot file in object store:
        object_store
            .put(
                &SnapshotInfoFilePath::new("test_host", prev_snapshot_seq),
                PutPayload::from_bytes(Bytes::from(snapshot_json)),
            )
            .await
            .unwrap();

        // setup the write buffer:
        let (wbuf, _ctx) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&object_store),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
                max_write_buffer_size: 100,
                flush_interval: Duration::from_millis(5),
                snapshot_size: 1,
            },
        )
        .await;

        // Assert that loading the snapshots sets NEXT_FILE_ID to the correct id number
        assert_eq!(NEXT_FILE_ID.load(Ordering::SeqCst), 500);

        // there should be one snapshot already, i.e., the one we created above:
        verify_snapshot_count(1, &wbuf.persister).await;
        // there aren't any catalogs yet:
        verify_catalog_count(0, object_store.clone()).await;

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
        verify_catalog_count(1, object_store.clone()).await;
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
            SequenceNumber::new(2),
            persisted_snapshot.catalog_sequence_number
        );
    }

    /// This is the reproducer for [#25277][see]
    ///
    /// [see]: https://github.com/influxdata/influxdb/issues/25277
    #[tokio::test]
    async fn writes_not_dropped_on_snapshot() {
        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (wbuf, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
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
            &wbuf,
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

        // Now drop the write buffer, and create a new one that replays:
        drop(wbuf);
        let (wbuf, ctx) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
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
        let (wbuf, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
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
            &wbuf,
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
        let (wbuf, ctx) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
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
        let (wbuf, _) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
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
            &wbuf,
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
        let (wbuf, ctx) = setup(
            Time::from_timestamp_nanos(0),
            Arc::clone(&obj_store),
            WalConfig {
                level_0_duration: Level0Duration::new_1m(),
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
    ) -> (WriteBufferImpl, IOxSessionContext) {
        let persister = Arc::new(Persister::new(Arc::clone(&object_store), "test_host"));
        let time_provider: Arc<dyn TimeProvider> = Arc::new(MockProvider::new(start));
        let (last_cache, catalog) = persister.load_last_cache_and_catalog().await.unwrap();
        let wbuf = WriteBufferImpl::new(
            Arc::clone(&persister),
            Arc::new(catalog),
            Arc::new(last_cache),
            Arc::clone(&time_provider),
            crate::test_help::make_exec(),
            wal_config,
        )
        .await
        .unwrap();
        let ctx = IOxSessionContext::with_testing();
        let runtime_env = ctx.inner().runtime_env();
        register_iox_object_store(runtime_env, "influxdb3", Arc::clone(&object_store));
        (wbuf, ctx)
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
