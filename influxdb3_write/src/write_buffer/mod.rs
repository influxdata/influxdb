//! Implementation of an in-memory buffer for writes that persists data into a wal if it is configured.

pub mod persisted_files;
mod queryable_buffer;
mod table_buffer;
pub(crate) mod validator;

use crate::cache::ParquetCache;
use crate::chunk::ParquetChunk;
use crate::last_cache::{self, CreateCacheArguments, LastCacheProvider};
use crate::persister::PersisterImpl;
use crate::write_buffer::persisted_files::PersistedFiles;
use crate::write_buffer::queryable_buffer::QueryableBuffer;
use crate::write_buffer::validator::WriteValidator;
use crate::{
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile, Persister,
    Precision, WriteBuffer, WriteLineError,
};
use async_trait::async_trait;
use data_types::{ChunkId, ChunkOrder, ColumnType, NamespaceName, NamespaceNameError};
use datafusion::common::DataFusionError;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::SendableRecordBatchStream;
use influxdb3_catalog::catalog::{Catalog, LastCacheDefinition};
use influxdb3_wal::object_store::WalObjectStore;
use influxdb3_wal::{Wal, WalConfig, WalFileNotifier, WalOp};
use iox_query::chunk_statistics::{create_chunk_statistics, NoColumnRanges};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use object_store::path::Path as ObjPath;
use object_store::{ObjectMeta, ObjectStore};
use observability_deps::tracing::{debug, error};
use parquet_file::storage::ParquetExecInput;
use schema::Schema;
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
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct WriteRequest<'a> {
    pub db_name: NamespaceName<'static>,
    pub line_protocol: &'a str,
    pub default_time: u64,
}

#[derive(Debug)]
pub struct WriteBufferImpl<T> {
    catalog: Arc<Catalog>,
    persister: Arc<PersisterImpl>,
    parquet_cache: Arc<ParquetCache>,
    persisted_files: Arc<PersistedFiles>,
    buffer: Arc<QueryableBuffer>,
    wal_config: WalConfig,
    wal: Arc<dyn Wal>,
    #[allow(dead_code)]
    time_provider: Arc<T>,
    last_cache: Arc<LastCacheProvider>,
}

impl<T: TimeProvider> WriteBufferImpl<T> {
    pub async fn new(
        persister: Arc<PersisterImpl>,
        time_provider: Arc<T>,
        executor: Arc<iox_query::exec::Executor>,
        wal_config: WalConfig,
    ) -> Result<Self> {
        let last_cache = Arc::new(LastCacheProvider::new());

        // load up the catalog, the snapshots, and replay the wal into the in memory buffer
        let catalog = persister.load_catalog().await?;
        let catalog = Arc::new(
            catalog
                .map(|c| Catalog::from_inner(c.catalog))
                .unwrap_or_else(Catalog::new),
        );
        let persisted_snapshots = persister.load_snapshots(1000).await?;
        let last_snapshot_wal_sequence = persisted_snapshots
            .first()
            .map(|s| s.wal_file_sequence_number);
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
        // teh background flush task.
        let wal = WalObjectStore::new(
            persister.object_store(),
            persister.host_identifier_prefix(),
            Arc::clone(&queryable_buffer) as Arc<dyn WalFileNotifier>,
            wal_config,
            last_snapshot_wal_sequence,
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
        let result = WriteValidator::initialize(db_name.clone(), self.catalog())?
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
        let result = WriteValidator::initialize(db_name.clone(), self.catalog())?
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
                self.persister.object_store_url(),
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
                object_store_url: self.persister.object_store_url(),
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
impl<T: TimeProvider> Bufferer for WriteBufferImpl<T> {
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
}

impl<T: TimeProvider> ChunkContainer for WriteBufferImpl<T> {
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
impl<T: TimeProvider> LastCacheManager for WriteBufferImpl<T> {
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
            let last_wal_file_number = self.wal.last_sequence_number().await;
            self.catalog.add_last_cache(db_name, tbl_name, info.clone());

            let inner_catalog = catalog.clone_inner();
            // Force persistence to the catalog, since we aren't going through the WAL:
            self.persister
                .persist_catalog(last_wal_file_number, Catalog::from_inner(inner_catalog))
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
        let catalog = self.catalog();
        self.last_cache
            .delete_cache(db_name, tbl_name, cache_name)?;
        catalog.delete_last_cache(db_name, tbl_name, cache_name);

        let last_wal_file_number = self.wal.last_sequence_number().await;
        // NOTE: if this fails then the cache will be gone from the running server, but will be
        // resurrected on server restart.
        let inner_catalog = catalog.clone_inner();
        // Force persistence to the catalog, since we aren't going through the WAL:
        self.persister
            .persist_catalog(last_wal_file_number, Catalog::from_inner(inner_catalog))
            .await?;
        Ok(())
    }
}

impl<T: TimeProvider> WriteBuffer for WriteBufferImpl<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::CatalogFilePath;
    use crate::persister::PersisterImpl;
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_eq;
    use datafusion_util::config::register_iox_object_store;
    use futures_util::StreamExt;
    use influxdb3_wal::Level0Duration;
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
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store), "test_host"));
        let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
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
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&persister),
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
    async fn persists_catalog_on_last_cache_create_and_delete() {
        let (wbuf, _ctx) = setup(
            Time::from_timestamp_nanos(0),
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
            Time::from_timestamp(30, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();
        // Create a last cache:
        wbuf.create_last_cache(db_name, tbl_name, Some(cache_name), None, None, None, None)
            .await
            .unwrap();
        // Check that the catalog was persisted, without advancing time:
        let object_store = wbuf.persister.object_store();
        let catalog_json = fetch_catalog_as_json(
            Arc::clone(&object_store),
            wbuf.persister.host_identifier_prefix(),
        )
        .await;
        insta::assert_json_snapshot!("catalog-immediately-after-last-cache-create", catalog_json);
        // Do another write that will update the state of the catalog, specifically, the table
        // that the last cache was created for, and add a new field to the table/cache `f2`:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},t1=a f1=true,f2=42i").as_str(),
            Time::from_timestamp(30, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();

        // do another write, which will force a snapshot of the WAL and thus the persistence of
        // the catalog
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},t1=b f1=false").as_str(),
            Time::from_timestamp(40, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();
        // Check the catalog again, to make sure it still has the last cache with the correct
        // configuration:
        let catalog_json = fetch_catalog_as_json(
            Arc::clone(&object_store),
            wbuf.persister.host_identifier_prefix(),
        )
        .await;
        // NOTE: the asserted snapshot is correct in-so-far as the catalog contains the last cache
        // configuration; however, it is not correct w.r.t. the fields. The second write adds a new
        // field `f2` to the last cache (which you can see in the query below), but the persisted
        // catalog does not have `f2` in the value columns. This will need to be fixed, see
        // https://github.com/influxdata/influxdb/issues/25171
        insta::assert_json_snapshot!(
            "catalog-after-allowing-time-to-persist-segments-after-create",
            catalog_json
        );
        // Fetch record batches from the last cache directly:
        let expected = [
            "+----+------+----------------------+----+",
            "| t1 | f1   | time                 | f2 |",
            "+----+------+----------------------+----+",
            "| a  | true | 1970-01-01T00:00:30Z | 42 |",
            "+----+------+----------------------+----+",
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
        // Catalog should be persisted, and no longer have the last cache, without advancing time:
        let catalog_json = fetch_catalog_as_json(
            Arc::clone(&object_store),
            wbuf.persister.host_identifier_prefix(),
        )
        .await;
        insta::assert_json_snapshot!("catalog-immediately-after-last-cache-delete", catalog_json);
        // Do another write so there is data to be persisted in the buffer:
        wbuf.write_lp(
            NamespaceName::new(db_name).unwrap(),
            format!("{tbl_name},t1=b f1=false,f2=1337i").as_str(),
            Time::from_timestamp(830, 0).unwrap(),
            false,
            Precision::Nanosecond,
        )
        .await
        .unwrap();
        // Advance time to allow for persistence of segment data:
        wbuf.time_provider
            .set(Time::from_timestamp(1600, 0).unwrap());
        let mut count = 0;
        loop {
            count += 1;
            tokio::time::sleep(Duration::from_millis(10)).await;
            if !wbuf.persisted_files.get_files(db_name, tbl_name).is_empty() {
                break;
            } else if count > 9 {
                panic!("not persisting");
            }
        }
        // Check the catalog again, to ensure the last cache is still gone:
        let catalog_json = fetch_catalog_as_json(
            Arc::clone(&object_store),
            wbuf.persister.host_identifier_prefix(),
        )
        .await;
        insta::assert_json_snapshot!(
            "catalog-after-allowing-time-to-persist-segments-after-delete",
            catalog_json
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn returns_chunks_across_parquet_and_buffered_data() {
        let (write_buffer, session_context) = setup(
            Time::from_timestamp_nanos(0),
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
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
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
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        let actual = get_table_batches(&write_buffer, "foo", "cpu", &session_context).await;
        assert_batches_eq!(&expected, &actual);

        // and now replay in a new write buffer and attempt to write
        let write_buffer = WriteBufferImpl::new(
            Arc::clone(&write_buffer.persister),
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
            "| 5.0 | 1970-01-01T00:05:00Z |",
            "| 6.0 | 1970-01-01T00:05:30Z |",
            "| 1.0 | 1970-01-01T00:00:10Z |",
            "| 2.0 | 1970-01-01T00:01:05Z |",
            "| 3.0 | 1970-01-01T00:02:27Z |",
            "| 4.0 | 1970-01-01T00:04:10Z |",
            "+-----+----------------------+",
        ];
        let actual = get_table_batches(&write_buffer, "foo", "cpu", &ctx).await;
        assert_batches_eq!(&expected, &actual);
    }

    async fn fetch_catalog_as_json(
        object_store: Arc<dyn ObjectStore>,
        host_identifier_prefix: &str,
    ) -> serde_json::Value {
        let mut list = object_store.list(Some(&CatalogFilePath::dir(host_identifier_prefix)));
        let Some(item) = list.next().await else {
            panic!("there should have been a catalog file persisted");
        };
        let item = item.expect("item from object store");
        let obj = object_store.get(&item.location).await.expect("get catalog");
        serde_json::from_slice::<serde_json::Value>(
            obj.bytes()
                .await
                .expect("get bytes from GetResult")
                .as_ref(),
        )
        .expect("parse bytes as JSON")
    }

    async fn setup(
        start: Time,
        wal_config: WalConfig,
    ) -> (WriteBufferImpl<MockProvider>, IOxSessionContext) {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let persister = Arc::new(PersisterImpl::new(Arc::clone(&object_store), "test_host"));
        let time_provider = Arc::new(MockProvider::new(start));
        let wbuf = WriteBufferImpl::new(
            Arc::clone(&persister),
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
        write_buffer: &WriteBufferImpl<MockProvider>,
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
