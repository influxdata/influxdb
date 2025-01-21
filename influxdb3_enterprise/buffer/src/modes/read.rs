use std::{sync::Arc, time::Duration};

use crate::replica::{CreateReplicasArgs, Replicas};
use anyhow::Context;
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{catalog::Session, error::DataFusionError, logical_expr::Expr};
use influxdb3_cache::distinct_cache::{CreateDistinctCacheArgs, DistinctCacheProvider};
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_cache::parquet_cache::ParquetCacheOracle;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_enterprise_compactor::compacted_data::CompactedData;
use influxdb3_id::{ColumnId, DbId, TableId};
use influxdb3_wal::{DistinctCacheDefinition, LastCacheDefinition, NoopWal, Wal};
use influxdb3_write::write_buffer::{self, parquet_chunk_from_file};
use influxdb3_write::{
    write_buffer::{Error as WriteBufferError, Result as WriteBufferResult},
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile,
    PersistedSnapshot, Precision, WriteBuffer,
};
use influxdb3_write::{BufferFilter, DatabaseManager, DistinctCacheManager};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use object_store::ObjectStore;
use tokio::sync::watch::Receiver;

#[derive(Debug)]
pub struct ReadMode {
    replicas: Replicas,
    compacted_data: Option<Arc<CompactedData>>,
}

#[derive(Debug)]
pub struct CreateReadModeArgs {
    pub last_cache: Arc<LastCacheProvider>,
    pub distinct_cache: Arc<DistinctCacheProvider>,
    pub object_store: Arc<dyn ObjectStore>,
    pub catalog: Arc<Catalog>,
    pub metric_registry: Arc<Registry>,
    pub replication_interval: Duration,
    pub writer_ids: Vec<String>,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub compacted_data: Option<Arc<CompactedData>>,
    pub time_provider: Arc<dyn TimeProvider>,
}

impl ReadMode {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        CreateReadModeArgs {
            last_cache,
            distinct_cache,
            object_store,
            catalog,
            metric_registry,
            replication_interval,
            writer_ids,
            parquet_cache,
            compacted_data,
            time_provider,
        }: CreateReadModeArgs,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            replicas: Replicas::new(CreateReplicasArgs {
                last_cache,
                distinct_cache,
                object_store,
                metric_registry,
                replication_interval,
                writer_ids,
                parquet_cache,
                catalog,
                time_provider,
                wal: None,
            })
            .await
            .context("failed to initialize replicas")?,
            compacted_data,
        })
    }
}

#[async_trait]
impl Bufferer for ReadMode {
    async fn write_lp(
        &self,
        _database: NamespaceName<'static>,
        _lp: &str,
        _ingest_time: Time,
        _accept_partial: bool,
        _precision: Precision,
    ) -> WriteBufferResult<BufferedWriteRequest> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.replicas.catalog())
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &BufferFilter,
    ) -> Vec<ParquetFile> {
        let mut files = self
            .replicas
            .parquet_files_filtered(db_id, table_id, filter);
        files.sort_unstable_by(|a, b| a.chunk_time.cmp(&b.chunk_time));
        files
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        unimplemented!("watch_persisted_snapshots not implemented for ReadMode")
    }

    fn wal(&self) -> Arc<dyn Wal> {
        Arc::new(NoopWal)
    }
}

impl ChunkContainer for ReadMode {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        _projection: Option<&Vec<usize>>,
        _ctx: &dyn Session,
    ) -> influxdb3_write::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let db_schema = self.catalog().db_schema(database_name).ok_or_else(|| {
            DataFusionError::Execution(format!("Database {} not found", database_name))
        })?;

        let table_def = db_schema
            .table_definition(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("Table {} not found", table_name)))?;

        let filter = BufferFilter::new(&table_def, filters)
            .map_err(|e| DataFusionError::Execution(format!("failed to evaluate filter: {e:?}")))?;

        let mut buffer_chunks = self
            .replicas
            .get_buffer_chunks(database_name, table_name, &filter)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        if let Some(compacted_data) = &self.compacted_data {
            let (parquet_files, writer_markers) = compacted_data
                .get_parquet_files_and_writer_markers(database_name, table_name, filters);

            buffer_chunks.extend(
                parquet_files
                    .into_iter()
                    .map(|file| {
                        Arc::new(parquet_chunk_from_file(
                            &file,
                            &table_def.schema,
                            self.replicas.object_store_url(),
                            self.replicas.object_store(),
                            buffer_chunks.len() as i64,
                        )) as Arc<dyn QueryChunk>
                    })
                    .collect::<Vec<_>>(),
            );

            let gen1_persisted_chunks = self.replicas.get_persisted_chunks(
                database_name,
                table_name,
                table_def.schema.clone(),
                &filter,
                &writer_markers,
                buffer_chunks.len() as i64,
            );
            buffer_chunks.extend(gen1_persisted_chunks);
        } else {
            let gen1_persisted_chunks = self.replicas.get_persisted_chunks(
                database_name,
                table_name,
                table_def.schema.clone(),
                &filter,
                &[],
                buffer_chunks.len() as i64,
            );

            buffer_chunks.extend(gen1_persisted_chunks);
        }

        Ok(buffer_chunks)
    }
}

#[async_trait::async_trait]
impl LastCacheManager for ReadMode {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        self.replicas.last_cache()
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_last_cache(
        &self,
        _db_id: DbId,
        _tbl_id: TableId,
        _cache_name: Option<&str>,
        _count: Option<usize>,
        _ttl: Option<Duration>,
        _key_columns: Option<Vec<ColumnId>>,
        _value_columns: Option<Vec<ColumnId>>,
    ) -> WriteBufferResult<Option<LastCacheDefinition>> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }

    async fn delete_last_cache(
        &self,
        _db_id: DbId,
        _tbl_id: TableId,
        _cache_name: &str,
    ) -> WriteBufferResult<()> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }
}

#[async_trait]
impl DistinctCacheManager for ReadMode {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        self.replicas.distinct_cache()
    }

    async fn create_distinct_cache(
        &self,
        _db_schema: Arc<DatabaseSchema>,
        _cache_name: Option<String>,
        _args: CreateDistinctCacheArgs,
    ) -> Result<Option<DistinctCacheDefinition>, WriteBufferError> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }

    async fn delete_distinct_cache(
        &self,
        _db_id: &DbId,
        _tbl_id: &TableId,
        _cache_name: &str,
    ) -> Result<(), WriteBufferError> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }
}

#[async_trait]
impl DatabaseManager for ReadMode {
    async fn create_database(&self, _name: String) -> Result<(), write_buffer::Error> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }

    async fn soft_delete_database(&self, _name: String) -> Result<(), WriteBufferError> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }

    async fn create_table(
        &self,
        _db: String,
        _table: String,
        _tags: Vec<String>,
        _fields: Vec<(String, String)>,
    ) -> Result<(), write_buffer::Error> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }

    async fn soft_delete_table(
        &self,
        _db_name: String,
        _table_name: String,
    ) -> Result<(), WriteBufferError> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }
}

impl WriteBuffer for ReadMode {}
