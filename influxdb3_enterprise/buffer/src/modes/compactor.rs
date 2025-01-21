//! This mode for the write buffer no-ops internal APIs and returns errors
//! for user facing APIs.

use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::Expr;
use influxdb3_cache::{
    distinct_cache::{CreateDistinctCacheArgs, DistinctCacheProvider},
    last_cache::LastCacheProvider,
};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_id::{ColumnId, DbId, TableId};
use influxdb3_wal::{DistinctCacheDefinition, LastCacheDefinition, NoopWal, Wal};
use influxdb3_write::{
    write_buffer::{self, Error as WriteBufferError, Result as WriteBufferResult},
    BufferFilter, BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile,
    PersistedSnapshot, Precision, WriteBuffer,
};
use influxdb3_write::{DatabaseManager, DistinctCacheManager};
use iox_query::QueryChunk;
use iox_time::Time;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;

#[derive(Debug, Clone)]
pub struct CompactorMode {
    /// The compactor's catalog, which is the union of writer catalogs being compacted
    catalog: Arc<Catalog>,
}

impl CompactorMode {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl Bufferer for CompactorMode {
    async fn write_lp(
        &self,
        _database: NamespaceName<'static>,
        _lp: &str,
        _ingest_time: Time,
        _accept_partial: bool,
        _precision: Precision,
    ) -> WriteBufferResult<BufferedWriteRequest> {
        Err(WriteBufferError::NoWriteInCompactorOnly)
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    fn parquet_files_filtered(
        &self,
        _db_id: DbId,
        _table_id: TableId,
        _filter: &BufferFilter,
    ) -> Vec<ParquetFile> {
        vec![]
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        unimplemented!("watch_persisted_snapshots not implemented for CompactorMode")
    }

    fn wal(&self) -> Arc<dyn Wal> {
        Arc::new(NoopWal)
    }
}

impl ChunkContainer for CompactorMode {
    fn get_table_chunks(
        &self,
        _database_name: &str,
        _table_name: &str,
        _filters: &[Expr],
        _projection: Option<&Vec<usize>>,
        _ctx: &dyn Session,
    ) -> influxdb3_write::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        Err(DataFusionError::Execution(
            "queries not supported in compactor only mode".to_string(),
        ))
    }
}

#[async_trait]
impl LastCacheManager for CompactorMode {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        unimplemented!("last_cache_provider not implemented for CompactorMode")
    }

    async fn create_last_cache(
        &self,
        _db_id: DbId,
        _tbl_id: TableId,
        _cache_name: Option<&str>,
        _count: Option<usize>,
        _ttl: Option<Duration>,
        _key_columns: Option<Vec<ColumnId>>,
        _value_columns: Option<Vec<ColumnId>>,
    ) -> influxdb3_write::Result<Option<LastCacheDefinition>, WriteBufferError> {
        unimplemented!("create_last_cache not implemented for CompactorMode")
    }

    async fn delete_last_cache(
        &self,
        _db_id: DbId,
        _tbl_id: TableId,
        _cache_name: &str,
    ) -> influxdb3_write::Result<(), WriteBufferError> {
        unimplemented!("delete_last_cache not implemented for CompactorMode")
    }
}

#[async_trait]
impl DistinctCacheManager for CompactorMode {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        unimplemented!("distinct_cache_provider not implemented for CompactorMode")
    }

    async fn create_distinct_cache(
        &self,
        _db_schema: Arc<DatabaseSchema>,
        _cache_name: Option<String>,
        _args: CreateDistinctCacheArgs,
    ) -> Result<Option<DistinctCacheDefinition>, WriteBufferError> {
        unimplemented!("create_distinct_cache not implemented for CompactorMode")
    }

    async fn delete_distinct_cache(
        &self,
        _db_id: &DbId,
        _tbl_id: &TableId,
        _cache_name: &str,
    ) -> Result<(), WriteBufferError> {
        unimplemented!("delete_distinct_cache not implemented for CompactorMode")
    }
}

#[async_trait]
impl DatabaseManager for CompactorMode {
    async fn create_database(&self, _name: String) -> Result<(), write_buffer::Error> {
        unimplemented!("create_database not implemented for CompactorMode")
    }
    async fn soft_delete_database(&self, _name: String) -> Result<(), WriteBufferError> {
        unimplemented!("soft_delete_database not implemented for CompactorMode")
    }

    async fn create_table(
        &self,
        _db: String,
        _table: String,
        _tags: Vec<String>,
        _fields: Vec<(String, String)>,
    ) -> Result<(), write_buffer::Error> {
        unimplemented!("create_table not implemented for CompactorMode")
    }
    async fn soft_delete_table(
        &self,
        _db_name: String,
        _table_name: String,
    ) -> Result<(), WriteBufferError> {
        unimplemented!("soft_delete_table not implemented for CompactorMode")
    }
}

impl WriteBuffer for CompactorMode {}
