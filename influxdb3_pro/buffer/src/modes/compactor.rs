//! This mode for the write buffer no-ops internal APIs and returns errors
//! for user facing APIs.

use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::Expr;
use influxdb3_cache::{
    last_cache::LastCacheProvider,
    meta_cache::{CreateMetaCacheArgs, MetaCacheProvider},
};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_id::{ColumnId, DbId, TableId};
use influxdb3_wal::{LastCacheDefinition, MetaCacheDefinition};
use influxdb3_write::{
    write_buffer::{Error as WriteBufferError, Result as WriteBufferResult},
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile,
    PersistedSnapshot, Precision, WriteBuffer,
};
use influxdb3_write::{DatabaseManager, MetaCacheManager};
use iox_query::QueryChunk;
use iox_time::Time;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;

#[derive(Debug, Default, Clone, Copy)]
pub struct CompactorMode {}

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

    async fn write_lp_v3(
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
        unimplemented!("catalog not implemented for CompactorMode")
    }

    fn parquet_files(&self, _db_id: DbId, _table_id: TableId) -> Vec<ParquetFile> {
        vec![]
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        unimplemented!("watch_persisted_snapshots not implemented for CompactorMode")
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
impl MetaCacheManager for CompactorMode {
    fn meta_cache_provider(&self) -> Arc<MetaCacheProvider> {
        unimplemented!("meta_cache_provider not implemented for CompactorMode")
    }

    async fn create_meta_cache(
        &self,
        _db_schema: Arc<DatabaseSchema>,
        _cache_name: Option<String>,
        _args: CreateMetaCacheArgs,
    ) -> Result<Option<MetaCacheDefinition>, WriteBufferError> {
        unimplemented!("create_meta_cache not implemented for CompactorMode")
    }

    async fn delete_meta_cache(
        &self,
        _db_id: &DbId,
        _tbl_id: &TableId,
        _cache_name: &str,
    ) -> Result<(), WriteBufferError> {
        unimplemented!("delete_meta_cache not implemented for CompactorMode")
    }
}

#[async_trait]
impl DatabaseManager for CompactorMode {
    async fn soft_delete_database(&self, _name: String) -> Result<(), WriteBufferError> {
        unimplemented!("soft_delete_database not implemented for CompactorMode")
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
