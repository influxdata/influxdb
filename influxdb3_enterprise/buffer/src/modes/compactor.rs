//! This mode for the write buffer no-ops internal APIs and returns errors
//! for user facing APIs.

use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::catalog::Session;
use datafusion::common::DataFusionError;
use influxdb3_cache::{distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{NoopWal, Wal};
use influxdb3_write::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, LastCacheManager, ParquetFile,
    Precision, WriteBuffer,
    write_buffer::{Error as WriteBufferError, Result as WriteBufferResult},
};
use influxdb3_write::{DistinctCacheManager, PersistedSnapshotVersion};
use iox_query::QueryChunk;
use iox_time::Time;
use std::sync::Arc;
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
        _no_sync: bool,
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
        _filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        vec![]
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshotVersion>> {
        unimplemented!("watch_persisted_snapshots not implemented for CompactorMode")
    }

    fn wal(&self) -> Arc<dyn Wal> {
        Arc::new(NoopWal)
    }
}

impl ChunkContainer for CompactorMode {
    fn get_table_chunks(
        &self,
        _db_schema: Arc<DatabaseSchema>,
        _table_def: Arc<TableDefinition>,
        _filter: &ChunkFilter<'_>,
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
}

#[async_trait]
impl DistinctCacheManager for CompactorMode {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        unimplemented!("distinct_cache_provider not implemented for CompactorMode")
    }
}

impl WriteBuffer for CompactorMode {}
