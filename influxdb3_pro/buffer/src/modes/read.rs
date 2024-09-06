use std::{sync::Arc, time::Duration};

use anyhow::Context;
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{error::DataFusionError, execution::context::SessionState, logical_expr::Expr};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_wal::LastCacheDefinition;
use influxdb3_write::{
    last_cache::LastCacheProvider,
    write_buffer::{Error as WriteBufferError, Result as WriteBufferResult},
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile, Precision,
    WriteBuffer,
};
use iox_query::QueryChunk;
use iox_time::Time;
use object_store::ObjectStore;

use crate::replica::Replicas;

#[derive(Debug)]
pub struct ReadMode {
    replicas: Replicas,
}

impl ReadMode {
    pub(crate) async fn new(
        catalog: Arc<Catalog>,
        last_cache: Arc<LastCacheProvider>,
        object_store: Arc<dyn ObjectStore>,
        replication_interval: Duration,
        hosts: Vec<String>,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            replicas: Replicas::new(
                catalog,
                last_cache,
                object_store,
                replication_interval,
                hosts,
            )
            .await
            .context("failed to initialize replicas")?,
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

    async fn write_lp_v3(
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
        self.replicas.catalog()
    }

    fn parquet_files(&self, db_name: &str, table_name: &str) -> Vec<ParquetFile> {
        let mut files = self.replicas.parquet_files(db_name, table_name);
        files.sort_unstable_by(|a, b| a.chunk_time.cmp(&b.chunk_time));
        files
    }
}

impl ChunkContainer for ReadMode {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &SessionState,
    ) -> influxdb3_write::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        self.replicas
            .get_table_chunks(database_name, table_name, filters, projection, ctx)
            .map_err(|e| DataFusionError::Execution(e.to_string()))
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
        _db_name: &str,
        _tbl_name: &str,
        _cache_name: Option<&str>,
        _count: Option<usize>,
        _ttl: Option<Duration>,
        _key_columns: Option<Vec<String>>,
        _value_columns: Option<Vec<String>>,
    ) -> WriteBufferResult<Option<LastCacheDefinition>> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }

    async fn delete_last_cache(
        &self,
        _db_name: &str,
        _tbl_name: &str,
        _cache_name: &str,
    ) -> WriteBufferResult<()> {
        Err(WriteBufferError::NoWriteInReadOnly)
    }
}

impl WriteBuffer for ReadMode {}
