use std::{sync::Arc, time::Duration};

use crate::replica::{CreateReplicasArgs, Replicas};
use anyhow::Context;
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{catalog::Session, error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_wal::LastCacheDefinition;
use influxdb3_write::{
    last_cache::LastCacheProvider,
    parquet_cache::ParquetCacheOracle,
    write_buffer::{Error as WriteBufferError, Result as WriteBufferResult},
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile,
    PersistedSnapshot, Precision, WriteBuffer,
};
use iox_query::QueryChunk;
use iox_time::Time;
use metric::Registry;
use object_store::ObjectStore;
use tokio::sync::watch::Receiver;

#[derive(Debug)]
pub struct ReadMode {
    replicas: Replicas,
    /// Unified snapshot channel for all replicas
    persisted_snapshot_notify_rx: Receiver<Option<PersistedSnapshot>>,
}

impl ReadMode {
    pub(crate) async fn new(
        catalog: Arc<Catalog>,
        last_cache: Arc<LastCacheProvider>,
        object_store: Arc<dyn ObjectStore>,
        metric_registry: Arc<Registry>,
        replication_interval: Duration,
        hosts: Vec<String>,
        parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    ) -> Result<Self, anyhow::Error> {
        let (persisted_snapshot_notify_tx, persisted_snapshot_notify_rx) =
            tokio::sync::watch::channel(None);

        Ok(Self {
            persisted_snapshot_notify_rx,
            replicas: Replicas::new(CreateReplicasArgs {
                catalog,
                last_cache,
                object_store,
                metric_registry,
                replication_interval,
                hosts,
                persisted_snapshot_notify_tx,
                parquet_cache,
            })
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

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        self.persisted_snapshot_notify_rx.clone()
    }
}

impl ChunkContainer for ReadMode {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
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
