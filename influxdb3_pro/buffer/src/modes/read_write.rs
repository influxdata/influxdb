use std::{sync::Arc, time::Duration};

use crate::replica::{CreateReplicasArgs, Replicas, ReplicationConfig};
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{catalog::Session, error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::catalog::Catalog;
use influxdb3_wal::{LastCacheDefinition, WalConfig};
use influxdb3_write::{
    last_cache::LastCacheProvider,
    parquet_cache::ParquetCacheOracle,
    persister::Persister,
    write_buffer::{self, WriteBufferImpl},
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile,
    PersistedSnapshot, Precision, WriteBuffer,
};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use tokio::sync::watch::Receiver;

#[derive(Debug)]
pub struct ReadWriteMode {
    primary: WriteBufferImpl,
    replicas: Option<Replicas>,
    /// Unified snapshot channels for primary and all replicas
    persisted_snapshot_notify_rx: Receiver<Option<PersistedSnapshot>>,
}

#[derive(Debug)]
pub struct ReadWriteArgs {
    pub persister: Arc<Persister>,
    pub catalog: Arc<Catalog>,
    pub last_cache: Arc<LastCacheProvider>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub executor: Arc<iox_query::exec::Executor>,
    pub wal_config: WalConfig,
    pub metric_registry: Arc<Registry>,
    pub replication_config: Option<ReplicationConfig>,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
}

impl ReadWriteMode {
    pub(crate) async fn new(
        ReadWriteArgs {
            persister,
            catalog,
            last_cache,
            time_provider,
            executor,
            wal_config,
            metric_registry,
            replication_config,
            parquet_cache,
        }: ReadWriteArgs,
    ) -> Result<Self, anyhow::Error> {
        let object_store = persister.object_store();
        let primary = WriteBufferImpl::new(
            persister,
            Arc::clone(&catalog),
            Arc::clone(&last_cache),
            time_provider,
            executor,
            wal_config,
            None,
        )
        .await?;

        let (persisted_snapshot_notify_tx, persisted_snapshot_notify_rx) =
            tokio::sync::watch::channel(None);

        // spawn tokio task to watch for persisted snapshots on primary and send to unified watch
        let mut primary_persisted_snapshot_notify_rx = primary.watch_persisted_snapshots();
        let persisted_snapshot_combined_notify_tx = persisted_snapshot_notify_tx.clone();
        tokio::spawn(async move {
            while primary_persisted_snapshot_notify_rx.changed().await.is_ok() {
                persisted_snapshot_combined_notify_tx
                    .send(
                        primary_persisted_snapshot_notify_rx
                            .borrow_and_update()
                            .clone(),
                    )
                    .expect("watch failed");
            }
        });

        let replicas = if let Some(ReplicationConfig {
            interval: replication_interval,
            hosts,
        }) = replication_config
        {
            Some(
                Replicas::new(CreateReplicasArgs {
                    catalog,
                    last_cache,
                    object_store,
                    metric_registry,
                    replication_interval,
                    hosts,
                    persisted_snapshot_notify_tx,
                    parquet_cache,
                })
                .await?,
            )
        } else {
            None
        };
        Ok(Self {
            primary,
            replicas,
            persisted_snapshot_notify_rx,
        })
    }
}

#[async_trait]
impl Bufferer for ReadWriteMode {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> write_buffer::Result<BufferedWriteRequest> {
        // Writes go to the primary buffer, so this only relies on that
        self.primary
            .write_lp(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    async fn write_lp_v3(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
    ) -> write_buffer::Result<BufferedWriteRequest> {
        // Writes go to the primary buffer, so this only relies on that
        self.primary
            .write_lp_v3(database, lp, ingest_time, accept_partial, precision)
            .await
    }

    fn catalog(&self) -> Arc<Catalog> {
        self.primary.catalog()
    }

    fn parquet_files(&self, db_name: &str, table_name: &str) -> Vec<ParquetFile> {
        // Parquet files need to be retrieved across primary and replicas
        // TODO: could this fall into another trait?
        let mut files = self.primary.parquet_files(db_name, table_name);
        if let Some(replicas) = &self.replicas {
            files.append(&mut replicas.parquet_files(db_name, table_name));
        }
        // NOTE: do we need to sort this since this is used by the system tables and the query
        // executor could sort if desired...
        files.sort_unstable_by(|a, b| a.chunk_time.cmp(&b.chunk_time));
        files
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        self.persisted_snapshot_notify_rx.clone()
    }
}

impl ChunkContainer for ReadWriteMode {
    fn get_table_chunks(
        &self,
        database_name: &str,
        table_name: &str,
        filters: &[Expr],
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        // Chunks are fetched from both primary and replicas
        // TODO: need to set the ChunkOrder on the chunks produced by the primary and replicas
        // such that the primary is prioritized over the replicas, and that for the replicas,
        // those with higher precedence are prioritized over those with lower precedence.
        let mut chunks =
            self.primary
                .get_table_chunks(database_name, table_name, filters, projection, ctx)?;
        if let Some(replicas) = &self.replicas {
            chunks.append(
                &mut replicas
                    .get_table_chunks(database_name, table_name, filters, projection, ctx)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            );
        }
        Ok(chunks)
    }
}

#[async_trait::async_trait]
impl LastCacheManager for ReadWriteMode {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        self.primary.last_cache_provider()
    }

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
    ) -> write_buffer::Result<Option<LastCacheDefinition>> {
        self.primary
            .create_last_cache(
                db_name,
                tbl_name,
                cache_name,
                count,
                ttl,
                key_columns,
                value_columns,
            )
            .await
    }

    async fn delete_last_cache(
        &self,
        db_name: &str,
        tbl_name: &str,
        cache_name: &str,
    ) -> write_buffer::Result<()> {
        self.primary
            .delete_last_cache(db_name, tbl_name, cache_name)
            .await
    }
}

impl WriteBuffer for ReadWriteMode {}
