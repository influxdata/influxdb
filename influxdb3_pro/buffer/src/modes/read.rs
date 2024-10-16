use std::{sync::Arc, time::Duration};

use crate::replica::{CreateReplicasArgs, Replicas};
use anyhow::Context;
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{catalog::Session, error::DataFusionError, logical_expr::Expr};
use influxdb3_catalog::DatabaseSchemaProvider;
use influxdb3_id::{DbId, TableId};
use influxdb3_pro_data_layout::compacted_data::CompactedData;
use influxdb3_wal::LastCacheDefinition;
use influxdb3_write::write_buffer::parquet_chunk_from_file;
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
    compacted_data: Option<Arc<CompactedData>>,
}

impl ReadMode {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        last_cache: Arc<LastCacheProvider>,
        object_store: Arc<dyn ObjectStore>,
        metric_registry: Arc<Registry>,
        replication_interval: Duration,
        hosts: Vec<String>,
        parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
        compacted_data: Option<Arc<CompactedData>>,
    ) -> Result<Self, anyhow::Error> {
        let (persisted_snapshot_notify_tx, persisted_snapshot_notify_rx) =
            tokio::sync::watch::channel(None);

        Ok(Self {
            persisted_snapshot_notify_rx,
            replicas: Replicas::new(CreateReplicasArgs {
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

    fn db_schema_provider(&self) -> Arc<dyn DatabaseSchemaProvider> {
        todo!("need to use the synthesized catalog");
    }

    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        let mut files = self.replicas.parquet_files(db_id, table_id);
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
        _projection: Option<&Vec<usize>>,
        _ctx: &dyn Session,
    ) -> influxdb3_write::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let (db_id, db_schema) = self
            .db_schema_provider()
            .db_schema_and_id(database_name)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("Database {} not found", database_name))
            })?;

        let (table_id, table_schema) = db_schema
            .table_schema_and_id(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("Table {} not found", table_name)))?;

        let mut buffer_chunks = self
            .replicas
            .get_buffer_chunks(database_name, table_name, filters)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        if let Some(compacted_data) = &self.compacted_data {
            let (parquet_files, host_markers) =
                compacted_data.get_parquet_files_and_host_markers(db_id, table_id, filters);

            buffer_chunks.extend(
                parquet_files
                    .into_iter()
                    .map(|file| {
                        Arc::new(parquet_chunk_from_file(
                            &file,
                            &table_schema,
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
                table_schema.clone(),
                filters,
                &host_markers,
                buffer_chunks.len() as i64,
            );
            buffer_chunks.extend(gen1_persisted_chunks);
        } else {
            let gen1_persisted_chunks = self.replicas.get_persisted_chunks(
                database_name,
                table_name,
                table_schema.clone(),
                filters,
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
        _key_columns: Option<Vec<String>>,
        _value_columns: Option<Vec<String>>,
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

impl WriteBuffer for ReadMode {}
