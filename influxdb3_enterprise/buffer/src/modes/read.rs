use std::{sync::Arc, time::Duration};

use crate::replica::{CreateReplicasArgs, Replicas};
use anyhow::Context;
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{catalog::Session, error::DataFusionError};
use influxdb3_cache::distinct_cache::DistinctCacheProvider;
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_cache::parquet_cache::ParquetCacheOracle;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_enterprise_compactor::compacted_data::CompactedData;
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{NoopWal, Wal};
use influxdb3_write::write_buffer::{cache_parquet_files, parquet_chunk_from_file};
use influxdb3_write::{
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile,
    PersistedSnapshot, Precision, WriteBuffer,
    write_buffer::{Error as WriteBufferError, Result as WriteBufferResult},
};
use influxdb3_write::{ChunkFilter, DistinctCacheManager};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use object_store::ObjectStore;
use tokio::sync::watch::Receiver;

#[derive(Debug)]
pub struct ReadMode {
    replicas: Replicas,
    compacted_data: Option<Arc<CompactedData>>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
}

#[derive(Debug)]
pub struct CreateReadModeArgs {
    pub last_cache: Arc<LastCacheProvider>,
    pub distinct_cache: Arc<DistinctCacheProvider>,
    pub object_store: Arc<dyn ObjectStore>,
    pub catalog: Arc<Catalog>,
    pub metric_registry: Arc<Registry>,
    pub replication_interval: Duration,
    pub node_ids: Vec<String>,
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
            node_ids,
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
                node_ids,
                parquet_cache: parquet_cache.clone(),
                catalog,
                time_provider,
            })
            .await
            .context("failed to initialize replicas")?,
            compacted_data,
            parquet_cache,
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
        _no_sync: bool,
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
        filter: &ChunkFilter<'_>,
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
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        _projection: Option<&Vec<usize>>,
        _ctx: &dyn Session,
    ) -> influxdb3_write::Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let mut buffer_chunks = self
            .replicas
            .get_buffer_chunks(Arc::clone(&db_schema), Arc::clone(&table_def), filter)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        if let Some(compacted_data) = &self.compacted_data {
            let (parquet_files, writer_markers) = compacted_data
                .get_parquet_files_and_writer_markers(
                    &db_schema.name,
                    &table_def.table_name,
                    filter,
                );

            cache_parquet_files(self.parquet_cache.clone(), &parquet_files);

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
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                filter,
                &writer_markers,
                buffer_chunks.len() as i64,
            );
            buffer_chunks.extend(gen1_persisted_chunks);
        } else {
            let gen1_persisted_chunks = self.replicas.get_persisted_chunks(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                filter,
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
}

#[async_trait]
impl DistinctCacheManager for ReadMode {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        self.replicas.distinct_cache()
    }
}

impl WriteBuffer for ReadMode {}
