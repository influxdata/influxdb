use std::sync::Arc;

use crate::replica::{CreateReplicasArgs, Replicas, ReplicationConfig};
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{
    catalog::Session, error::DataFusionError, execution::object_store::ObjectStoreUrl,
};
use influxdb3_cache::{
    distinct_cache::DistinctCacheProvider, last_cache::LastCacheProvider,
    parquet_cache::ParquetCacheOracle,
};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_enterprise_compactor::compacted_data::CompactedData;
use influxdb3_id::{DbId, TableId};
use influxdb3_wal::{NoopWal, Wal, WalConfig};
use influxdb3_write::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, DistinctCacheManager,
    LastCacheManager, ParquetFile, PersistedSnapshotVersion, Precision, WriteBuffer,
    chunk::enterprise::CompactedParquetChunk,
    persister::{DEFAULT_OBJECT_STORE_URL, Persister},
    write_buffer::{
        self, Error as WriteBufferError, WriteBufferImpl, WriteBufferImplArgs, cache_parquet_files,
        parquet_chunk_from_file, persisted_files::PersistedFiles,
    },
};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use object_store::ObjectStore;
use observability_deps::tracing::trace;
use tokio::sync::watch::Receiver;

#[derive(Clone, Debug)]
struct IngestComponents {
    primary: Arc<WriteBufferImpl>,
    node_id: Arc<str>,
}

#[derive(Debug)]
pub struct IngestArgs {
    pub node_id: Arc<str>,
    pub persister: Arc<Persister>,
    pub wal_config: WalConfig,
    pub executor: Arc<iox_query::exec::Executor>,
    pub snapshotted_wal_files_to_keep: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct QueryArgs {
    pub replication_config: ReplicationConfig,
}

#[derive(Debug)]
pub struct IngestQueryMode {
    ingest: Option<IngestComponents>,
    replicas: Option<Replicas>,

    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,

    compacted_data: Option<Arc<CompactedData>>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    catalog: Arc<Catalog>,
}

#[derive(Debug)]
pub struct CreateIngestQueryModeArgs {
    pub ingest_args: Option<IngestArgs>,
    pub query_args: Option<QueryArgs>,

    // common between ingest and query modes
    pub last_cache: Arc<LastCacheProvider>,
    pub distinct_cache: Arc<DistinctCacheProvider>,
    pub object_store: Arc<dyn ObjectStore>,
    pub catalog: Arc<Catalog>,
    pub metric_registry: Arc<Registry>,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub compacted_data: Option<Arc<CompactedData>>,
    pub time_provider: Arc<dyn TimeProvider>,
}

impl IngestQueryMode {
    pub(crate) async fn new(
        CreateIngestQueryModeArgs {
            object_store,
            catalog,
            last_cache,
            distinct_cache,
            time_provider,
            metric_registry,
            parquet_cache,
            compacted_data,
            ingest_args,
            query_args,
        }: CreateIngestQueryModeArgs,
    ) -> Result<Self, anyhow::Error> {
        if ingest_args.is_none() && query_args.is_none() {
            return Err(anyhow::Error::msg("must provide ingest or query args"));
        }
        let ingest = if let Some(IngestArgs {
            node_id,
            persister,
            wal_config,
            executor,
            snapshotted_wal_files_to_keep,
        }) = ingest_args
        {
            let primary = WriteBufferImpl::new(WriteBufferImplArgs {
                persister,
                catalog: Arc::clone(&catalog),
                last_cache: Arc::clone(&last_cache),
                distinct_cache: Arc::clone(&distinct_cache),
                time_provider: Arc::clone(&time_provider),
                executor,
                wal_config,
                parquet_cache: parquet_cache.clone(),
                metric_registry: Arc::clone(&metric_registry),
                snapshotted_wal_files_to_keep,
                // NOTE: this is a core only limit
                query_file_limit: None,
            })
            .await?;
            Some(IngestComponents { primary, node_id })
        } else {
            None
        };

        let replicas = if let Some(QueryArgs { replication_config }) = query_args {
            Some(
                Replicas::new(CreateReplicasArgs {
                    last_cache,
                    distinct_cache,
                    object_store: Arc::clone(&object_store),
                    metric_registry,
                    replication_interval: replication_config.interval,
                    parquet_cache: parquet_cache.clone(),
                    catalog: Arc::clone(&catalog),
                    time_provider,
                })
                .await?,
            )
        } else {
            None
        };

        Ok(Self {
            ingest,
            replicas,
            catalog,
            compacted_data,
            object_store,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            parquet_cache,
        })
    }

    pub fn persisted_files(&self) -> Option<Arc<PersistedFiles>> {
        self.ingest.as_ref().map(|i| i.primary.persisted_files())
    }

    pub fn write_buffer_impl(&self) -> Option<Arc<WriteBufferImpl>> {
        self.ingest.as_ref().map(|i| Arc::clone(&i.primary))
    }
}

#[async_trait]
impl Bufferer for IngestQueryMode {
    async fn write_lp(
        &self,
        database: NamespaceName<'static>,
        lp: &str,
        ingest_time: Time,
        accept_partial: bool,
        precision: Precision,
        no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest> {
        // Writes go to the primary buffer, so this only relies on that
        if let Some(ingest) = &self.ingest {
            ingest
                .primary
                .write_lp(
                    database,
                    lp,
                    ingest_time,
                    accept_partial,
                    precision,
                    no_sync,
                )
                .await
        } else {
            // TODO: update this error to clarify that write is only enabled with "ingest" mode
            Err(WriteBufferError::NoWriteInReadOnly)
        }
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog)
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        // Parquet files need to be retrieved across primary and replicas
        // TODO: could this fall into another trait?
        let mut files = Vec::new();
        if let Some(ingest) = &self.ingest {
            files.append(
                &mut ingest
                    .primary
                    .parquet_files_filtered(db_id, table_id, filter),
            );
        }
        if let Some(replicas) = &self.replicas {
            files.append(&mut replicas.parquet_files_filtered(db_id, table_id, filter));
        }
        // NOTE: do we need to sort this since this is used by the system tables and the query
        // executor could sort if desired...
        files.sort_unstable_by(|a, b| a.chunk_time.cmp(&b.chunk_time));
        files
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshotVersion>> {
        unimplemented!("watch_persisted_snapshots not implemented")
    }

    fn wal(&self) -> Arc<dyn Wal> {
        self.ingest
            .as_ref()
            .map(|i| i.primary.wal())
            .unwrap_or(Arc::new(NoopWal))
    }
}

impl ChunkContainer for IngestQueryMode {
    fn get_table_chunks(
        &self,
        db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        filter: &ChunkFilter<'_>,
        projection: Option<&Vec<usize>>,
        ctx: &dyn Session,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        // first add in all the buffer chunks from primary and replicas. These chunks have the
        // highest precedence set in chunk order
        let mut buffer_chunks: Vec<Arc<dyn QueryChunk>> = Vec::new();

        if let Some(ingest) = &self.ingest {
            buffer_chunks.extend(ingest.primary.get_buffer_chunks(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                filter,
                projection,
                ctx,
            )?);
        }

        if let Some(replicas) = &self.replicas {
            buffer_chunks.extend(
                replicas
                    .get_buffer_chunks(Arc::clone(&db_schema), Arc::clone(&table_def), filter)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            );
        }

        // now add in the compacted chunks so that they have the lowest chunk order precedence and
        // pull out the writer markers to get gen1 chunks from primary and replicas
        let writer_markers = if let Some(compacted_data) = &self.compacted_data {
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
                        let parquet_chunk = parquet_chunk_from_file(
                            &file,
                            &table_def.schema,
                            self.object_store_url.clone(),
                            Arc::clone(&self.object_store),
                            buffer_chunks.len() as i64,
                        );

                        Arc::new(CompactedParquetChunk {
                            core: parquet_chunk,
                        }) as Arc<dyn QueryChunk>
                    })
                    .collect::<Vec<_>>(),
            );

            Some(writer_markers)
        } else {
            None
        };

        // add the gen1 persisted chunks from the replicas
        if let Some(replicas) = &self.replicas {
            let gen1_persisted_chunks = if let Some(writer_markers) = &writer_markers {
                replicas.get_persisted_chunks(
                    Arc::clone(&db_schema),
                    Arc::clone(&table_def),
                    filter,
                    writer_markers,
                    buffer_chunks.len() as i64,
                )
            } else {
                replicas.get_persisted_chunks(
                    Arc::clone(&db_schema),
                    Arc::clone(&table_def),
                    filter,
                    &[],
                    buffer_chunks.len() as i64,
                )
            };
            buffer_chunks.extend(gen1_persisted_chunks);
        }

        if let Some(ingest) = &self.ingest {
            // now add in the gen1 chunks from primary
            let next_non_compacted_parquet_file_id = writer_markers.as_ref().and_then(|markers| {
                markers.iter().find_map(|marker| {
                    if marker.node_id == ingest.node_id {
                        Some(marker.next_file_id)
                    } else {
                        None
                    }
                })
            });

            let gen1_persisted_chunks = ingest.primary.get_persisted_chunks(
                Arc::clone(&db_schema),
                Arc::clone(&table_def),
                filter,
                next_non_compacted_parquet_file_id,
                buffer_chunks.len() as i64,
            );
            buffer_chunks.extend(gen1_persisted_chunks);
        }

        trace!(?buffer_chunks, "QueryChunks from buffer");

        Ok(buffer_chunks)
    }
}

#[async_trait::async_trait]
impl LastCacheManager for IngestQueryMode {
    fn last_cache_provider(&self) -> Arc<LastCacheProvider> {
        let replicas = self.replicas.as_ref();
        self.ingest
            .as_ref()
            .map(|i| i.primary.last_cache_provider())
            .or_else(|| replicas.map(|r| r.last_cache()))
            .expect("last cache only available with 'query' or 'ingest' mode enabled")
    }
}

#[async_trait]
impl DistinctCacheManager for IngestQueryMode {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        let replicas = self.replicas.as_ref();
        self.ingest
            .as_ref()
            .map(|i| i.primary.distinct_cache_provider())
            .or_else(|| replicas.map(|r| r.distinct_cache()))
            .expect("distinct cache only available with 'query' or 'ingest' mode enabled")
    }
}

impl WriteBuffer for IngestQueryMode {}
