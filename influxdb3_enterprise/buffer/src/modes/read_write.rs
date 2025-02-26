use std::{sync::Arc, time::Duration};

use crate::replica::{CreateReplicasArgs, Replicas, ReplicationConfig};
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::{
    catalog::Session, error::DataFusionError, execution::object_store::ObjectStoreUrl,
};
use influxdb3_cache::{
    distinct_cache::{CreateDistinctCacheArgs, DistinctCacheProvider},
    last_cache::LastCacheProvider,
    parquet_cache::ParquetCacheOracle,
};
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, TableDefinition};
use influxdb3_enterprise_compactor::compacted_data::CompactedData;
use influxdb3_id::{ColumnId, DbId, TableId};
use influxdb3_wal::{DistinctCacheDefinition, LastCacheDefinition, Wal, WalConfig};
use influxdb3_write::{
    BufferedWriteRequest, Bufferer, ChunkContainer, ChunkFilter, DatabaseManager,
    DistinctCacheManager, LastCacheManager, ParquetFile, PersistedSnapshot, Precision, WriteBuffer,
    persister::{DEFAULT_OBJECT_STORE_URL, Persister},
    write_buffer::{
        self, WriteBufferImpl, WriteBufferImplArgs, cache_parquet_files, parquet_chunk_from_file,
        persisted_files::PersistedFiles,
    },
};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use object_store::ObjectStore;
use tokio::sync::watch::Receiver;

#[derive(Debug)]
pub struct ReadWriteMode {
    primary: Arc<WriteBufferImpl>,
    node_id: Arc<str>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    replicas: Option<Replicas>,
    compacted_data: Option<Arc<CompactedData>>,
    parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
}

#[derive(Debug)]
pub struct CreateReadWriteModeArgs {
    pub node_id: Arc<str>,
    pub persister: Arc<Persister>,
    pub catalog: Arc<Catalog>,
    pub last_cache: Arc<LastCacheProvider>,
    pub distinct_cache: Arc<DistinctCacheProvider>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub executor: Arc<iox_query::exec::Executor>,
    pub wal_config: WalConfig,
    pub metric_registry: Arc<Registry>,
    pub replication_config: Option<ReplicationConfig>,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub compacted_data: Option<Arc<CompactedData>>,
    pub snapshotted_wal_files_to_keep: u64,
}

impl ReadWriteMode {
    pub(crate) async fn new(
        CreateReadWriteModeArgs {
            node_id,
            persister,
            catalog,
            last_cache,
            distinct_cache,
            time_provider,
            executor,
            wal_config,
            metric_registry,
            replication_config,
            parquet_cache,
            compacted_data,
            snapshotted_wal_files_to_keep,
        }: CreateReadWriteModeArgs,
    ) -> Result<Self, anyhow::Error> {
        let object_store = persister.object_store();
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

        let replicas = if let Some(ReplicationConfig {
            interval: replication_interval,
            node_ids,
        }) = replication_config.and_then(|mut config| {
            // remove this writer from the list of replicas if it was provided to prevent from
            // replicating the local primary buffer.
            config.node_ids.retain(|h| h != node_id.as_ref());
            (!config.node_ids.is_empty()).then_some(config)
        }) {
            Some(
                Replicas::new(CreateReplicasArgs {
                    last_cache,
                    distinct_cache,
                    object_store: Arc::clone(&object_store),
                    metric_registry,
                    replication_interval,
                    node_ids,
                    parquet_cache: parquet_cache.clone(),
                    catalog,
                    time_provider,
                    wal: Some(primary.wal()),
                })
                .await?,
            )
        } else {
            None
        };
        Ok(Self {
            node_id,
            primary,
            replicas,
            compacted_data,
            object_store,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
            parquet_cache,
        })
    }

    pub fn persisted_files(&self) -> Arc<PersistedFiles> {
        self.primary.persisted_files()
    }

    pub fn write_buffer_impl(&self) -> Arc<WriteBufferImpl> {
        Arc::clone(&self.primary)
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
        no_sync: bool,
    ) -> write_buffer::Result<BufferedWriteRequest> {
        // Writes go to the primary buffer, so this only relies on that
        self.primary
            .write_lp(
                database,
                lp,
                ingest_time,
                accept_partial,
                precision,
                no_sync,
            )
            .await
    }

    fn catalog(&self) -> Arc<Catalog> {
        Arc::clone(&self.primary.catalog())
    }

    fn parquet_files_filtered(
        &self,
        db_id: DbId,
        table_id: TableId,
        filter: &ChunkFilter<'_>,
    ) -> Vec<ParquetFile> {
        // Parquet files need to be retrieved across primary and replicas
        // TODO: could this fall into another trait?
        let mut files = self.primary.parquet_files_filtered(db_id, table_id, filter);
        if let Some(replicas) = &self.replicas {
            files.append(&mut replicas.parquet_files_filtered(db_id, table_id, filter));
        }
        // NOTE: do we need to sort this since this is used by the system tables and the query
        // executor could sort if desired...
        files.sort_unstable_by(|a, b| a.chunk_time.cmp(&b.chunk_time));
        files
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        unimplemented!("watch_persisted_snapshots not implemented for ReadWriteMode")
    }

    fn wal(&self) -> Arc<dyn Wal> {
        self.primary.wal()
    }
}

impl ChunkContainer for ReadWriteMode {
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
        let mut chunks = self.primary.get_buffer_chunks(
            Arc::clone(&db_schema),
            Arc::clone(&table_def),
            filter,
            projection,
            ctx,
        )?;

        if let Some(replicas) = &self.replicas {
            chunks.extend(
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

            chunks.extend(
                parquet_files
                    .into_iter()
                    .map(|file| {
                        Arc::new(parquet_chunk_from_file(
                            &file,
                            &table_def.schema,
                            self.object_store_url.clone(),
                            Arc::clone(&self.object_store),
                            chunks.len() as i64,
                        )) as Arc<dyn QueryChunk>
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
                    chunks.len() as i64,
                )
            } else {
                replicas.get_persisted_chunks(
                    Arc::clone(&db_schema),
                    Arc::clone(&table_def),
                    filter,
                    &[],
                    chunks.len() as i64,
                )
            };
            chunks.extend(gen1_persisted_chunks);
        }

        // now add in the gen1 chunks from primary
        let next_non_compacted_parquet_file_id = writer_markers.as_ref().and_then(|markers| {
            markers.iter().find_map(|marker| {
                if marker.node_id == self.node_id.as_ref() {
                    Some(marker.next_file_id)
                } else {
                    None
                }
            })
        });

        let gen1_persisted_chunks = self.primary.get_persisted_chunks(
            Arc::clone(&db_schema),
            Arc::clone(&table_def),
            filter,
            next_non_compacted_parquet_file_id,
            chunks.len() as i64,
        );
        chunks.extend(gen1_persisted_chunks);

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
        db_id: DbId,
        tbl_id: TableId,
        cache_name: Option<&str>,
        count: Option<usize>,
        ttl: Option<Duration>,
        key_columns: Option<Vec<ColumnId>>,
        value_columns: Option<Vec<ColumnId>>,
    ) -> write_buffer::Result<Option<LastCacheDefinition>> {
        self.primary
            .create_last_cache(
                db_id,
                tbl_id,
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
        db_id: DbId,
        tbl_id: TableId,
        cache_name: &str,
    ) -> write_buffer::Result<()> {
        self.primary
            .delete_last_cache(db_id, tbl_id, cache_name)
            .await
    }
}

#[async_trait]
impl DistinctCacheManager for ReadWriteMode {
    fn distinct_cache_provider(&self) -> Arc<DistinctCacheProvider> {
        self.primary.distinct_cache_provider()
    }

    async fn create_distinct_cache(
        &self,
        db_schema: Arc<DatabaseSchema>,
        cache_name: Option<String>,
        args: CreateDistinctCacheArgs,
    ) -> write_buffer::Result<Option<DistinctCacheDefinition>> {
        self.primary
            .create_distinct_cache(db_schema, cache_name, args)
            .await
    }

    async fn delete_distinct_cache(
        &self,
        db_id: &DbId,
        tbl_id: &TableId,
        cache_name: &str,
    ) -> write_buffer::Result<()> {
        self.primary
            .delete_distinct_cache(db_id, tbl_id, cache_name)
            .await
    }
}

#[async_trait]
impl DatabaseManager for ReadWriteMode {
    async fn create_database(&self, name: String) -> Result<(), write_buffer::Error> {
        self.primary.create_database(name).await
    }

    async fn soft_delete_database(&self, name: String) -> write_buffer::Result<()> {
        self.primary.soft_delete_database(name).await
    }

    async fn create_table(
        &self,
        db: String,
        table: String,
        tags: Vec<String>,
        fields: Vec<(String, String)>,
    ) -> Result<(), write_buffer::Error> {
        self.primary.create_table(db, table, tags, fields).await
    }

    async fn soft_delete_table(
        &self,
        db_name: String,
        table_name: String,
    ) -> write_buffer::Result<()> {
        self.primary.soft_delete_table(db_name, table_name).await
    }
}

impl WriteBuffer for ReadWriteMode {}
