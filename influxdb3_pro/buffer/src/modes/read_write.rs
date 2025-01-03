use std::{sync::Arc, time::Duration};

use crate::replica::{CreateReplicasArgs, Replicas, ReplicationConfig};
use async_trait::async_trait;
use data_types::NamespaceName;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::{catalog::Session, error::DataFusionError, logical_expr::Expr};
use influxdb3_cache::last_cache::LastCacheProvider;
use influxdb3_cache::meta_cache::{CreateMetaCacheArgs, MetaCacheProvider};
use influxdb3_cache::parquet_cache::ParquetCacheOracle;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema};
use influxdb3_id::{ColumnId, DbId, TableId};
use influxdb3_pro_compactor::compacted_data::CompactedData;
use influxdb3_wal::{
    LastCacheDefinition, MetaCacheDefinition, PluginType, TriggerSpecificationDefinition, WalConfig,
};
use influxdb3_write::persister::DEFAULT_OBJECT_STORE_URL;
use influxdb3_write::write_buffer::persisted_files::PersistedFiles;
use influxdb3_write::write_buffer::plugins::ProcessingEngineManager;
use influxdb3_write::write_buffer::{parquet_chunk_from_file, WriteBufferImplArgs};
use influxdb3_write::{
    persister::Persister,
    write_buffer::{self, WriteBufferImpl},
    BufferedWriteRequest, Bufferer, ChunkContainer, LastCacheManager, ParquetFile,
    PersistedSnapshot, Precision, WriteBuffer,
};
use influxdb3_write::{DatabaseManager, MetaCacheManager};
use iox_query::QueryChunk;
use iox_time::{Time, TimeProvider};
use metric::Registry;
use object_store::ObjectStore;
use tokio::sync::watch::Receiver;

#[derive(Debug)]
pub struct ReadWriteMode {
    primary: Arc<WriteBufferImpl>,
    host_id: Arc<str>,
    object_store: Arc<dyn ObjectStore>,
    object_store_url: ObjectStoreUrl,
    replicas: Option<Replicas>,
    compacted_data: Option<Arc<CompactedData>>,
}

#[derive(Debug)]
pub struct CreateReadWriteModeArgs {
    pub host_id: Arc<str>,
    pub persister: Arc<Persister>,
    pub catalog: Arc<Catalog>,
    pub last_cache: Arc<LastCacheProvider>,
    pub meta_cache: Arc<MetaCacheProvider>,
    pub time_provider: Arc<dyn TimeProvider>,
    pub executor: Arc<iox_query::exec::Executor>,
    pub wal_config: WalConfig,
    pub metric_registry: Arc<Registry>,
    pub replication_config: Option<ReplicationConfig>,
    pub parquet_cache: Option<Arc<dyn ParquetCacheOracle>>,
    pub compacted_data: Option<Arc<CompactedData>>,
}

impl ReadWriteMode {
    pub(crate) async fn new(
        CreateReadWriteModeArgs {
            host_id,
            persister,
            catalog,
            last_cache,
            meta_cache,
            time_provider,
            executor,
            wal_config,
            metric_registry,
            replication_config,
            parquet_cache,
            compacted_data,
        }: CreateReadWriteModeArgs,
    ) -> Result<Self, anyhow::Error> {
        let object_store = persister.object_store();
        let primary = WriteBufferImpl::new(WriteBufferImplArgs {
            persister,
            catalog: Arc::clone(&catalog),
            last_cache: Arc::clone(&last_cache),
            meta_cache: Arc::clone(&meta_cache),
            time_provider: Arc::clone(&time_provider),
            executor,
            wal_config,
            parquet_cache: parquet_cache.clone(),
            metric_registry: Arc::clone(&metric_registry),
        })
        .await?;

        let replicas = if let Some(ReplicationConfig {
            interval: replication_interval,
            hosts,
        }) = replication_config.and_then(|mut config| {
            // remove this host from the list of replicas if it was provided to prevent from
            // replicating the local primary buffer.
            config.hosts.retain(|h| h != host_id.as_ref());
            (!config.hosts.is_empty()).then_some(config)
        }) {
            Some(
                Replicas::new(CreateReplicasArgs {
                    last_cache,
                    meta_cache,
                    object_store: Arc::clone(&object_store),
                    metric_registry,
                    replication_interval,
                    hosts,
                    parquet_cache,
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
            host_id,
            primary,
            replicas,
            compacted_data,
            object_store,
            object_store_url: ObjectStoreUrl::parse(DEFAULT_OBJECT_STORE_URL).unwrap(),
        })
    }

    pub fn persisted_files(&self) -> Arc<PersistedFiles> {
        self.primary.persisted_files()
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
        Arc::clone(&self.primary.catalog())
    }

    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFile> {
        // Parquet files need to be retrieved across primary and replicas
        // TODO: could this fall into another trait?
        let mut files = self.primary.parquet_files(db_id, table_id);
        if let Some(replicas) = &self.replicas {
            files.append(&mut replicas.parquet_files(db_id, table_id));
        }
        // NOTE: do we need to sort this since this is used by the system tables and the query
        // executor could sort if desired...
        files.sort_unstable_by(|a, b| a.chunk_time.cmp(&b.chunk_time));
        files
    }

    fn watch_persisted_snapshots(&self) -> Receiver<Option<PersistedSnapshot>> {
        unimplemented!("watch_persisted_snapshots not implemented for ReadWriteMode")
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
        let db_schema = self.catalog().db_schema(database_name).ok_or_else(|| {
            DataFusionError::Execution(format!("Database {} not found", database_name))
        })?;

        let table_schema = db_schema
            .table_schema(table_name)
            .ok_or_else(|| DataFusionError::Execution(format!("Table {} not found", table_name)))?;

        // first add in all the buffer chunks from primary and replicas. These chunks have the
        // highest precedence set in chunk order
        let mut chunks = self.primary.get_buffer_chunks(
            Arc::clone(&db_schema),
            table_name,
            filters,
            projection,
            ctx,
        )?;

        if let Some(replicas) = &self.replicas {
            chunks.extend(
                replicas
                    .get_buffer_chunks(database_name, table_name, filters)
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?,
            );
        }

        // now add in the compacted chunks so that they have the lowest chunk order precedence and
        // pull out the host markers to get gen1 chunks from primary and replicas
        let host_markers =
            if let Some(compacted_data) = &self.compacted_data {
                let (parquet_files, host_markers) = compacted_data
                    .get_parquet_files_and_host_markers(database_name, table_name, filters);

                chunks.extend(
                    parquet_files
                        .into_iter()
                        .map(|file| {
                            Arc::new(parquet_chunk_from_file(
                                &file,
                                &table_schema,
                                self.object_store_url.clone(),
                                Arc::clone(&self.object_store),
                                chunks.len() as i64,
                            )) as Arc<dyn QueryChunk>
                        })
                        .collect::<Vec<_>>(),
                );

                Some(host_markers)
            } else {
                None
            };

        // add the gen1 persisted chunks from the replicas
        if let Some(replicas) = &self.replicas {
            let gen1_persisted_chunks = if let Some(host_markers) = &host_markers {
                replicas.get_persisted_chunks(
                    database_name,
                    table_name,
                    table_schema.clone(),
                    filters,
                    host_markers,
                    chunks.len() as i64,
                )
            } else {
                replicas.get_persisted_chunks(
                    database_name,
                    table_name,
                    table_schema.clone(),
                    filters,
                    &[],
                    chunks.len() as i64,
                )
            };
            chunks.extend(gen1_persisted_chunks);
        }

        // now add in the gen1 chunks from primary
        let next_non_compacted_parquet_file_id = host_markers.as_ref().and_then(|markers| {
            markers.iter().find_map(|marker| {
                if marker.host_id == self.host_id.as_ref() {
                    Some(marker.next_file_id)
                } else {
                    None
                }
            })
        });

        let gen1_persisted_chunks = self.primary.get_persisted_chunks(
            database_name,
            table_name,
            table_schema.clone(),
            filters,
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
impl MetaCacheManager for ReadWriteMode {
    fn meta_cache_provider(&self) -> Arc<MetaCacheProvider> {
        self.primary.meta_cache_provider()
    }

    async fn create_meta_cache(
        &self,
        db_schema: Arc<DatabaseSchema>,
        cache_name: Option<String>,
        args: CreateMetaCacheArgs,
    ) -> write_buffer::Result<Option<MetaCacheDefinition>> {
        self.primary
            .create_meta_cache(db_schema, cache_name, args)
            .await
    }

    async fn delete_meta_cache(
        &self,
        db_id: &DbId,
        tbl_id: &TableId,
        cache_name: &str,
    ) -> write_buffer::Result<()> {
        self.primary
            .delete_meta_cache(db_id, tbl_id, cache_name)
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

#[async_trait]
impl ProcessingEngineManager for ReadWriteMode {
    async fn insert_plugin(
        &self,
        db: &str,
        plugin_name: String,
        code: String,
        function_name: String,
        plugin_type: PluginType,
    ) -> Result<(), write_buffer::Error> {
        self.primary
            .insert_plugin(db, plugin_name, code, function_name, plugin_type)
            .await
    }

    async fn insert_trigger(
        &self,
        db_name: &str,
        trigger_name: String,
        plugin_name: String,
        trigger_specification: TriggerSpecificationDefinition,
    ) -> Result<(), write_buffer::Error> {
        self.primary
            .insert_trigger(db_name, trigger_name, plugin_name, trigger_specification)
            .await
    }

    async fn run_trigger(
        &self,
        write_buffer: Arc<dyn WriteBuffer>,
        db_name: &str,
        trigger_name: &str,
    ) -> Result<(), write_buffer::Error> {
        self.primary
            .run_trigger(write_buffer, db_name, trigger_name)
            .await
    }
}

impl WriteBuffer for ReadWriteMode {}
