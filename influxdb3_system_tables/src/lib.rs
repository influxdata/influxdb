use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::{
    catalog::{SchemaProvider, Session},
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::Expr,
};
use influxdb3_authz::TokenInfo;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, INTERNAL_DB_NAME, TableDefinition};
use influxdb3_id::{DbId, TableId};
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_py_api::logging::processing_engine_logs_schema;
use influxdb3_system_tables_common::{
    DatabasesTable, DistinctCachesTable, GenerationDurationsTable, InfluxdbSchemaTable,
    LastCachesTable, NodeSystemTable, ParquetFileRow, ParquetFilesSource, ParquetFilesTable,
    PluginFileRow, PluginsSource, PluginsTable, ProcessingEngineLogsSource,
    ProcessingEngineLogsTable, ProcessingEngineTriggerArgumentsTable, ProcessingEngineTriggerTable,
    QueriesTable, TablesTable, TokenPermissionsFormatter, TokenSystemTable,
    processing_engine_logs_view,
};
use influxdb3_write::{ChunkFilter, WriteBuffer};
use iox_query::{QueryChunk, query_log::QueryLog};
use iox_system_tables::SystemTableProvider;
use observability_deps::tracing::warn;
use tonic::async_trait;

/// Global system schema name used in queries
///
/// # Example
/// ```sql
/// SELECT * FROM system.queries;
/// ```
pub const SYSTEM_SCHEMA_NAME: &str = "system";

pub const QUERIES_TABLE_NAME: &str = "queries";
pub const LAST_CACHES_TABLE_NAME: &str = "last_caches";
pub const DISTINCT_CACHES_TABLE_NAME: &str = "distinct_caches";
pub const PARQUET_FILES_TABLE_NAME: &str = "parquet_files";
pub const TOKENS_TABLE_NAME: &str = "tokens";
pub const DATABASES_TABLE_NAME: &str = "databases";
pub const TABLES_TABLE_NAME: &str = "tables";
pub const NODES_TABLE_NAME: &str = "nodes";
pub const GENERATION_DURATIONS_TABLE_NAME: &str = "generation_durations";
pub const INFLUXDB_SCHEMA_TABLE_NAME: &str = "influxdb_schema";
pub const PLUGIN_FILES_TABLE_NAME: &str = "plugin_files";

pub const PROCESSING_ENGINE_TRIGGERS_TABLE_NAME: &str = "processing_engine_triggers";
pub const PROCESSING_ENGINE_TRIGGER_ARGUMENTS_TABLE_NAME: &str =
    "processing_engine_trigger_arguments";
pub const PROCESSING_ENGINE_LOGS_TABLE_NAME: &str = "processing_engine_logs";

#[derive(Debug)]
pub enum SystemSchemaProvider {
    AllSystemSchemaTables(AllSystemSchemaTablesProvider),
}

#[async_trait]
impl SchemaProvider for SystemSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        match self {
            Self::AllSystemSchemaTables(all_system_tables) => all_system_tables.table_names(),
        }
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        match self {
            Self::AllSystemSchemaTables(all_system_tables) => all_system_tables.table(name).await,
        }
    }

    fn table_exist(&self, name: &str) -> bool {
        match self {
            Self::AllSystemSchemaTables(all_system_tables) => all_system_tables.table_exist(name),
        }
    }
}

pub struct AllSystemSchemaTablesProvider {
    tables: HashMap<&'static str, Arc<dyn TableProvider>>,
}

impl std::fmt::Debug for AllSystemSchemaTablesProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys = self.tables.keys().copied().collect::<Vec<_>>();
        keys.sort_unstable();

        f.debug_struct("AllSystemSchemaTablesProvider")
            .field("tables", &keys.join(", "))
            .finish()
    }
}

impl AllSystemSchemaTablesProvider {
    pub fn new(
        db_schema: Arc<DatabaseSchema>,
        query_log: Arc<QueryLog>,
        buffer: Arc<dyn WriteBuffer>,
        catalog: Arc<Catalog>,
        started_with_auth: bool,
        processing_engine: Option<Arc<ProcessingEngineManagerImpl>>,
    ) -> Self {
        let mut tables = HashMap::<&'static str, Arc<dyn TableProvider>>::new();
        let queries = Arc::new(SystemTableProvider::new(Arc::new(QueriesTable::new(
            query_log,
        ))));
        tables.insert(QUERIES_TABLE_NAME, queries);
        let last_caches = Arc::new(SystemTableProvider::new(Arc::new(LastCachesTable::new(
            Arc::clone(&db_schema),
        ))));
        tables.insert(LAST_CACHES_TABLE_NAME, last_caches);
        let distinct_caches = Arc::new(SystemTableProvider::new(Arc::new(
            DistinctCachesTable::new(Arc::clone(&db_schema)),
        )));
        tables.insert(DISTINCT_CACHES_TABLE_NAME, distinct_caches);
        let parquet_files = Arc::new(SystemTableProvider::new(Arc::new(ParquetFilesTable::new(
            db_schema.id,
            Arc::new(WriteBufferParquetFilesSource(Arc::clone(&buffer))),
        ))));
        tables.insert(
            PROCESSING_ENGINE_TRIGGERS_TABLE_NAME,
            Arc::new(SystemTableProvider::new(Arc::new(
                ProcessingEngineTriggerTable::new(
                    db_schema
                        .processing_engine_triggers
                        .resource_iter()
                        .cloned()
                        .collect(),
                ),
            ))),
        );
        tables.insert(
            PROCESSING_ENGINE_TRIGGER_ARGUMENTS_TABLE_NAME,
            Arc::new(SystemTableProvider::new(Arc::new(
                ProcessingEngineTriggerArgumentsTable::new(
                    db_schema
                        .processing_engine_triggers
                        .resource_iter()
                        .cloned()
                        .collect(),
                ),
            ))),
        );
        tables.insert(PARQUET_FILES_TABLE_NAME, parquet_files);
        let logs_table: Arc<dyn TableProvider> = Arc::new(ProcessingEngineLogsTable::new(
            Arc::clone(&db_schema),
            Arc::new(WriteBufferProcessingEngineLogsSource(Arc::clone(&buffer))),
            Arc::new(processing_engine_logs_schema()),
        ));
        let logs_provider = match processing_engine_logs_view(Arc::clone(&logs_table)) {
            Ok(view) => view,
            Err(error) => {
                warn!(
                    %error,
                    "failed to build processing_engine_logs view; serving table without event_time"
                );
                logs_table
            }
        };
        tables.insert(PROCESSING_ENGINE_LOGS_TABLE_NAME, logs_provider);
        tables.insert(
            INFLUXDB_SCHEMA_TABLE_NAME,
            Arc::new(SystemTableProvider::new(Arc::new(
                InfluxdbSchemaTable::new(Arc::clone(&db_schema)),
            ))),
        );
        if db_schema.name.as_ref() == INTERNAL_DB_NAME {
            tables.insert(
                TOKENS_TABLE_NAME,
                Arc::new(SystemTableProvider::new(Arc::new(TokenSystemTable::new(
                    Arc::clone(&catalog),
                    started_with_auth,
                    Arc::new(CoreTokenPermissionsFormatter),
                )))),
            );
            tables.insert(
                PLUGIN_FILES_TABLE_NAME,
                Arc::new(SystemTableProvider::new(Arc::new(PluginsTable::new(
                    processing_engine.map(|pe| Arc::new(ProcessingEnginePluginsSource(pe)) as _),
                )))),
            );
            tables.insert(
                NODES_TABLE_NAME,
                Arc::new(SystemTableProvider::new(Arc::new(NodeSystemTable::new(
                    Arc::clone(&catalog),
                )))),
            );
            tables.insert(
                DATABASES_TABLE_NAME,
                Arc::new(SystemTableProvider::new(Arc::new(DatabasesTable::new(
                    Arc::clone(&catalog),
                )))),
            );
            tables.insert(
                TABLES_TABLE_NAME,
                Arc::new(SystemTableProvider::new(Arc::new(TablesTable::new(
                    Arc::clone(&catalog),
                )))),
            );
            tables.insert(
                GENERATION_DURATIONS_TABLE_NAME,
                Arc::new(SystemTableProvider::new(Arc::new(
                    GenerationDurationsTable::new(Arc::clone(&catalog)),
                ))),
            );
        }
        Self { tables }
    }
}

#[async_trait]
impl SchemaProvider for AllSystemSchemaTablesProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        let mut names = self
            .tables
            .keys()
            .map(|s| (*s).to_owned())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        Ok(self.tables.get(name).cloned())
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

/// Implement API to provide information about Parquet files for the
/// `system.parquet_files` system table for [`WriteBuffer`]
#[derive(Debug)]
struct WriteBufferParquetFilesSource(Arc<dyn WriteBuffer>);

impl ParquetFilesSource for WriteBufferParquetFilesSource {
    fn catalog(&self) -> Arc<Catalog> {
        self.0.catalog()
    }

    fn parquet_files(&self, db_id: DbId, table_id: TableId) -> Vec<ParquetFileRow> {
        self.0
            .parquet_files(db_id, table_id)
            .into_iter()
            .map(|file| ParquetFileRow {
                path: file.path,
                size_bytes: file.size_bytes,
                row_count: file.row_count,
                min_time: file.min_time,
                max_time: file.max_time,
            })
            .collect()
    }
}

/// Implement [`ProcessingEngineLogsSource`] for [`WriteBuffer`], used by the
/// `system.processing_engine_logs` system table.
#[derive(Debug)]
struct WriteBufferProcessingEngineLogsSource(Arc<dyn WriteBuffer>);

impl ProcessingEngineLogsSource for WriteBufferProcessingEngineLogsSource {
    fn catalog(&self) -> Arc<Catalog> {
        self.0.catalog()
    }

    fn chunks(
        &self,
        internal_db_schema: Arc<DatabaseSchema>,
        table_def: Arc<TableDefinition>,
        ctx: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        let mut filter = ChunkFilter::new(&table_def, filters)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;

        let catalog = self.0.catalog();
        if let Some(retention_cutoff) = internal_db_schema.get_retention_period_cutoff_ts_nanos(
            catalog.time_provider().now(),
            &table_def.table_id,
        ) {
            filter.time_lower_bound_ns = filter
                .time_lower_bound_ns
                .map(|lb| lb.max(retention_cutoff.timestamp_nanos()))
                .or(Some(retention_cutoff.timestamp_nanos()));
        }

        self.0
            .get_table_chunks(internal_db_schema, table_def, &filter, projection, ctx)
    }
}

/// Implement [`PluginsSource`] for [`ProcessingEngineManagerImpl`]
#[derive(Debug)]
struct ProcessingEnginePluginsSource(Arc<ProcessingEngineManagerImpl>);

#[async_trait]
impl PluginsSource for ProcessingEnginePluginsSource {
    async fn list_plugin_files(&self) -> Vec<PluginFileRow> {
        self.0
            .list_plugin_files()
            .await
            .into_iter()
            .map(|f| PluginFileRow {
                plugin_name: f.plugin_name,
                file_name: f.file_name,
                file_path: f.file_path,
                size_bytes: f.size_bytes,
                last_modified_millis: f.last_modified_millis,
            })
            .collect()
    }
}

/// Format the `permissions` column of `system.tokens`. Core doesn't track
/// per-resource permissions, so every token reports full access.
#[derive(Debug)]
struct CoreTokenPermissionsFormatter;

impl TokenPermissionsFormatter for CoreTokenPermissionsFormatter {
    fn format_permissions(&self, tokens: &[Arc<TokenInfo>]) -> Vec<String> {
        tokens.iter().map(|_| "*:*:*".to_string()).collect()
    }
}
