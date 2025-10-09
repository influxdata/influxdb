use std::{any::Any, collections::HashMap, ops::Deref, sync::Arc};

use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{BinaryExpr, Expr, Operator, col},
    scalar::ScalarValue,
};
use generations::GenerationDurationsTable;
use influxdb3_catalog::catalog::{Catalog, DatabaseSchema, INTERNAL_DB_NAME};
use influxdb3_processing_engine::ProcessingEngineManagerImpl;
use influxdb3_sys_events::SysEventStore;
use influxdb3_write::WriteBuffer;
use iox_query::query_log::QueryLog;
use iox_system_tables::SystemTableProvider;
use tonic::async_trait;

mod databases;
use databases::DatabasesTable;
mod distinct_caches;
use distinct_caches::DistinctCachesTable;
mod generations;
mod influxdb_schema;
use influxdb_schema::InfluxdbSchemaTable;
mod last_caches;
use last_caches::LastCachesTable;
mod nodes;
use nodes::NodeSystemTable;
mod parquet_files;
use parquet_files::ParquetFilesTable;
mod plugins;
use plugins::PluginsTable;
mod python_call;
use python_call::{
    ProcessingEngineLogsTable, ProcessingEngineTriggerArgumentsTable, ProcessingEngineTriggerTable,
};
mod queries;
use queries::QueriesTable;
mod tables;
use tables::TablesTable;
mod tokens;
use tokens::TokenSystemTable;
/// The default timezone used in the system schema.
pub const DEFAULT_TIMEZONE: &str = "UTC";
/// Global system schema name used in queries
///
/// # Example
/// ```sql
/// SELECT * FROM system.queries;
/// ```
pub const SYSTEM_SCHEMA_NAME: &str = "system";
pub const TABLE_NAME_PREDICATE: &str = "table_name";

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
        sys_events_store: Arc<SysEventStore>,
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
            Arc::clone(&buffer),
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
        let logs_table = Arc::new(SystemTableProvider::new(Arc::new(
            ProcessingEngineLogsTable::new(sys_events_store),
        )));
        tables.insert(PROCESSING_ENGINE_LOGS_TABLE_NAME, logs_table);
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
                )))),
            );
            tables.insert(
                PLUGIN_FILES_TABLE_NAME,
                Arc::new(SystemTableProvider::new(Arc::new(PluginsTable::new(
                    processing_engine,
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

/// Used in queries to the system.{table_name} table
///
/// # Example
/// ```sql
/// SELECT * FROM system.parquet_files WHERE table_name = 'foo'
/// ```
pub fn find_table_name_in_filter(filters: Option<Vec<Expr>>) -> Option<Arc<str>> {
    filters.map(|all_filters| {
        all_filters.iter().find_map(|f| match f {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if left.deref() == &col(TABLE_NAME_PREDICATE) && op == &Operator::Eq {
                    match right.deref() {
                        Expr::Literal(
                            ScalarValue::Utf8(Some(s))
                            | ScalarValue::LargeUtf8(Some(s))
                            | ScalarValue::Utf8View(Some(s)),
                            _,
                        ) => Some(s.as_str().into()),
                        _ => None,
                    }
                } else {
                    None
                }
            }
            _ => None,
        })
    })?
}
