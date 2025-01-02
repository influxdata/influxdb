use std::{any::Any, collections::HashMap, ops::Deref, sync::Arc};

use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{col, BinaryExpr, Expr, Operator},
    scalar::ScalarValue,
};
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_sys_events::SysEventStore;
use influxdb3_write::WriteBuffer;
use iox_query::query_log::QueryLog;
use iox_system_tables::SystemTableProvider;
use meta_caches::MetaCachesTable;
use parquet_files::ParquetFilesTable;
use tonic::async_trait;

use self::{last_caches::LastCachesTable, queries::QueriesTable};

mod last_caches;
mod meta_caches;
mod parquet_files;
use crate::system_tables::python_call::{
    ProcessingEnginePluginTable, ProcessingEngineTriggerTable,
};

mod python_call;
mod queries;

pub const SYSTEM_SCHEMA_NAME: &str = "system";
pub const TABLE_NAME_PREDICATE: &str = "table_name";

pub(crate) const QUERIES_TABLE_NAME: &str = "queries";
pub(crate) const LAST_CACHES_TABLE_NAME: &str = "last_caches";
pub(crate) const META_CACHES_TABLE_NAME: &str = "meta_caches";
pub(crate) const PARQUET_FILES_TABLE_NAME: &str = "parquet_files";

const PROCESSING_ENGINE_PLUGINS_TABLE_NAME: &str = "processing_engine_plugins";

const PROCESSING_ENGINE_TRIGGERS_TABLE_NAME: &str = "processing_engine_triggers";

#[derive(Debug)]
pub(crate) enum SystemSchemaProvider {
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

pub(crate) struct AllSystemSchemaTablesProvider {
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
    pub(crate) fn new(
        db_schema: Arc<DatabaseSchema>,
        query_log: Arc<QueryLog>,
        buffer: Arc<dyn WriteBuffer>,
        _sys_events_store: Arc<SysEventStore>,
    ) -> Self {
        let mut tables = HashMap::<&'static str, Arc<dyn TableProvider>>::new();
        let queries = Arc::new(SystemTableProvider::new(Arc::new(QueriesTable::new(
            query_log,
        ))));
        tables.insert(QUERIES_TABLE_NAME, queries);
        let last_caches = Arc::new(SystemTableProvider::new(Arc::new(LastCachesTable::new(
            Arc::clone(&db_schema),
            buffer.last_cache_provider(),
        ))));
        tables.insert(LAST_CACHES_TABLE_NAME, last_caches);
        let meta_caches = Arc::new(SystemTableProvider::new(Arc::new(MetaCachesTable::new(
            Arc::clone(&db_schema),
            buffer.meta_cache_provider(),
        ))));
        tables.insert(META_CACHES_TABLE_NAME, meta_caches);
        let parquet_files = Arc::new(SystemTableProvider::new(Arc::new(ParquetFilesTable::new(
            db_schema.id,
            buffer,
        ))));
        tables.insert(
            PROCESSING_ENGINE_PLUGINS_TABLE_NAME,
            Arc::new(SystemTableProvider::new(Arc::new(
                ProcessingEnginePluginTable::new(
                    db_schema
                        .processing_engine_plugins
                        .iter()
                        .map(|(_name, call)| call.clone())
                        .collect(),
                ),
            ))),
        );
        tables.insert(
            PROCESSING_ENGINE_TRIGGERS_TABLE_NAME,
            Arc::new(SystemTableProvider::new(Arc::new(
                ProcessingEngineTriggerTable::new(
                    db_schema
                        .processing_engine_triggers
                        .iter()
                        .map(|(_name, trigger)| trigger.clone())
                        .collect(),
                ),
            ))),
        );
        tables.insert(PARQUET_FILES_TABLE_NAME, parquet_files);
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
pub(crate) fn find_table_name_in_filter(filters: Option<Vec<Expr>>) -> Option<Arc<str>> {
    filters.map(|all_filters| {
        all_filters.iter().find_map(|f| match f {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if left.deref() == &col(TABLE_NAME_PREDICATE) && op == &Operator::Eq {
                    match right.deref() {
                        Expr::Literal(
                            ScalarValue::Utf8(Some(s))
                            | ScalarValue::LargeUtf8(Some(s))
                            | ScalarValue::Utf8View(Some(s)),
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
