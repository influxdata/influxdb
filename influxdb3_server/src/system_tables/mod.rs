use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::{catalog::SchemaProvider, datasource::TableProvider, error::DataFusionError};
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
#[cfg(test)]
pub(crate) use parquet_files::table_name_predicate_error;

mod python_call;
mod queries;

pub const SYSTEM_SCHEMA_NAME: &str = "system";

const LAST_CACHES_TABLE_NAME: &str = "last_caches";
const META_CACHES_TABLE_NAME: &str = "meta_caches";
const PARQUET_FILES_TABLE_NAME: &str = "parquet_files";
const QUERIES_TABLE_NAME: &str = "queries";

const PROCESSING_ENGINE_PLUGINS_TABLE_NAME: &str = "processing_engine_plugins";

const PROCESSING_ENGINE_TRIGGERS_TABLE_NAME: &str = "processing_engine_triggers";

pub(crate) struct SystemSchemaProvider {
    tables: HashMap<&'static str, Arc<dyn TableProvider>>,
}

impl std::fmt::Debug for SystemSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keys = self.tables.keys().copied().collect::<Vec<_>>();
        keys.sort_unstable();

        f.debug_struct("SystemSchemaProvider")
            .field("tables", &keys.join(", "))
            .finish()
    }
}

impl SystemSchemaProvider {
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
impl SchemaProvider for SystemSchemaProvider {
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
