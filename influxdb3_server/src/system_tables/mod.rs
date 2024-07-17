use std::{any::Any, collections::HashMap, sync::Arc};

use datafusion::{
    catalog::schema::SchemaProvider, datasource::TableProvider, error::DataFusionError,
};
use influxdb3_write::{catalog::Catalog, last_cache::LastCacheProvider};
use iox_query::query_log::QueryLog;
use iox_system_tables::SystemTableProvider;
use tonic::async_trait;

use self::{last_caches::LastCachesTable, queries::QueriesTable};

mod last_caches;
mod queries;

pub const SYSTEM_SCHEMA: &str = "system";

const QUERIES_TABLE: &str = "queries";
const LAST_CACHES_TABLE: &str = "last_caches";
const _PARQUET_FILES_TABLE: &str = "parquet_files";

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
        db_name: impl Into<String>,
        _catalog: Arc<Catalog>,
        query_log: Arc<QueryLog>,
        last_cache_provider: Arc<LastCacheProvider>,
    ) -> Self {
        let mut tables = HashMap::<&'static str, Arc<dyn TableProvider>>::new();
        let queries = Arc::new(SystemTableProvider::new(Arc::new(QueriesTable::new(
            query_log,
        ))));
        tables.insert(QUERIES_TABLE, queries);
        let last_caches = Arc::new(SystemTableProvider::new(Arc::new(LastCachesTable::new(
            db_name,
            last_cache_provider,
        ))));
        tables.insert(LAST_CACHES_TABLE, last_caches);
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
