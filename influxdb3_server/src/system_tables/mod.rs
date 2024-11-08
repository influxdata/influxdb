use std::{any::Any, collections::HashMap, ops::Deref, sync::Arc};

use datafusion::{
    catalog::SchemaProvider,
    datasource::TableProvider,
    error::DataFusionError,
    logical_expr::{col, BinaryExpr, Expr, Operator},
    scalar::ScalarValue,
};
use influxdb3_catalog::catalog::DatabaseSchema;
use influxdb3_pro_data_layout::CompactedDataSystemTableView;
use influxdb3_write::WriteBuffer;
use iox_query::query_log::QueryLog;
use iox_system_tables::SystemTableProvider;
use parquet_files::ParquetFilesTable;
use tonic::async_trait;

use crate::system_tables::compacted_data::CompactedDataTable;

use self::{last_caches::LastCachesTable, queries::QueriesTable};

mod compacted_data;
mod last_caches;
mod parquet_files;
mod queries;

pub const SYSTEM_SCHEMA_NAME: &str = "system";
pub const TABLE_NAME_PREDICATE: &str = "table_name";

pub(crate) const QUERIES_TABLE_NAME: &str = "queries";
pub(crate) const LAST_CACHES_TABLE_NAME: &str = "last_caches";
pub(crate) const PARQUET_FILES_TABLE_NAME: &str = "parquet_files";
pub(crate) const COMPACTED_DATA_TABLE_NAME: &str = "compacted_data";

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
        compacted_data: Option<Arc<dyn CompactedDataSystemTableView>>,
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
        let parquet_files = Arc::new(SystemTableProvider::new(Arc::new(ParquetFilesTable::new(
            db_schema.id,
            buffer,
        ))));
        tables.insert(PARQUET_FILES_TABLE_NAME, parquet_files);

        let compacted_data_table = Arc::new(SystemTableProvider::new(Arc::new(
            CompactedDataTable::new(Arc::clone(&db_schema), compacted_data),
        )));
        tables.insert(COMPACTED_DATA_TABLE_NAME, compacted_data_table);
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

/// Used in queries to the system.{table_name} table
///
/// # Example
/// ```sql
/// SELECT * FROM system.parquet_files WHERE table_name = 'foo'
/// ```
pub(crate) fn find_table_name_in_filter(filters: Option<Vec<Expr>>) -> Option<String> {
    filters.map(|all_filters| {
        all_filters.iter().find_map(|f| match f {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                if left.deref() == &col(TABLE_NAME_PREDICATE) && op == &Operator::Eq {
                    match right.deref() {
                        Expr::Literal(
                            ScalarValue::Utf8(Some(s))
                            | ScalarValue::LargeUtf8(Some(s))
                            | ScalarValue::Utf8View(Some(s)),
                        ) => Some(s.to_owned()),
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

pub(crate) fn table_name_predicate_error(table_name: &str) -> DataFusionError {
    DataFusionError::Plan(format!(
        "must provide a {TABLE_NAME_PREDICATE} = '<table_name>' predicate in queries to \
            {SYSTEM_SCHEMA_NAME}.{table_name}"
    ))
}
