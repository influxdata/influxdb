use std::{any::Any, collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
};
use predicate::{rpc_predicate::QueryDatabaseMeta, Predicate};
use query::{
    exec::{ExecutionContextProvider, ExecutorType, IOxExecutionContext},
    QueryChunk, QueryCompletedToken, QueryDatabase, QueryText, DEFAULT_SCHEMA,
};
use schema::Schema;
use trace::ctx::SpanContext;

use crate::{namespace::QuerierNamespace, table::QuerierTable};

impl QueryDatabaseMeta for QuerierNamespace {
    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tables.read().keys().map(|s| s.to_string()).collect();
        names.sort();
        names
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.tables
            .read()
            .get(table_name)
            .map(|t| Arc::clone(t.schema()))
    }
}

#[async_trait]
impl QueryDatabase for QuerierNamespace {
    async fn chunks(&self, table_name: &str, predicate: &Predicate) -> Vec<Arc<dyn QueryChunk>> {
        // get table metadata
        let table = match self.tables.read().get(table_name).map(Arc::clone) {
            Some(table) => table,
            None => {
                // table gone
                return vec![];
            }
        };

        let mut chunks = table.chunks().await;

        // if there is a field restriction on the predicate, only
        // chunks with that field should be returned. If the chunk has
        // none of the fields specified, then it doesn't match
        // TODO: test this branch
        if let Some(field_columns) = &predicate.field_columns {
            chunks.retain(|chunk| {
                let schema = chunk.schema();
                // keep chunk if it has any of the columns requested
                field_columns
                    .iter()
                    .any(|col| schema.find_index_of(col).is_some())
            })
        }

        let pruner = table.chunk_pruner();
        pruner.prune_chunks(table_name, Arc::clone(table.schema()), chunks, predicate)
    }

    fn record_query(
        &self,
        _ctx: &IOxExecutionContext,
        _query_type: &str,
        _query_text: QueryText,
    ) -> QueryCompletedToken {
        // TODO: implement query recording again (https://github.com/influxdata/influxdb_iox/issues/4084)
        QueryCompletedToken::new(|_success| {})
    }
}

pub struct QuerierCatalogProvider {
    /// A snapshot of all tables.
    tables: Arc<HashMap<Arc<str>, Arc<QuerierTable>>>,
}

impl QuerierCatalogProvider {
    fn from_namespace(namespace: &QuerierNamespace) -> Self {
        Self {
            tables: Arc::clone(&namespace.tables.read()),
        }
    }
}

impl CatalogProvider for QuerierCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        // TODO: system tables (https://github.com/influxdata/influxdb_iox/issues/4085)
        vec![
            DEFAULT_SCHEMA.to_string(),
            // system_tables::SYSTEM_SCHEMA.to_string(),
        ]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            DEFAULT_SCHEMA => Some(Arc::new(UserSchemaProvider {
                tables: Arc::clone(&self.tables),
            })),
            // SYSTEM_SCHEMA => Some(Arc::clone(&self.system_tables) as Arc<dyn SchemaProvider>),
            _ => None,
        }
    }
}

impl CatalogProvider for QuerierNamespace {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        QuerierCatalogProvider::from_namespace(self).schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        QuerierCatalogProvider::from_namespace(self).schema(name)
    }
}

/// Provider for user-provided tables in [`DEFAULT_SCHEMA`].
struct UserSchemaProvider {
    /// A snapshot of all tables.
    tables: Arc<HashMap<Arc<str>, Arc<QuerierTable>>>,
}

impl SchemaProvider for UserSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tables.keys().map(|s| s.to_string()).collect();
        names.sort();
        names
    }

    fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|t| Arc::clone(t) as _)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

impl ExecutionContextProvider for QuerierNamespace {
    fn new_query_context(&self, span_ctx: Option<SpanContext>) -> IOxExecutionContext {
        self.exec
            .new_execution_config(ExecutorType::Query)
            .with_default_catalog(Arc::new(QuerierCatalogProvider::from_namespace(self)) as _)
            .with_span_context(span_ctx)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::test_util::querier_namespace;
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_sorted_eq;
    use data_types2::ColumnType;
    use iox_tests::util::TestCatalog;
    use query::frontend::sql::SqlQueryPlanner;

    #[tokio::test]
    async fn test_query() {
        let catalog = TestCatalog::new();

        let ns = catalog.create_namespace("ns").await;

        let sequencer1 = ns.create_sequencer(1).await;
        let sequencer2 = ns.create_sequencer(2).await;

        let table_cpu = ns.create_table("cpu").await;
        let table_mem = ns.create_table("mem").await;

        table_cpu.create_column("host", ColumnType::Tag).await;
        table_cpu.create_column("time", ColumnType::Time).await;
        table_cpu.create_column("load", ColumnType::F64).await;
        table_cpu.create_column("foo", ColumnType::I64).await;
        table_mem.create_column("host", ColumnType::Tag).await;
        table_mem.create_column("time", ColumnType::Time).await;
        table_mem.create_column("perc", ColumnType::F64).await;

        let partition_cpu_a_1 = table_cpu
            .with_sequencer(&sequencer1)
            .create_partition("a")
            .await;
        let partition_cpu_a_2 = table_cpu
            .with_sequencer(&sequencer2)
            .create_partition("a")
            .await;
        let partition_cpu_b_1 = table_cpu
            .with_sequencer(&sequencer1)
            .create_partition("b")
            .await;
        let partition_mem_c_1 = table_mem
            .with_sequencer(&sequencer1)
            .create_partition("c")
            .await;
        let partition_mem_c_2 = table_mem
            .with_sequencer(&sequencer2)
            .create_partition("c")
            .await;

        partition_cpu_a_1
            .create_parquet_file("cpu,host=a load=1 11")
            .await;
        partition_cpu_a_1
            .create_parquet_file("cpu,host=a load=2 22")
            .await
            .flag_for_delete()
            .await;
        partition_cpu_a_1
            .create_parquet_file("cpu,host=a load=3 33")
            .await;
        partition_cpu_a_2
            .create_parquet_file("cpu,host=a load=4 10001")
            .await;
        partition_cpu_b_1
            .create_parquet_file("cpu,host=b load=5 11")
            .await;
        partition_mem_c_1
            .create_parquet_file("mem,host=c perc=50 11\nmem,host=c perc=51 12\nmem,host=d perc=52 13\nmem,host=d perc=53 14")
            .await;
        partition_mem_c_2
            .create_parquet_file("mem,host=c perc=50 1001")
            .await
            .flag_for_delete()
            .await;
        partition_mem_c_1
            .create_parquet_file("mem,host=d perc=55 1")
            .await;

        table_mem
            .with_sequencer(&sequencer1)
            .create_tombstone(1, 1, 13, "host=d")
            .await;

        let querier_namespace = Arc::new(querier_namespace(&catalog, &ns));
        querier_namespace.sync().await;

        assert_query(
            &querier_namespace,
            "SELECT * FROM cpu ORDER BY host,time",
            &[
                "+-----+------+------+--------------------------------+",
                "| foo | host | load | time                           |",
                "+-----+------+------+--------------------------------+",
                "|     | a    | 1    | 1970-01-01T00:00:00.000000011Z |",
                "|     | a    | 3    | 1970-01-01T00:00:00.000000033Z |",
                "|     | a    | 4    | 1970-01-01T00:00:00.000010001Z |",
                "|     | b    | 5    | 1970-01-01T00:00:00.000000011Z |",
                "+-----+------+------+--------------------------------+",
            ],
        )
        .await;
        assert_query(
            &querier_namespace,
            "SELECT * FROM mem ORDER BY host,time",
            &[
                "+------+------+--------------------------------+",
                "| host | perc | time                           |",
                "+------+------+--------------------------------+",
                "| c    | 50   | 1970-01-01T00:00:00.000000011Z |",
                "| c    | 51   | 1970-01-01T00:00:00.000000012Z |",
                "| d    | 53   | 1970-01-01T00:00:00.000000014Z |",
                "+------+------+--------------------------------+",
            ],
        )
        .await;
    }

    async fn assert_query(
        querier_namespace: &Arc<QuerierNamespace>,
        sql: &str,
        expected_lines: &[&str],
    ) {
        let planner = SqlQueryPlanner::default();
        let ctx = querier_namespace.new_query_context(None);

        let physical_plan = planner
            .query(sql, &ctx)
            .await
            .expect("built plan successfully");

        let results: Vec<RecordBatch> = ctx.collect(physical_plan).await.expect("Running plan");
        assert_batches_sorted_eq!(expected_lines, &results);
    }
}
