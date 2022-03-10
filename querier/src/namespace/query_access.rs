use std::{any::Any, sync::Arc};

use async_trait::async_trait;
use datafusion::catalog::{catalog::CatalogProvider, schema::SchemaProvider};
use db::chunk::DbChunk;
use predicate::{rpc_predicate::QueryDatabaseMeta, Predicate};
use query::{
    exec::{ExecutionContextProvider, ExecutorType, IOxExecutionContext},
    QueryCompletedToken, QueryDatabase, QueryText,
};
use schema::Schema;
use trace::ctx::SpanContext;

use crate::namespace::QuerierNamespace;

impl QueryDatabaseMeta for QuerierNamespace {
    fn table_names(&self) -> Vec<String> {
        self.catalog_access.table_names()
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.catalog_access.table_schema(table_name)
    }
}

#[async_trait]
impl QueryDatabase for QuerierNamespace {
    type Chunk = DbChunk;

    fn chunks(&self, table_name: &str, predicate: &Predicate) -> Vec<Arc<Self::Chunk>> {
        self.catalog_access.chunks(table_name, predicate)
    }

    fn record_query(
        &self,
        ctx: &IOxExecutionContext,
        query_type: impl Into<String>,
        query_text: QueryText,
    ) -> QueryCompletedToken {
        self.catalog_access
            .record_query(ctx, query_type, query_text)
    }
}

impl CatalogProvider for QuerierNamespace {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        self.catalog_access.schema_names()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.catalog_access.schema(name)
    }
}

impl ExecutionContextProvider for QuerierNamespace {
    fn new_query_context(self: &Arc<Self>, span_ctx: Option<SpanContext>) -> IOxExecutionContext {
        self.exec
            .new_execution_config(ExecutorType::Query)
            .with_default_catalog(Arc::<Self>::clone(self))
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
