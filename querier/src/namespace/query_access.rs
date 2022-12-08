//! This module contains implementations of [`iox_query`] interfaces for [QuerierNamespace].

use crate::{
    namespace::QuerierNamespace,
    query_log::QueryLog,
    system_tables::{SystemSchemaProvider, SYSTEM_SCHEMA},
    table::QuerierTable,
};
use async_trait::async_trait;
use data_types::NamespaceId;
use datafusion::{
    catalog::{catalog::CatalogProvider, schema::SchemaProvider},
    datasource::TableProvider,
    error::DataFusionError,
};
use datafusion_util::config::DEFAULT_SCHEMA;
use iox_query::{
    exec::{ExecutionContextProvider, ExecutorType, IOxSessionContext},
    QueryChunk, QueryCompletedToken, QueryNamespace, QueryText,
};
use observability_deps::tracing::{debug, trace};
use predicate::{rpc_predicate::QueryNamespaceMeta, Predicate};
use schema::Schema;
use std::{any::Any, collections::HashMap, sync::Arc};
use trace::ctx::SpanContext;

impl QueryNamespaceMeta for QuerierNamespace {
    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tables.keys().map(|s| s.to_string()).collect();
        names.sort();
        names
    }

    fn table_schema(&self, table_name: &str) -> Option<Arc<Schema>> {
        self.tables.get(table_name).map(|t| Arc::clone(t.schema()))
    }
}

#[async_trait]
impl QueryNamespace for QuerierNamespace {
    async fn chunks(
        &self,
        table_name: &str,
        predicate: &Predicate,
        projection: Option<&Vec<usize>>,
        ctx: IOxSessionContext,
    ) -> Result<Vec<Arc<dyn QueryChunk>>, DataFusionError> {
        debug!(%table_name, %predicate, "Finding chunks for table");
        // get table metadata
        let table = match self.tables.get(table_name).map(Arc::clone) {
            Some(table) => table,
            None => {
                // table gone
                trace!(%table_name, "No entry for table");
                return Ok(vec![]);
            }
        };

        let mut chunks = table
            .chunks(
                predicate,
                ctx.span().map(|span| span.child("querier table chunks")),
                projection,
            )
            .await?;

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
        Ok(chunks)
    }

    fn record_query(
        &self,
        ctx: &IOxSessionContext,
        query_type: &str,
        query_text: QueryText,
    ) -> QueryCompletedToken {
        // When the query token is dropped the query entry's completion time
        // will be set.
        let query_log = Arc::clone(&self.query_log);
        let trace_id = ctx.span().map(|s| s.ctx.trace_id);
        let entry = query_log.push(self.id, query_type, query_text, trace_id);
        QueryCompletedToken::new(move |success| query_log.set_completed(entry, success))
    }

    fn as_meta(&self) -> &dyn QueryNamespaceMeta {
        self
    }
}

pub struct QuerierCatalogProvider {
    /// Namespace ID.
    namespace_id: NamespaceId,

    /// A snapshot of all tables.
    tables: Arc<HashMap<Arc<str>, Arc<QuerierTable>>>,

    /// Query log.
    query_log: Arc<QueryLog>,
}

impl QuerierCatalogProvider {
    fn from_namespace(namespace: &QuerierNamespace) -> Self {
        Self {
            namespace_id: namespace.id,
            tables: Arc::clone(&namespace.tables),
            query_log: Arc::clone(&namespace.query_log),
        }
    }
}

impl CatalogProvider for QuerierCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn schema_names(&self) -> Vec<String> {
        vec![DEFAULT_SCHEMA.to_string(), SYSTEM_SCHEMA.to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        match name {
            DEFAULT_SCHEMA => Some(Arc::new(UserSchemaProvider {
                tables: Arc::clone(&self.tables),
            })),
            SYSTEM_SCHEMA => Some(Arc::new(SystemSchemaProvider::new(
                Arc::clone(&self.query_log),
                self.namespace_id,
            ))),
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
    fn new_query_context(&self, span_ctx: Option<SpanContext>) -> IOxSessionContext {
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
    use crate::namespace::test_util::{clear_parquet_cache, querier_namespace};
    use arrow::record_batch::RecordBatch;
    use arrow_util::assert_batches_sorted_eq;
    use data_types::ColumnType;
    use datafusion::common::DataFusionError;
    use iox_query::frontend::sql::SqlQueryPlanner;
    use iox_tests::util::{TestCatalog, TestParquetFileBuilder};
    use metric::{Observation, RawReporter};
    use regex::Regex;
    use snafu::{ResultExt, Snafu};
    use trace::{span::SpanStatus, RingBufferTraceCollector};

    #[tokio::test]
    async fn test_query() {
        test_helpers::maybe_start_logging();

        let catalog = TestCatalog::new();

        // namespace with infinite retention policy
        let ns = catalog.create_namespace_with_retention("ns", None).await;

        let shard1 = ns.create_shard(1).await;
        let shard2 = ns.create_shard(2).await;

        let table_cpu = ns.create_table("cpu").await;
        let table_mem = ns.create_table("mem").await;

        table_cpu.create_column("host", ColumnType::Tag).await;
        table_cpu.create_column("time", ColumnType::Time).await;
        table_cpu.create_column("load", ColumnType::F64).await;
        table_cpu.create_column("foo", ColumnType::I64).await;
        table_mem.create_column("host", ColumnType::Tag).await;
        table_mem.create_column("time", ColumnType::Time).await;
        table_mem.create_column("perc", ColumnType::F64).await;

        let partition_cpu_a_1 = table_cpu.with_shard(&shard1).create_partition("a").await;
        let partition_cpu_a_2 = table_cpu.with_shard(&shard2).create_partition("a").await;
        let partition_cpu_b_1 = table_cpu.with_shard(&shard1).create_partition("b").await;
        let partition_mem_c_1 = table_mem.with_shard(&shard1).create_partition("c").await;
        let partition_mem_c_2 = table_mem.with_shard(&shard2).create_partition("c").await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("cpu,host=a load=1 11")
            .with_max_seq(1)
            .with_min_time(11)
            .with_max_time(11);
        partition_cpu_a_1.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("cpu,host=a load=2 22")
            .with_max_seq(2)
            .with_min_time(22)
            .with_max_time(22);
        partition_cpu_a_1
            .create_parquet_file(builder)
            .await
            .flag_for_delete() // will be pruned because of soft delete
            .await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("cpu,host=z load=0 0")
            .with_max_seq(2)
            .with_min_time(22)
            .with_max_time(22);
        partition_cpu_a_1.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("cpu,host=a load=3 33")
            .with_max_seq(3)
            .with_min_time(33)
            .with_max_time(33);
        partition_cpu_a_1.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("cpu,host=a load=4 10001")
            .with_max_seq(4)
            .with_min_time(10_001)
            .with_max_time(10_001);
        partition_cpu_a_2.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("cpu,host=b load=5 11")
            .with_max_seq(5)
            .with_min_time(11)
            .with_max_time(11);
        partition_cpu_b_1.create_parquet_file(builder).await;

        // row `host=d perc=52 13` will be removed by the tombstone
        let lp = [
            "mem,host=c perc=50 11",
            "mem,host=c perc=51 12",
            "mem,host=d perc=52 13",
            "mem,host=d perc=53 14",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol(&lp)
            .with_max_seq(6)
            .with_min_time(11)
            .with_max_time(14);
        partition_mem_c_1.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("mem,host=c perc=50 1001")
            .with_max_seq(7)
            .with_min_time(1001)
            .with_max_time(1001);
        partition_mem_c_2
            .create_parquet_file(builder)
            .await
            .flag_for_delete()
            .await;

        // will be pruned by the tombstone
        let builder = TestParquetFileBuilder::default()
            .with_line_protocol("mem,host=d perc=55 1")
            .with_max_seq(7)
            .with_min_time(1)
            .with_max_time(1);
        partition_mem_c_1.create_parquet_file(builder).await;

        table_mem
            .with_shard(&shard1)
            .create_tombstone(1000, 1, 13, "host=d")
            .await;

        let querier_namespace = Arc::new(querier_namespace(&ns).await);

        let traces = Arc::new(RingBufferTraceCollector::new(100));
        let span_ctx = SpanContext::new(Arc::clone(&traces) as _);

        assert_query_with_span_ctx(
            &querier_namespace,
            "SELECT * FROM cpu WHERE host != 'z' ORDER BY host,time",
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
            Some(span_ctx),
        )
        .await;

        // check span
        let span = traces
            .spans()
            .into_iter()
            .find(|s| s.name == "querier table chunks")
            .expect("tracing span not found");
        assert_eq!(span.status, SpanStatus::Ok);

        // check metrics
        let mut reporter = RawReporter::default();
        catalog.metric_registry().report(&mut reporter);
        assert_eq!(
            reporter
                .metric("query_pruner_chunks")
                .unwrap()
                .observation(&[("result", "pruned_early")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("query_pruner_rows")
                .unwrap()
                .observation(&[("result", "pruned_early")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("query_pruner_bytes")
                .unwrap()
                .observation(&[("result", "pruned_early")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("query_pruner_chunks")
                .unwrap()
                .observation(&[("result", "pruned_late")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("query_pruner_rows")
                .unwrap()
                .observation(&[("result", "pruned_late")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("query_pruner_bytes")
                .unwrap()
                .observation(&[("result", "pruned_late")])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("query_pruner_chunks")
                .unwrap()
                .observation(&[("result", "not_pruned")])
                .unwrap(),
            &Observation::U64Counter(5),
        );
        assert_eq!(
            reporter
                .metric("query_pruner_rows")
                .unwrap()
                .observation(&[("result", "not_pruned")])
                .unwrap(),
            &Observation::U64Counter(5),
        );
        if let Observation::U64Counter(bytes) = reporter
            .metric("query_pruner_bytes")
            .unwrap()
            .observation(&[("result", "not_pruned")])
            .unwrap()
        {
            assert!(*bytes > 6000, "bytes ({bytes}) must be > 6000");
        } else {
            panic!("Wrong metrics type");
        }
        assert_eq!(
            reporter
                .metric("query_pruner_chunks")
                .unwrap()
                .observation(&[
                    ("result", "could_not_prune"),
                    ("reason", "No expression on predicate")
                ])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("query_pruner_rows")
                .unwrap()
                .observation(&[
                    ("result", "could_not_prune"),
                    ("reason", "No expression on predicate")
                ])
                .unwrap(),
            &Observation::U64Counter(0),
        );
        assert_eq!(
            reporter
                .metric("query_pruner_bytes")
                .unwrap()
                .observation(&[
                    ("result", "could_not_prune"),
                    ("reason", "No expression on predicate")
                ])
                .unwrap(),
            &Observation::U64Counter(0),
        );

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

        // ---------------------------------------------------------
        // EXPLAIN

        // 5 chunks but one was flaged for deleted -> 4 chunks left
        // all chunks are persisted and do not overlap -> they will be scanned in one IOxReadFilterNode node
        assert_explain(
            &querier_namespace,
            "EXPLAIN SELECT * FROM cpu",
            &[
                "+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                                                                                                                                                                                                                                                  |",
                "+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Projection: cpu.foo, cpu.host, cpu.load, cpu.time                                                                                                                                                                                                                                                                                                                     |",
                "|               |   TableScan: cpu projection=[foo, host, load, time]                                                                                                                                                                                                                                                                                                                   |",
                "| physical_plan | ProjectionExec: expr=[foo@0 as foo, host@1 as host, load@2 as load, time@3 as time]                                                                                                                                                                                                                                                                                   |",
                "|               |   ParquetExec: limit=None, partitions={1 group: [[1/1/1/1/<uuid>.parquet, 1/1/1/1/<uuid>.parquet, 1/1/1/1/<uuid>.parquet, 1/1/2/2/<uuid>.parquet, 1/1/1/3/<uuid>.parquet]]}, projection=[foo, host, load, time] |",
                "|               |                                                                                                                                                                                                                                                                                                                                                                       |",
                "+---------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
        )
            .await;

        // 3 chunks but 1 (with time = 1) got pruned by the tombstone  --> 2 chunks left
        // The 2 participated chunks in the plan do not overlap -> no deduplication, no sort. Final sort is for order by
        // FilterExec is for the tombstone
        assert_explain(
            &querier_namespace,
            "EXPLAIN SELECT * FROM mem ORDER BY host,time",
            &[
                "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| plan_type     | plan                                                                                                                                               |",
                "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------+",
                "| logical_plan  | Sort: mem.host ASC NULLS LAST, mem.time ASC NULLS LAST                                                                                             |",
                "|               |   Projection: mem.host, mem.perc, mem.time                                                                                                         |",
                "|               |     TableScan: mem projection=[host, perc, time]                                                                                                   |",
                "| physical_plan | SortExec: [host@0 ASC NULLS LAST,time@2 ASC NULLS LAST]                                                                                            |",
                "|               |   CoalescePartitionsExec                                                                                                                           |",
                "|               |     ProjectionExec: expr=[host@0 as host, perc@1 as perc, time@2 as time]                                                                          |",
                "|               |       UnionExec                                                                                                                                    |",
                "|               |         CoalesceBatchesExec: target_batch_size=4096                                                                                                |",
                "|               |           FilterExec: time@2 < 1 OR time@2 > 13 OR NOT host@0 = CAST(d AS Dictionary(Int32, Utf8))                                                 |",
                "|               |             ParquetExec: limit=None, partitions={1 group: [[1/2/1/4/<uuid>.parquet]]}, projection=[host, perc, time] |",
                "|               |         CoalesceBatchesExec: target_batch_size=4096                                                                                                |",
                "|               |           FilterExec: time@2 < 1 OR time@2 > 13 OR NOT host@0 = CAST(d AS Dictionary(Int32, Utf8))                                                 |",
                "|               |             ParquetExec: limit=None, partitions={1 group: [[1/2/1/4/<uuid>.parquet]]}, projection=[host, perc, time] |",
                "|               |                                                                                                                                                    |",
                "+---------------+----------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
        )
            .await;

        // -----------
        // Add an overlapped chunk
        // (overlaps `partition_cpu_a_2`)
        let builder = TestParquetFileBuilder::default()
            // duplicate row with different field value (load=14)
            .with_line_protocol("cpu,host=a load=14 10001")
            .with_max_seq(2_000)
            .with_min_time(10_001)
            .with_max_time(10_001);
        partition_cpu_a_2.create_parquet_file(builder).await;

        // Since we made a new parquet file, we need to tell querier about it
        clear_parquet_cache(&querier_namespace, table_cpu.table.id);

        assert_query(
            &querier_namespace,
            "SELECT * FROM cpu", // no need `order by` because data is sorted before comparing in assert_query
            &[
                "+-----+------+------+--------------------------------+",
                "| foo | host | load | time                           |",
                "+-----+------+------+--------------------------------+",
                "|     | a    | 1    | 1970-01-01T00:00:00.000000011Z |",
                "|     | a    | 3    | 1970-01-01T00:00:00.000000033Z |",
                "|     | a    | 14   | 1970-01-01T00:00:00.000010001Z |", // load has most recent value 14
                "|     | b    | 5    | 1970-01-01T00:00:00.000000011Z |",
                "|     | z    | 0    | 1970-01-01T00:00:00Z           |",
                "+-----+------+------+--------------------------------+",
            ],
        )
        .await;

        // 5 chunks:
        //   . 2 chunks overlap  with each other and must be deduplicated but no sort needed because they are sorted on the same sort key
        //   . 3 chunks do not overlap and have no duplicated --> will be scanned in one IOxReadFilterNode node
        assert_explain(
            &querier_namespace,
            "EXPLAIN SELECT * FROM cpu",
            &[
    "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
    "| plan_type     | plan                                                                                                                                                                                                                                                                                                              |",
    "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
    "| logical_plan  | Projection: cpu.foo, cpu.host, cpu.load, cpu.time                                                                                                                                                                                                                                                                 |",
    "|               |   TableScan: cpu projection=[foo, host, load, time]                                                                                                                                                                                                                                                               |",
    "| physical_plan | ProjectionExec: expr=[foo@0 as foo, host@1 as host, load@2 as load, time@3 as time]                                                                                                                                                                                                                               |",
    "|               |   UnionExec                                                                                                                                                                                                                                                                                                       |",
    "|               |     DeduplicateExec: [host@1 ASC,time@3 ASC]                                                                                                                                                                                                                                                                      |",
    "|               |       SortPreservingMergeExec: [host@1 ASC,time@3 ASC]                                                                                                                                                                                                                                                            |",
    "|               |         UnionExec                                                                                                                                                                                                                                                                                                 |",
    "|               |           ParquetExec: limit=None, partitions={1 group: [[1/1/2/2/<uuid>.parquet]]}, output_ordering=[host@1 ASC, time@3 ASC], projection=[foo, host, load, time]                                                                                                                   |",
    "|               |           ParquetExec: limit=None, partitions={1 group: [[1/1/2/2/<uuid>.parquet]]}, output_ordering=[host@1 ASC, time@3 ASC], projection=[foo, host, load, time]                                                                                                                   |",
    "|               |     ParquetExec: limit=None, partitions={1 group: [[1/1/1/1/<uuid>.parquet, 1/1/1/1/<uuid>.parquet, 1/1/1/1/<uuid>.parquet, 1/1/1/3/<uuid>.parquet]]}, projection=[foo, host, load, time] |",
    "|               |                                                                                                                                                                                                                                                                                                                   |",
    "+---------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+",
            ],
        )
            .await;
    }

    async fn assert_query(
        querier_namespace: &Arc<QuerierNamespace>,
        sql: &str,
        expected_lines: &[&str],
    ) {
        assert_query_with_span_ctx(querier_namespace, sql, expected_lines, None).await
    }

    async fn assert_query_with_span_ctx(
        querier_namespace: &Arc<QuerierNamespace>,
        sql: &str,
        expected_lines: &[&str],
        span_ctx: Option<SpanContext>,
    ) {
        let results = run(querier_namespace, sql, span_ctx).await;
        assert_batches_sorted_eq!(expected_lines, &results);
    }

    async fn assert_explain(
        querier_namespace: &Arc<QuerierNamespace>,
        sql: &str,
        expected_lines: &[&str],
    ) {
        let results = run(querier_namespace, sql, None).await;
        let formatted = arrow_util::display::pretty_format_batches(&results).unwrap();

        let regex = Regex::new("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
            .expect("UUID regex");
        let actual_lines = formatted
            .trim()
            .split('\n')
            .map(|s| regex.replace_all(s, "<uuid>").to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            expected_lines, actual_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    }

    async fn run(
        querier_namespace: &Arc<QuerierNamespace>,
        sql: &str,
        span_ctx: Option<SpanContext>,
    ) -> Vec<RecordBatch> {
        run_res(querier_namespace, sql, span_ctx)
            .await
            .expect("Build+run plan")
    }

    #[derive(Debug, Snafu)]
    enum RunError {
        #[snafu(display("Cannot build plan: {}", source))]
        Build { source: DataFusionError },

        #[snafu(display("Cannot run plan: {}", source))]
        Run { source: DataFusionError },
    }

    async fn run_res(
        querier_namespace: &Arc<QuerierNamespace>,
        sql: &str,
        span_ctx: Option<SpanContext>,
    ) -> Result<Vec<RecordBatch>, RunError> {
        let planner = SqlQueryPlanner::default();
        let ctx = querier_namespace.new_query_context(span_ctx);

        let physical_plan = planner.query(sql, &ctx).await.context(BuildSnafu)?;

        ctx.collect(physical_plan).await.context(RunSnafu)
    }
}
