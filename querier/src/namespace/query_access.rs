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
    catalog::{schema::SchemaProvider, CatalogProvider},
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

    fn table_schema(&self, table_name: &str) -> Option<Schema> {
        self.tables.get(table_name).map(|t| t.schema().clone())
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
                ctx.child_span("QuerierNamespace chunks"),
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

    fn retention_time_ns(&self) -> Option<i64> {
        self.retention_period.map(|d| {
            self.catalog_cache.time_provider().now().timestamp_nanos() - d.as_nanos() as i64
        })
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

    /// Include debug info tables.
    include_debug_info_tables: bool,
}

impl QuerierCatalogProvider {
    fn from_namespace(namespace: &QuerierNamespace) -> Self {
        Self {
            namespace_id: namespace.id,
            tables: Arc::clone(&namespace.tables),
            query_log: Arc::clone(&namespace.query_log),
            include_debug_info_tables: namespace.include_debug_info_tables,
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
                self.include_debug_info_tables,
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

#[async_trait]
impl SchemaProvider for UserSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self as &dyn Any
    }

    fn table_names(&self) -> Vec<String> {
        let mut names: Vec<_> = self.tables.keys().map(|s| s.to_string()).collect();
        names.sort();
        names
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|t| Arc::clone(t) as _)
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

impl ExecutionContextProvider for QuerierNamespace {
    fn new_query_context(&self, span_ctx: Option<SpanContext>) -> IOxSessionContext {
        let mut cfg = self
            .exec
            .new_execution_config(ExecutorType::Query)
            .with_default_catalog(Arc::new(QuerierCatalogProvider::from_namespace(self)) as _)
            .with_span_context(span_ctx);

        for (k, v) in self.datafusion_config.as_ref() {
            cfg = cfg.with_config_option(k, v);
        }

        cfg.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::namespace::test_util::{clear_parquet_cache, querier_namespace};
    use arrow::record_batch::RecordBatch;
    use arrow_util::test_util::{batches_to_sorted_lines, Normalizer};
    use data_types::ColumnType;
    use datafusion::common::DataFusionError;
    use iox_query::frontend::sql::SqlQueryPlanner;
    use iox_tests::{TestCatalog, TestParquetFileBuilder};
    use iox_time::Time;
    use metric::{Observation, RawReporter};
    use snafu::{ResultExt, Snafu};
    use trace::{span::SpanStatus, RingBufferTraceCollector};

    #[tokio::test]
    async fn test_query() {
        test_helpers::maybe_start_logging();

        let catalog = TestCatalog::new();

        // namespace with infinite retention policy
        let ns = catalog.create_namespace_with_retention("ns", None).await;

        let table_cpu = ns.create_table("cpu").await;
        let table_mem = ns.create_table("mem").await;

        table_cpu.create_column("host", ColumnType::Tag).await;
        table_cpu.create_column("time", ColumnType::Time).await;
        table_cpu.create_column("load", ColumnType::F64).await;
        table_cpu.create_column("foo", ColumnType::I64).await;
        table_mem.create_column("host", ColumnType::Tag).await;
        table_mem.create_column("time", ColumnType::Time).await;
        table_mem.create_column("perc", ColumnType::F64).await;

        let partition_cpu_a_1 = table_cpu.create_partition("a").await;
        let partition_cpu_a_2 = table_cpu.create_partition("a").await;
        let partition_cpu_b_1 = table_cpu.create_partition("b").await;
        let partition_mem_c_1 = table_mem.create_partition("c").await;
        let partition_mem_c_2 = table_mem.create_partition("c").await;

        let builder = TestParquetFileBuilder::default()
            .with_max_l0_created_at(Time::from_timestamp_nanos(1))
            .with_line_protocol("cpu,host=a load=1 11")
            .with_min_time(11)
            .with_max_time(11);
        partition_cpu_a_1.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_max_l0_created_at(Time::from_timestamp_nanos(2))
            .with_line_protocol("cpu,host=a load=2 22")
            .with_min_time(22)
            .with_max_time(22);
        partition_cpu_a_1
            .create_parquet_file(builder)
            .await
            .flag_for_delete() // will be pruned because of soft delete
            .await;

        let builder = TestParquetFileBuilder::default()
            .with_max_l0_created_at(Time::from_timestamp_nanos(3))
            .with_line_protocol("cpu,host=z load=0 0")
            .with_min_time(22)
            .with_max_time(22);
        partition_cpu_a_1.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_max_l0_created_at(Time::from_timestamp_nanos(4))
            .with_line_protocol("cpu,host=a load=3 33")
            .with_min_time(33)
            .with_max_time(33);
        partition_cpu_a_1.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_max_l0_created_at(Time::from_timestamp_nanos(5))
            .with_line_protocol("cpu,host=a load=4 10001")
            .with_min_time(10_001)
            .with_max_time(10_001);
        partition_cpu_a_2.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_creation_time(Time::from_timestamp_nanos(6))
            .with_line_protocol("cpu,host=b load=5 11")
            .with_min_time(11)
            .with_max_time(11);
        partition_cpu_b_1.create_parquet_file(builder).await;

        let lp = [
            "mem,host=c perc=50 11",
            "mem,host=c perc=51 12",
            "mem,host=d perc=53 14",
        ]
        .join("\n");
        let builder = TestParquetFileBuilder::default()
            .with_max_l0_created_at(Time::from_timestamp_nanos(7))
            .with_line_protocol(&lp)
            .with_min_time(11)
            .with_max_time(14);
        partition_mem_c_1.create_parquet_file(builder).await;

        let builder = TestParquetFileBuilder::default()
            .with_max_l0_created_at(Time::from_timestamp_nanos(8))
            .with_line_protocol("mem,host=c perc=50 1001")
            .with_min_time(1001)
            .with_max_time(1001);
        partition_mem_c_2
            .create_parquet_file(builder)
            .await
            .flag_for_delete()
            .await;

        let querier_namespace = Arc::new(querier_namespace(&ns).await);

        let traces = Arc::new(RingBufferTraceCollector::new(100));
        let span_ctx = SpanContext::new(Arc::clone(&traces) as _);

        insta::assert_yaml_snapshot!(
            format_query_with_span_ctx(
                &querier_namespace,
                "SELECT * FROM cpu WHERE host != 'z' ORDER BY host,time",
                Some(span_ctx),
            ).await,
            @r###"
        ---
        - +-----+------+------+--------------------------------+
        - "| foo | host | load | time                           |"
        - +-----+------+------+--------------------------------+
        - "|     | a    | 1.0  | 1970-01-01T00:00:00.000000011Z |"
        - "|     | a    | 3.0  | 1970-01-01T00:00:00.000000033Z |"
        - "|     | a    | 4.0  | 1970-01-01T00:00:00.000010001Z |"
        - "|     | b    | 5.0  | 1970-01-01T00:00:00.000000011Z |"
        - +-----+------+------+--------------------------------+
        "###
        );

        // check span
        let span = traces
            .spans()
            .into_iter()
            .find(|s| s.name == "QuerierTable chunks")
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

        insta::assert_yaml_snapshot!(
            format_query(
                &querier_namespace,
                "SELECT * FROM mem ORDER BY host,time"
            ).await,
            @r###"
        ---
        - +------+------+--------------------------------+
        - "| host | perc | time                           |"
        - +------+------+--------------------------------+
        - "| c    | 50.0 | 1970-01-01T00:00:00.000000011Z |"
        - "| c    | 51.0 | 1970-01-01T00:00:00.000000012Z |"
        - "| d    | 53.0 | 1970-01-01T00:00:00.000000014Z |"
        - +------+------+--------------------------------+
        "###
        );

        // ---------------------------------------------------------
        // EXPLAIN

        // 5 chunks but one was flaged for deleted -> 4 chunks left
        // all chunks are persisted and do not overlap -> they will be scanned in one IOxReadFilterNode node
        insta::assert_yaml_snapshot!(
            format_explain(&querier_namespace, "EXPLAIN SELECT * FROM cpu").await,
            @r###"
        ---
        - "----------"
        - "| plan_type    | plan    |"
        - "----------"
        - "| logical_plan    | TableScan: cpu projection=[foo, host, load, time]    |"
        - "| physical_plan    | ParquetExec: file_groups={1 group: [[1/1/1/00000000-0000-0000-0000-000000000000.parquet, 1/1/1/00000000-0000-0000-0000-000000000001.parquet, 1/1/1/00000000-0000-0000-0000-000000000002.parquet, 1/1/1/00000000-0000-0000-0000-000000000003.parquet, 1/1/1/00000000-0000-0000-0000-000000000004.parquet]]}, projection=[foo, host, load, time]    |"
        - "|    |    |"
        - "----------"
        "###
        );

        // The 2 participated chunks in the plan do not overlap -> no deduplication, no sort. Final
        // sort is for order by.
        insta::assert_yaml_snapshot!(
            format_explain(&querier_namespace, "EXPLAIN SELECT * FROM mem ORDER BY host,time").await,
            @r###"
        ---
        - "----------"
        - "| plan_type    | plan    |"
        - "----------"
        - "| logical_plan    | Sort: mem.host ASC NULLS LAST, mem.time ASC NULLS LAST    |"
        - "|    |   TableScan: mem projection=[host, perc, time]    |"
        - "| physical_plan    | SortExec: expr=[host@0 ASC NULLS LAST,time@2 ASC NULLS LAST]    |"
        - "|    |   ParquetExec: file_groups={1 group: [[1/1/1/00000000-0000-0000-0000-000000000000.parquet]]}, projection=[host, perc, time], output_ordering=[host@0 ASC, time@2 ASC]    |"
        - "|    |    |"
        - "----------"
        "###
        );

        // -----------
        // Add an overlapped chunk
        // (overlaps `partition_cpu_a_2`)
        let builder = TestParquetFileBuilder::default()
            .with_max_l0_created_at(Time::from_timestamp_nanos(10))
            // duplicate row with different field value (load=14)
            .with_line_protocol("cpu,host=a load=14 10001")
            .with_min_time(10_001)
            .with_max_time(10_001);
        partition_cpu_a_2.create_parquet_file(builder).await;

        // Since we made a new parquet file, we need to tell querier about it
        clear_parquet_cache(&querier_namespace, table_cpu.table.id);

        insta::assert_yaml_snapshot!(
            format_query(&querier_namespace,
                         "SELECT * FROM cpu", // no need `order by` because data is sorted before comparing in assert_query
            ).await,
            @r###"
        ---
        - +-----+------+------+--------------------------------+
        - "| foo | host | load | time                           |"
        - +-----+------+------+--------------------------------+
        - "|     | a    | 1.0  | 1970-01-01T00:00:00.000000011Z |"
        - "|     | a    | 14.0 | 1970-01-01T00:00:00.000010001Z |"
        - "|     | a    | 3.0  | 1970-01-01T00:00:00.000000033Z |"
        - "|     | b    | 5.0  | 1970-01-01T00:00:00.000000011Z |"
        - "|     | z    | 0.0  | 1970-01-01T00:00:00Z           |"
        - +-----+------+------+--------------------------------+
        "###
        );

        // 5 chunks:
        //   . 2 chunks overlap  with each other and must be deduplicated but no sort needed because they are sorted on the same sort key
        //   . 3 chunks do not overlap and have no duplicated --> will be scanned in one IOxReadFilterNode node
        insta::assert_yaml_snapshot!(
            format_explain(&querier_namespace, "EXPLAIN SELECT * FROM cpu").await,
            @r###"
        ---
        - "----------"
        - "| plan_type    | plan    |"
        - "----------"
        - "| logical_plan    | TableScan: cpu projection=[foo, host, load, time]    |"
        - "| physical_plan    | UnionExec    |"
        - "|    |   ParquetExec: file_groups={1 group: [[1/1/1/00000000-0000-0000-0000-000000000000.parquet]]}, projection=[foo, host, load, time], output_ordering=[host@1 ASC, time@3 ASC]    |"
        - "|    |   ParquetExec: file_groups={1 group: [[1/1/1/00000000-0000-0000-0000-000000000001.parquet, 1/1/1/00000000-0000-0000-0000-000000000002.parquet, 1/1/1/00000000-0000-0000-0000-000000000003.parquet]]}, projection=[foo, host, load, time]    |"
        - "|    |   ProjectionExec: expr=[foo@1 as foo, host@2 as host, load@3 as load, time@4 as time]    |"
        - "|    |     DeduplicateExec: [host@2 ASC,time@4 ASC]    |"
        - "|    |       SortPreservingMergeExec: [host@2 ASC,time@4 ASC,__chunk_order@0 ASC]    |"
        - "|    |         ParquetExec: file_groups={2 groups: [[1/1/1/00000000-0000-0000-0000-000000000004.parquet], [1/1/1/00000000-0000-0000-0000-000000000005.parquet]]}, projection=[__chunk_order, foo, host, load, time], output_ordering=[host@2 ASC, time@4 ASC, __chunk_order@0 ASC]    |"
        - "|    |    |"
        - "----------"
        "###
        );
    }

    async fn format_query(querier_namespace: &Arc<QuerierNamespace>, sql: &str) -> Vec<String> {
        format_query_with_span_ctx(querier_namespace, sql, None).await
    }

    async fn format_query_with_span_ctx(
        querier_namespace: &Arc<QuerierNamespace>,
        sql: &str,
        span_ctx: Option<SpanContext>,
    ) -> Vec<String> {
        let results = run(querier_namespace, sql, span_ctx).await;
        batches_to_sorted_lines(&results)
    }

    async fn format_explain(querier_namespace: &Arc<QuerierNamespace>, sql: &str) -> Vec<String> {
        let results = run(querier_namespace, sql, None).await;
        let normalizer = Normalizer {
            normalized_uuids: true,
            ..Default::default()
        };
        normalizer.normalize_results(results)
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
