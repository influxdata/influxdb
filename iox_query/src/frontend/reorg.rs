//! planning for physical reorganization operations (e.g. COMPACT)

use std::sync::Arc;

use datafusion::logical_plan::{
    col, lit_timestamp_nano, provider_as_source, Expr, LogicalPlan, LogicalPlanBuilder,
};
use observability_deps::tracing::debug;
use schema::{sort::SortKey, Schema, TIME_COLUMN_NAME};

use crate::{
    exec::make_stream_split,
    provider::{ChunkTableProvider, ProviderBuilder},
    QueryChunk,
};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Chunk schema not compatible for compact plan: {}", source))]
    ChunkSchemaNotCompatible { source: schema::merge::Error },

    #[snafu(display("Reorg planner got error building plan: {}", source))]
    BuildingPlan {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(display(
        "Reorg planner got error adding chunk for table {}: {}",
        table_name,
        source
    ))]
    CreatingProvider {
        table_name: String,
        source: crate::provider::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Planner for physically rearranging chunk data. This planner
/// creates COMPACT and SPLIT plans for use in the database lifecycle manager
#[derive(Debug, Default)]
pub struct ReorgPlanner {}

impl ReorgPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an execution plan for a full scan of a single chunk.
    /// This plan is primarilty used to load chunks from one storage medium to
    /// another.
    pub fn scan_single_chunk_plan(
        &self,
        schema: Arc<Schema>,
        chunk: Arc<dyn QueryChunk>,
    ) -> Result<LogicalPlan> {
        self.scan_single_chunk_plan_with_filter(schema, chunk, None, vec![])
    }

    /// Creates an execution plan for a scan and filter data of a single chunk
    pub fn scan_single_chunk_plan_with_filter(
        &self,
        schema: Arc<Schema>,
        chunk: Arc<dyn QueryChunk>,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
    ) -> Result<LogicalPlan> {
        let table_name = chunk.table_name();
        // Prepare the plan for the table
        let mut builder = ProviderBuilder::new(table_name, schema);

        // There are no predicates in these plans, so no need to prune them
        builder = builder.add_no_op_pruner();
        builder = builder.add_chunk(Arc::clone(&chunk));

        let provider = builder
            .build()
            .context(CreatingProviderSnafu { table_name })?;

        let source = provider_as_source(Arc::new(provider));

        // Logical plan to scan given columns and apply predicates
        let plan = LogicalPlanBuilder::scan_with_filters(table_name, source, projection, filters)
            .context(BuildingPlanSnafu)?
            .build()
            .context(BuildingPlanSnafu)?;

        debug!(%table_name, plan=%plan.display_indent_schema(),
               "created single chunk scan plan");
        Ok(plan)
    }

    /// Creates an execution plan for the COMPACT operations which does the following:
    ///
    /// 1. Merges chunks together into a single stream
    /// 2. Deduplicates via PK as necessary
    /// 3. Sorts the result according to the requested key
    ///
    /// The plan looks like:
    ///
    /// (Sort on output_sort)
    ///   (Scan chunks) <-- any needed deduplication happens here
    pub fn compact_plan<I>(
        &self,
        schema: Arc<Schema>,
        chunks: I,
        sort_key: SortKey,
    ) -> Result<LogicalPlan>
    where
        I: IntoIterator<Item = Arc<dyn QueryChunk>>,
    {
        let ScanPlan {
            plan_builder,
            provider,
        } = self.sorted_scan_plan(schema, chunks, sort_key)?;
        let plan = plan_builder.build().context(BuildingPlanSnafu)?;

        debug!(table_name=provider.table_name(), plan=%plan.display_indent_schema(),
               "created compact plan for table");

        Ok(plan)
    }

    /// Creates an execution plan for the SPLIT operations which does the following:
    ///
    /// 1. Merges chunks together into a single stream
    /// 2. Deduplicates via PK as necessary
    /// 3. Sorts the result according to the requested key
    /// 4. Splits the stream on value of the `time` column: Those
    ///    rows that are on or before the time and those that are after
    ///
    /// The plan looks like:
    ///
    /// (Split on Time)
    ///   (Sort on output_sort)
    ///     (Scan chunks) <-- any needed deduplication happens here
    ///
    /// The output execution plan has two "output streams" (DataFusion partition):
    /// 1. Rows that have `time` *on or before* the split_time
    /// 2. Rows that have `time` *after* the split_time
    ///
    /// For example, if the input looks like:
    /// ```text
    ///  X | time
    /// ---+-----
    ///  b | 2000
    ///  a | 1000
    ///  c | 4000
    ///  d | 2000
    ///  e | 3000
    /// ```
    /// A split plan with `sort=time` and `split_time=2000` will produce the following two output streams
    ///
    /// ```text
    ///  X | time
    /// ---+-----
    ///  a | 1000
    ///  b | 2000
    ///  d | 2000
    /// ```
    /// and
    /// ```text
    ///  X | time
    /// ---+-----
    ///  e | 3000
    ///  c | 4000
    /// ```
    pub fn split_plan<I>(
        &self,
        schema: Arc<Schema>,
        chunks: I,
        sort_key: SortKey,
        split_time: i64,
    ) -> Result<LogicalPlan>
    where
        I: IntoIterator<Item = Arc<dyn QueryChunk>>,
    {
        let ScanPlan {
            plan_builder,
            provider,
        } = self.sorted_scan_plan(schema, chunks, sort_key)?;

        // time <= split_time
        let split_expr = col(TIME_COLUMN_NAME).lt_eq(lit_timestamp_nano(split_time));

        let plan = plan_builder.build().context(BuildingPlanSnafu)?;
        let plan = make_stream_split(plan, split_expr);

        debug!(table_name=provider.table_name(), plan=%plan.display_indent_schema(),
               "created split plan for table");

        Ok(plan)
    }

    /// Creates a scan plan for the given set of chunks.
    ///
    /// Output data of the scan will be deduplicated
    /// Output of the data will be sorted on the given sort_key if provided
    ///
    /// Refer to query::provider::build_scan_plan for the detail of the plan
    ///
    fn sorted_scan_plan<I>(
        &self,
        schema: Arc<Schema>,
        chunks: I,
        sort_key: SortKey,
    ) -> Result<ScanPlan>
    where
        I: IntoIterator<Item = Arc<dyn QueryChunk>>,
    {
        let mut chunks = chunks.into_iter().peekable();
        let table_name = match chunks.peek() {
            Some(chunk) => chunk.table_name().to_string(),
            None => panic!("No chunks provided to compact plan"),
        };
        let table_name = &table_name;

        // Prepare the plan for the table
        let mut builder = ProviderBuilder::new(table_name, schema)
            // There are no predicates in these plans, so no need to prune them
            .add_no_op_pruner()
            // Tell the scan of this provider to sort its output on the given sort_key
            .with_sort_key(sort_key);

        for chunk in chunks {
            // check that it is consistent with this table_name
            assert_eq!(
                chunk.table_name(),
                table_name,
                "Chunk {} expected table mismatch",
                chunk.id(),
            );

            builder = builder.add_chunk(chunk);
        }

        let provider = builder
            .build()
            .context(CreatingProviderSnafu { table_name })?;

        let provider = Arc::new(provider);
        let source = provider_as_source(Arc::clone(&provider) as _);

        // Scan all columns
        let projection = None;

        let plan_builder =
            LogicalPlanBuilder::scan(table_name, source, projection).context(BuildingPlanSnafu)?;

        Ok(ScanPlan {
            plan_builder,
            provider,
        })
    }
}

struct ScanPlan {
    plan_builder: LogicalPlanBuilder,
    provider: Arc<ChunkTableProvider>,
}

#[cfg(test)]
mod test {
    use arrow_util::{assert_batches_eq, assert_batches_sorted_eq};
    use datafusion_util::{test_collect, test_collect_partition};
    use schema::merge::SchemaMerger;
    use schema::sort::SortKeyBuilder;

    use crate::{
        exec::{Executor, ExecutorType},
        test::{raw_data, TestChunk},
    };

    use super::*;

    async fn get_test_chunks() -> (Arc<Schema>, Vec<Arc<dyn QueryChunk>>) {
        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(50), Some(7000))
                .with_tag_column_with_stats("tag1", Some("AL"), Some("MT"))
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        // Chunk 2 has an extra field, and only 4 fields
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(28000), Some(220000))
                .with_tag_column_with_stats("tag1", Some("UT"), Some("WA"))
                .with_i64_field_column("field_int")
                .with_i64_field_column("field_int2")
                .with_may_contain_pk_duplicates(true)
                .with_four_rows_of_data(),
        ) as Arc<dyn QueryChunk>;

        let expected = vec![
            "+-----------+------+--------------------------------+",
            "| field_int | tag1 | time                           |",
            "+-----------+------+--------------------------------+",
            "| 1000      | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 10        | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 5         | MT   | 1970-01-01T00:00:00.000005Z    |",
            "+-----------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&[Arc::clone(&chunk1)]).await);

        let expected = vec![
            "+-----------+------------+------+-----------------------------+",
            "| field_int | field_int2 | tag1 | time                        |",
            "+-----------+------------+------+-----------------------------+",
            "| 1000      | 1000       | WA   | 1970-01-01T00:00:00.000028Z |",
            "| 10        | 10         | VT   | 1970-01-01T00:00:00.000210Z |",
            "| 70        | 70         | UT   | 1970-01-01T00:00:00.000220Z |",
            "| 50        | 50         | VT   | 1970-01-01T00:00:00.000210Z |",
            "+-----------+------------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&[Arc::clone(&chunk2)]).await);

        let schema = SchemaMerger::new()
            .merge(&chunk1.schema())
            .unwrap()
            .merge(&chunk2.schema())
            .unwrap()
            .build();

        (Arc::new(schema), vec![chunk1, chunk2])
    }

    #[tokio::test]
    async fn test_sorted_scan_plan() {
        test_helpers::maybe_start_logging();

        let (schema, chunks) = get_test_chunks().await;
        let scan_plan = ReorgPlanner::new()
            .scan_single_chunk_plan(schema, chunks.into_iter().next().unwrap())
            .expect("created compact plan");

        let executor = Executor::new(1);
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&scan_plan)
            .await
            .unwrap();

        // single chunk processed
        assert_eq!(physical_plan.output_partitioning().partition_count(), 1);

        let batches = test_collect(physical_plan).await;

        // all data from chunk
        let expected = vec![
            "+-----------+------------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | time                           |",
            "+-----------+------------+------+--------------------------------+",
            "| 100       |            | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        |            | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      |            | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 5         |            | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        |            | MT   | 1970-01-01T00:00:00.000007Z    |",
            "+-----------+------------+------+--------------------------------+",
        ];

        assert_batches_sorted_eq!(&expected, &batches);

        executor.join().await;
    }

    #[tokio::test]
    async fn test_compact_plan() {
        test_helpers::maybe_start_logging();

        let (schema, chunks) = get_test_chunks().await;

        let sort_key = SortKeyBuilder::with_capacity(2)
            .with_col_opts("tag1", true, true)
            .with_col_opts(TIME_COLUMN_NAME, false, false)
            .build();

        let compact_plan = ReorgPlanner::new()
            .compact_plan(schema, chunks, sort_key)
            .expect("created compact plan");

        let executor = Executor::new(1);
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&compact_plan)
            .await
            .unwrap();
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            1,
            "{:?}",
            physical_plan.output_partitioning()
        );

        let batches = test_collect(physical_plan).await;

        // sorted on state ASC and time
        let expected = vec![
            "+-----------+------------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | time                           |",
            "+-----------+------------+------+--------------------------------+",
            "| 1000      | 1000       | WA   | 1970-01-01T00:00:00.000028Z    |",
            "| 50        | 50         | VT   | 1970-01-01T00:00:00.000210Z    |",
            "| 70        | 70         | UT   | 1970-01-01T00:00:00.000220Z    |",
            "| 1000      |            | MT   | 1970-01-01T00:00:00.000001Z    |",
            "| 5         |            | MT   | 1970-01-01T00:00:00.000005Z    |",
            "| 10        |            | MT   | 1970-01-01T00:00:00.000007Z    |",
            "| 70        |            | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 100       |            | AL   | 1970-01-01T00:00:00.000000050Z |",
            "+-----------+------------+------+--------------------------------+",
        ];

        assert_batches_eq!(&expected, &batches);

        executor.join().await;
    }

    #[tokio::test]
    async fn test_split_plan() {
        test_helpers::maybe_start_logging();
        // validate that the plumbing is all hooked up. The logic of
        // the operator is tested in its own module.
        let (schema, chunks) = get_test_chunks().await;

        let sort_key = SortKeyBuilder::with_capacity(2)
            .with_col_opts("time", false, false)
            .with_col_opts("tag1", false, true)
            .build();

        // split on 1000 should have timestamps 1000, 5000, and 7000
        let split_plan = ReorgPlanner::new()
            .split_plan(schema, chunks, sort_key, 1000)
            .expect("created compact plan");

        let executor = Executor::new(1);
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&split_plan)
            .await
            .unwrap();

        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            2,
            "{:?}",
            physical_plan.output_partitioning()
        );

        // verify that the stream was split
        let batches0 = test_collect_partition(Arc::clone(&physical_plan), 0).await;

        // Note sorted on time
        let expected = vec![
            "+-----------+------------+------+--------------------------------+",
            "| field_int | field_int2 | tag1 | time                           |",
            "+-----------+------------+------+--------------------------------+",
            "| 100       |            | AL   | 1970-01-01T00:00:00.000000050Z |",
            "| 70        |            | CT   | 1970-01-01T00:00:00.000000100Z |",
            "| 1000      |            | MT   | 1970-01-01T00:00:00.000001Z    |",
            "+-----------+------------+------+--------------------------------+",
        ];
        assert_batches_eq!(&expected, &batches0);

        let batches1 = test_collect_partition(physical_plan, 1).await;

        // Sorted on time
        let expected = vec![
            "+-----------+------------+------+-----------------------------+",
            "| field_int | field_int2 | tag1 | time                        |",
            "+-----------+------------+------+-----------------------------+",
            "| 5         |            | MT   | 1970-01-01T00:00:00.000005Z |",
            "| 10        |            | MT   | 1970-01-01T00:00:00.000007Z |",
            "| 1000      | 1000       | WA   | 1970-01-01T00:00:00.000028Z |",
            "| 50        | 50         | VT   | 1970-01-01T00:00:00.000210Z |",
            "| 70        | 70         | UT   | 1970-01-01T00:00:00.000220Z |",
            "+-----------+------------+------+-----------------------------+",
        ];

        assert_batches_eq!(&expected, &batches1);

        executor.join().await;
    }
}
