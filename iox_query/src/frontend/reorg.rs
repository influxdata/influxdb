//! planning for physical reorganization operations (e.g. COMPACT)

use std::sync::Arc;

use datafusion::{
    logical_expr::LogicalPlan,
    prelude::{col, lit_timestamp_nano},
};
use observability_deps::tracing::debug;
use schema::{sort::SortKey, Schema, TIME_COLUMN_NAME};

use crate::{
    exec::make_stream_split, provider::ProviderBuilder, util::logical_sort_key_exprs, QueryChunk,
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
        "Reorg planner got error adding creating scan for {}: {}",
        table_name,
        source
    ))]
    CreatingScan {
        table_name: String,
        source: crate::provider::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<datafusion::error::DataFusionError> for Error {
    fn from(source: datafusion::error::DataFusionError) -> Self {
        Self::BuildingPlan { source }
    }
}

/// Planner for physically rearranging chunk data. This planner
/// creates COMPACT and SPLIT plans for use in the database lifecycle manager
#[derive(Debug, Default)]
pub struct ReorgPlanner {}

impl ReorgPlanner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an execution plan for the COMPACT operations which does the following:
    ///
    /// 1. Merges chunks together into a single stream
    /// 2. Deduplicates via PK as necessary
    /// 3. Sorts the result according to the requested `output_sort_key` (if necessary)
    ///
    /// The plan looks like:
    ///
    /// ```text
    /// (Optional Sort on output_sort_key)
    ///   (Scan chunks) <-- any needed deduplication happens here
    /// ```
    pub fn compact_plan<I>(
        &self,
        table_name: Arc<str>,
        schema: &Schema,
        chunks: I,
        output_sort_key: SortKey,
    ) -> Result<LogicalPlan>
    where
        I: IntoIterator<Item = Arc<dyn QueryChunk>>,
    {
        let mut builder = ProviderBuilder::new(Arc::clone(&table_name), schema.clone())
            .with_enable_deduplication(true);

        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        let provider = builder.build().context(CreatingScanSnafu {
            table_name: table_name.as_ref(),
        })?;
        let plan_builder = Arc::new(provider)
            .into_logical_plan_builder()
            .context(BuildingPlanSnafu)?;
        let sort_expr = logical_sort_key_exprs(&output_sort_key);
        let plan = plan_builder
            .sort(sort_expr)
            .context(BuildingPlanSnafu)?
            .build()
            .context(BuildingPlanSnafu)?;

        debug!(table_name=table_name.as_ref(), plan=%plan.display_indent_schema(),
               "created compact plan for table");

        Ok(plan)
    }

    /// Creates an execution plan for the SPLIT operations which does the following:
    ///
    /// 1. Merges chunks together into a single stream
    /// 2. Deduplicates via PK as necessary
    /// 3. Sorts the result according to the requested output_sort_key
    /// 4. Splits the stream on value of the `time` column: Those
    ///    rows that are on or before the time and those that are after
    ///
    /// The plan looks like:
    ///
    /// ```text
    /// (Split on Time)
    ///   (Sort on output_sort)
    ///     (Scan chunks) <-- any needed deduplication happens here
    /// ```
    ///
    /// The output execution plan has `N` "output streams" (DataFusion
    /// partitions) where `N` = `split_times.len() + 1`. The
    /// time ranges of the streams are:
    ///
    /// Stream 0: Rows that have `time` *on or before* the `split_times[0]`
    ///
    /// Stream i, where 0 < i < split_times.len():
    /// Rows have: `time` in range `(split_times[i-1], split_times[i]]`,
    ///  Which is: greater than `split_times[i-1]` up to and including `split_times[i]`.
    ///
    /// Stream n, where n = split_times.len()): Rows that have `time`
    /// *after* `split_times[n-1]` as well as NULL rows
    ///
    /// # Panics
    ///
    /// The code will panic if split_times are not in monotonically increasing order
    ///
    /// # Example
    /// if the input looks like:
    /// ```text
    ///  X | time
    /// ---+-----
    ///  b | 2000
    ///  a | 1000
    ///  c | 4000
    ///  d | 2000
    ///  e | 3000
    /// ```
    /// A split plan with `sort=time` and `split_times=[2000, 3000]` will produce the following three output streams
    ///
    /// ```text
    ///  X | time
    /// ---+-----
    ///  a | 1000
    ///  b | 2000
    ///  d | 2000
    /// ```
    ///
    /// ```text
    ///  X | time
    /// ---+-----
    ///  e | 3000
    /// ```
    ///
    /// ```text
    ///  X | time
    /// ---+-----
    ///  c | 4000
    /// ```
    pub fn split_plan<I>(
        &self,
        table_name: Arc<str>,
        schema: &Schema,
        chunks: I,
        output_sort_key: SortKey,
        split_times: Vec<i64>,
    ) -> Result<LogicalPlan>
    where
        I: IntoIterator<Item = Arc<dyn QueryChunk>>,
    {
        // split_times must have values
        if split_times.is_empty() {
            panic!("Split plan does not accept empty split_times");
        }

        let mut builder = ProviderBuilder::new(Arc::clone(&table_name), schema.clone())
            .with_enable_deduplication(true);

        for chunk in chunks {
            builder = builder.add_chunk(chunk);
        }

        let provider = builder.build().context(CreatingScanSnafu {
            table_name: table_name.as_ref(),
        })?;
        let plan_builder = Arc::new(provider)
            .into_logical_plan_builder()
            .context(BuildingPlanSnafu)?;
        let sort_expr = logical_sort_key_exprs(&output_sort_key);
        let plan = plan_builder
            .sort(sort_expr)
            .context(BuildingPlanSnafu)?
            .build()
            .context(BuildingPlanSnafu)?;

        let mut split_exprs = Vec::with_capacity(split_times.len());
        // time <= split_times[0]
        split_exprs.push(col(TIME_COLUMN_NAME).lt_eq(lit_timestamp_nano(split_times[0])));
        // split_times[i-1] , time <= split_time[i]
        for i in 1..split_times.len() {
            if split_times[i - 1] >= split_times[i] {
                panic!(
                    "split_times[{}]: {} must be smaller than split_times[{}]: {}",
                    i - 1,
                    split_times[i - 1],
                    i,
                    split_times[i]
                );
            }
            split_exprs.push(
                col(TIME_COLUMN_NAME)
                    .gt(lit_timestamp_nano(split_times[i - 1]))
                    .and(col(TIME_COLUMN_NAME).lt_eq(lit_timestamp_nano(split_times[i]))),
            );
        }
        let plan = make_stream_split(plan, split_exprs);

        debug!(table_name=table_name.as_ref(), plan=%plan.display_indent_schema(),
               "created split plan for table");

        Ok(plan)
    }
}

#[cfg(test)]
mod test {
    use arrow_util::assert_batches_eq;
    use datafusion_util::{test_collect, test_collect_partition};
    use schema::merge::SchemaMerger;
    use schema::sort::SortKeyBuilder;

    use crate::{
        exec::{Executor, ExecutorType},
        test::{format_execution_plan, raw_data, TestChunk},
    };

    use super::*;

    async fn get_test_chunks() -> (Schema, Vec<Arc<dyn QueryChunk>>) {
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
            .merge(chunk1.schema())
            .unwrap()
            .merge(chunk2.schema())
            .unwrap()
            .build();

        (schema, vec![chunk1, chunk2])
    }

    async fn get_sorted_test_chunks() -> (Schema, Vec<Arc<dyn QueryChunk>>) {
        // Chunk 1
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(1000), Some(1000))
                .with_tag_column_with_stats("tag1", Some("A"), Some("A"))
                .with_i64_field_column("field_int")
                .with_one_row_of_specific_data("A", 1, 1000),
        ) as Arc<dyn QueryChunk>;

        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 1         | A    | 1970-01-01T00:00:00.000001Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&[Arc::clone(&chunk1)]).await);

        // Chunk 2
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(2000), Some(2000))
                .with_tag_column_with_stats("tag1", Some("B"), Some("B"))
                .with_i64_field_column("field_int")
                .with_one_row_of_specific_data("B", 2, 2000),
        ) as Arc<dyn QueryChunk>;

        let expected = vec![
            "+-----------+------+-----------------------------+",
            "| field_int | tag1 | time                        |",
            "+-----------+------+-----------------------------+",
            "| 2         | B    | 1970-01-01T00:00:00.000002Z |",
            "+-----------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &raw_data(&[Arc::clone(&chunk2)]).await);

        (chunk1.schema().clone(), vec![chunk1, chunk2])
    }

    #[tokio::test]
    async fn test_compact_plan_sorted() {
        test_helpers::maybe_start_logging();

        // ensures that the output is actually sorted
        // https://github.com/influxdata/influxdb_iox/issues/6125
        let (schema, chunks) = get_sorted_test_chunks().await;

        let chunk_orders = vec![
            // reverse order
            vec![Arc::clone(&chunks[1]), Arc::clone(&chunks[0])],
            chunks,
        ];

        // executor has only 1 thread
        let executor = Executor::new_testing();
        for chunks in chunk_orders {
            let sort_key = SortKeyBuilder::with_capacity(2)
                .with_col_opts("tag1", false, true)
                .with_col_opts(TIME_COLUMN_NAME, false, true)
                .build();

            let compact_plan = ReorgPlanner::new()
                .compact_plan(Arc::from("t"), &schema, chunks, sort_key)
                .expect("created compact plan");

            let physical_plan = executor
                .new_context(ExecutorType::Reorg)
                .create_physical_plan(&compact_plan)
                .await
                .unwrap();

            let batches = test_collect(physical_plan).await;

            // should be sorted on tag1 then timestamp
            let expected = vec![
                "+-----------+------+-----------------------------+",
                "| field_int | tag1 | time                        |",
                "+-----------+------+-----------------------------+",
                "| 1         | A    | 1970-01-01T00:00:00.000001Z |",
                "| 2         | B    | 1970-01-01T00:00:00.000002Z |",
                "+-----------+------+-----------------------------+",
            ];

            assert_batches_eq!(&expected, &batches);
        }
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
            .compact_plan(Arc::from("t"), &schema, chunks, sort_key)
            .expect("created compact plan");

        let executor = Executor::new_testing();
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&compact_plan)
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(
            format_execution_plan(&physical_plan),
            @r###"
        ---
        - " SortPreservingMergeExec: [tag1@2 DESC,time@3 ASC NULLS LAST]"
        - "   UnionExec"
        - "     SortExec: expr=[tag1@2 DESC,time@3 ASC NULLS LAST]"
        - "       RecordBatchesExec: chunks=1"
        - "     SortExec: expr=[tag1@2 DESC,time@3 ASC NULLS LAST]"
        - "       ProjectionExec: expr=[field_int@1 as field_int, field_int2@2 as field_int2, tag1@3 as tag1, time@4 as time]"
        - "         DeduplicateExec: [tag1@3 ASC,time@4 ASC]"
        - "           SortExec: expr=[tag1@3 ASC,time@4 ASC,__chunk_order@0 ASC]"
        - "             RecordBatchesExec: chunks=1"
        "###
        );

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
            .split_plan(Arc::from("t"), &schema, chunks, sort_key, vec![1000])
            .expect("created compact plan");

        let executor = Executor::new_testing();
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&split_plan)
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(
            format_execution_plan(&physical_plan),
            @r###"
        ---
        - " StreamSplitExec"
        - "   SortPreservingMergeExec: [time@3 ASC NULLS LAST,tag1@2 ASC]"
        - "     UnionExec"
        - "       SortExec: expr=[time@3 ASC NULLS LAST,tag1@2 ASC]"
        - "         RecordBatchesExec: chunks=1"
        - "       SortExec: expr=[time@3 ASC NULLS LAST,tag1@2 ASC]"
        - "         ProjectionExec: expr=[field_int@1 as field_int, field_int2@2 as field_int2, tag1@3 as tag1, time@4 as time]"
        - "           DeduplicateExec: [tag1@3 ASC,time@4 ASC]"
        - "             SortExec: expr=[tag1@3 ASC,time@4 ASC,__chunk_order@0 ASC]"
        - "               RecordBatchesExec: chunks=1"
        "###
        );

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
    }

    #[tokio::test]
    async fn test_split_plan_multi_exps() {
        test_helpers::maybe_start_logging();
        // validate that the plumbing is all hooked up. The logic of
        // the operator is tested in its own module.
        let (schema, chunks) = get_test_chunks().await;

        let sort_key = SortKeyBuilder::with_capacity(2)
            .with_col_opts("time", false, false)
            .with_col_opts("tag1", false, true)
            .build();

        // split on 1000 and 7000
        let split_plan = ReorgPlanner::new()
            .split_plan(Arc::from("t"), &schema, chunks, sort_key, vec![1000, 7000])
            .expect("created compact plan");

        let executor = Executor::new_testing();
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .create_physical_plan(&split_plan)
            .await
            .unwrap();

        insta::assert_yaml_snapshot!(
            format_execution_plan(&physical_plan),
            @r###"
        ---
        - " StreamSplitExec"
        - "   SortPreservingMergeExec: [time@3 ASC NULLS LAST,tag1@2 ASC]"
        - "     UnionExec"
        - "       SortExec: expr=[time@3 ASC NULLS LAST,tag1@2 ASC]"
        - "         RecordBatchesExec: chunks=1"
        - "       SortExec: expr=[time@3 ASC NULLS LAST,tag1@2 ASC]"
        - "         ProjectionExec: expr=[field_int@1 as field_int, field_int2@2 as field_int2, tag1@3 as tag1, time@4 as time]"
        - "           DeduplicateExec: [tag1@3 ASC,time@4 ASC]"
        - "             SortExec: expr=[tag1@3 ASC,time@4 ASC,__chunk_order@0 ASC]"
        - "               RecordBatchesExec: chunks=1"
        "###
        );

        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            3,
            "{:?}",
            physical_plan.output_partitioning()
        );

        // Verify that the stream was split

        // Note sorted on time
        // Should include time <= 1000
        let batches0 = test_collect_partition(Arc::clone(&physical_plan), 0).await;
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

        // Sorted on time
        // Should include 1000 < time <= 7000
        let batches1 = test_collect_partition(Arc::clone(&physical_plan), 1).await;
        let expected = vec![
            "+-----------+------------+------+-----------------------------+",
            "| field_int | field_int2 | tag1 | time                        |",
            "+-----------+------------+------+-----------------------------+",
            "| 5         |            | MT   | 1970-01-01T00:00:00.000005Z |",
            "| 10        |            | MT   | 1970-01-01T00:00:00.000007Z |",
            "+-----------+------------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &batches1);

        // Sorted on time
        // Should include 7000 < time
        let batches2 = test_collect_partition(physical_plan, 2).await;
        let expected = vec![
            "+-----------+------------+------+-----------------------------+",
            "| field_int | field_int2 | tag1 | time                        |",
            "+-----------+------------+------+-----------------------------+",
            "| 1000      | 1000       | WA   | 1970-01-01T00:00:00.000028Z |",
            "| 50        | 50         | VT   | 1970-01-01T00:00:00.000210Z |",
            "| 70        | 70         | UT   | 1970-01-01T00:00:00.000220Z |",
            "+-----------+------------+------+-----------------------------+",
        ];
        assert_batches_eq!(&expected, &batches2);
    }

    #[tokio::test]
    #[should_panic(expected = "Split plan does not accept empty split_times")]
    async fn test_split_plan_panic_empty() {
        test_helpers::maybe_start_logging();
        // validate that the plumbing is all hooked up. The logic of
        // the operator is tested in its own module.
        let (schema, chunks) = get_test_chunks().await;

        let sort_key = SortKeyBuilder::with_capacity(2)
            .with_col_opts("time", false, false)
            .with_col_opts("tag1", false, true)
            .build();

        // split on 1000 and 7000
        let _split_plan = ReorgPlanner::new()
            .split_plan(Arc::from("t"), &schema, chunks, sort_key, vec![]) // reason of panic: empty split_times
            .expect("created compact plan");
    }

    #[tokio::test]
    #[should_panic(expected = "split_times[0]: 1000 must be smaller than split_times[1]: 500")]
    async fn test_split_plan_panic_times() {
        test_helpers::maybe_start_logging();
        // validate that the plumbing is all hooked up. The logic of
        // the operator is tested in its own module.
        let (schema, chunks) = get_test_chunks().await;

        let sort_key = SortKeyBuilder::with_capacity(2)
            .with_col_opts("time", false, false)
            .with_col_opts("tag1", false, true)
            .build();

        // split on 1000 and 7000
        let _split_plan = ReorgPlanner::new()
            .split_plan(Arc::from("t"), &schema, chunks, sort_key, vec![1000, 500]) // reason of panic: split_times not in ascending order
            .expect("created compact plan");
    }
}
