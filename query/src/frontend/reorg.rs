//! planning for physical reorganization operations (e.g. COMPACT)

use std::sync::Arc;

use datafusion::{
    logical_plan::{col, Expr, LogicalPlan, LogicalPlanBuilder},
    scalar::ScalarValue,
};
use datafusion_util::AsExpr;
use internal_types::schema::{sort::SortKey, Schema, TIME_COLUMN_NAME};
use observability_deps::tracing::{debug, trace};

use crate::{
    exec::make_stream_split,
    provider::{ChunkTableProvider, ProviderBuilder},
    QueryChunk,
};
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Chunk schema not compatible for compact plan: {}", source))]
    ChunkSchemaNotCompatible {
        source: internal_types::schema::merge::Error,
    },

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
    pub fn compact_plan<C, I>(
        &self,
        schema: Arc<Schema>,
        chunks: I,
        output_sort: SortKey<'_>,
    ) -> Result<(Arc<Schema>, LogicalPlan)>
    where
        C: QueryChunk + 'static,
        I: IntoIterator<Item = Arc<C>>,
    {
        let ScanPlan {
            plan_builder,
            provider,
        } = self.scan_and_sort_plan(schema, chunks, output_sort.clone())?;

        let mut schema = provider.iox_schema();

        // Set the sort_key of the schema to the compacted chunk's sort key
        // Try to do this only if the sort key changes so we avoid unnecessary schema copies.
        trace!(input_schema=?schema, "Setting sort key on schema for compact plan");
        if schema
            .sort_key()
            .map_or(true, |existing_key| existing_key != output_sort)
        {
            let mut schema_cloned = schema.as_ref().clone();
            schema_cloned.set_sort_key(&output_sort);
            schema = Arc::new(schema_cloned);
        }
        trace!(output_schema=?schema, "Setting sort key on schema for compact plan");

        let plan = plan_builder.build().context(BuildingPlan)?;

        debug!(table_name=provider.table_name(), plan=%plan.display_indent_schema(),
               "created compact plan for table");

        Ok((schema, plan))
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
    pub fn split_plan<C, I>(
        &self,
        schema: Arc<Schema>,
        chunks: I,
        output_sort: SortKey<'_>,
        split_time: i64,
    ) -> Result<(Arc<Schema>, LogicalPlan)>
    where
        C: QueryChunk + 'static,
        I: IntoIterator<Item = Arc<C>>,
    {
        let ScanPlan {
            plan_builder,
            provider,
        } = self.scan_and_sort_plan(schema, chunks, output_sort.clone())?;

        let mut schema = provider.iox_schema();

        // Set output_sort as the sort_key of the schema
        // Try to do this only if the sort key changes so we avoid unnecessary schema copies.
        trace!(input_schema=?schema, "Setting sort key on schema for split plan");
        if schema
            .sort_key()
            .map_or(true, |existing_key| existing_key != output_sort)
        {
            let mut schema_cloned = schema.as_ref().clone();
            schema_cloned.set_sort_key(&output_sort);
            schema = Arc::new(schema_cloned);
        }
        trace!(output_schema=?schema, "Setting sort key on schema for split plan");

        // time <= split_time
        let ts_literal = Expr::Literal(ScalarValue::TimestampNanosecond(Some(split_time)));
        let split_expr = col(TIME_COLUMN_NAME).lt_eq(ts_literal);

        let plan = plan_builder.build().context(BuildingPlan)?;

        let plan = make_stream_split(plan, split_expr);

        debug!(table_name=provider.table_name(), plan=%plan.display_indent_schema(),
               "created split plan for table");

        Ok((schema, plan))
    }

    /// Creates a scan plan for the set of chunks that:
    ///
    /// 1. Merges chunks together into a single stream
    /// 2. Deduplicates via PK as necessary
    /// 3. Sorts the result according to the requested key
    ///
    /// The plan looks like:
    ///
    /// (Sort on output_sort)
    ///   (Scan chunks) <-- any needed deduplication happens here
    fn scan_and_sort_plan<C, I>(
        &self,
        schema: Arc<Schema>,
        chunks: I,
        output_sort: SortKey<'_>,
    ) -> Result<ScanPlan<C>>
    where
        C: QueryChunk + 'static,
        I: IntoIterator<Item = Arc<C>>,
    {
        let mut chunks = chunks.into_iter().peekable();
        let table_name = match chunks.peek() {
            Some(chunk) => chunk.table_name().to_string(),
            None => panic!("No chunks provided to compact plan"),
        };
        let table_name = &table_name;

        // Prepare the plan for the table
        let mut builder = ProviderBuilder::new(table_name, schema);

        // There are no predicates in these plans, so no need to prune them
        builder = builder.add_no_op_pruner();

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

        let provider = builder.build().context(CreatingProvider { table_name })?;
        let provider = Arc::new(provider);

        // Scan all columns
        let projection = None;

        // figure out the sort expression
        let sort_exprs = output_sort
            .iter()
            .map(|(column_name, sort_options)| Expr::Sort {
                expr: Box::new(column_name.as_expr()),
                asc: !sort_options.descending,
                nulls_first: sort_options.nulls_first,
            });

        let plan_builder =
            LogicalPlanBuilder::scan(table_name, Arc::clone(&provider) as _, projection)
                .context(BuildingPlan)?
                .sort(sort_exprs)
                .context(BuildingPlan)?;

        Ok(ScanPlan {
            plan_builder,
            provider,
        })
    }
}

struct ScanPlan<C: QueryChunk + 'static> {
    plan_builder: LogicalPlanBuilder,
    provider: Arc<ChunkTableProvider<C>>,
}

#[cfg(test)]
mod test {
    use arrow::compute::SortOptions;
    use arrow_util::assert_batches_eq;
    use internal_types::schema::merge::SchemaMerger;

    use crate::{
        exec::{Executor, ExecutorType},
        test::{raw_data, TestChunk},
        QueryChunkMeta,
    };

    use super::*;

    async fn get_test_chunks() -> (Arc<Schema>, Vec<Arc<TestChunk>>) {
        // Chunk 1 with 5 rows of data on 2 tags
        let chunk1 = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(5), Some(7000))
                .with_tag_column_with_stats("tag1", Some("AL"), Some("MT"))
                .with_i64_field_column("field_int")
                .with_five_rows_of_data(),
        );

        // Chunk 2 has an extra field, and only 4 fields
        let chunk2 = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(28000), Some(220000))
                .with_tag_column_with_stats("tag1", Some("UT"), Some("WA"))
                .with_i64_field_column("field_int")
                .with_i64_field_column("field_int2")
                .with_may_contain_pk_duplicates(true)
                .with_four_rows_of_data(),
        );

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
    async fn test_compact_plan() {
        test_helpers::maybe_start_logging();

        let (schema, chunks) = get_test_chunks().await;

        let mut sort_key = SortKey::with_capacity(2);
        sort_key.push(
            "tag1",
            SortOptions {
                descending: true,
                nulls_first: true,
            },
        );
        sort_key.push(
            "time",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );

        let (_, compact_plan) = ReorgPlanner::new()
            .compact_plan(schema, chunks, sort_key)
            .expect("created compact plan");

        let executor = Executor::new(1);
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .prepare_plan(&compact_plan)
            .unwrap();
        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            1,
            "{:?}",
            physical_plan.output_partitioning()
        );

        let batches = datafusion::physical_plan::collect(physical_plan)
            .await
            .unwrap();

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

        let mut sort_key = SortKey::with_capacity(1);
        sort_key.push(
            "time",
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        );

        // split on 1000 should have timestamps 1000, 5000, and 7000
        let (_, split_plan) = ReorgPlanner::new()
            .split_plan(schema, chunks, sort_key, 1000)
            .expect("created compact plan");

        let executor = Executor::new(1);
        let physical_plan = executor
            .new_context(ExecutorType::Reorg)
            .prepare_plan(&split_plan)
            .unwrap();

        assert_eq!(
            physical_plan.output_partitioning().partition_count(),
            2,
            "{:?}",
            physical_plan.output_partitioning()
        );

        // verify that the stream was split
        let stream0 = physical_plan.execute(0).await.expect("ran the plan");
        let batches0 = datafusion::physical_plan::common::collect(stream0)
            .await
            .expect("plan ran without error");

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

        let stream1 = physical_plan.execute(1).await.expect("ran the plan");
        let batches1 = datafusion::physical_plan::common::collect(stream1)
            .await
            .expect("plan ran without error");

        // Note sorted on time
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
}
