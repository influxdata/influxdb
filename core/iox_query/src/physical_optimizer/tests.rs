//! Optimizer edge cases.
//!
//! These are NOT part of the usual end2end query tests because they depend on very specific chunk arrangements that are
//! hard to reproduce in an end2end setting.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::{
    common::DFSchema,
    datasource::provider_as_source,
    functions_aggregate::expr_fn::count,
    logical_expr::{Expr, ExprSchemable, LogicalPlanBuilder, col, lit},
    scalar::ScalarValue,
};
use executor::DedicatedExecutor;
use schema::sort::SortKey;
use test_helpers::maybe_start_logging;

use crate::{
    QueryChunk,
    exec::{Executor, ExecutorConfig},
    provider::ProviderBuilder,
    test::{TestChunk, format_execution_plan},
};

/// Test that reconstructs specific case where parquet files may unnecessarily be sorted.
///
/// See:
/// - <https://github.com/influxdata/EAR/issues/4468>
/// - <https://github.com/influxdata/influxdb_iox/issues/9451>
#[tokio::test]
async fn test_parquet_should_not_be_resorted() {
    // DF session setup
    let config = ExecutorConfig {
        target_query_partitions: 16.try_into().unwrap(),
        ..ExecutorConfig::testing()
    };
    let exec = Executor::new_with_config_and_executor(config, DedicatedExecutor::new_testing());
    let ctx = exec.new_context();
    let state = ctx.inner().state();

    // chunks
    let c = TestChunk::new("t")
        .with_row_count(10_000)
        .with_tag_column("tag")
        .with_time_column_with_full_stats(Some(0), Some(10), None);
    let c_mem = c.clone().with_may_contain_pk_duplicates(true);
    let c_file = c
        .clone()
        .with_dummy_parquet_file_and_size(1000)
        .with_may_contain_pk_duplicates(false)
        .with_sort_key(SortKey::from_columns([Arc::from("tag"), Arc::from("time")]));
    let schema = c.schema().clone();
    let provider = ProviderBuilder::new("t".into(), schema)
        .add_chunk(Arc::new(c_mem.clone().with_id(1).with_order(i64::MAX)))
        .add_chunk(Arc::new(c_file.clone().with_id(2).with_order(2)))
        .add_chunk(Arc::new(c_file.clone().with_id(3).with_order(3)))
        .build()
        .unwrap();

    // initial plan
    // NOTE: we NEED two time predicates for the bug to trigger!
    let expr = col("time")
        .gt(lit(ScalarValue::TimestampNanosecond(Some(0), None)))
        .and(col("time").gt(lit(ScalarValue::TimestampNanosecond(Some(2), None))));

    let plan =
        LogicalPlanBuilder::scan("t".to_owned(), provider_as_source(Arc::new(provider)), None)
            .unwrap()
            .filter(expr)
            .unwrap()
            .aggregate(
                std::iter::empty::<Expr>(),
                [count(lit(true)).alias("count")],
            )
            .unwrap()
            .project([col("count")])
            .unwrap()
            .build()
            .unwrap();

    let plan = state.create_physical_plan(&plan).await.unwrap();

    // The output of the parquet files should not be resorted
    insta::assert_yaml_snapshot!(
        format_execution_plan(&plan),
        @r#"
    - " AggregateExec: mode=Final, gby=[], aggr=[count]"
    - "   CoalescePartitionsExec"
    - "     AggregateExec: mode=Partial, gby=[], aggr=[count]"
    - "       RepartitionExec: partitioning=RoundRobinBatch(16), input_partitions=1"
    - "         ProjectionExec: expr=[]"
    - "           DeduplicateExec: [tag@0 ASC,time@1 ASC]"
    - "             SortPreservingMergeExec: [tag@0 ASC, time@1 ASC, __chunk_order@2 ASC]"
    - "               UnionExec"
    - "                 SortExec: expr=[tag@0 ASC, time@1 ASC, __chunk_order@2 ASC], preserve_partitioning=[true]"
    - "                   CoalesceBatchesExec: target_batch_size=8192"
    - "                     FilterExec: time@1 > 2"
    - "                       RepartitionExec: partitioning=RoundRobinBatch(16), input_partitions=1"
    - "                         RecordBatchesExec: chunks=1, projection=[tag, time, __chunk_order]"
    - "                 DataSourceExec: file_groups={16 groups: [[2.parquet:0..125], [3.parquet:0..125], [2.parquet:125..250], [3.parquet:125..250], [2.parquet:250..375], ...]}, projection=[tag, time, __chunk_order], output_ordering=[tag@0 ASC, time@1 ASC, __chunk_order@2 ASC], file_type=parquet, predicate=time@1 > 2, pruning_predicate=time_null_count@1 != row_count@2 AND time_max@0 > 2, required_guarantees=[]"
    "#
    );
}

/// Bug reproducer for:
/// - <https://github.com/influxdata/EAR/issues/4728>
/// - <https://github.com/influxdata/influxdb_iox/issues/9450>
#[tokio::test]
async fn test_parquet_must_resorted() {
    maybe_start_logging();

    // DF session setup
    let config = ExecutorConfig {
        target_query_partitions: 6.try_into().unwrap(),
        ..ExecutorConfig::testing()
    };
    let exec = Executor::new_with_config_and_executor(config, DedicatedExecutor::new_testing());
    let ctx = exec.new_context();
    let state = ctx.inner().state();

    // chunks
    let c = TestChunk::new("t")
        .with_row_count(10_000)
        .with_tag_column("tag")
        .with_f64_field_column("field")
        .with_time_column_with_full_stats(Some(0), Some(10), None)
        .with_may_contain_pk_duplicates(false)
        .with_sort_key(SortKey::from_columns([Arc::from("tag"), Arc::from("time")]));
    let schema = c.schema().clone();
    let df_schema = DFSchema::try_from(schema.as_arrow().as_ref().clone()).unwrap();
    let provider = ProviderBuilder::new("t".into(), schema)
        // need a small file followed by a big one
        .add_chunk(Arc::new(
            c.clone()
                .with_id(1)
                .with_order(1)
                .with_dummy_parquet_file_and_size(1),
        ))
        .add_chunk(Arc::new(
            c.clone()
                .with_id(2)
                .with_order(2)
                .with_dummy_parquet_file_and_size(100_000_000),
        ))
        .build()
        .unwrap();

    // initial plan
    let expr = col("tag")
        .gt(lit("foo"))
        .and(col("time").gt(lit(ScalarValue::TimestampNanosecond(Some(2), None))))
        .and(
            col("field")
                .cast_to(&DataType::Utf8, &df_schema)
                .unwrap()
                .not_eq(lit("")),
        );

    let plan =
        LogicalPlanBuilder::scan("t".to_owned(), provider_as_source(Arc::new(provider)), None)
            .unwrap()
            .filter(expr)
            .unwrap()
            .project([col("tag")])
            .unwrap()
            .build()
            .unwrap();

    let plan = state.create_physical_plan(&plan).await.unwrap();

    // The output of the parquet files should not be resorted and there should
    // be no SortExec in the following plan.
    //
    // In order for correct results, the parquet files can not be intermixed
    // across file groups: only one parquet file can be in each group, though
    // subranges of the same parquet file can be in multiple groups.
    //
    // Prior to https://github.com/influxdata/influxdb_iox/issues/9450, the plan
    // called for the DataSourceExec to read the files in parallel (using subranges) like:
    // ```
    // {6 groups: [[1.parquet:0..1, 2.parquet:0..16666666], [2.parquet:16666666..33333333],...
    // ```
    //
    // Groups with more than one file produce an output partition that is the
    // result of concatenating them together, so even if the output of each
    // individual file is sorted, the output of the partition is not, due to the
    // concatenation.
    insta::assert_yaml_snapshot!(
        format_execution_plan(&plan),
        @r#"
    - " CoalesceBatchesExec: target_batch_size=8192"
    - "   FilterExec: CAST(field@1 AS Utf8) != , projection=[tag@0]"
    - "     RepartitionExec: partitioning=RoundRobinBatch(6), input_partitions=1"
    - "       ProjectionExec: expr=[tag@0 as tag, field@1 as field]"
    - "         DeduplicateExec: [tag@0 ASC,time@2 ASC]"
    - "           SortPreservingMergeExec: [tag@0 ASC, time@2 ASC, __chunk_order@3 ASC]"
    - "             DataSourceExec: file_groups={6 groups: [[1.parquet:0..1], [2.parquet:0..20000000], [2.parquet:20000000..40000000], [2.parquet:40000000..60000000], [2.parquet:60000000..80000000], ...]}, projection=[tag, field, time, __chunk_order], output_ordering=[tag@0 ASC, time@2 ASC, __chunk_order@3 ASC], file_type=parquet, predicate=tag@1 > foo AND time@2 > 2, pruning_predicate=tag_null_count@1 != row_count@2 AND tag_max@0 > foo AND time_null_count@4 != row_count@2 AND time_max@3 > 2, required_guarantees=[]"
    "#
    );
}
