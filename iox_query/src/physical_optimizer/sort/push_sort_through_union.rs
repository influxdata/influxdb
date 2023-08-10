use std::sync::Arc;

use datafusion::{
    common::tree_node::{RewriteRecursion, Transformed, TreeNode, TreeNodeRewriter},
    config::ConfigOptions,
    error::Result,
    physical_expr::{
        utils::ordering_satisfy_requirement,
        {PhysicalSortExpr, PhysicalSortRequirement},
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        repartition::RepartitionExec, sorts::sort::SortExec, union::UnionExec, ExecutionPlan,
    },
};

/// Pushes a [`SortExec`] through a [`UnionExec`], possibly
/// including multiple [`RepartitionExec`] nodes (converting them
/// to be sort-preserving in the process), provided that at least
/// one of the children of the union is already sorted.
///
/// In other words, a typical plan like this
/// ```text
/// DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]
///   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]
///     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8
///       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4
///         UnionExec
///           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0
///           ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]
/// ```
/// will become:
/// ```text
/// DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]
///   SortPreservingRepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8
///     SortPreservingRepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4
///       UnionExec
///         SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]
///           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0
///         ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]
/// ```
///
/// There is a tension between:
/// - Wanting to do sorts in parallel
/// - Sorting fewer rows
///
/// DataFusion will not push down a sort through a `RepartitionExec`
/// because it could reduce the parallelism of the sort. However,
/// in IOx, unsorted children of `UnionExec` will tend to be
/// [`RecordBatchesExec`] which is likely to have many fewer rows than
/// other children which will tend to be [`ParquetExec`].
/// So making this transformation will generally have a dramatic effect
/// on the amount of data being sorted.
///
/// [`RecordBatchesExec`]: crate::provider::RecordBatchesExec
/// [`ParquetExec`]: datafusion::datasource::physical_plan::ParquetExec
pub(crate) struct PushSortThroughUnion;

impl PhysicalOptimizerRule for PushSortThroughUnion {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() else {
                return Ok(Transformed::No(plan));
            };

            if !sort_should_be_pushed_down(sort_exec) {
                return Ok(Transformed::No(plan));
            }

            let mut plan = Arc::clone(sort_exec.input());
            let mut rewriter = SortRewriter {
                ordering: sort_exec.output_ordering().unwrap().to_vec(),
            };

            plan = plan.rewrite(&mut rewriter)?;

            // As a sanity check, make sure plan has the same ordering as before.
            // If this fails, there is a bug in this optimization.
            let required_order = sort_exec.output_ordering().map(sort_exprs_to_requirement);
            if !ordering_satisfy_requirement(
                plan.output_ordering(),
                required_order.as_deref(),
                || plan.equivalence_properties(),
                || plan.ordering_equivalence_properties(),
            ) {
                return Err(datafusion::error::DataFusionError::Internal(
                    "PushSortThroughUnion corrupted plan sort order".into(),
                ));
            }

            Ok(Transformed::Yes(plan))
        })
    }

    fn name(&self) -> &str {
        "push_sort_through_union"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Returns true if the [`SortExec`] can be pushed down beneath a [`UnionExec`].
fn sort_should_be_pushed_down(sort_exec: &SortExec) -> bool {
    // Skip over any RepartitionExecs
    let mut input = sort_exec.input();
    while input.as_any().is::<RepartitionExec>() {
        input = input
            .as_any()
            .downcast_ref::<RepartitionExec>()
            .expect("this must be a RepartitionExec")
            .input();
    }

    let Some(union_exec) = input.as_any().downcast_ref::<UnionExec>() else {
        return false
    };

    let required_ordering = sort_exec.output_ordering().map(sort_exprs_to_requirement);

    // Push down the sort if any of the children are already sorted.
    // This means we will need to sort fewer rows than if we didn't
    // push down the sort.
    union_exec.children().iter().any(|child| {
        ordering_satisfy_requirement(
            child.output_ordering(),
            required_ordering.as_deref(),
            || child.equivalence_properties(),
            || child.ordering_equivalence_properties(),
        )
    })
}

/// Rewrites a plan:
/// - Any [`RepartitionExec`] nodes are converted to be sort-preserving
/// - Any children of a [`UnionExec`] that are not sorted get a [`SortExec`]
///   added to them.
/// - Any other nodes will stop the rewrite.
struct SortRewriter {
    ordering: Vec<PhysicalSortExpr>,
}

impl TreeNodeRewriter for SortRewriter {
    type N = Arc<dyn ExecutionPlan>;

    fn pre_visit(&mut self, plan: &Self::N) -> Result<RewriteRecursion> {
        if plan.as_any().is::<RepartitionExec>() {
            Ok(datafusion::common::tree_node::RewriteRecursion::Continue)
        } else if plan.as_any().is::<UnionExec>() {
            Ok(datafusion::common::tree_node::RewriteRecursion::Mutate)
        } else {
            Ok(datafusion::common::tree_node::RewriteRecursion::Stop)
        }
    }

    fn mutate(&mut self, plan: Self::N) -> Result<Self::N> {
        if let Some(repartition_exec) = plan.as_any().downcast_ref::<RepartitionExec>() {
            // Convert any RepartitionExec to be sort-preserving
            Ok(Arc::new(
                RepartitionExec::try_new(
                    Arc::clone(repartition_exec.input()),
                    repartition_exec.output_partitioning(),
                )?
                .with_preserve_order(true),
            ))
        } else if let Some(union_exec) = plan.as_any().downcast_ref::<UnionExec>() {
            // Any children of the UnionExec that are not already sorted,
            // need to be sorted.
            let required_ordering = Some(sort_exprs_to_requirement(self.ordering.as_ref()));

            let new_children = union_exec
                .children()
                .into_iter()
                .map(|child| {
                    if !ordering_satisfy_requirement(
                        child.output_ordering(),
                        required_ordering.as_deref(),
                        || child.equivalence_properties(),
                        || child.ordering_equivalence_properties(),
                    ) {
                        let sort_exec = SortExec::new(self.ordering.clone(), child)
                            .with_preserve_partitioning(true);
                        Arc::new(sort_exec)
                    } else {
                        child
                    }
                })
                .collect();

            Ok(Arc::new(UnionExec::new(new_children)))
        } else {
            Ok(plan)
        }
    }
}

fn sort_exprs_to_requirement(sort_exprs: &[PhysicalSortExpr]) -> Vec<PhysicalSortRequirement> {
    sort_exprs
        .iter()
        .map(|sort_expr| sort_expr.clone().into())
        .collect()
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::datatypes::SchemaRef;
    use datafusion::{
        datasource::{
            listing::PartitionedFile,
            object_store::ObjectStoreUrl,
            physical_plan::{FileScanConfig, ParquetExec},
        },
        physical_expr::PhysicalSortExpr,
        physical_plan::{
            coalesce_batches::CoalesceBatchesExec, expressions::Column,
            repartition::RepartitionExec, sorts::sort::SortExec, union::UnionExec, ExecutionPlan,
            Partitioning, Statistics,
        },
    };
    use object_store::{path::Path, ObjectMeta};
    use schema::{InfluxFieldType, SchemaBuilder as IOxSchemaBuilder};

    use crate::{
        physical_optimizer::{
            sort::push_sort_through_union::PushSortThroughUnion, test_util::OptimizationTest,
        },
        provider::{DeduplicateExec, RecordBatchesExec},
        test::TestChunk,
        CHUNK_ORDER_COLUMN_NAME,
    };

    #[test]
    fn test_push_sort_through_union() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering(["col2", "col1", "time", CHUNK_ORDER_COLUMN_NAME], &schema);

        let plan_parquet = parquet_exec(&schema, &order);
        let plan_batches = record_batches_exec(2);

        let plan = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet]));
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(8)).unwrap());
        let hash_exprs = order.iter().cloned().map(|e| e.expr).collect();
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::Hash(hash_exprs, 8)).unwrap());
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        let opt = PushSortThroughUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
          - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
          - "         UnionExec"
          - "           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "           ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "   SortPreservingRepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
            - "     SortPreservingRepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
            - "       UnionExec"
            - "         SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "         ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_push_sort_through_union_top_level_sort() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering(["col2", "col1", "time", CHUNK_ORDER_COLUMN_NAME], &schema);

        let plan_parquet = parquet_exec(&schema, &order);
        let plan_batches = record_batches_exec(2);

        let plan = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet]));
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(8)).unwrap());
        let hash_exprs = order.iter().cloned().map(|e| e.expr).collect();
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::Hash(hash_exprs, 8)).unwrap());
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        let output_order = ordering(["time"], &schema);
        let plan = Arc::new(SortExec::new(output_order, plan));

        // Nothing is done with the SortExec at the top level, because
        // it does not match the pattern.
        let opt = PushSortThroughUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: expr=[time@3 ASC]"
          - "   DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "     SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "       RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
          - "         RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
          - "           UnionExec"
          - "             RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "             ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        output:
          Ok:
            - " SortExec: expr=[time@3 ASC]"
            - "   DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "     SortPreservingRepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
            - "       SortPreservingRepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
            - "         UnionExec"
            - "           SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "             RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "           ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_push_sort_through_union_no_repartition() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering(["col2", "col1", "time", CHUNK_ORDER_COLUMN_NAME], &schema);

        let plan_parquet = parquet_exec(&schema, &order);
        let plan_batches = record_batches_exec(2);

        let plan = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet]));
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        // RepartitionExec does not need to be present for the optimization to apply
        // (Although DF *will* handle this case)
        let opt = PushSortThroughUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "     UnionExec"
          - "       RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "       ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "   UnionExec"
            - "     SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "       RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "     ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_no_sorted_children() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering(["col2", "col1", "time", CHUNK_ORDER_COLUMN_NAME], &schema);

        let plan_batches_1 = record_batches_exec(2);
        let plan_batches_2 = record_batches_exec(2);

        let plan = Arc::new(UnionExec::new(vec![plan_batches_1, plan_batches_2]));
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(8)).unwrap());
        let hash_exprs = order.iter().cloned().map(|e| e.expr).collect();
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::Hash(hash_exprs, 8)).unwrap());
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        // No children of the union are sorted, so the sort will not be pushed down.
        let opt = PushSortThroughUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
          - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
          - "         UnionExec"
          - "           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
            - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
            - "         UnionExec"
            - "           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
        "###
        );
    }

    #[test]
    fn test_all_sorted_children() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering(["col2", "col1", "time", CHUNK_ORDER_COLUMN_NAME], &schema);

        let plan_parquet_1 = parquet_exec(&schema, &order);
        let plan_parquet_2 = parquet_exec(&schema, &order);

        let plan = Arc::new(UnionExec::new(vec![plan_parquet_1, plan_parquet_2]));
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(8)).unwrap());
        let hash_exprs = order.iter().cloned().map(|e| e.expr).collect();
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::Hash(hash_exprs, 8)).unwrap());
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        // All children of the union are sorted, so RepartitionExec nodes are converted to
        // be sort-preserving.
        let opt = PushSortThroughUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
          - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
          - "         UnionExec"
          - "           ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
          - "           ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "   SortPreservingRepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
            - "     SortPreservingRepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
            - "       UnionExec"
            - "         ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
            - "         ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_no_union() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering(["col2", "col1", "time", CHUNK_ORDER_COLUMN_NAME], &schema);

        let plan = parquet_exec(&schema, &order);

        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(8)).unwrap());
        let hash_exprs = order.iter().cloned().map(|e| e.expr).collect();
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::Hash(hash_exprs, 8)).unwrap());
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        // There is no union in the plan, so the pattern does not match.
        let opt = PushSortThroughUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
          - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=2"
          - "         ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
            - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=2"
            - "         ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_two_sorts() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering(["col2", "col1", "time", CHUNK_ORDER_COLUMN_NAME], &schema);

        let plan_parquet = parquet_exec(&schema, &order);
        let plan_batches = record_batches_exec(2);

        let plan = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet]));
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(8)).unwrap());
        let hash_exprs = order.iter().cloned().map(|e| e.expr).collect();
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::Hash(hash_exprs, 8)).unwrap());
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        // With two identical sorts in the plan, both of them will be removed,
        // because the transformation is applied bottom-up.
        let opt = PushSortThroughUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "     SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "       RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
          - "         RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
          - "           UnionExec"
          - "             RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "             ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "   SortPreservingRepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
            - "     SortPreservingRepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
            - "       UnionExec"
            - "         SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "         ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_extra_node() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering(["col2", "col1", "time", CHUNK_ORDER_COLUMN_NAME], &schema);

        let plan_parquet = parquet_exec(&schema, &order);
        let plan_batches = record_batches_exec(2);

        let plan = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet]));
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(8)).unwrap());
        let hash_exprs = order.iter().cloned().map(|e| e.expr).collect();
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::Hash(hash_exprs, 8)).unwrap());
        let plan = Arc::new(CoalesceBatchesExec::new(plan, 4096));
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        // Extra nodes in the plan, like CoalesceBatchesExec, will break the pattern matching
        // and prevent the transformation from occurring.
        let opt = PushSortThroughUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "     CoalesceBatchesExec: target_batch_size=4096"
          - "       RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
          - "         RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
          - "           UnionExec"
          - "             RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "             ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "     CoalesceBatchesExec: target_batch_size=4096"
            - "       RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
            - "         RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
            - "           UnionExec"
            - "             RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "             ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, time@3 ASC, __chunk_order@4 ASC]"
        "###
        );
    }

    #[test]
    fn test_wrong_order() {
        test_helpers::maybe_start_logging();

        let schema = schema();
        let order = ordering(["col2", "col1", "time", CHUNK_ORDER_COLUMN_NAME], &schema);

        let wrong_order = ordering(["col1", "col2", "time", CHUNK_ORDER_COLUMN_NAME], &schema);
        let plan_parquet = parquet_exec(&schema, &wrong_order);
        let plan_batches = record_batches_exec(2);

        let plan = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet]));
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::RoundRobinBatch(8)).unwrap());
        let hash_exprs = order.iter().cloned().map(|e| e.expr).collect();
        let plan =
            Arc::new(RepartitionExec::try_new(plan, Partitioning::Hash(hash_exprs, 8)).unwrap());
        let plan = Arc::new(SortExec::new(order.clone(), plan));
        let plan = Arc::new(DeduplicateExec::new(plan, order, true));

        // The ParquetExec has the wrong output order so no children of the union have the right
        // sort order. Therefore the optimization is not applied.
        let opt = PushSortThroughUnion;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
          - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
          - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
          - "         UnionExec"
          - "           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
          - "           ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col1@0 ASC, col2@1 ASC, time@3 ASC, __chunk_order@4 ASC]"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "   SortExec: expr=[col2@1 ASC,col1@0 ASC,time@3 ASC,__chunk_order@4 ASC]"
            - "     RepartitionExec: partitioning=Hash([col2@1, col1@0, time@3, __chunk_order@4], 8), input_partitions=8"
            - "       RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=4"
            - "         UnionExec"
            - "           RecordBatchesExec: batches_groups=2 batches=0 total_rows=0"
            - "           ParquetExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, field1, time, __chunk_order], output_ordering=[col1@0 ASC, col2@1 ASC, time@3 ASC, __chunk_order@4 ASC]"
        "###
        );
    }

    fn record_batches_exec(n_chunks: usize) -> Arc<dyn ExecutionPlan> {
        let chunks = std::iter::repeat(Arc::new(TestChunk::new("t")) as _)
            .take(n_chunks)
            .collect::<Vec<_>>();
        Arc::new(RecordBatchesExec::new(chunks, schema(), None))
    }

    fn parquet_exec(schema: &SchemaRef, order: &[PhysicalSortExpr]) -> Arc<dyn ExecutionPlan> {
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(schema),
            file_groups: vec![vec![file(1)], vec![file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![order.to_vec()],
            infinite_source: false,
        };
        Arc::new(ParquetExec::new(base_config, None, None))
    }

    fn schema() -> SchemaRef {
        IOxSchemaBuilder::new()
            .tag("col1")
            .tag("col2")
            .influx_field("field1", InfluxFieldType::Float)
            .timestamp()
            .influx_field(CHUNK_ORDER_COLUMN_NAME, InfluxFieldType::Integer)
            .build()
            .unwrap()
            .into()
    }

    fn file(n: u128) -> PartitionedFile {
        PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::parse(format!("{n}.parquet")).unwrap(),
                last_modified: Default::default(),
                size: 0,
                e_tag: None,
            },
            partition_values: vec![],
            range: None,
            extensions: None,
        }
    }

    fn ordering<const N: usize>(cols: [&str; N], schema: &SchemaRef) -> Vec<PhysicalSortExpr> {
        cols.into_iter()
            .map(|col| PhysicalSortExpr {
                expr: Arc::new(Column::new_with_schema(col, schema.as_ref()).unwrap()),
                options: Default::default(),
            })
            .collect()
    }
}
