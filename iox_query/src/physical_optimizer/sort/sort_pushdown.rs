use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{sorts::sort::SortExec, union::UnionExec, ExecutionPlan},
};

/// Pushes [`SortExec`] closer to the data source.
///
/// This is especially useful when there are [`UnionExec`]s within the plan, since they determine a common sort key but
/// often some children may already be sorted.
#[derive(Debug, Default)]
pub struct SortPushdown;

impl PhysicalOptimizerRule for SortPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let plan_any = plan.as_any();

            if let Some(sort_exec) = plan_any.downcast_ref::<SortExec>() {
                let child = sort_exec.input();
                let child_any = child.as_any();

                if let Some(child_union) = child_any.downcast_ref::<UnionExec>() {
                    let new_union = UnionExec::new(
                        child_union
                            .children()
                            .into_iter()
                            .map(|plan| {
                                let new_sort_exec = SortExec::try_new(
                                    sort_exec.expr().to_vec(),
                                    plan,
                                    sort_exec.fetch(),
                                )?;
                                Ok(Arc::new(new_sort_exec) as _)
                            })
                            .collect::<Result<Vec<_>>>()?,
                    );
                    return Ok(Transformed::Yes(Arc::new(new_union)));
                }
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "sort_pushown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::{
        physical_expr::PhysicalSortExpr,
        physical_plan::{empty::EmptyExec, expressions::Column},
    };

    use crate::physical_optimizer::test_util::OptimizationTest;

    use super::*;

    #[test]
    fn test_pushdown() {
        let schema = schema();
        let input = Arc::new(UnionExec::new(
            (0..2)
                .map(|_| Arc::new(EmptyExec::new(true, Arc::clone(&schema))) as _)
                .collect(),
        ));
        let plan = Arc::new(
            SortExec::try_new(
                sort_expr(schema.as_ref()),
                Arc::new(UnionExec::new(vec![input])),
                Some(10),
            )
            .unwrap(),
        );
        let opt = SortPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col@0 ASC]"
          - "   UnionExec"
          - "     UnionExec"
          - "       EmptyExec: produce_one_row=true"
          - "       EmptyExec: produce_one_row=true"
        output:
          Ok:
            - " UnionExec"
            - "   UnionExec"
            - "     SortExec: fetch=10, expr=[col@0 ASC]"
            - "       EmptyExec: produce_one_row=true"
            - "     SortExec: fetch=10, expr=[col@0 ASC]"
            - "       EmptyExec: produce_one_row=true"
        "###
        );
    }

    fn sort_expr(schema: &Schema) -> Vec<PhysicalSortExpr> {
        vec![PhysicalSortExpr {
            expr: Arc::new(Column::new_with_schema("col", schema).unwrap()),
            options: Default::default(),
        }]
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("col", DataType::Int64, false)]))
    }
}
