use std::sync::Arc;

use datafusion::{
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{sorts::sort::SortExec, tree_node::TreeNodeRewritable, ExecutionPlan},
};

/// Removes [`SortExec`] if it is no longer needed.
#[derive(Debug, Default)]
pub struct RedundantSort;

impl PhysicalOptimizerRule for RedundantSort {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let plan_any = plan.as_any();

            if let Some(sort_exec) = plan_any.downcast_ref::<SortExec>() {
                let child = sort_exec.input();

                if child.output_ordering() == Some(sort_exec.expr()) {
                    return Ok(Some(Arc::clone(child)));
                }
            }

            Ok(None)
        })
    }

    fn name(&self) -> &str {
        "redundant_sort"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::{
        datasource::object_store::ObjectStoreUrl,
        physical_expr::PhysicalSortExpr,
        physical_plan::{
            expressions::Column,
            file_format::{FileScanConfig, ParquetExec},
            Statistics,
        },
    };

    use crate::physical_optimizer::test_util::OptimizationTest;

    use super::*;

    #[test]
    fn test_not_redundant() {
        let schema = schema();
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: None,
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan =
            Arc::new(SortExec::try_new(sort_expr(schema.as_ref()), input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col@0 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col]"
        output:
          Ok:
            - " SortExec: fetch=10, expr=[col@0 ASC]"
            - "   ParquetExec: limit=None, partitions={0 groups: []}, projection=[col]"
        "###
        );
    }

    #[test]
    fn test_redundant() {
        let schema = schema();
        let sort_expr = sort_expr(schema.as_ref());
        let input = Arc::new(ParquetExec::new(
            FileScanConfig {
                object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
                file_schema: Arc::clone(&schema),
                file_groups: vec![],
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
                output_ordering: Some(sort_expr.clone()),
                infinite_source: false,
            },
            None,
            None,
        ));
        let plan = Arc::new(SortExec::try_new(sort_expr, input, Some(10)).unwrap());
        let opt = RedundantSort::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=10, expr=[col@0 ASC]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, output_ordering=[col@0 ASC], projection=[col]"
        output:
          Ok:
            - " ParquetExec: limit=None, partitions={0 groups: []}, output_ordering=[col@0 ASC], projection=[col]"
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
