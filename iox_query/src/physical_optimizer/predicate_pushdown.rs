use std::{collections::HashSet, sync::Arc};

use datafusion::{
    common::tree_node::{RewriteRecursion, Transformed, TreeNode, TreeNodeRewriter},
    config::ConfigOptions,
    datasource::physical_plan::ParquetExec,
    error::{DataFusionError, Result},
    logical_expr::Operator,
    physical_expr::{split_conjunction, utils::collect_columns},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        empty::EmptyExec,
        expressions::{BinaryExpr, Column},
        filter::FilterExec,
        union::UnionExec,
        ExecutionPlan, PhysicalExpr,
    },
};

use crate::provider::DeduplicateExec;

/// Push down predicates.
#[derive(Debug, Default)]
pub struct PredicatePushdown;

impl PhysicalOptimizerRule for PredicatePushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let plan_any = plan.as_any();

            if let Some(filter_exec) = plan_any.downcast_ref::<FilterExec>() {
                let mut children = filter_exec.children();
                assert_eq!(children.len(), 1);
                let child = children.remove(0);

                let child_any = child.as_any();
                if let Some(child_empty) = child_any.downcast_ref::<EmptyExec>() {
                    if !child_empty.produce_one_row() {
                        return Ok(Transformed::Yes(child));
                    }
                } else if let Some(child_union) = child_any.downcast_ref::<UnionExec>() {
                    let new_inputs = child_union
                        .inputs()
                        .iter()
                        .map(|input| {
                            FilterExec::try_new(
                                Arc::clone(filter_exec.predicate()),
                                Arc::clone(input),
                            )
                            .map(|p| Arc::new(p) as Arc<dyn ExecutionPlan>)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let new_union = UnionExec::new(new_inputs);
                    return Ok(Transformed::Yes(Arc::new(new_union)));
                } else if let Some(child_parquet) = child_any.downcast_ref::<ParquetExec>() {
                    let existing = child_parquet
                        .predicate()
                        .map(split_conjunction)
                        .unwrap_or_default();
                    let both = conjunction(
                        existing
                            .into_iter()
                            .chain(split_conjunction(filter_exec.predicate()))
                            .cloned(),
                    );

                    let new_node = Arc::new(FilterExec::try_new(
                        Arc::clone(filter_exec.predicate()),
                        Arc::new(ParquetExec::new(
                            child_parquet.base_config().clone(),
                            both,
                            None,
                        )),
                    )?);
                    return Ok(Transformed::Yes(new_node));
                } else if let Some(child_dedup) = child_any.downcast_ref::<DeduplicateExec>() {
                    let dedup_cols = child_dedup.sort_columns();
                    let (pushdown, no_pushdown): (Vec<_>, Vec<_>) =
                        split_conjunction(filter_exec.predicate())
                            .into_iter()
                            .cloned()
                            .partition(|expr| {
                                collect_columns(expr)
                                    .into_iter()
                                    .all(|c| dedup_cols.contains(c.name()))
                            });

                    if !pushdown.is_empty() {
                        let mut grandchildren = child_dedup.children();
                        assert_eq!(grandchildren.len(), 1);
                        let grandchild = grandchildren.remove(0);

                        let mut new_node: Arc<dyn ExecutionPlan> = Arc::new(DeduplicateExec::new(
                            Arc::new(FilterExec::try_new(
                                conjunction(pushdown).expect("not empty"),
                                grandchild,
                            )?),
                            child_dedup.sort_keys().to_vec(),
                            child_dedup.use_chunk_order_col(),
                        ));
                        if !no_pushdown.is_empty() {
                            new_node = Arc::new(FilterExec::try_new(
                                conjunction(no_pushdown).expect("not empty"),
                                new_node,
                            )?);
                        }
                        return Ok(Transformed::Yes(new_node));
                    }
                }
            }

            Ok(Transformed::No(plan))
        })
    }

    fn name(&self) -> &str {
        "predicate_pushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(Debug, Default)]
struct ColumnCollector {
    cols: HashSet<Column>,
}

impl TreeNodeRewriter for ColumnCollector {
    type N = Arc<dyn PhysicalExpr>;

    fn pre_visit(
        &mut self,
        node: &Arc<dyn PhysicalExpr>,
    ) -> Result<RewriteRecursion, DataFusionError> {
        if let Some(column) = node.as_any().downcast_ref::<Column>() {
            self.cols.insert(column.clone());
        }
        Ok(RewriteRecursion::Continue)
    }

    fn mutate(
        &mut self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        Ok(expr)
    }
}

fn conjunction(
    parts: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
) -> Option<Arc<dyn PhysicalExpr>> {
    parts
        .into_iter()
        .reduce(|lhs, rhs| Arc::new(BinaryExpr::new(lhs, Operator::And, rhs)))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::{
        datasource::object_store::ObjectStoreUrl,
        datasource::physical_plan::FileScanConfig,
        logical_expr::Operator,
        physical_expr::PhysicalSortExpr,
        physical_plan::{
            expressions::{BinaryExpr, Column, Literal},
            PhysicalExpr, Statistics,
        },
        scalar::ScalarValue,
    };
    use schema::sort::SortKeyBuilder;

    use crate::{physical_optimizer::test_util::OptimizationTest, util::arrow_sort_key_exprs};

    use super::*;

    #[test]
    fn test_empty_no_rows() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_tag(&schema),
                Arc::new(EmptyExec::new(false, schema)),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " FilterExec: tag1@0 = foo"
          - "   EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " EmptyExec: produce_one_row=false"
        "###
        );
    }

    #[test]
    fn test_empty_with_rows() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_tag(&schema),
                Arc::new(EmptyExec::new(true, schema)),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " FilterExec: tag1@0 = foo"
          - "   EmptyExec: produce_one_row=true"
        output:
          Ok:
            - " FilterExec: tag1@0 = foo"
            - "   EmptyExec: produce_one_row=true"
        "###
        );
    }

    #[test]
    fn test_union() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_tag(&schema),
                Arc::new(UnionExec::new(
                    (0..2)
                        .map(|_| Arc::new(EmptyExec::new(true, Arc::clone(&schema))) as _)
                        .collect(),
                )),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " FilterExec: tag1@0 = foo"
          - "   UnionExec"
          - "     EmptyExec: produce_one_row=true"
          - "     EmptyExec: produce_one_row=true"
        output:
          Ok:
            - " UnionExec"
            - "   FilterExec: tag1@0 = foo"
            - "     EmptyExec: produce_one_row=true"
            - "   FilterExec: tag1@0 = foo"
            - "     EmptyExec: produce_one_row=true"
        "###
        );
    }

    #[test]
    fn test_union_nested() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_tag(&schema),
                Arc::new(UnionExec::new(vec![Arc::new(UnionExec::new(
                    (0..2)
                        .map(|_| Arc::new(EmptyExec::new(true, Arc::clone(&schema))) as _)
                        .collect(),
                ))])),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " FilterExec: tag1@0 = foo"
          - "   UnionExec"
          - "     UnionExec"
          - "       EmptyExec: produce_one_row=true"
          - "       EmptyExec: produce_one_row=true"
        output:
          Ok:
            - " UnionExec"
            - "   UnionExec"
            - "     FilterExec: tag1@0 = foo"
            - "       EmptyExec: produce_one_row=true"
            - "     FilterExec: tag1@0 = foo"
            - "       EmptyExec: produce_one_row=true"
        "###
        );
    }

    #[test]
    fn test_parquet() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![vec![]],
            infinite_source: false,
        };
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_mixed(&schema),
                Arc::new(ParquetExec::new(
                    base_config,
                    Some(predicate_tag(&schema)),
                    None,
                )),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " FilterExec: tag1@0 = field@2"
          - "   ParquetExec: file_groups={0 groups: []}, projection=[tag1, tag2, field], predicate=tag1@0 = foo, pruning_predicate=tag1_min@0 <= foo AND foo <= tag1_max@1"
        output:
          Ok:
            - " FilterExec: tag1@0 = field@2"
            - "   ParquetExec: file_groups={0 groups: []}, projection=[tag1, tag2, field], predicate=tag1@0 = foo AND tag1@0 = field@2, pruning_predicate=tag1_min@0 <= foo AND foo <= tag1_max@1"
        "###
        );
    }

    #[test]
    fn test_dedup_no_pushdown() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_field(&schema),
                Arc::new(DeduplicateExec::new(
                    Arc::new(EmptyExec::new(true, Arc::clone(&schema))),
                    sort_expr(&schema),
                    false,
                )),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " FilterExec: field@2 = val"
          - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
          - "     EmptyExec: produce_one_row=true"
        output:
          Ok:
            - " FilterExec: field@2 = val"
            - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
            - "     EmptyExec: produce_one_row=true"
        "###
        );
    }

    #[test]
    fn test_dedup_all_pushdown() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_tag(&schema),
                Arc::new(DeduplicateExec::new(
                    Arc::new(EmptyExec::new(true, Arc::clone(&schema))),
                    sort_expr(&schema),
                    false,
                )),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " FilterExec: tag1@0 = foo"
          - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
          - "     EmptyExec: produce_one_row=true"
        output:
          Ok:
            - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
            - "   FilterExec: tag1@0 = foo"
            - "     EmptyExec: produce_one_row=true"
        "###
        );
    }

    #[test]
    fn test_dedup_mixed() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                conjunction([
                    predicate_tag(&schema),
                    predicate_tags(&schema),
                    predicate_field(&schema),
                    predicate_mixed(&schema),
                    predicate_other(),
                ])
                .expect("not empty"),
                Arc::new(DeduplicateExec::new(
                    Arc::new(EmptyExec::new(true, Arc::clone(&schema))),
                    sort_expr(&schema),
                    false,
                )),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " FilterExec: tag1@0 = foo AND tag1@0 = tag2@1 AND field@2 = val AND tag1@0 = field@2 AND true"
          - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
          - "     EmptyExec: produce_one_row=true"
        output:
          Ok:
            - " FilterExec: field@2 = val AND tag1@0 = field@2"
            - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
            - "     FilterExec: tag1@0 = foo AND tag1@0 = tag2@1 AND true"
            - "       EmptyExec: produce_one_row=true"
        "###
        );
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("field", DataType::UInt8, true),
        ]))
    }

    fn sort_expr(schema: &SchemaRef) -> Vec<PhysicalSortExpr> {
        let sort_key = SortKeyBuilder::new()
            .with_col("tag1")
            .with_col("tag2")
            .build();
        arrow_sort_key_exprs(&sort_key, schema)
    }

    fn predicate_tag(schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("tag1", schema).unwrap()),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::from("foo"))),
        ))
    }

    fn predicate_tags(schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("tag1", schema).unwrap()),
            Operator::Eq,
            Arc::new(Column::new_with_schema("tag2", schema).unwrap()),
        ))
    }

    fn predicate_field(schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("field", schema).unwrap()),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::from("val"))),
        ))
    }

    fn predicate_mixed(schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            Arc::new(Column::new_with_schema("tag1", schema).unwrap()),
            Operator::Eq,
            Arc::new(Column::new_with_schema("field", schema).unwrap()),
        ))
    }

    fn predicate_other() -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(ScalarValue::from(true)))
    }
}
