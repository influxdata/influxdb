use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use arrow::datatypes::SchemaRef;
use datafusion::{
    config::ConfigOptions,
    error::{DataFusionError, Result},
    physical_expr::{
        utils::{collect_columns, reassign_predicate_columns},
        PhysicalSortExpr,
    },
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        empty::EmptyExec,
        expressions::Column,
        file_format::{FileScanConfig, ParquetExec},
        filter::FilterExec,
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        tree_node::TreeNodeRewritable,
        union::UnionExec,
        ExecutionPlan, PhysicalExpr,
    },
};

use crate::provider::{DeduplicateExec, RecordBatchesExec};

/// Push down projections.
#[derive(Debug, Default)]
pub struct ProjectionPushdown;

impl PhysicalOptimizerRule for ProjectionPushdown {
    #[allow(clippy::only_used_in_recursion)]
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let plan_any = plan.as_any();

            if let Some(projection_exec) = plan_any.downcast_ref::<ProjectionExec>() {
                let child = projection_exec.input();

                let mut column_indices = Vec::with_capacity(projection_exec.expr().len());
                let mut column_names = Vec::with_capacity(projection_exec.expr().len());
                for (expr, output_name) in projection_exec.expr() {
                    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
                        if column.name() == output_name {
                            column_indices.push(column.index());
                            column_names.push(output_name.as_str());
                        } else {
                            // don't bother w/ renames
                            return Ok(None);
                        }
                    } else {
                        // don't bother to deal w/ calculation within projection nodes
                        return Ok(None);
                    }
                }

                let child_any = child.as_any();
                if let Some(child_empty) = child_any.downcast_ref::<EmptyExec>() {
                    let new_child = EmptyExec::new(
                        child_empty.produce_one_row(),
                        Arc::new(child_empty.schema().project(&column_indices)?),
                    );
                    return Ok(Some(Arc::new(new_child)));
                } else if let Some(child_union) = child_any.downcast_ref::<UnionExec>() {
                    let new_inputs = child_union
                        .inputs()
                        .iter()
                        .map(|input| {
                            let exec = ProjectionExec::try_new(
                                projection_exec.expr().to_vec(),
                                Arc::clone(input),
                            )?;
                            Ok(Arc::new(exec) as _)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let new_union = UnionExec::new(new_inputs);
                    return Ok(Some(Arc::new(new_union)));
                } else if let Some(child_parquet) = child_any.downcast_ref::<ParquetExec>() {
                    let projection = match child_parquet.base_config().projection.as_ref() {
                        Some(projection) => column_indices
                            .into_iter()
                            .map(|idx| {
                                projection.get(idx).copied().ok_or_else(|| {
                                    DataFusionError::Execution("Projection broken".to_string())
                                })
                            })
                            .collect::<Result<Vec<_>>>()?,
                        None => column_indices,
                    };
                    let base_config = FileScanConfig {
                        projection: Some(projection),
                        ..child_parquet.base_config().clone()
                    };
                    let new_child =
                        ParquetExec::new(base_config, child_parquet.predicate().cloned(), None);
                    return Ok(Some(Arc::new(new_child)));
                } else if let Some(child_filter) = child_any.downcast_ref::<FilterExec>() {
                    let filter_required_cols = collect_columns(child_filter.predicate());
                    let filter_required_cols = filter_required_cols
                        .iter()
                        .map(|col| col.name())
                        .collect::<HashSet<_>>();

                    let plan = wrap_user_into_projections(
                        &filter_required_cols,
                        &column_names,
                        Arc::clone(child_filter.input()),
                        |plan| {
                            Ok(Arc::new(FilterExec::try_new(
                                reassign_predicate_columns(
                                    Arc::clone(child_filter.predicate()),
                                    &plan.schema(),
                                    false,
                                )?,
                                plan,
                            )?))
                        },
                    )?;

                    return Ok(Some(plan));
                } else if let Some(child_sort) = child_any.downcast_ref::<SortExec>() {
                    let sort_required_cols = child_sort
                        .expr()
                        .iter()
                        .map(|expr| collect_columns(&expr.expr))
                        .collect::<Vec<_>>();
                    let sort_required_cols = sort_required_cols
                        .iter()
                        .flat_map(|cols| cols.iter())
                        .map(|col| col.name())
                        .collect::<HashSet<_>>();

                    let plan = wrap_user_into_projections(
                        &sort_required_cols,
                        &column_names,
                        Arc::clone(child_sort.input()),
                        |plan| {
                            Ok(Arc::new(SortExec::try_new(
                                reassign_sort_exprs_columns(child_sort.expr(), &plan.schema())?,
                                plan,
                                child_sort.fetch(),
                            )?))
                        },
                    )?;

                    return Ok(Some(plan));
                } else if let Some(child_sort) = child_any.downcast_ref::<SortPreservingMergeExec>()
                {
                    let sort_required_cols = child_sort
                        .expr()
                        .iter()
                        .map(|expr| collect_columns(&expr.expr))
                        .collect::<Vec<_>>();
                    let sort_required_cols = sort_required_cols
                        .iter()
                        .flat_map(|cols| cols.iter())
                        .map(|col| col.name())
                        .collect::<HashSet<_>>();

                    let plan = wrap_user_into_projections(
                        &sort_required_cols,
                        &column_names,
                        Arc::clone(child_sort.input()),
                        |plan| {
                            Ok(Arc::new(SortPreservingMergeExec::new(
                                reassign_sort_exprs_columns(child_sort.expr(), &plan.schema())?,
                                plan,
                            )))
                        },
                    )?;

                    return Ok(Some(plan));
                } else if let Some(child_proj) = child_any.downcast_ref::<ProjectionExec>() {
                    let expr = column_indices
                        .iter()
                        .map(|idx| child_proj.expr()[*idx].clone())
                        .collect();
                    let plan = Arc::new(ProjectionExec::try_new(
                        expr,
                        Arc::clone(child_proj.input()),
                    )?);

                    // need to call `optimize` directly on the plan, because otherwise we would continue with the child
                    // and miss the optimization of that particular new ProjectionExec
                    let plan = self.optimize(plan, config)?;

                    return Ok(Some(plan));
                } else if let Some(child_dedup) = child_any.downcast_ref::<DeduplicateExec>() {
                    let dedup_required_cols = child_dedup.sort_columns();

                    let mut children = child_dedup.children();
                    assert_eq!(children.len(), 1);
                    let input = children.pop().expect("just checked len");

                    let plan = wrap_user_into_projections(
                        &dedup_required_cols,
                        &column_names,
                        input,
                        |plan| {
                            let sort_keys = reassign_sort_exprs_columns(
                                child_dedup.sort_keys(),
                                &plan.schema(),
                            )?;
                            Ok(Arc::new(DeduplicateExec::new(plan, sort_keys)))
                        },
                    )?;

                    return Ok(Some(plan));
                } else if let Some(child_recordbatches) =
                    child_any.downcast_ref::<RecordBatchesExec>()
                {
                    let new_child = RecordBatchesExec::new(
                        child_recordbatches.chunks().cloned(),
                        Arc::new(child_recordbatches.schema().project(&column_indices)?),
                    );
                    return Ok(Some(Arc::new(new_child)));
                }
            }

            Ok(None)
        })
    }

    fn name(&self) -> &str {
        "projection_pushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn schema_name_projection(
    schema: &SchemaRef,
    cols: &[&str],
) -> Result<Vec<(Arc<dyn PhysicalExpr>, String)>> {
    let idx_lookup = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, field)| (field.name().as_str(), idx))
        .collect::<HashMap<_, _>>();

    cols.iter()
        .map(|col| {
            let idx = *idx_lookup.get(col).ok_or_else(|| {
                DataFusionError::Execution(format!("Cannot find column to project: {col}"))
            })?;

            let expr = Arc::new(Column::new(col, idx)) as _;
            Ok((expr, (*col).to_owned()))
        })
        .collect::<Result<Vec<_>>>()
}

/// Wraps an intermediate node (like [`FilterExec`]) that has a single input but also uses some columns itself into
/// appropriate projections.
///
/// This will turn:
///
/// ```yaml
/// ---
/// projection:
///   user:  # e.g. FilterExec
///     inner:
/// ```
///
/// into
///
/// ```yaml
/// ---
/// projection:  # if `user` outputs too many cols
///   user:
///     projection:  # if `inner` outputs too many cols
///       inner:
/// ```
fn wrap_user_into_projections<F>(
    user_required_cols: &HashSet<&str>,
    outer_cols: &[&str],
    inner_plan: Arc<dyn ExecutionPlan>,
    user_constructor: F,
) -> Result<Arc<dyn ExecutionPlan>>
where
    F: FnOnce(Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>>,
{
    let mut plan = inner_plan;

    let inner_required_cols = user_required_cols
        .iter()
        .chain(outer_cols.iter())
        .copied()
        .collect::<HashSet<_>>();
    if inner_required_cols.len() < plan.schema().fields().len() {
        let mut inner_projection_cols = inner_required_cols.iter().copied().collect::<Vec<_>>();
        inner_projection_cols.sort();
        let expr = schema_name_projection(&plan.schema(), &inner_projection_cols)?;
        plan = Arc::new(ProjectionExec::try_new(expr, plan)?);
    }

    plan = user_constructor(plan)?;

    if outer_cols.len() < plan.schema().fields().len() {
        let expr = schema_name_projection(&plan.schema(), outer_cols)?;
        plan = Arc::new(ProjectionExec::try_new(expr, plan)?);
    }

    Ok(plan)
}

fn reassign_sort_exprs_columns(
    sort_exprs: &[PhysicalSortExpr],
    schema: &SchemaRef,
) -> Result<Vec<PhysicalSortExpr>> {
    sort_exprs
        .iter()
        .map(|expr| {
            Ok(PhysicalSortExpr {
                expr: reassign_predicate_columns(Arc::clone(&expr.expr), schema, false)?,
                options: expr.options,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow::{
        compute::SortOptions,
        datatypes::{DataType, Field, Schema, SchemaRef},
    };
    use datafusion::{
        datasource::object_store::ObjectStoreUrl,
        logical_expr::Operator,
        physical_plan::{
            expressions::{BinaryExpr, Literal},
            PhysicalExpr, Statistics,
        },
        scalar::ScalarValue,
    };

    use crate::{physical_optimizer::test_util::OptimizationTest, test::TestChunk};

    use super::*;

    #[test]
    fn test_empty_pushdown_select() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(EmptyExec::new(false, schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " EmptyExec: produce_one_row=false"
        "###
        );

        let empty_exec = test
            .output_plan()
            .unwrap()
            .as_any()
            .downcast_ref::<EmptyExec>()
            .unwrap();
        let expected_schema = Schema::new(vec![Field::new("tag1", DataType::Utf8, true)]);
        assert_eq!(empty_exec.schema().as_ref(), &expected_schema);
    }

    #[test]
    fn test_empty_pushdown_reorder() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (expr_col("tag2", &schema), String::from("tag2")),
                    (expr_col("tag1", &schema), String::from("tag1")),
                    (expr_col("field", &schema), String::from("field")),
                ],
                Arc::new(EmptyExec::new(false, schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag2@1 as tag2, tag1@0 as tag1, field@2 as field]"
          - "   EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " EmptyExec: produce_one_row=false"
        "###
        );

        let empty_exec = test
            .output_plan()
            .unwrap()
            .as_any()
            .downcast_ref::<EmptyExec>()
            .unwrap();
        let expected_schema = Schema::new(vec![
            Field::new("tag2", DataType::Utf8, true),
            Field::new("tag1", DataType::Utf8, true),
            Field::new("field", DataType::UInt64, true),
        ]);
        assert_eq!(empty_exec.schema().as_ref(), &expected_schema);
    }

    #[test]
    fn test_ignore_when_only_impure_projection_rename() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag2", &schema), String::from("tag1"))],
                Arc::new(EmptyExec::new(false, schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag2@1 as tag1]"
          - "   EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " ProjectionExec: expr=[tag2@1 as tag1]"
            - "   EmptyExec: produce_one_row=false"
        "###
        );
    }

    #[test]
    fn test_ignore_when_partial_impure_projection_rename() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (expr_col("tag1", &schema), String::from("tag1")),
                    (expr_col("tag2", &schema), String::from("tag3")),
                ],
                Arc::new(EmptyExec::new(false, schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag3]"
          - "   EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag3]"
            - "   EmptyExec: produce_one_row=false"
        "###
        );
    }

    #[test]
    fn test_ignore_impure_projection_calc() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    Arc::new(Literal::new(ScalarValue::from("foo"))),
                    String::from("tag1"),
                )],
                Arc::new(EmptyExec::new(false, schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[foo as tag1]"
          - "   EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " ProjectionExec: expr=[foo as tag1]"
            - "   EmptyExec: produce_one_row=false"
        "###
        );
    }

    #[test]
    fn test_unknown_node_type() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(TestExec::new(schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   Test"
        "###
        );
    }

    #[test]
    fn test_union() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(UnionExec::new(vec![
                    Arc::new(TestExec::new(Arc::clone(&schema))),
                    Arc::new(TestExec::new(schema)),
                ])),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   UnionExec"
          - "     Test"
          - "     Test"
        output:
          Ok:
            - " UnionExec"
            - "   ProjectionExec: expr=[tag1@0 as tag1]"
            - "     Test"
            - "   ProjectionExec: expr=[tag1@0 as tag1]"
            - "     Test"
        "###
        );
    }

    #[test]
    fn test_nested_union() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(UnionExec::new(vec![
                    Arc::new(UnionExec::new(vec![
                        Arc::new(TestExec::new(Arc::clone(&schema))),
                        Arc::new(TestExec::new(Arc::clone(&schema))),
                    ])),
                    Arc::new(TestExec::new(schema)),
                ])),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   UnionExec"
          - "     UnionExec"
          - "       Test"
          - "       Test"
          - "     Test"
        output:
          Ok:
            - " UnionExec"
            - "   UnionExec"
            - "     ProjectionExec: expr=[tag1@0 as tag1]"
            - "       Test"
            - "     ProjectionExec: expr=[tag1@0 as tag1]"
            - "       Test"
            - "   ProjectionExec: expr=[tag1@0 as tag1]"
            - "     Test"
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
            projection: Some(vec![1, 2]),
            limit: None,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, Some(expr_string_cmp("tag1", &schema)), None);
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag2", &inner.schema()), String::from("tag2"))],
                Arc::new(inner),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag2@0 as tag2]"
          - "   ParquetExec: limit=None, partitions={0 groups: []}, predicate=tag1@0 = foo, pruning_predicate=tag1_min@0 <= foo AND foo <= tag1_max@1, projection=[tag2, field]"
        output:
          Ok:
            - " ParquetExec: limit=None, partitions={0 groups: []}, predicate=tag1@0 = foo, pruning_predicate=tag1_min@0 <= foo AND foo <= tag1_max@1, projection=[tag2]"
        "###
        );

        let parquet_exec = test
            .output_plan()
            .unwrap()
            .as_any()
            .downcast_ref::<ParquetExec>()
            .unwrap();
        let expected_schema = Schema::new(vec![Field::new("tag2", DataType::Utf8, true)]);
        assert_eq!(parquet_exec.schema().as_ref(), &expected_schema);
    }

    #[test]
    fn test_filter_projection_split() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(
                    FilterExec::try_new(
                        expr_string_cmp("tag2", &schema),
                        Arc::new(TestExec::new(schema)),
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   FilterExec: tag2@1 = foo"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   FilterExec: tag2@1 = foo"
            - "     ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag2]"
            - "       Test"
        "###
        );
    }

    #[test]
    fn test_filter_inner_does_not_need_projection() {
        let schema = schema();
        let inner = TestExec::new(Arc::new(schema.project(&[0, 1]).unwrap()));
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &inner.schema()), String::from("tag1"))],
                Arc::new(
                    FilterExec::try_new(expr_string_cmp("tag2", &inner.schema()), Arc::new(inner))
                        .unwrap(),
                ),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   FilterExec: tag2@1 = foo"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   FilterExec: tag2@1 = foo"
            - "     Test"
        "###
        );
    }

    #[test]
    fn test_filter_outer_does_not_need_projection() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag2", &schema), String::from("tag2"))],
                Arc::new(
                    FilterExec::try_new(
                        expr_string_cmp("tag2", &schema),
                        Arc::new(TestExec::new(schema)),
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag2@1 as tag2]"
          - "   FilterExec: tag2@1 = foo"
          - "     Test"
        output:
          Ok:
            - " FilterExec: tag2@0 = foo"
            - "   ProjectionExec: expr=[tag2@1 as tag2]"
            - "     Test"
        "###
        );
    }

    #[test]
    fn test_filter_all_projections_unnecessary() {
        let schema = schema();
        let inner = TestExec::new(Arc::new(schema.project(&[1]).unwrap()));
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag2", &inner.schema()), String::from("tag2"))],
                Arc::new(
                    FilterExec::try_new(expr_string_cmp("tag2", &inner.schema()), Arc::new(inner))
                        .unwrap(),
                ),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag2@0 as tag2]"
          - "   FilterExec: tag2@0 = foo"
          - "     Test"
        output:
          Ok:
            - " FilterExec: tag2@0 = foo"
            - "   Test"
        "###
        );
    }

    // since `SortExec` and `FilterExec` both use `wrap_user_into_projections`, we only test one variant for `SortExec`
    #[test]
    fn test_sort_projection_split() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(
                    SortExec::try_new(
                        vec![PhysicalSortExpr {
                            expr: expr_col("tag2", &schema),
                            options: SortOptions {
                                descending: true,
                                ..Default::default()
                            },
                        }],
                        Arc::new(TestExec::new(schema)),
                        Some(42),
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   SortExec: fetch=42, expr=[tag2@1 DESC]"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   SortExec: fetch=42, expr=[tag2@1 DESC]"
            - "     ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag2]"
            - "       Test"
        "###
        );
    }

    // since `SortPreservingMergeExec` and `FilterExec` both use `wrap_user_into_projections`, we only test one variant for `SortPreservingMergeExec`
    #[test]
    fn test_sortpreservingmerge_projection_split() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(SortPreservingMergeExec::new(
                    vec![PhysicalSortExpr {
                        expr: expr_col("tag2", &schema),
                        options: SortOptions {
                            descending: true,
                            ..Default::default()
                        },
                    }],
                    Arc::new(TestExec::new(schema)),
                )),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   SortPreservingMergeExec: [tag2@1 DESC]"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   SortPreservingMergeExec: [tag2@1 DESC]"
            - "     ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag2]"
            - "       Test"
        "###
        );
    }

    #[test]
    fn test_nested_proj_inner_is_impure() {
        let schema = schema();
        let plan = Arc::new(EmptyExec::new(false, schema));
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (
                        Arc::new(Literal::new(ScalarValue::from("foo"))),
                        String::from("tag1"),
                    ),
                    (
                        Arc::new(Literal::new(ScalarValue::from("bar"))),
                        String::from("tag2"),
                    ),
                ],
                plan,
            )
            .unwrap(),
        );
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &plan.schema()), String::from("tag1"))],
                plan,
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   ProjectionExec: expr=[foo as tag1, bar as tag2]"
          - "     EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " ProjectionExec: expr=[foo as tag1]"
            - "   EmptyExec: produce_one_row=false"
        "###
        );
    }

    #[test]
    fn test_nested_proj_inner_is_pure() {
        let schema = schema();
        let plan = Arc::new(EmptyExec::new(false, schema));
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (expr_col("tag1", &plan.schema()), String::from("tag1")),
                    (expr_col("tag2", &plan.schema()), String::from("tag2")),
                ],
                plan,
            )
            .unwrap(),
        );
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &plan.schema()), String::from("tag1"))],
                plan,
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag2]"
          - "     EmptyExec: produce_one_row=false"
        output:
          Ok:
            - " EmptyExec: produce_one_row=false"
        "###
        );
        let empty_exec = test
            .output_plan()
            .unwrap()
            .as_any()
            .downcast_ref::<EmptyExec>()
            .unwrap();
        let expected_schema = Schema::new(vec![Field::new("tag1", DataType::Utf8, true)]);
        assert_eq!(empty_exec.schema().as_ref(), &expected_schema);
    }

    // since `DeduplicateExec` and `FilterExec` both use `wrap_user_into_projections`, we only test a few variants for `DeduplicateExec`
    #[test]
    fn test_dedup_projection_split1() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(DeduplicateExec::new(
                    Arc::new(TestExec::new(Arc::clone(&schema))),
                    vec![PhysicalSortExpr {
                        expr: expr_col("tag2", &schema),
                        options: SortOptions {
                            descending: true,
                            ..Default::default()
                        },
                    }],
                )),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   DeduplicateExec: [tag2@1 DESC]"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   DeduplicateExec: [tag2@1 DESC]"
            - "     ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag2]"
            - "       Test"
        "###
        );
    }

    #[test]
    fn test_dedup_projection_split2() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("field1", DataType::UInt64, true),
            Field::new("field2", DataType::UInt64, true),
        ]));
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (expr_col("tag1", &schema), String::from("tag1")),
                    (expr_col("field1", &schema), String::from("field1")),
                ],
                Arc::new(DeduplicateExec::new(
                    Arc::new(TestExec::new(Arc::clone(&schema))),
                    vec![
                        PhysicalSortExpr {
                            expr: expr_col("tag1", &schema),
                            options: SortOptions {
                                descending: true,
                                ..Default::default()
                            },
                        },
                        PhysicalSortExpr {
                            expr: expr_col("tag2", &schema),
                            options: SortOptions {
                                descending: false,
                                ..Default::default()
                            },
                        },
                    ],
                )),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1, field1@2 as field1]"
          - "   DeduplicateExec: [tag1@0 DESC,tag2@1 ASC]"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@1 as tag1, field1@0 as field1]"
            - "   DeduplicateExec: [tag1@1 DESC,tag2@2 ASC]"
            - "     ProjectionExec: expr=[field1@2 as field1, tag1@0 as tag1, tag2@1 as tag2]"
            - "       Test"
        "###
        );
    }

    #[test]
    fn test_recordbatches() {
        let schema = schema();
        let chunk = TestChunk::new("table")
            .with_tag_column("tag1")
            .with_u64_column("field");
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(RecordBatchesExec::new(vec![Arc::new(chunk) as _], schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        output:
          Ok:
            - " RecordBatchesExec: batches_groups=1 batches=0 total_rows=0"
        "###
        );

        let recordbatches_exec = test
            .output_plan()
            .unwrap()
            .as_any()
            .downcast_ref::<RecordBatchesExec>()
            .unwrap();
        let expected_schema = Schema::new(vec![Field::new("tag1", DataType::Utf8, true)]);
        assert_eq!(recordbatches_exec.schema().as_ref(), &expected_schema);
    }

    #[test]
    fn test_integration() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("field1", DataType::UInt64, true),
            Field::new("field2", DataType::UInt64, true),
        ]));
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        };
        let plan = Arc::new(ParquetExec::new(base_config, None, None));
        let plan = Arc::new(UnionExec::new(vec![plan]));
        let plan_schema = plan.schema();
        let plan = Arc::new(DeduplicateExec::new(
            plan,
            vec![
                PhysicalSortExpr {
                    expr: expr_col("tag1", &plan_schema),
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: expr_col("tag2", &plan_schema),
                    options: Default::default(),
                },
            ],
        ));
        let plan =
            Arc::new(FilterExec::try_new(expr_string_cmp("tag2", &plan.schema()), plan).unwrap());
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("field1", &plan.schema()), String::from("field1"))],
                plan,
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ProjectionExec: expr=[field1@2 as field1]"
          - "   FilterExec: tag2@1 = foo"
          - "     DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
          - "       UnionExec"
          - "         ParquetExec: limit=None, partitions={0 groups: []}, projection=[tag1, tag2, field1, field2]"
        output:
          Ok:
            - " ProjectionExec: expr=[field1@0 as field1]"
            - "   FilterExec: tag2@1 = foo"
            - "     ProjectionExec: expr=[field1@0 as field1, tag2@2 as tag2]"
            - "       DeduplicateExec: [tag1@1 ASC,tag2@2 ASC]"
            - "         UnionExec"
            - "           ParquetExec: limit=None, partitions={0 groups: []}, projection=[field1, tag1, tag2]"
        "###
        );
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("field", DataType::UInt64, true),
        ]))
    }

    fn expr_col(name: &str, schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        Arc::new(Column::new_with_schema(name, schema).unwrap())
    }

    fn expr_string_cmp(col: &str, schema: &SchemaRef) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(
            expr_col(col, schema),
            Operator::Eq,
            Arc::new(Literal::new(ScalarValue::from("foo"))),
        ))
    }

    #[derive(Debug)]
    struct TestExec {
        schema: SchemaRef,
    }

    impl TestExec {
        fn new(schema: SchemaRef) -> Self {
            Self { schema }
        }
    }

    impl ExecutionPlan for TestExec {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
        }

        fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
            None
        }

        fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
            vec![]
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            assert!(children.is_empty());
            Ok(self)
        }

        fn execute(
            &self,
            _partition: usize,
            _context: Arc<datafusion::execution::context::TaskContext>,
        ) -> Result<datafusion::physical_plan::SendableRecordBatchStream> {
            unimplemented!()
        }

        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "Test")
        }

        fn statistics(&self) -> datafusion::physical_plan::Statistics {
            unimplemented!()
        }
    }
}
