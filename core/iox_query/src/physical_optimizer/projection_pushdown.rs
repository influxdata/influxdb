use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use datafusion::{
    common::{
        plan_datafusion_err,
        tree_node::{Transformed, TreeNode},
    },
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileScanConfig, FileScanConfigBuilder},
        source::DataSourceExec,
    },
    error::{DataFusionError, Result},
    physical_expr::{LexOrdering, PhysicalSortExpr, utils::collect_columns},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan, PhysicalExpr,
        empty::EmptyExec,
        expressions::Column,
        filter::FilterExec,
        placeholder_row::PlaceholderRowExec,
        projection::ProjectionExec,
        sorts::{sort::SortExec, sort_preserving_merge::SortPreservingMergeExec},
        union::UnionExec,
    },
};

use crate::provider::{DeduplicateExec, RecordBatchesExec};

/// Push down projections.
#[derive(Debug, Default)]
pub struct ProjectionPushdown;

impl PhysicalOptimizerRule for ProjectionPushdown {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(optimize_plan).map(|t| t.data)
    }

    fn name(&self) -> &str {
        "projection_pushdown"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// Run the projection pushdown optimization on a single plan node.
fn optimize_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let plan_any = plan.as_any();

    let Some(projection_exec) = plan_any.downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };
    let child = projection_exec.input();

    let mut columns = Vec::with_capacity(projection_exec.expr().len());
    let mut column_indices = Vec::with_capacity(projection_exec.expr().len());
    for projection_expr in projection_exec.expr() {
        if let Some(column) = projection_expr.expr.as_any().downcast_ref::<Column>()
            && column.name() == projection_expr.alias
        {
            columns.push(column.clone());
            column_indices.push(column.index());
        } else {
            // don't bother w/ renames or to deal w/ calculation within projection nodes
            return Ok(Transformed::no(plan));
        }
    }
    let child_any = child.as_any();
    if let Some(child_empty) = child_any.downcast_ref::<EmptyExec>() {
        let new_child = EmptyExec::new(Arc::new(child_empty.schema().project(&column_indices)?));
        return Ok(Transformed::yes(Arc::new(new_child)));
    } else if let Some(child_placeholder) = child_any.downcast_ref::<PlaceholderRowExec>() {
        let new_child = PlaceholderRowExec::new(Arc::new(
            child_placeholder.schema().project(&column_indices)?,
        ));
        return Ok(Transformed::yes(Arc::new(new_child)));
    } else if let Some(child_union) = child_any.downcast_ref::<UnionExec>() {
        let new_inputs = child_union
            .inputs()
            .iter()
            .map(|input| {
                let exec =
                    ProjectionExec::try_new(projection_exec.expr().to_vec(), Arc::clone(input))?;
                Ok(Arc::new(exec) as _)
            })
            .collect::<Result<Vec<_>>>()?;
        let new_union = UnionExec::new(new_inputs);
        return Ok(Transformed::yes(Arc::new(new_union)));
    } else if let Some(child_parquet) = child_any.downcast_ref::<DataSourceExec>() {
        let Some(file_scan_config) = child_parquet
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
        else {
            return Ok(Transformed::no(plan));
        };
        let projection = match file_scan_config.projection.as_ref() {
            Some(projection) => column_indices
                .into_iter()
                .map(|idx| {
                    projection
                        .get(idx)
                        .copied()
                        .ok_or_else(|| DataFusionError::Execution("Projection broken".to_string()))
                })
                .collect::<Result<Vec<_>>>()?,
            None => column_indices,
        };

        let col_map = columns
            .iter()
            .cloned()
            .enumerate()
            .map(|(idx, col)| (col, idx))
            .collect();
        let output_ordering = file_scan_config
            .output_ordering
            .iter()
            .map(|output_ordering| project_output_ordering(output_ordering, &col_map))
            .collect::<Result<Vec<_>>>()?;
        // if there's any empty output order, we need to drop everything, at least until https://github.com/apache/datafusion/issues/17354 is fixed
        let output_ordering = output_ordering.into_iter().try_fold(
            Vec::with_capacity(file_scan_config.output_ordering.len()),
            |mut out, next| {
                let next = next?;
                out.push(next);
                Some(out)
            },
        );

        let mut file_scan_config_builder =
            FileScanConfigBuilder::from(file_scan_config.clone()).with_projection(Some(projection));
        if let Some(output_ordering) = output_ordering {
            file_scan_config_builder =
                file_scan_config_builder.with_output_ordering(output_ordering);
        }
        let file_scan_config = file_scan_config_builder.build();
        return Ok(Transformed::yes(DataSourceExec::from_data_source(
            file_scan_config,
        )));
    } else if let Some(child_filter) = child_any.downcast_ref::<FilterExec>() {
        let filter_required_cols = collect_columns(child_filter.predicate());

        let plan = wrap_user_into_projections(
            filter_required_cols,
            &columns,
            Arc::clone(child_filter.input()),
            |plan, col_map| {
                Ok(Arc::new(FilterExec::try_new(
                    remap_columns(Arc::clone(child_filter.predicate()), col_map)?,
                    plan,
                )?))
            },
        )?;

        return Ok(Transformed::yes(plan));
    } else if let Some(child_sort) = child_any.downcast_ref::<SortExec>() {
        let sort_required_cols = child_sort
            .expr()
            .iter()
            .flat_map(|expr| collect_columns(&expr.expr))
            .collect::<HashSet<_>>();

        let plan = wrap_user_into_projections(
            sort_required_cols,
            &columns,
            Arc::clone(child_sort.input()),
            |plan, col_map| {
                Ok(Arc::new(
                    SortExec::new(
                        LexOrdering::new(reassign_sort_exprs_columns(child_sort.expr(), col_map)?)
                            .ok_or_else(|| plan_datafusion_err!("empty SortExec ordering"))?,
                        plan,
                    )
                    .with_preserve_partitioning(child_sort.preserve_partitioning())
                    .with_fetch(child_sort.fetch()),
                ))
            },
        )?;

        return Ok(Transformed::yes(plan));
    } else if let Some(child_sort) = child_any.downcast_ref::<SortPreservingMergeExec>() {
        let sort_required_cols = child_sort
            .expr()
            .iter()
            .flat_map(|expr| collect_columns(&expr.expr))
            .collect::<HashSet<_>>();

        let plan = wrap_user_into_projections(
            sort_required_cols,
            &columns,
            Arc::clone(child_sort.input()),
            |plan, col_map| {
                Ok(Arc::new(SortPreservingMergeExec::new(
                    LexOrdering::new(reassign_sort_exprs_columns(child_sort.expr(), col_map)?)
                        .ok_or_else(|| {
                            plan_datafusion_err!("SortPreservingMergeExec sort key empty")
                        })?,
                    plan,
                )))
            },
        )?;

        return Ok(Transformed::yes(plan));
    } else if let Some(child_proj) = child_any.downcast_ref::<ProjectionExec>() {
        let expr = column_indices
            .iter()
            .map(|idx| child_proj.expr()[*idx].clone())
            .collect::<Vec<_>>();
        let plan = Arc::new(ProjectionExec::try_new(
            expr,
            Arc::clone(child_proj.input()),
        )?);

        // need to call `optimize_plan` directly on the plan, because otherwise
        // we would continue with the child and miss the optimization of that
        // particular new ProjectionExec
        let plan = optimize_plan(plan)?.data;

        return Ok(Transformed::yes(plan));
    } else if let Some(child_dedup) = child_any.downcast_ref::<DeduplicateExec>() {
        let dedup_required_cols = child_dedup.sort_column_iter();

        let mut children = child_dedup.children();
        assert_eq!(children.len(), 1);
        let input = children.pop().expect("just checked len");

        let plan = wrap_user_into_projections(
            dedup_required_cols,
            &columns,
            Arc::clone(input),
            |plan, col_map| {
                let sort_keys = reassign_sort_exprs_columns(child_dedup.sort_keys(), col_map)?;
                Ok(Arc::new(DeduplicateExec::new(
                    plan,
                    LexOrdering::new(sort_keys)
                        .ok_or_else(|| plan_datafusion_err!("de-dup sort key empty"))?,
                    child_dedup.use_chunk_order_col(),
                )))
            },
        )?;

        return Ok(Transformed::yes(plan));
    } else if let Some(child_recordbatches) = child_any.downcast_ref::<RecordBatchesExec>() {
        let new_child = RecordBatchesExec::new(
            child_recordbatches.chunks().cloned(),
            Arc::new(child_recordbatches.schema().project(&column_indices)?),
            child_recordbatches.output_sort_key_memo().cloned(),
        );
        return Ok(Transformed::yes(Arc::new(new_child)));
    }

    Ok(Transformed::no(plan))
}

/// Given the output ordering and a projected schema, returns the
/// largest prefix of the ordering that is in the projection
///
/// For example,
///
/// ```text
/// output_ordering: a, b, c
/// projection: a, c
/// returns --> a
/// ```
///
/// To see why the input has to be a prefix, consider this input:
///
/// ```text
/// a    b
/// 1    1
/// 2    2
/// 3    1
/// ``
///
/// It is sorted on `a,b` but *not* sorted on `b`
fn project_output_ordering(
    output_ordering: &LexOrdering,
    col_map: &HashMap<Column, usize>,
) -> Result<Option<LexOrdering>> {
    // take longest prefix
    let sort_exprs = output_ordering
        .iter()
        .take_while(|expr| {
            if let Some(col) = expr.expr.as_any().downcast_ref::<Column>() {
                col_map.contains_key(col)
            } else {
                // do not keep exprs like `a+1` or `-a` as they may
                // not maintain ordering
                false
            }
        })
        .cloned()
        .collect::<Vec<_>>();

    Ok(LexOrdering::new(reassign_sort_exprs_columns(
        &sort_exprs,
        col_map,
    )?))
}

/// remap the column references in the expression to the new
/// indices specified in the map.
fn remap_columns(
    expr: Arc<dyn PhysicalExpr>,
    col_map: &HashMap<Column, usize>,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.transform_down(|expr| {
        if let Some(col) = expr.as_any().downcast_ref::<Column>() {
            let idx = *col_map.get(col).unwrap();
            if idx != col.index() {
                return Ok(Transformed::yes(Arc::new(Column::new(col.name(), idx))));
            }
        }
        Ok(Transformed::no(expr))
    })
    .map(|t| t.data)
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
fn wrap_user_into_projections<I, F>(
    user_required_cols: I,
    outer_cols: &[Column],
    inner_plan: Arc<dyn ExecutionPlan>,
    user_constructor: F,
) -> Result<Arc<dyn ExecutionPlan>>
where
    I: IntoIterator<Item = Column>,
    F: FnOnce(Arc<dyn ExecutionPlan>, &HashMap<Column, usize>) -> Result<Arc<dyn ExecutionPlan>>,
{
    let mut plan = inner_plan;

    // put the user_required_cols in a determinisic order.
    let mut user_required_cols = user_required_cols.into_iter().collect::<Vec<_>>();
    user_required_cols.sort_by_key(|col| col.index());
    let cap = user_required_cols.len() + outer_cols.len();
    let mut inner_projection_cols = Vec::with_capacity(cap);
    let mut col_map = HashMap::with_capacity(cap);
    for col in outer_cols.iter().chain(user_required_cols.iter()) {
        if !col_map.contains_key(col) {
            col_map.insert(col.clone(), col_map.len());
            inner_projection_cols.push(col.clone());
        }
    }
    let plan_schema = plan.schema();
    let plan_cols = plan_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| Column::new(f.name(), idx))
        .collect::<Vec<_>>();
    if plan_cols != inner_projection_cols {
        let expr = column_projection(inner_projection_cols);
        plan = Arc::new(ProjectionExec::try_new(expr, plan)?);
    }

    plan = user_constructor(plan, &col_map)?;

    if outer_cols.len() < plan.schema().fields().len() {
        let expr = column_projection(outer_cols.iter().map(|col| {
            let idx = col_map.get(col).copied().unwrap();
            Column::new(col.name(), idx)
        }));
        plan = Arc::new(ProjectionExec::try_new(expr, plan)?);
    }

    Ok(plan)
}

/// Create a projection expression for the given columns.
fn column_projection<T>(columns: T) -> Vec<(Arc<dyn PhysicalExpr>, String)>
where
    T: IntoIterator<Item = Column>,
{
    columns
        .into_iter()
        .map(|c| {
            let name = c.name().to_string();
            (Arc::new(c) as _, name)
        })
        .collect()
}

/// Take a series of sort_exprs, and remap the projections.
fn reassign_sort_exprs_columns(
    sort_exprs: &[PhysicalSortExpr],
    col_map: &HashMap<Column, usize>,
) -> Result<Vec<PhysicalSortExpr>> {
    sort_exprs
        .iter()
        .map(|expr| {
            Ok(PhysicalSortExpr {
                expr: remap_columns(Arc::clone(&expr.expr), col_map)?,
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
    use datafusion::physical_plan::projection::ProjectionExpr;
    use datafusion::{
        common::JoinType,
        datasource::object_store::ObjectStoreUrl,
        functions::core::coalesce,
        logical_expr::Operator,
        physical_expr::{EquivalenceProperties, ScalarFunctionExpr},
        physical_plan::{
            DisplayAs, PhysicalExpr, PlanProperties,
            execution_plan::{Boundedness, EmissionType},
            expressions::{BinaryExpr, IsNullExpr, Literal},
            joins::{HashJoinExec, NestedLoopJoinExec, PartitionMode, utils::JoinFilter},
        },
        scalar::ScalarValue,
    };
    use datafusion::{
        common::NullEquality,
        datasource::physical_plan::{FileScanConfigBuilder, ParquetSource},
    };
    use datafusion_util::config::table_parquet_options;
    use serde::Serialize;

    use crate::{
        physical_optimizer::test_util::{OptimizationTest, assert_unknown_partitioning},
        test::TestChunk,
    };

    use super::*;

    #[test]
    fn test_empty_pushdown_select() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(EmptyExec::new(schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   EmptyExec"
        output:
          Ok:
            - " EmptyExec"
        "#
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
                Arc::new(EmptyExec::new(schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " ProjectionExec: expr=[tag2@1 as tag2, tag1@0 as tag1, field@2 as field]"
          - "   EmptyExec"
        output:
          Ok:
            - " EmptyExec"
        "#
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
                Arc::new(EmptyExec::new(schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag2@1 as tag1]"
          - "   EmptyExec"
        output:
          Ok:
            - " ProjectionExec: expr=[tag2@1 as tag1]"
            - "   EmptyExec"
        "#
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
                Arc::new(EmptyExec::new(schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag3]"
          - "   EmptyExec"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag3]"
            - "   EmptyExec"
        "#
        );
    }

    #[test]
    fn test_ignore_impure_projection_calc() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![ProjectionExpr::new(
                    Arc::new(Literal::new(ScalarValue::from("foo"))),
                    String::from("tag1"),
                )],
                Arc::new(EmptyExec::new(schema)),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[foo as tag1]"
          - "   EmptyExec"
        output:
          Ok:
            - " ProjectionExec: expr=[foo as tag1]"
            - "   EmptyExec"
        "#
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
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   Test"
        "#
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
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
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
        "#
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
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
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
        "#
        );
    }

    #[test]
    fn test_parquet() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("tag3", DataType::Utf8, true),
            Field::new("field", DataType::UInt64, true),
        ]));
        let projection = vec![3, 2, 1];
        let schema_projected = Arc::new(schema.project(&projection).unwrap());
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(
                ParquetSource::new(table_parquet_options())
                    .with_predicate(expr_string_cmp("tag1", &schema)),
            ),
        )
        .with_projection(Some(projection))
        .with_output_ordering(vec![
            LexOrdering::new(vec![
                PhysicalSortExpr {
                    expr: expr_col("tag3", &schema_projected),
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: expr_col("field", &schema_projected),
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: expr_col("tag2", &schema_projected),
                    options: Default::default(),
                },
            ])
            .unwrap(),
        ]);
        let inner = DataSourceExec::from_data_source(file_scan_config.build());
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (expr_col("tag2", &inner.schema()), String::from("tag2")),
                    (expr_col("tag3", &inner.schema()), String::from("tag3")),
                ],
                inner,
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " ProjectionExec: expr=[tag2@2 as tag2, tag3@1 as tag3]"
          - "   DataSourceExec: file_groups={0 groups: []}, projection=[field, tag3, tag2], output_ordering=[tag3@1 ASC, field@0 ASC, tag2@2 ASC], file_type=parquet, predicate=tag1@0 = foo, pruning_predicate=tag1_null_count@2 != row_count@3 AND tag1_min@0 <= foo AND foo <= tag1_max@1, required_guarantees=[tag1 in (foo)]"
        output:
          Ok:
            - " DataSourceExec: file_groups={0 groups: []}, projection=[tag2, tag3], output_ordering=[tag3@1 ASC], file_type=parquet, predicate=tag1@0 = foo, pruning_predicate=tag1_null_count@2 != row_count@3 AND tag1_min@0 <= foo AND foo <= tag1_max@1, required_guarantees=[tag1 in (foo)]"
        "#
        );

        let data_source_exec = test
            .output_plan()
            .unwrap()
            .as_any()
            .downcast_ref::<DataSourceExec>()
            .unwrap();
        let expected_schema = Schema::new(vec![
            Field::new("tag2", DataType::Utf8, true),
            Field::new("tag3", DataType::Utf8, true),
        ]);
        assert_eq!(data_source_exec.schema().as_ref(), &expected_schema);
    }

    #[test]
    fn test_parquet_empty_ordering_after_projection() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
        ]));
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_output_ordering(vec![
            LexOrdering::new(vec![PhysicalSortExpr {
                expr: expr_col("tag2", &schema),
                options: Default::default(),
            }])
            .unwrap(),
        ]);
        let inner = DataSourceExec::from_data_source(file_scan_config.build());
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &inner.schema()), String::from("tag1"))],
                inner,
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   DataSourceExec: file_groups={0 groups: []}, projection=[tag1, tag2], output_ordering=[tag2@1 ASC], file_type=parquet"
        output:
          Ok:
            - " DataSourceExec: file_groups={0 groups: []}, projection=[tag1], file_type=parquet"
        "#
        );
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
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
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
        "#
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
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   FilterExec: tag2@1 = foo"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   FilterExec: tag2@1 = foo"
            - "     Test"
        "#
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
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag2@1 as tag2]"
          - "   FilterExec: tag2@1 = foo"
          - "     Test"
        output:
          Ok:
            - " FilterExec: tag2@0 = foo"
            - "   ProjectionExec: expr=[tag2@1 as tag2]"
            - "     Test"
        "#
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
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag2@0 as tag2]"
          - "   FilterExec: tag2@0 = foo"
          - "     Test"
        output:
          Ok:
            - " FilterExec: tag2@0 = foo"
            - "   Test"
        "#
        );
    }

    #[test]
    fn test_filter_uses_resorted_cols() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![
                    (expr_col("tag2", &schema), String::from("tag2")),
                    (expr_col("tag1", &schema), String::from("tag1")),
                    (expr_col("field", &schema), String::from("field")),
                ],
                Arc::new(
                    FilterExec::try_new(
                        expr_and(
                            expr_string_cmp("tag2", &schema),
                            expr_string_cmp("tag1", &schema),
                        ),
                        Arc::new(TestExec::new(schema)),
                    )
                    .unwrap(),
                ),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag2@1 as tag2, tag1@0 as tag1, field@2 as field]"
          - "   FilterExec: tag2@1 = foo AND tag1@0 = foo"
          - "     Test"
        output:
          Ok:
            - " FilterExec: tag2@0 = foo AND tag1@1 = foo"
            - "   ProjectionExec: expr=[tag2@1 as tag2, tag1@0 as tag1, field@2 as field]"
            - "     Test"
        "#
        );
    }

    // since `SortExec` and `FilterExec` both use `wrap_user_into_projections`, we only test a few variants for `SortExec`
    #[test]
    fn test_sort_projection_split() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(
                    SortExec::new(
                        LexOrdering::new(vec![PhysicalSortExpr {
                            expr: expr_col("tag2", &schema),
                            options: SortOptions {
                                descending: true,
                                ..Default::default()
                            },
                        }])
                        .unwrap(),
                        Arc::new(TestExec::new(schema)),
                    )
                    .with_fetch(Some(42)),
                ),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   SortExec: TopK(fetch=42), expr=[tag2@1 DESC], preserve_partitioning=[false]"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   SortExec: TopK(fetch=42), expr=[tag2@1 DESC], preserve_partitioning=[false]"
            - "     ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag2]"
            - "       Test"
        "#
        );
    }

    #[test]
    fn test_sort_preserve_partitioning() {
        let schema = schema();
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![(expr_col("tag1", &schema), String::from("tag1"))],
                Arc::new(
                    SortExec::new(
                        LexOrdering::new(vec![PhysicalSortExpr {
                            expr: expr_col("tag2", &schema),
                            options: SortOptions {
                                descending: true,
                                ..Default::default()
                            },
                        }])
                        .unwrap(),
                        Arc::new(TestExec::new_with_partitions(schema, 2)),
                    )
                    .with_preserve_partitioning(true)
                    .with_fetch(Some(42)),
                ),
            )
            .unwrap(),
        );

        assert_unknown_partitioning(plan.properties().output_partitioning().clone(), 2);

        let opt = ProjectionPushdown;
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   SortExec: TopK(fetch=42), expr=[tag2@1 DESC], preserve_partitioning=[true]"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1]"
            - "   SortExec: TopK(fetch=42), expr=[tag2@1 DESC], preserve_partitioning=[true]"
            - "     ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag2]"
            - "       Test"
        "#
        );

        assert_unknown_partitioning(
            test.output_plan()
                .unwrap()
                .properties()
                .output_partitioning()
                .clone(),
            2,
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
                    LexOrdering::new(vec![PhysicalSortExpr {
                        expr: expr_col("tag2", &schema),
                        options: SortOptions {
                            descending: true,
                            ..Default::default()
                        },
                    }])
                    .unwrap(),
                    Arc::new(TestExec::new(schema)),
                )),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
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
        "#
        );
    }

    #[test]
    fn test_nested_proj_inner_is_impure() {
        let schema = schema();
        let plan = Arc::new(EmptyExec::new(schema));
        let plan = Arc::new(
            ProjectionExec::try_new(
                vec![
                    ProjectionExpr::new(
                        Arc::new(Literal::new(ScalarValue::from("foo"))),
                        String::from("tag1"),
                    ),
                    ProjectionExpr::new(
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
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   ProjectionExec: expr=[foo as tag1, bar as tag2]"
          - "     EmptyExec"
        output:
          Ok:
            - " ProjectionExec: expr=[foo as tag1]"
            - "   EmptyExec"
        "#
        );
    }

    #[test]
    fn test_nested_proj_inner_is_pure() {
        let schema = schema();
        let plan = Arc::new(EmptyExec::new(schema));
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
        let opt = ProjectionPushdown;
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   ProjectionExec: expr=[tag1@0 as tag1, tag2@1 as tag2]"
          - "     EmptyExec"
        output:
          Ok:
            - " EmptyExec"
        "#
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
                    LexOrdering::new(vec![PhysicalSortExpr {
                        expr: expr_col("tag2", &schema),
                        options: SortOptions {
                            descending: true,
                            ..Default::default()
                        },
                    }])
                    .unwrap(),
                    false,
                )),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
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
        "#
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
                    LexOrdering::new(vec![
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
                    ])
                    .unwrap(),
                    false,
                )),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1, field1@2 as field1]"
          - "   DeduplicateExec: [tag1@0 DESC,tag2@1 ASC]"
          - "     Test"
        output:
          Ok:
            - " ProjectionExec: expr=[tag1@0 as tag1, field1@1 as field1]"
            - "   DeduplicateExec: [tag1@0 DESC,tag2@2 ASC]"
            - "     ProjectionExec: expr=[tag1@0 as tag1, field1@2 as field1, tag2@1 as tag2]"
            - "       Test"
        "#
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
                Arc::new(RecordBatchesExec::new(
                    vec![Arc::new(chunk) as _],
                    schema,
                    None,
                )),
            )
            .unwrap(),
        );
        let opt = ProjectionPushdown;
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " ProjectionExec: expr=[tag1@0 as tag1]"
          - "   RecordBatchesExec: chunks=1, projection=[tag1, tag2, field]"
        output:
          Ok:
            - " RecordBatchesExec: chunks=1, projection=[tag1]"
        "#
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
    fn test_preserve_schema_with_duplicate_column_names() {
        let config_options = Arc::new(ConfigOptions::new());

        let table_schema = Arc::new(Schema::new([Arc::new(Field::new(
            "col1",
            DataType::Utf8,
            true,
        ))]));
        let plan = HashJoinExec::try_new(
            Arc::new(EmptyExec::new(Arc::clone(&table_schema))),
            Arc::new(EmptyExec::new(Arc::clone(&table_schema))),
            vec![(
                Arc::new(Column::new("col1", 0)),
                Arc::new(Column::new("col1", 0)),
            )],
            None,
            &JoinType::Full,
            None,
            PartitionMode::Auto,
            NullEquality::NullEqualsNothing,
        )
        .unwrap();
        let plan = ProjectionExec::try_new(
            [
                ProjectionExpr::new(
                    Arc::new(ScalarFunctionExpr::new(
                        "COALESCE",
                        coalesce(),
                        vec![
                            Arc::new(Column::new("col1", 0)),
                            Arc::new(Column::new("col1", 1)),
                        ],
                        Arc::new(Field::new("r", DataType::Utf8, false)),
                        Arc::clone(&config_options),
                    )),
                    "__common_expr".to_string(),
                ),
                ProjectionExpr::new(Arc::new(Column::new("col1", 0)), "col1".to_string()),
                ProjectionExpr::new(Arc::new(Column::new("col1", 1)), "col1".to_string()),
            ],
            Arc::new(plan),
        )
        .unwrap();
        let plan = FilterExec::try_new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("__common_expr", 0)),
                Operator::Lt,
                Arc::new(Literal::new(ScalarValue::Utf8(Some("a".to_string())))),
            )),
            Arc::new(plan),
        )
        .unwrap();
        let plan = ProjectionExec::try_new(
            vec![
                ProjectionExpr::new(Arc::new(Column::new("col1", 1)), "col1".to_string()),
                ProjectionExpr::new(Arc::new(Column::new("col1", 2)), "col1".to_string()),
            ],
            Arc::new(plan),
        )
        .unwrap();
        let plan = ProjectionExec::try_new(
            vec![ProjectionExpr::new(
                Arc::new(ScalarFunctionExpr::new(
                    "COALESCE",
                    coalesce(),
                    vec![
                        Arc::new(Column::new("col1", 0)),
                        Arc::new(Column::new("col1", 1)),
                    ],
                    Arc::new(Field::new("r", DataType::Utf8, false)),
                    config_options,
                )),
                "value".to_string(),
            )],
            Arc::new(plan),
        )
        .unwrap();
        let test = OptimizationTest::new(Arc::new(plan), ProjectionPushdown);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " ProjectionExec: expr=[COALESCE(col1@0, col1@1) as value]"
          - "   ProjectionExec: expr=[col1@1 as col1, col1@2 as col1]"
          - "     FilterExec: __common_expr@0 < a"
          - "       ProjectionExec: expr=[COALESCE(col1@0, col1@1) as __common_expr, col1@0 as col1, col1@1 as col1]"
          - "         HashJoinExec: mode=Auto, join_type=Full, on=[(col1@0, col1@0)]"
          - "           EmptyExec"
          - "           EmptyExec"
        output:
          Ok:
            - " ProjectionExec: expr=[COALESCE(col1@0, col1@1) as value]"
            - "   ProjectionExec: expr=[col1@0 as col1, col1@1 as col1]"
            - "     FilterExec: __common_expr@2 < a"
            - "       ProjectionExec: expr=[col1@0 as col1, col1@1 as col1, COALESCE(col1@0, col1@1) as __common_expr]"
            - "         HashJoinExec: mode=Auto, join_type=Full, on=[(col1@0, col1@0)]"
            - "           EmptyExec"
            - "           EmptyExec"
        "#
        );
    }

    #[test]
    fn test_preserve_schema_with_duplicate_input_column_names() {
        let table_schema = Arc::new(Schema::new([
            Arc::new(Field::new("col1", DataType::Int64, false)),
            Arc::new(Field::new("col2", DataType::Utf8, true)),
        ]));
        let filter = JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("col2", 1)),
                    Operator::NotEq,
                    Arc::new(Column::new("col2", 3)),
                )),
                Operator::And,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("col1", 0)),
                    Operator::Gt,
                    Arc::new(Column::new("col1", 1)),
                )),
            )),
            JoinFilter::build_column_indices(vec![0, 1], vec![0, 1]),
            Schema::new([
                Arc::new(Field::new("col1", DataType::Int64, false)),
                Arc::new(Field::new("col2", DataType::Utf8, true)),
                Arc::new(Field::new("col1", DataType::Int64, false)),
                Arc::new(Field::new("col2", DataType::Utf8, true)),
            ])
            .into(),
        );
        let plan = NestedLoopJoinExec::try_new(
            Arc::new(EmptyExec::new(Arc::clone(&table_schema))),
            Arc::new(EmptyExec::new(Arc::clone(&table_schema))),
            Some(filter),
            &JoinType::Inner,
            None,
        )
        .unwrap();
        let plan = ProjectionExec::try_new(
            vec![
                ProjectionExpr::new(Arc::new(Column::new("col1", 0)), "col1".to_string()),
                ProjectionExpr::new(Arc::new(Column::new("col1", 2)), "col1".to_string()),
            ],
            Arc::new(plan),
        )
        .unwrap();
        let filter = JoinFilter::new(
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("col1", 0)),
                    Operator::Gt,
                    Arc::new(Column::new("col1", 2)),
                )),
                Operator::And,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("col1", 2)),
                    Operator::Gt,
                    Arc::new(Column::new("col1", 1)),
                )),
            )),
            JoinFilter::build_column_indices(vec![0, 1], vec![0]),
            Schema::new([
                Arc::new(Field::new("col1", DataType::Int64, false)),
                Arc::new(Field::new("col1", DataType::Int64, false)),
                Arc::new(Field::new("col1", DataType::Int64, false)),
            ])
            .into(),
        );
        let plan = NestedLoopJoinExec::try_new(
            Arc::new(plan),
            Arc::new(EmptyExec::new(Arc::clone(&table_schema))),
            Some(filter),
            &JoinType::Left,
            None,
        )
        .unwrap();
        let plan = ProjectionExec::try_new(
            vec![
                ProjectionExpr::new(Arc::new(Column::new("col1", 0)), "col1".to_string()),
                ProjectionExpr::new(Arc::new(Column::new("col1", 2)), "col1".to_string()),
            ],
            Arc::new(plan),
        )
        .unwrap();
        let plan = FilterExec::try_new(
            Arc::new(IsNullExpr::new(Arc::new(Column::new("col1", 1)))),
            Arc::new(plan),
        )
        .unwrap();
        let plan = ProjectionExec::try_new(
            vec![ProjectionExpr::new(
                Arc::new(Column::new("col1", 0)),
                "col1".to_string(),
            )],
            Arc::new(plan),
        )
        .unwrap();

        let test = OptimizationTest::new(Arc::new(plan), ProjectionPushdown);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " ProjectionExec: expr=[col1@0 as col1]"
          - "   FilterExec: col1@1 IS NULL"
          - "     ProjectionExec: expr=[col1@0 as col1, col1@2 as col1]"
          - "       NestedLoopJoinExec: join_type=Left, filter=col1@0 > col1@2 AND col1@2 > col1@1"
          - "         ProjectionExec: expr=[col1@0 as col1, col1@2 as col1]"
          - "           NestedLoopJoinExec: join_type=Inner, filter=col2@1 != col2@3 AND col1@0 > col1@1"
          - "             EmptyExec"
          - "             EmptyExec"
          - "         EmptyExec"
        output:
          Ok:
            - " ProjectionExec: expr=[col1@0 as col1]"
            - "   FilterExec: col1@1 IS NULL"
            - "     ProjectionExec: expr=[col1@0 as col1, col1@2 as col1]"
            - "       NestedLoopJoinExec: join_type=Left, filter=col1@0 > col1@2 AND col1@2 > col1@1"
            - "         ProjectionExec: expr=[col1@0 as col1, col1@2 as col1]"
            - "           NestedLoopJoinExec: join_type=Inner, filter=col2@1 != col2@3 AND col1@0 > col1@1"
            - "             EmptyExec"
            - "             EmptyExec"
            - "         EmptyExec"
        "#
        );
    }

    #[test]
    fn test_integration() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("field1", DataType::UInt64, true),
            Field::new("field2", DataType::UInt64, true),
        ]));
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .build();
        let plan = DataSourceExec::from_data_source(file_scan_config);
        let plan_schema = plan.schema();
        let plan = Arc::new(DeduplicateExec::new(
            plan,
            LexOrdering::new(vec![
                PhysicalSortExpr {
                    expr: expr_col("tag1", &plan_schema),
                    options: Default::default(),
                },
                PhysicalSortExpr {
                    expr: expr_col("tag2", &plan_schema),
                    options: Default::default(),
                },
            ])
            .unwrap(),
            false,
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
        let opt = ProjectionPushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " ProjectionExec: expr=[field1@2 as field1]"
          - "   FilterExec: tag2@1 = foo"
          - "     DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
          - "       DataSourceExec: file_groups={0 groups: []}, projection=[tag1, tag2, field1, field2], file_type=parquet"
        output:
          Ok:
            - " ProjectionExec: expr=[field1@0 as field1]"
            - "   FilterExec: tag2@1 = foo"
            - "     ProjectionExec: expr=[field1@0 as field1, tag2@1 as tag2]"
            - "       DeduplicateExec: [tag1@2 ASC,tag2@1 ASC]"
            - "         DataSourceExec: file_groups={0 groups: []}, projection=[field1, tag2, tag1], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_project_output_ordering_keep() {
        let schema = schema();
        let projection = vec!["tag1", "tag2"];
        let output_ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: expr_col("tag1", &schema),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: expr_col("tag2", &schema),
                options: Default::default(),
            },
        ])
        .unwrap();

        insta::assert_yaml_snapshot!(
            ProjectOutputOrdering::new(&schema, output_ordering, projection),
            @r"
        output_ordering:
          - tag1@0
          - tag2@1
        projection:
          - tag1
          - tag2
        projected_ordering:
          - tag1@0
          - tag2@1
        "
        );
    }

    #[test]
    fn test_project_output_ordering_project_prefix() {
        let schema = schema();
        let projection = vec!["tag1"]; // prefix of the sort key
        let output_ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: expr_col("tag1", &schema),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: expr_col("tag2", &schema),
                options: Default::default(),
            },
        ])
        .unwrap();

        insta::assert_yaml_snapshot!(
            ProjectOutputOrdering::new(&schema, output_ordering, projection),
            @r"
        output_ordering:
          - tag1@0
          - tag2@1
        projection:
          - tag1
        projected_ordering:
          - tag1@0
        "
        );
    }

    #[test]
    fn test_project_output_ordering_project_non_prefix() {
        let schema = schema();
        let projection = vec!["tag2"]; // in sort key, but not prefix
        let output_ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: expr_col("tag1", &schema),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: expr_col("tag2", &schema),
                options: Default::default(),
            },
        ])
        .unwrap();

        insta::assert_yaml_snapshot!(
            ProjectOutputOrdering::new(&schema, output_ordering, projection),
            @r"
        output_ordering:
          - tag1@0
          - tag2@1
        projection:
          - tag2
        projected_ordering: []
        "
        );
    }

    #[test]
    fn test_project_output_ordering_projection_reorder() {
        let schema = schema();
        let projection = vec!["tag2", "tag1", "field"]; // in different order than sort key
        let output_ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: expr_col("tag1", &schema),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: expr_col("tag2", &schema),
                options: Default::default(),
            },
        ])
        .unwrap();

        insta::assert_yaml_snapshot!(
            ProjectOutputOrdering::new(&schema, output_ordering, projection),
            @r"
        output_ordering:
          - tag1@0
          - tag2@1
        projection:
          - tag2
          - tag1
          - field
        projected_ordering:
          - tag1@1
          - tag2@0
        "
        );
    }

    #[test]
    fn test_project_output_ordering_constant() {
        let schema = schema();
        let projection = vec!["tag2"];
        let output_ordering = LexOrdering::new(vec![
            // ordering by a constant is ignored
            PhysicalSortExpr {
                expr: datafusion::physical_plan::expressions::lit(1),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: expr_col("tag2", &schema),
                options: Default::default(),
            },
        ])
        .unwrap();

        insta::assert_yaml_snapshot!(
            ProjectOutputOrdering::new(&schema, output_ordering, projection),
            @r#"
        output_ordering:
          - "1"
          - tag2@1
        projection:
          - tag2
        projected_ordering: []
        "#
        );
    }

    #[test]
    fn test_project_output_ordering_constant_second_position() {
        let schema = schema();
        let projection = vec!["tag2"];
        let output_ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: expr_col("tag2", &schema),
                options: Default::default(),
            },
            // ordering by a constant is ignored
            PhysicalSortExpr {
                expr: datafusion::physical_plan::expressions::lit(1),
                options: Default::default(),
            },
        ])
        .unwrap();

        insta::assert_yaml_snapshot!(
            ProjectOutputOrdering::new(&schema, output_ordering, projection),
            @r#"
        output_ordering:
          - tag2@1
          - "1"
        projection:
          - tag2
        projected_ordering:
          - tag2@0
        "#
        );
    }

    /// project the output_ordering with the projection,
    // derive serde to make a nice 'insta' snapshot
    #[derive(Debug, Serialize)]
    struct ProjectOutputOrdering {
        output_ordering: Vec<String>,
        projection: Vec<String>,
        projected_ordering: Vec<String>,
    }

    impl ProjectOutputOrdering {
        fn new(
            schema: &Schema,
            output_ordering: LexOrdering,
            projection: Vec<&'static str>,
        ) -> Self {
            let col_map = projection
                .iter()
                .map(|field_name| {
                    Column::new(
                        field_name,
                        schema.index_of(field_name).expect("finding field"),
                    )
                })
                .enumerate()
                .map(|(idx, col)| (col, idx))
                .collect();

            let projected_ordering = project_output_ordering(&output_ordering, &col_map);

            let projected_ordering = match projected_ordering {
                Ok(Some(projected_ordering)) => format_sort_exprs(&projected_ordering),
                Ok(None) => {
                    vec![]
                }
                Err(e) => vec![e.to_string()],
            };

            Self {
                output_ordering: format_sort_exprs(&output_ordering),
                projection: projection.iter().map(|s| s.to_string()).collect(),
                projected_ordering,
            }
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("field", DataType::UInt64, true),
        ]))
    }

    /// Take a series of sort_exprs, and convert to strings.
    fn format_sort_exprs(sort_exprs: &[PhysicalSortExpr]) -> Vec<String> {
        sort_exprs
            .iter()
            .map(|expr| {
                let PhysicalSortExpr { expr, options: _ } = expr;
                expr.to_string()
            })
            .collect::<Vec<_>>()
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

    fn expr_and(a: Arc<dyn PhysicalExpr>, b: Arc<dyn PhysicalExpr>) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(a, Operator::And, b))
    }

    #[derive(Debug)]
    struct TestExec {
        schema: SchemaRef,
        /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
        cache: PlanProperties,
    }

    impl TestExec {
        fn new(schema: SchemaRef) -> Self {
            Self::new_with_partitions(schema, 1)
        }

        fn new_with_partitions(schema: SchemaRef, partitions: usize) -> Self {
            let cache = Self::compute_properties(Arc::clone(&schema), partitions);
            Self { schema, cache }
        }

        /// This function creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
        fn compute_properties(schema: SchemaRef, partitions: usize) -> PlanProperties {
            let eq_properties = EquivalenceProperties::new(schema);

            let output_partitioning =
                datafusion::physical_plan::Partitioning::UnknownPartitioning(partitions);

            PlanProperties::new(
                eq_properties,
                output_partitioning,
                EmissionType::Incremental,
                Boundedness::Bounded,
            )
        }
    }

    impl ExecutionPlan for TestExec {
        fn name(&self) -> &str {
            Self::static_name()
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn properties(&self) -> &PlanProperties {
            &self.cache
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
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

        fn statistics(&self) -> Result<datafusion::physical_plan::Statistics, DataFusionError> {
            Ok(datafusion::physical_plan::Statistics::new_unknown(
                &self.schema(),
            ))
        }
    }

    impl DisplayAs for TestExec {
        fn fmt_as(
            &self,
            _t: datafusion::physical_plan::DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "Test")
        }
    }
}
