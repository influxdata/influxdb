use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource},
        source::DataSourceExec,
    },
    error::Result,
    logical_expr::Operator,
    physical_expr::{split_conjunction, utils::collect_columns},
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ExecutionPlan, PhysicalExpr, empty::EmptyExec, expressions::BinaryExpr, filter::FilterExec,
        union::UnionExec,
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
        plan.transform_down(|plan| {
            let plan_any = plan.as_any();

            if let Some(filter_exec) = plan_any.downcast_ref::<FilterExec>() {
                let mut children = filter_exec.children();
                assert_eq!(children.len(), 1);
                let child = children.remove(0);

                let child_any = child.as_any();
                if child_any.downcast_ref::<EmptyExec>().is_some() {
                    return Ok(Transformed::yes(Arc::clone(child)));
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
                    return Ok(Transformed::yes(Arc::new(new_union)));
                } else if let Some(child_parquet) = child_any.downcast_ref::<DataSourceExec>() {
                    let Some(file_scan_config) = child_parquet
                        .data_source()
                        .as_any()
                        .downcast_ref::<FileScanConfig>()
                    else {
                        return Ok(Transformed::no(plan));
                    };
                    let Some(parquet_source) = file_scan_config
                        .file_source()
                        .as_any()
                        .downcast_ref::<ParquetSource>()
                    else {
                        return Ok(Transformed::no(plan));
                    };
                    let mut builder = parquet_source.clone();
                    let existing = parquet_source
                        .predicate()
                        .map(split_conjunction)
                        .unwrap_or_default();
                    let both = conjunction(
                        existing
                            .into_iter()
                            .chain(split_conjunction(filter_exec.predicate()))
                            .cloned(),
                    );
                    if let Some(predicate) = both {
                        builder = builder.with_predicate(predicate);
                    }

                    let mut new_node: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
                        FileScanConfigBuilder::from(file_scan_config.clone())
                            .with_source(Arc::new(builder))
                            .build(),
                    );
                    if !parquet_source
                        .table_parquet_options()
                        .global
                        .pushdown_filters
                    {
                        let filter =
                            FilterExec::try_new(Arc::clone(filter_exec.predicate()), new_node)?;
                        new_node = Arc::new(filter);
                    }
                    return Ok(Transformed::yes(new_node));
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
                                Arc::clone(grandchild),
                            )?),
                            child_dedup.sort_keys().clone(),
                            child_dedup.use_chunk_order_col(),
                        ));
                        if !no_pushdown.is_empty() {
                            new_node = Arc::new(FilterExec::try_new(
                                conjunction(no_pushdown).expect("not empty"),
                                new_node,
                            )?);
                        }
                        return Ok(Transformed::yes(new_node));
                    }
                }
            }

            Ok(Transformed::no(plan))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "predicate_pushdown"
    }

    fn schema_check(&self) -> bool {
        true
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
        logical_expr::Operator,
        physical_expr::LexOrdering,
        physical_plan::{
            PhysicalExpr,
            expressions::{BinaryExpr, Column, Literal},
            placeholder_row::PlaceholderRowExec,
        },
        scalar::ScalarValue,
    };
    use datafusion_util::config::table_parquet_options;
    use schema::sort::SortKeyBuilder;

    use crate::{physical_optimizer::test_util::OptimizationTest, util::arrow_sort_key_exprs};

    use super::*;

    #[test]
    fn test_parquet_pushdown_disabled() {
        // basically just the same as the test_parquet but with pushdown disabled to ensure the
        // FilterExec is still preserved if so
        let schema = schema();
        let mut table_opts = table_parquet_options();
        table_opts.global.pushdown_filters = false;
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_opts).with_predicate(predicate_tag(&schema))),
        )
        .build();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_mixed(&schema),
                DataSourceExec::from_data_source(file_scan_config),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " FilterExec: tag1@0 = field@2"
          - "   DataSourceExec: file_groups={0 groups: []}, projection=[tag1, tag2, field], file_type=parquet, predicate=tag1@0 = foo, pruning_predicate=tag1_null_count@2 != row_count@3 AND tag1_min@0 <= foo AND foo <= tag1_max@1, required_guarantees=[tag1 in (foo)]"
        output:
          Ok:
            - " FilterExec: tag1@0 = field@2"
            - "   DataSourceExec: file_groups={0 groups: []}, projection=[tag1, tag2, field], file_type=parquet, predicate=tag1@0 = foo AND tag1@0 = field@2, pruning_predicate=tag1_null_count@2 != row_count@3 AND tag1_min@0 <= foo AND foo <= tag1_max@1, required_guarantees=[tag1 in (foo)]"
        "#
        );
    }

    #[test]
    fn test_empty_no_rows() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(predicate_tag(&schema), Arc::new(EmptyExec::new(schema))).unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " FilterExec: tag1@0 = foo"
          - "   EmptyExec"
        output:
          Ok:
            - " EmptyExec"
        "#
        );
    }

    #[test]
    fn test_empty_with_rows() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_tag(&schema),
                Arc::new(PlaceholderRowExec::new(schema)),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " FilterExec: tag1@0 = foo"
          - "   PlaceholderRowExec"
        output:
          Ok:
            - " FilterExec: tag1@0 = foo"
            - "   PlaceholderRowExec"
        "#
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
                        .map(|_| Arc::new(PlaceholderRowExec::new(Arc::clone(&schema))) as _)
                        .collect(),
                )),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " FilterExec: tag1@0 = foo"
          - "   UnionExec"
          - "     PlaceholderRowExec"
          - "     PlaceholderRowExec"
        output:
          Ok:
            - " UnionExec"
            - "   FilterExec: tag1@0 = foo"
            - "     PlaceholderRowExec"
            - "   FilterExec: tag1@0 = foo"
            - "     PlaceholderRowExec"
        "#
        );
    }

    #[test]
    fn test_union_nested() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_tag(&schema),
                Arc::new(UnionExec::new(vec![
                    Arc::new(UnionExec::new(
                        (0..2)
                            .map(|_| Arc::new(PlaceholderRowExec::new(Arc::clone(&schema))) as _)
                            .collect(),
                    )),
                    Arc::new(PlaceholderRowExec::new(Arc::clone(&schema))),
                ])),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " FilterExec: tag1@0 = foo"
          - "   UnionExec"
          - "     UnionExec"
          - "       PlaceholderRowExec"
          - "       PlaceholderRowExec"
          - "     PlaceholderRowExec"
        output:
          Ok:
            - " UnionExec"
            - "   UnionExec"
            - "     FilterExec: tag1@0 = foo"
            - "       PlaceholderRowExec"
            - "     FilterExec: tag1@0 = foo"
            - "       PlaceholderRowExec"
            - "   FilterExec: tag1@0 = foo"
            - "     PlaceholderRowExec"
        "#
        );
    }

    #[test]
    fn test_parquet() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(
                ParquetSource::new(table_parquet_options()).with_predicate(predicate_tag(&schema)),
            ),
        )
        .build();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_mixed(&schema),
                DataSourceExec::from_data_source(file_scan_config),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " FilterExec: tag1@0 = field@2"
          - "   DataSourceExec: file_groups={0 groups: []}, projection=[tag1, tag2, field], file_type=parquet, predicate=tag1@0 = foo, pruning_predicate=tag1_null_count@2 != row_count@3 AND tag1_min@0 <= foo AND foo <= tag1_max@1, required_guarantees=[tag1 in (foo)]"
        output:
          Ok:
            - " DataSourceExec: file_groups={0 groups: []}, projection=[tag1, tag2, field], file_type=parquet, predicate=tag1@0 = foo AND tag1@0 = field@2, pruning_predicate=tag1_null_count@2 != row_count@3 AND tag1_min@0 <= foo AND foo <= tag1_max@1, required_guarantees=[tag1 in (foo)]"
        "#
        );
    }

    #[test]
    fn test_dedup_no_pushdown() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_field(&schema),
                Arc::new(DeduplicateExec::new(
                    Arc::new(PlaceholderRowExec::new(Arc::clone(&schema))),
                    sort_expr(&schema),
                    false,
                )),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " FilterExec: field@2 = val"
          - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
          - "     PlaceholderRowExec"
        output:
          Ok:
            - " FilterExec: field@2 = val"
            - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
            - "     PlaceholderRowExec"
        "#
        );
    }

    #[test]
    fn test_dedup_all_pushdown() {
        let schema = schema();
        let plan = Arc::new(
            FilterExec::try_new(
                predicate_tag(&schema),
                Arc::new(DeduplicateExec::new(
                    Arc::new(PlaceholderRowExec::new(Arc::clone(&schema))),
                    sort_expr(&schema),
                    false,
                )),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " FilterExec: tag1@0 = foo"
          - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
          - "     PlaceholderRowExec"
        output:
          Ok:
            - " DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
            - "   FilterExec: tag1@0 = foo"
            - "     PlaceholderRowExec"
        "#
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
                    Arc::new(PlaceholderRowExec::new(Arc::clone(&schema))),
                    sort_expr(&schema),
                    false,
                )),
            )
            .unwrap(),
        );
        let opt = PredicatePushdown;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " FilterExec: tag1@0 = foo AND tag1@0 = tag2@1 AND field@2 = val AND tag1@0 = field@2 AND true"
          - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
          - "     PlaceholderRowExec"
        output:
          Ok:
            - " FilterExec: field@2 = val AND tag1@0 = field@2"
            - "   DeduplicateExec: [tag1@0 ASC,tag2@1 ASC]"
            - "     FilterExec: tag1@0 = foo AND tag1@0 = tag2@1 AND true"
            - "       PlaceholderRowExec"
        "#
        );
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("tag1", DataType::Utf8, true),
            Field::new("tag2", DataType::Utf8, true),
            Field::new("field", DataType::UInt8, true),
        ]))
    }

    /// Get the sort order defined for these tests.
    fn sort_expr(schema: &SchemaRef) -> LexOrdering {
        let sort_key = SortKeyBuilder::new()
            .with_col("tag1")
            .with_col("tag2")
            .build();
        arrow_sort_key_exprs(&sort_key, schema).unwrap()
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
