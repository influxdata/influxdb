use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter},
    config::ConfigOptions,
    datasource::{
        physical_plan::{FileGroup, FileScanConfig},
        source::DataSourceExec,
    },
    error::Result,
    physical_expr::LexOrdering,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{ExecutionPlan, sorts::sort::SortExec},
};
use tracing::warn;

use crate::config::IoxConfigExt;

/// Trade wider fan-out of not having to sort parquet files.
///
/// This will fan-out [`DataSourceExec`] nodes beyond [`target_partitions`] if it is under a node that desires sorting, e.g.:
///
/// - [`SortExec`] itself
/// - any other node that requires sorting, e.g. [`DeduplicateExec`]
///
/// [`DeduplicateExec`]: crate::provider::DeduplicateExec
/// [`target_partitions`]: datafusion::common::config::ExecutionOptions::target_partitions
#[derive(Debug, Default)]
pub struct ParquetSortness;

impl PhysicalOptimizerRule for ParquetSortness {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|plan| {
            let Some(children_with_sort) = detect_children_with_desired_ordering(plan.as_ref())
            else {
                return Ok(Transformed::no(plan));
            };
            let mut children_new = Vec::with_capacity(children_with_sort.len());
            for (child, desired_ordering) in children_with_sort {
                let mut rewriter = ParquetSortnessRewriter {
                    config,
                    desired_ordering: &desired_ordering,
                };
                let child = Arc::clone(&child).rewrite(&mut rewriter)?;
                children_new.push(child.data);
            }

            Ok(Transformed::yes(plan.with_new_children(children_new)?))
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "parquet_sortness"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

type ChildWithSorting = (Arc<dyn ExecutionPlan>, LexOrdering);

fn detect_children_with_desired_ordering(
    plan: &dyn ExecutionPlan,
) -> Option<Vec<ChildWithSorting>> {
    if let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() {
        return Some(vec![(
            Arc::clone(sort_exec.input()),
            sort_exec.expr().clone(),
        )]);
    }

    let required_input_ordering = plan.required_input_ordering();
    if !required_input_ordering.iter().all(|expr| expr.is_some()) {
        // not all inputs require sorting, ignore it
        return None;
    }

    let children = plan.children();
    if children.len() != required_input_ordering.len() {
        // this should normally not happen, but we ignore it
        return None;
    }
    if children.is_empty() {
        // leaf node
        return None;
    }

    Some(
        children
            .into_iter()
            .zip(
                required_input_ordering
                    .into_iter()
                    .map(|requirement| requirement.expect("just checked"))
                    .map(|requirement| requirement.into_single().into()),
            )
            .map(|(arc, sort_exprs)| (Arc::clone(arc), sort_exprs))
            .collect(),
    )
}

#[derive(Debug)]
struct ParquetSortnessRewriter<'a> {
    config: &'a ConfigOptions,
    desired_ordering: &'a LexOrdering,
}

impl TreeNodeRewriter for ParquetSortnessRewriter<'_> {
    type Node = Arc<dyn ExecutionPlan>;

    fn f_down(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        if detect_children_with_desired_ordering(node.as_ref()).is_some() {
            // another sort or sort-desiring node
            Ok(Transformed::new(node, false, TreeNodeRecursion::Jump))
        } else {
            Ok(Transformed::no(node))
        }
    }

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        let Some(data_source_exec) = node.as_any().downcast_ref::<DataSourceExec>() else {
            // not a `DataSourceExec`
            return Ok(Transformed::no(node));
        };
        let Some(file_scan_config) = data_source_exec
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
        else {
            // not a `DataSourceExec`
            return Ok(Transformed::no(node));
        };

        if file_scan_config.output_ordering.is_empty() {
            // no output ordering requested
            return Ok(Transformed::no(node));
        }

        if file_scan_config.file_groups.iter().all(|g| g.len() < 2) {
            // already flat
            return Ok(Transformed::no(node));
        }

        // Protect against degenerative plans
        let n_files = file_scan_config
            .file_groups
            .iter()
            .map(|g| g.len())
            .sum::<usize>();
        let max_parquet_fanout = self
            .config
            .extensions
            .get::<IoxConfigExt>()
            .cloned()
            .unwrap_or_default()
            .max_parquet_fanout;
        if n_files > max_parquet_fanout {
            warn!(
                n_files,
                max_parquet_fanout, "cannot use pre-sorted parquet files, fan-out too wide"
            );
            return Ok(Transformed::no(node));
        }

        let file_scan_config = FileScanConfig {
            file_groups: file_scan_config
                .file_groups
                .iter()
                .flat_map(|g| g.iter())
                .map(|f| FileGroup::new(vec![f.clone()]))
                .collect(),
            ..file_scan_config.clone()
        };
        let new_data_source_exec = DataSourceExec::from_data_source(file_scan_config);

        // did this help?
        if new_data_source_exec.properties().output_ordering() == Some(self.desired_ordering) {
            Ok(Transformed::yes(new_data_source_exec))
        } else {
            Ok(Transformed::no(node))
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
    use datafusion::{
        common::{ColumnStatistics, Statistics, stats::Precision},
        datasource::{
            listing::PartitionedFile,
            object_store::ObjectStoreUrl,
            physical_plan::{FileScanConfigBuilder, ParquetSource},
        },
        physical_expr::PhysicalSortExpr,
        physical_plan::{
            expressions::Column, placeholder_row::PlaceholderRowExec, sorts::sort::SortExec,
            union::UnionExec,
        },
        scalar::ScalarValue,
    };
    use datafusion_util::config::table_parquet_options;

    use crate::{
        CHUNK_ORDER_COLUMN_NAME, chunk_order_field,
        physical_optimizer::test_util::{OptimizationTest, assert_unknown_partitioning},
        provider::{DeduplicateExec, RecordBatchesExec},
    };

    use super::*;

    #[test]
    fn test_happy_path_sort() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![FileGroup::new(vec![file(1), file(2)])])
        .with_table_partition_cols(vec![])
        .with_output_ordering(vec![ordering(["col2", "col1"], &schema)])
        .build();
        let plan = Arc::new(
            SortExec::new(
                ordering(["col2", "col1"], &schema),
                DataSourceExec::from_data_source(file_scan_config),
            )
            .with_fetch(Some(42)),
        );
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false], sort_prefix=[col2@1 ASC, col1@0 ASC]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        output:
          Ok:
            - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false], sort_prefix=[col2@1 ASC, col1@0 ASC]"
            - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_happy_path_dedup() {
        let schema = schema_with_chunk_order();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![FileGroup::new(vec![file(1), file(2)])])
        .with_output_ordering(vec![ordering(
            ["col2", "col1", CHUNK_ORDER_COLUMN_NAME],
            &schema,
        )])
        .build();
        let plan = Arc::new(DeduplicateExec::new(
            DataSourceExec::from_data_source(file_scan_config),
            ordering(["col2", "col1"], &schema),
            true,
        ));
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, __chunk_order@3 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC]"
            - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, col3, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, __chunk_order@3 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_sort_partitioning() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![
            FileGroup::new(vec![file(1), file(2)]),
            FileGroup::new(vec![file(3)]),
        ])
        .with_output_ordering(vec![ordering(["col2", "col1"], &schema)])
        .build();
        let plan = Arc::new(
            SortExec::new(
                ordering(["col2", "col1"], &schema),
                DataSourceExec::from_data_source(file_scan_config),
            )
            .with_preserve_partitioning(true)
            .with_fetch(Some(42)),
        );

        assert_unknown_partitioning(plan.properties().output_partitioning().clone(), 2);

        let opt = ParquetSortness;
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r#"
        input:
          - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[true], sort_prefix=[col2@1 ASC, col1@0 ASC]"
          - "   DataSourceExec: file_groups={2 groups: [[1.parquet, 2.parquet], [3.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        output:
          Ok:
            - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[true], sort_prefix=[col2@1 ASC, col1@0 ASC]"
            - "   DataSourceExec: file_groups={3 groups: [[1.parquet], [2.parquet], [3.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        "#
        );

        assert_unknown_partitioning(
            test.output_plan()
                .unwrap()
                .properties()
                .output_partitioning()
                .clone(),
            3,
        );
    }

    #[test]
    fn test_parquet_already_flat() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![
            FileGroup::new(vec![file(1)]),
            FileGroup::new(vec![file(2)]),
        ])
        .with_output_ordering(vec![ordering(["col2", "col1"], &schema)])
        .build();

        let plan = Arc::new(
            SortExec::new(
                ordering(["col2", "col1"], &schema),
                DataSourceExec::from_data_source(file_scan_config),
            )
            .with_fetch(Some(42)),
        );
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false], sort_prefix=[col2@1 ASC, col1@0 ASC]"
          - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        output:
          Ok:
            - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false], sort_prefix=[col2@1 ASC, col1@0 ASC]"
            - "   DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_parquet_has_different_ordering() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![FileGroup::new(vec![file(1), file(2)])])
        .with_output_ordering(vec![ordering(["col1", "col2"], &schema)])
        .build();
        let plan = Arc::new(
            SortExec::new(
                ordering(["col2", "col1"], &schema),
                DataSourceExec::from_data_source(file_scan_config),
            )
            .with_fetch(Some(42)),
        );
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col1@0 ASC, col2@1 ASC], file_type=parquet"
        output:
          Ok:
            - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
            - "   DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col1@0 ASC, col2@1 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_parquet_has_no_ordering() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![FileGroup::new(vec![file(1), file(2)])])
        .build();
        let plan = Arc::new(
            SortExec::new(
                ordering(["col2", "col1"], &schema),
                DataSourceExec::from_data_source(file_scan_config),
            )
            .with_fetch(Some(42)),
        );
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], file_type=parquet"
        output:
          Ok:
            - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
            - "   DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_fanout_limit() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![FileGroup::new(vec![file(1), file(2), file(3)])])
        .with_output_ordering(vec![ordering(["col2", "col1"], &schema)])
        .build();
        let plan = Arc::new(
            SortExec::new(
                ordering(["col2", "col1"], &schema),
                DataSourceExec::from_data_source(file_scan_config),
            )
            .with_fetch(Some(42)),
        );
        let opt = ParquetSortness;
        let mut config = ConfigOptions::default();
        config.extensions.insert(IoxConfigExt {
            max_parquet_fanout: 2,
            ..Default::default()
        });
        insta::assert_yaml_snapshot!(
            OptimizationTest::new_with_config(plan, opt, &config),
            @r#"
        input:
          - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false], sort_prefix=[col2@1 ASC, col1@0 ASC]"
          - "   DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet, 3.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        output:
          Ok:
            - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false], sort_prefix=[col2@1 ASC, col1@0 ASC]"
            - "   DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet, 3.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_other_node() {
        let schema = schema();
        let inner = PlaceholderRowExec::new(Arc::clone(&schema));
        let plan = Arc::new(
            SortExec::new(ordering(["col2", "col1"], &schema), Arc::new(inner))
                .with_fetch(Some(42)),
        );
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
          - "   PlaceholderRowExec"
        output:
          Ok:
            - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
            - "   PlaceholderRowExec"
        "#
        );
    }

    #[test]
    fn test_does_not_touch_freestanding_data_source_exec() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![FileGroup::new(vec![file(1), file(2)])])
        .with_output_ordering(vec![ordering(["col2", "col1"], &schema)])
        .build();
        let plan = DataSourceExec::from_data_source(file_scan_config);
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        output:
          Ok:
            - " DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col2@1 ASC, col1@0 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_ignore_outer_sort_if_inner_preform_resort() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![FileGroup::new(vec![file(1), file(2)])])
        .with_output_ordering(vec![ordering(["col1", "col2"], &schema)])
        .build();
        let plan = DataSourceExec::from_data_source(file_scan_config);
        let plan =
            Arc::new(SortExec::new(ordering(["col2", "col1"], &schema), plan).with_fetch(Some(42)));
        let plan =
            Arc::new(SortExec::new(ordering(["col1", "col2"], &schema), plan).with_fetch(Some(42)));
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " SortExec: TopK(fetch=42), expr=[col1@0 ASC, col2@1 ASC], preserve_partitioning=[false]"
          - "   SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
          - "     DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col1@0 ASC, col2@1 ASC], file_type=parquet"
        output:
          Ok:
            - " SortExec: TopK(fetch=42), expr=[col1@0 ASC, col2@1 ASC], preserve_partitioning=[false]"
            - "   SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
            - "     DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col1@0 ASC, col2@1 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_honor_inner_sort_even_if_outer_preform_resort() {
        let schema = schema();
        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![FileGroup::new(vec![file(1), file(2)])])
        .with_output_ordering(vec![ordering(["col1", "col2"], &schema)])
        .build();
        let plan = DataSourceExec::from_data_source(file_scan_config);
        let plan =
            Arc::new(SortExec::new(ordering(["col1", "col2"], &schema), plan).with_fetch(Some(42)));
        let plan =
            Arc::new(SortExec::new(ordering(["col2", "col1"], &schema), plan).with_fetch(Some(42)));
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
          - "   SortExec: TopK(fetch=42), expr=[col1@0 ASC, col2@1 ASC], preserve_partitioning=[false], sort_prefix=[col1@0 ASC, col2@1 ASC]"
          - "     DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col1@0 ASC, col2@1 ASC], file_type=parquet"
        output:
          Ok:
            - " SortExec: TopK(fetch=42), expr=[col2@1 ASC, col1@0 ASC], preserve_partitioning=[false]"
            - "   SortExec: TopK(fetch=42), expr=[col1@0 ASC, col2@1 ASC], preserve_partitioning=[false], sort_prefix=[col1@0 ASC, col2@1 ASC]"
            - "     DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, col3], output_ordering=[col1@0 ASC, col2@1 ASC], file_type=parquet"
        "#
        );
    }

    #[test]
    fn test_issue_idpe_17556() {
        let schema = schema_with_chunk_order();

        let file_scan_config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("test://").unwrap(),
            Arc::clone(&schema),
            Arc::new(ParquetSource::new(table_parquet_options())),
        )
        .with_file_groups(vec![FileGroup::new(vec![file(1), file(2)])])
        .with_output_ordering(vec![ordering(
            ["col2", "col1", CHUNK_ORDER_COLUMN_NAME],
            &schema,
        )])
        .build();
        let plan_parquet = DataSourceExec::from_data_source(file_scan_config);
        let plan_batches = Arc::new(RecordBatchesExec::new(vec![], Arc::clone(&schema), None));

        let plan = Arc::new(UnionExec::new(vec![plan_batches, plan_parquet]));
        let plan = Arc::new(DeduplicateExec::new(
            plan,
            ordering(["col2", "col1"], &schema),
            true,
        ));
        let opt = ParquetSortness;
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r#"
        input:
          - " DeduplicateExec: [col2@1 ASC,col1@0 ASC]"
          - "   UnionExec"
          - "     RecordBatchesExec: chunks=0, projection=[col1, col2, col3, __chunk_order]"
          - "     DataSourceExec: file_groups={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, __chunk_order@3 ASC], file_type=parquet"
        output:
          Ok:
            - " DeduplicateExec: [col2@1 ASC,col1@0 ASC]"
            - "   UnionExec"
            - "     RecordBatchesExec: chunks=0, projection=[col1, col2, col3, __chunk_order]"
            - "     DataSourceExec: file_groups={2 groups: [[1.parquet], [2.parquet]]}, projection=[col1, col2, col3, __chunk_order], output_ordering=[col2@1 ASC, col1@0 ASC, __chunk_order@3 ASC], file_type=parquet"
        "#
        );
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Int64, false),
            Field::new("col3", DataType::Int64, false),
        ]))
    }

    fn schema_with_chunk_order() -> SchemaRef {
        Arc::new(Schema::new(
            schema()
                .fields()
                .iter()
                .cloned()
                .chain(std::iter::once(chunk_order_field()))
                .collect::<Fields>(),
        ))
    }

    fn file(n: i64) -> PartitionedFile {
        let mut stats = Statistics::default();

        // for all columns including chunk order col
        for _ in 0..(schema().fields().len() + 1) {
            stats = stats.add_column_statistics(
                ColumnStatistics::default()
                    .with_min_value(Precision::Exact(ScalarValue::from(n)))
                    .with_max_value(Precision::Exact(ScalarValue::from(n)))
                    .with_null_count(Precision::Exact(0)),
            );
        }

        PartitionedFile::new(format!("{n}.parquet"), 0).with_statistics(Arc::new(stats))
    }

    fn ordering<const N: usize>(cols: [&str; N], schema: &SchemaRef) -> LexOrdering {
        LexOrdering::new(cols.into_iter().map(|col| PhysicalSortExpr {
            expr: Arc::new(Column::new_with_schema(col, schema.as_ref()).unwrap()),
            options: Default::default(),
        }))
        .unwrap()
    }
}
