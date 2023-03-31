use std::sync::Arc;

use datafusion::{
    common::tree_node::{Transformed, TreeNode},
    config::ConfigOptions,
    error::Result,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        file_format::{FileScanConfig, ParquetExec},
        sorts::sort::SortExec,
        ExecutionPlan,
    },
};
use observability_deps::tracing::warn;

use crate::config::IoxConfigExt;

/// Trade wider fan-out of not having to sort parquet files.
///
/// This will fan-out [`ParquetExec`] nodes beyond [`target_partitions`] if it is under a [`SortExec`]. You should
/// likely run [`RedundantSort`] afterwards to eliminate the [`SortExec`].
///
///
/// [`RedundantSort`]: super::redundant_sort::RedundantSort
/// [`target_partitions`]: datafusion::common::config::ExecutionOptions::target_partitions
#[derive(Debug, Default)]
pub struct ParquetSortness;

impl PhysicalOptimizerRule for ParquetSortness {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(&|plan| {
            let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() else {
                return Ok(Transformed::No(plan));
            };

            let transformed_child = Arc::clone(sort_exec.input()).transform_down(&|plan| {
                let Some(parquet_exec) = plan.as_any().downcast_ref::<ParquetExec>() else {
                    return Ok(Transformed::No(plan));
                };

                let base_config = parquet_exec.base_config();
                if base_config.output_ordering.is_none() {
                    // no output ordering requested
                    return Ok(Transformed::No(plan));
                }

                if base_config.file_groups.iter().all(|g| g.len() < 2) {
                    // already flat
                    return Ok(Transformed::No(plan));
                }

                // Protect against degenerative plans
                let n_files = base_config.file_groups.iter().map(Vec::len).sum::<usize>();
                let max_parquet_fanout = config
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
                    return Ok(Transformed::No(plan));
                }

                let base_config = FileScanConfig {
                    file_groups: base_config
                        .file_groups
                        .iter()
                        .flat_map(|g| g.iter())
                        .map(|f| vec![f.clone()])
                        .collect(),
                    ..base_config.clone()
                };
                let new_parquet_exec =
                    ParquetExec::new(base_config, parquet_exec.predicate().cloned(), None);
                Ok(Transformed::Yes(Arc::new(new_parquet_exec)))
            })?;

            if transformed_child.output_ordering() == Some(sort_exec.expr()) {
                Ok(Transformed::Yes(
                    plan.with_new_children(vec![transformed_child])?,
                ))
            } else {
                Ok(Transformed::No(plan))
            }
        })
    }

    fn name(&self) -> &str {
        "parquet_sortness"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::{
        datasource::{listing::PartitionedFile, object_store::ObjectStoreUrl},
        physical_expr::PhysicalSortExpr,
        physical_plan::{empty::EmptyExec, expressions::Column, Statistics},
    };
    use object_store::{path::Path, ObjectMeta};

    use crate::physical_optimizer::test_util::{assert_unknown_partitioning, OptimizationTest};

    use super::*;

    #[test]
    fn test_happy_path() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::try_new(
                ordering(["col2", "col1"], &schema),
                Arc::new(inner),
                Some(42),
            )
            .unwrap(),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={2 groups: [[1.parquet], [2.parquet]]}, output_ordering=[col2@1 ASC, col1@0 ASC], projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_sort_partitioning() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)], vec![file(3)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(SortExec::new_with_partitioning(
            ordering(["col2", "col1"], &schema),
            Arc::new(inner),
            true,
            Some(42),
        ));

        assert_unknown_partitioning(plan.output_partitioning(), 2);

        let opt = ParquetSortness::default();
        let test = OptimizationTest::new(plan, opt);
        insta::assert_yaml_snapshot!(
            test,
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={2 groups: [[1.parquet, 2.parquet], [3.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={3 groups: [[1.parquet], [2.parquet], [3.parquet]]}, output_ordering=[col2@1 ASC, col1@0 ASC], projection=[col1, col2, col3]"
        "###
        );

        assert_unknown_partitioning(test.output_plan().unwrap().output_partitioning(), 3);
    }

    #[test]
    fn test_parquet_already_flat() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1)], vec![file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::try_new(
                ordering(["col2", "col1"], &schema),
                Arc::new(inner),
                Some(42),
            )
            .unwrap(),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={2 groups: [[1.parquet], [2.parquet]]}, output_ordering=[col2@1 ASC, col1@0 ASC], projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={2 groups: [[1.parquet], [2.parquet]]}, output_ordering=[col2@1 ASC, col1@0 ASC], projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_parquet_has_different_ordering() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col1", "col2"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::try_new(
                ordering(["col2", "col1"], &schema),
                Arc::new(inner),
                Some(42),
            )
            .unwrap(),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_parquet_has_no_ordering() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: None,
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::try_new(
                ordering(["col2", "col1"], &schema),
                Arc::new(inner),
                Some(42),
            )
            .unwrap(),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_fanout_limit() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2), file(3)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let inner = ParquetExec::new(base_config, None, None);
        let plan = Arc::new(
            SortExec::try_new(
                ordering(["col2", "col1"], &schema),
                Arc::new(inner),
                Some(42),
            )
            .unwrap(),
        );
        let opt = ParquetSortness::default();
        let mut config = ConfigOptions::default();
        config.extensions.insert(IoxConfigExt {
            max_parquet_fanout: 2,
            ..Default::default()
        });
        insta::assert_yaml_snapshot!(
            OptimizationTest::new_with_config(plan, opt, &config),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet, 3.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet, 3.parquet]]}, projection=[col1, col2, col3]"
        "###
        );
    }

    #[test]
    fn test_other_node() {
        let schema = schema();
        let inner = EmptyExec::new(true, Arc::clone(&schema));
        let plan = Arc::new(
            SortExec::try_new(
                ordering(["col2", "col1"], &schema),
                Arc::new(inner),
                Some(42),
            )
            .unwrap(),
        );
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
          - "   EmptyExec: produce_one_row=true"
        output:
          Ok:
            - " SortExec: fetch=42, expr=[col2@1 ASC,col1@0 ASC]"
            - "   EmptyExec: produce_one_row=true"
        "###
        );
    }

    #[test]
    fn test_does_not_touch_freestanding_parquet_exec() {
        let schema = schema();
        let base_config = FileScanConfig {
            object_store_url: ObjectStoreUrl::parse("test://").unwrap(),
            file_schema: Arc::clone(&schema),
            file_groups: vec![vec![file(1), file(2)]],
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: Some(ordering(["col2", "col1"], &schema)),
            infinite_source: false,
        };
        let plan = Arc::new(ParquetExec::new(base_config, None, None));
        let opt = ParquetSortness::default();
        insta::assert_yaml_snapshot!(
            OptimizationTest::new(plan, opt),
            @r###"
        ---
        input:
          - " ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        output:
          Ok:
            - " ParquetExec: limit=None, partitions={1 group: [[1.parquet, 2.parquet]]}, projection=[col1, col2, col3]"
        "###
        );
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Utf8, true),
            Field::new("col2", DataType::Utf8, true),
            Field::new("col3", DataType::Utf8, true),
        ]))
    }

    fn file(n: u128) -> PartitionedFile {
        PartitionedFile {
            object_meta: ObjectMeta {
                location: Path::parse(format!("{n}.parquet")).unwrap(),
                last_modified: Default::default(),
                size: 0,
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
