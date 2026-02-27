use std::sync::Arc;

use datafusion::{
    common::{DFSchemaRef, Result, tree_node::Transformed},
    logical_expr::{
        Expr, Extension, LogicalPlan, LogicalPlanBuilder, SortExpr, UserDefinedLogicalNode, lit,
    },
};

use crate::exec::{
    gapfill::GapFill,
    series_limit::{LimitExpr, SeriesLimit},
};

/// Extension trait for [`LogicalPlanBuilder`] that adds IOx-specific query plan operations.
///
/// This trait provides methods to construct logical plan nodes for IOx-specific operations
/// that extend DataFusion's standard query planning capabilities.
pub trait LogicalPlanBuilderExt: Sized {
    /// Adds a series limit operation to the logical plan.
    ///
    /// Series limiting applies LIMIT/OFFSET semantics per-series rather than globally,
    /// where a series is defined by a unique combination of the grouping expressions.
    /// This is commonly used in time series queries to limit the number of points
    /// returned per series.
    ///
    /// # Arguments
    ///
    /// * `series_expr` - Expressions that define series grouping (typically tag columns)
    /// * `order_expr` - Sort expressions that determine ordering within each series
    /// * `limit_expr` - Additional limit expressions for the operation
    /// * `skip` - Number of rows to skip at the beginning of each series (OFFSET)
    /// * `fetch` - Maximum number of rows to return from each series (LIMIT)
    ///
    /// # Returns
    ///
    /// A new [`LogicalPlanBuilder`] with the series limit operation applied.
    ///
    /// # Errors
    ///
    /// Returns an error if the series limit node cannot be constructed or if the
    /// underlying plan cannot be built.
    fn series_limit(
        self,
        series_expr: impl IntoIterator<Item = impl Into<Expr>>,
        order_expr: impl IntoIterator<Item = impl Into<SortExpr>>,
        limit_expr: impl IntoIterator<Item = impl Into<LimitExpr>>,
        skip: usize,
        fetch: Option<usize>,
    ) -> Result<Self>;
}

impl LogicalPlanBuilderExt for LogicalPlanBuilder {
    fn series_limit(
        self,
        series_expr: impl IntoIterator<Item = impl Into<Expr>>,
        order_expr: impl IntoIterator<Item = impl Into<SortExpr>>,
        limit_expr: impl IntoIterator<Item = impl Into<LimitExpr>>,
        skip: usize,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let input = Arc::new(self.build()?);
        let series_expr: Vec<Expr> = series_expr.into_iter().map(|e| e.into()).collect();
        let order_expr: Vec<SortExpr> = order_expr.into_iter().map(|e| e.into()).collect();
        let limit_expr: Vec<LimitExpr> = limit_expr.into_iter().map(|e| e.into()).collect();
        let skip_expr = if skip == 0 {
            None
        } else {
            Some(Box::new(lit(skip as u64)))
        };
        let fetch_expr = fetch.map(|f| Box::new(lit(f as u64)));
        let node = SeriesLimit::try_new(
            input,
            series_expr,
            order_expr,
            limit_expr,
            skip_expr,
            fetch_expr,
        )?;

        Ok(Self::new(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        })))
    }
}

/// Transform the schema of a single [`LogicalPlan`] node by applying a function to its schema.
///
/// This function is a wrapper around [`datafusion_util::transform_plan_schema`] that provides
/// support for IOx-specific extension nodes ([`GapFill`] and [`SeriesLimit`]). It applies the
/// transformation function `f` to the schema field of the given plan node, without recursing
/// into child nodes. It returns a [`Transformed`] result indicating whether the plan was
/// modified and how tree traversal should proceed when used with DataFusion's tree traversal APIs.
///
/// # Parameters
///
/// * `plan` - The logical plan node to transform
/// * `f` - A mutable closure that transforms a [`DFSchemaRef`] and returns a [`Transformed`]
///   result indicating whether the schema was modified
///
/// # Returns
///
/// Returns a [`Transformed<LogicalPlan>`] that contains:
/// - The plan node with its schema potentially modified
/// - A boolean indicating whether any transformation occurred
/// - A [`TreeNodeRecursion`](datafusion::common::tree_node::TreeNodeRecursion)
///   value controlling tree traversal (typically `Continue`, or `Stop` for extension nodes
///   to avoid recursing into their internals)
///
/// # Supported Plan Nodes
///
/// This function handles schema transformation for all standard DataFusion plan node types
/// (see [`datafusion_util::transform_plan_schema`]) plus the following IOx-specific extension nodes:
/// - [`GapFill`] - Time series gap-filling operator
/// - [`SeriesLimit`] - Per-series LIMIT/OFFSET operator
///
/// Plan nodes without schemas (`Filter`, `Sort`, `Limit`, etc.) return unchanged with
/// `Transformed::no(plan)`.
///
/// Extension nodes that are not [`GapFill`] or [`SeriesLimit`] will return unchanged.
///
/// # Errors
///
/// Returns an error if the transformation function `f` returns an error.
///
/// # Example
///
/// ```ignore
/// use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion, Transformed};
/// use iox_query::plan::transform_plan_schema;
///
/// // Transform the schema to add metadata
/// let transformed_plan = plan.transform_down(|plan| {
///     transform_plan_schema(plan, |schema| {
///         let mut metadata = schema.metadata().clone();
///         metadata.insert("source".to_string(), "iox".to_string());
///         let new_schema = DFSchema::new_with_metadata(
///             schema.iter().map(|(q, f)| (q.cloned(), Arc::clone(f))).collect(),
///             metadata
///         )?;
///         Ok(Transformed::new(Arc::new(new_schema), true, TreeNodeRecursion::Stop))
///     })
/// })?;
/// ```
///
/// # Note
///
/// This function makes no attempt to validate the correctness of the modified schema.
/// The intended use-case is for modifying schema metadata. Changes to the schema
/// information used by DataFusion (field names, types, qualifiers) may result in
/// incorrect query plans or runtime errors.
pub fn transform_plan_schema<F>(plan: LogicalPlan, f: F) -> Result<Transformed<LogicalPlan>>
where
    F: FnMut(DFSchemaRef) -> Result<Transformed<DFSchemaRef>>,
{
    datafusion_util::transform_plan_schema(
        plan,
        |node, f| {
            if let Some(gapfill) = node.as_any().downcast_ref::<GapFill>() {
                f(Arc::clone(&gapfill.schema))?.map_data(|schema| {
                    Ok(Arc::new(GapFill {
                        schema,
                        ..gapfill.clone()
                    }) as Arc<dyn UserDefinedLogicalNode>)
                })
            } else if let Some(series_limit) = node.as_any().downcast_ref::<SeriesLimit>() {
                f(Arc::clone(&series_limit.schema))?.map_data(|schema| {
                    Ok(Arc::new(SeriesLimit {
                        schema,
                        ..series_limit.clone()
                    }) as Arc<dyn UserDefinedLogicalNode>)
                })
            } else {
                Ok(Transformed::no(node))
            }
        },
        f,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::{
        common::{
            DFSchema,
            tree_node::{TreeNode, TreeNodeRecursion},
        },
        logical_expr::{LogicalPlanBuilder, LogicalTableSource, col},
        scalar::ScalarValue,
        sql::sqlparser::ast::NullTreatment,
    };
    use std::ops::Bound;

    /// Helper function to create a simple test input.
    fn create_test_source() -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("tag1", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
        ]));
        LogicalPlanBuilder::scan("test", Arc::new(LogicalTableSource::new(schema)), None)
            .unwrap()
            .build()
            .unwrap()
    }

    /// Helper function to create a modified schema with different metadata
    fn add_metadata_to_schema(schema: DFSchemaRef, key: &str, value: &str) -> Result<DFSchemaRef> {
        let mut metadata = schema.metadata().clone();
        metadata.insert(key.to_string(), value.to_string());
        Ok(Arc::new(DFSchema::new_with_metadata(
            schema
                .iter()
                .map(|(q, f)| (q.cloned(), Arc::clone(f)))
                .collect(),
            metadata,
        )?))
    }

    #[test]
    fn test_transform_gapfill_schema() {
        let input = create_test_source();
        let schema = input.schema().as_ref().clone();

        // Create a GapFill extension node with bounded time range
        let gapfill = Arc::new(GapFill {
            input: Arc::new(input),
            series_expr: vec![col("tag1")],
            time_expr: col("time"),
            fill_expr: vec![],
            time_range: Bound::Included(lit(0_i64))..Bound::Excluded(lit(1000_i64)),
            schema: Arc::new(schema),
        });

        let extension = LogicalPlan::Extension(Extension { node: gapfill });

        // Apply transformation
        let Transformed {
            data, transformed, ..
        } = extension
            .transform_down(|plan| {
                transform_plan_schema(plan, |s| {
                    let new_schema = add_metadata_to_schema(s, "transformed", "true")?;
                    Ok(Transformed::new(new_schema, true, TreeNodeRecursion::Stop))
                })
            })
            .unwrap();

        assert!(transformed);
        assert_eq!(
            data.schema().as_arrow().metadata().get("transformed"),
            Some(&"true".to_string())
        );

        // Verify it's still a GapFill extension
        match data {
            LogicalPlan::Extension(Extension { node }) => {
                assert!(node.as_any().is::<GapFill>());
            }
            _ => panic!("Expected Extension node"),
        }
    }

    #[test]
    fn test_transform_series_limit_schema() {
        let input = create_test_source();

        // Create a SeriesLimit using the builder extension
        let extension = LogicalPlanBuilder::from(input)
            .series_limit(
                vec![col("tag1")],
                Vec::<SortExpr>::new(),
                vec![LimitExpr {
                    expr: col("value"),
                    null_treatment: NullTreatment::RespectNulls,
                    default_value: lit(ScalarValue::Float64(None)),
                }],
                0,
                Some(10),
            )
            .unwrap()
            .build()
            .unwrap();

        // Apply transformation
        let Transformed {
            data, transformed, ..
        } = extension
            .transform_down(|plan| {
                transform_plan_schema(plan, |s| {
                    let new_schema = add_metadata_to_schema(s, "transformed", "true")?;
                    Ok(Transformed::new(new_schema, true, TreeNodeRecursion::Stop))
                })
            })
            .unwrap();

        assert!(transformed);
        assert_eq!(
            data.schema().as_arrow().metadata().get("transformed"),
            Some(&"true".to_string())
        );

        // Verify it's still a SeriesLimit extension
        match data {
            LogicalPlan::Extension(Extension { node }) => {
                assert!(node.as_any().is::<SeriesLimit>());
            }
            _ => panic!("Expected Extension node"),
        }
    }

    #[test]
    fn test_transform_no_change_series_limit() {
        use datafusion::sql::sqlparser::ast::NullTreatment;

        let input = create_test_source();

        // Create a SeriesLimit using the builder extension
        let extension = LogicalPlanBuilder::from(input)
            .series_limit(
                vec![col("tag1")],
                Vec::<SortExpr>::new(),
                vec![LimitExpr {
                    expr: col("value"),
                    null_treatment: NullTreatment::RespectNulls,
                    default_value: lit(ScalarValue::Float64(None)),
                }],
                0,
                None,
            )
            .unwrap()
            .build()
            .unwrap();

        // Apply transformation that doesn't change the schema
        let Transformed {
            data, transformed, ..
        } = extension
            .transform_down(|plan| transform_plan_schema(plan, |s| Ok(Transformed::no(s))))
            .unwrap();

        assert!(!transformed);

        // Verify it's still a SeriesLimit extension
        match data {
            LogicalPlan::Extension(Extension { node }) => {
                assert!(node.as_any().is::<SeriesLimit>());
            }
            _ => panic!("Expected Extension node"),
        }
    }

    #[test]
    fn test_transform_nested_plan_with_gapfill() {
        let input = create_test_source();
        let schema = input.schema().as_ref().clone();

        // Create a GapFill extension node with bounded time range
        let gapfill = Arc::new(GapFill {
            input: Arc::new(input),
            series_expr: vec![col("tag1")],
            time_expr: col("time"),
            fill_expr: vec![],
            time_range: Bound::Included(lit(0_i64))..Bound::Excluded(lit(1000_i64)),
            schema: Arc::new(schema),
        });

        let extension = LogicalPlan::Extension(Extension { node: gapfill });

        // Wrap in a filter (which has no schema)
        let plan = LogicalPlanBuilder::from(extension)
            .filter(lit(true))
            .unwrap()
            .build()
            .unwrap();

        // Apply transformation - should transform the nested GapFill
        let Transformed {
            data, transformed, ..
        } = plan
            .transform_down(|plan| {
                transform_plan_schema(plan, |s| {
                    let new_schema = add_metadata_to_schema(s, "nested", "true")?;
                    Ok(Transformed::new(new_schema, true, TreeNodeRecursion::Stop))
                })
            })
            .unwrap();

        assert!(transformed);

        // The filter's child should have the transformed schema
        match data {
            LogicalPlan::Filter(filter) => {
                assert_eq!(
                    filter.input.schema().as_arrow().metadata().get("nested"),
                    Some(&"true".to_string())
                );
            }
            _ => panic!("Expected Filter node"),
        }
    }
}
