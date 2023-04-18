use crate::plan::error;
use arrow::datatypes::DataType;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::utils::find_column_exprs;
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_util::AsExpr;
use generated_types::influxdata::iox::querier::v1::influx_ql_metadata::TagKeyColumn;
use influxdb_influxql_parser::common::OrderByClause;
use influxdb_influxql_parser::expression::{Expr as IQLExpr, VarRef, VarRefDataType};
use influxdb_influxql_parser::select::Field;
use schema::INFLUXQL_MEASUREMENT_COLUMN_NAME;
use std::collections::HashMap;
use std::ops::Deref;

/// Determines that all [`Expr::Column`] references in `exprs` refer to a
/// column in `columns`.
pub(crate) fn check_exprs_satisfy_columns(columns: &[Expr], exprs: &[Expr]) -> Result<()> {
    if !columns.iter().all(|c| matches!(c, Expr::Column(_))) {
        return error::internal("expected Expr::Column");
    }
    let column_exprs = find_column_exprs(exprs);
    if column_exprs.iter().any(|expr| !columns.contains(expr)) {
        return error::query("mixing aggregate and non-aggregate columns is not supported");
    }
    Ok(())
}

pub(super) fn make_tag_key_column_meta(
    fields: &[Field],
    tag_set: &[&str],
    is_projected: &[bool],
) -> Vec<TagKeyColumn> {
    /// There is always a [INFLUXQL_MEASUREMENT_COLUMN_NAME] and `time` column projected in the LogicalPlan,
    /// therefore the start index is 2 for determining the offsets of the
    /// tag key columns in the column projection list.
    const START_INDEX: usize = 1;

    // Create a map of tag key columns to their respective index in the projection
    let index_map = fields
        .iter()
        .enumerate()
        .filter_map(|(index, f)| match &f.expr {
            IQLExpr::VarRef(VarRef {
                name,
                data_type: Some(VarRefDataType::Tag) | None,
            }) => Some((name.deref().as_str(), index + START_INDEX)),
            _ => None,
        })
        .collect::<HashMap<_, _>>();

    // tag_set was previously sorted, so tag_key_columns will be in the correct order
    tag_set
        .iter()
        .zip(is_projected)
        .map(|(tag_key, is_projected)| TagKeyColumn {
            tag_key: (*tag_key).to_owned(),
            column_index: *index_map.get(*tag_key).unwrap() as _,
            is_projected: *is_projected,
        })
        .collect()
}

/// Create a plan that sorts the input plan.
///
/// The ordering of the results is as follows:
///
/// iox::measurement, [group by tag 0, .., group by tag n], time, [projection tag 0, .., projection tag n]
///
/// ## NOTE
///
/// Sort expressions referring to tag keys are always specified in lexicographically ascending order.
pub(super) fn plan_with_sort(
    plan: LogicalPlan,
    mut sort_exprs: Vec<Expr>,
    sort_by_measurement: bool,
    group_by_tag_set: &[&str],
    projection_tag_set: &[&str],
) -> Result<LogicalPlan> {
    // If there are multiple measurements, we need to sort by the measurement column
    // NOTE: Ideally DataFusion would maintain the order of the UNION ALL, which would eliminate
    //  the need to sort by measurement.
    //  See: https://github.com/influxdata/influxdb_iox/issues/7062
    let mut series_sort = if sort_by_measurement {
        vec![Expr::sort(
            INFLUXQL_MEASUREMENT_COLUMN_NAME.as_expr(),
            true,
            false,
        )]
    } else {
        vec![]
    };

    /// Map the fields to DataFusion [`Expr::Sort`] expressions, excluding those columns that
    /// are [`DataType::Null`]'s, as sorting these column types is not supported and unnecessary.
    fn map_to_expr<'a>(
        schema: &'a DFSchemaRef,
        fields: &'a [&str],
    ) -> impl Iterator<Item = Expr> + 'a {
        fields
            .iter()
            .filter(|f| {
                if let Ok(df) = schema.field_with_unqualified_name(f) {
                    *df.data_type() != DataType::Null
                } else {
                    false
                }
            })
            .map(|f| Expr::sort(f.as_expr(), true, false))
    }

    let schema = plan.schema();

    if !group_by_tag_set.is_empty() {
        series_sort.extend(map_to_expr(schema, group_by_tag_set));
    };

    series_sort.append(&mut sort_exprs);

    series_sort.extend(map_to_expr(schema, projection_tag_set));

    LogicalPlanBuilder::from(plan).sort(series_sort)?.build()
}

/// Trait to convert the receiver to a [`Expr::Sort`] expression.
pub(super) trait ToSortExpr {
    /// Create a sort expression.
    fn to_sort_expr(&self) -> Expr;
}

impl ToSortExpr for Option<OrderByClause> {
    fn to_sort_expr(&self) -> Expr {
        "time".as_expr().sort(
            match self {
                // Default behaviour is to sort by time in ascending order if there is no ORDER BY
                None | Some(OrderByClause::Ascending) => true,
                Some(OrderByClause::Descending) => false,
            },
            false,
        )
    }
}

/// Map the fields to DataFusion [`Expr::Column`] expressions, excluding those columns that
/// are [`DataType::Null`]'s.
pub(super) fn fields_to_exprs_no_nulls<'a>(
    schema: &'a DFSchemaRef,
    fields: &'a [&str],
) -> impl Iterator<Item = Expr> + 'a {
    fields
        .iter()
        .filter(|f| {
            if let Ok(df) = schema.field_with_unqualified_name(f) {
                *df.data_type() != DataType::Null
            } else {
                false
            }
        })
        .map(|f| f.as_expr())
}
