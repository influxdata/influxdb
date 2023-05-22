use crate::plan::ir::Field;
use arrow::datatypes::DataType;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_util::AsExpr;
use generated_types::influxdata::iox::querier::v1::influx_ql_metadata::TagKeyColumn;
use influxdb_influxql_parser::expression::{Expr as IQLExpr, VarRef, VarRefDataType};
use itertools::Itertools;
use schema::{InfluxColumnType, INFLUXQL_MEASUREMENT_COLUMN_NAME};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;

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
        fields_to_exprs_no_nulls(schema, fields).map(|f| Expr::sort(f, true, false))
    }

    let schema = plan.schema();

    if !group_by_tag_set.is_empty() {
        series_sort.extend(map_to_expr(schema, group_by_tag_set));
    };

    series_sort.append(&mut sort_exprs);

    series_sort.extend(map_to_expr(schema, projection_tag_set));

    LogicalPlanBuilder::from(plan).sort(series_sort)?.build()
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

/// Contains an expanded `SELECT` projection
pub(super) struct ProjectionInfo<'a> {
    /// A copy of the `SELECT` fields that includes tags from the `GROUP BY` that were not
    /// specified in the original `SELECT` projection.
    pub(super) fields: Vec<Field>,

    /// A list of tag column names specified in the `GROUP BY` clause.
    pub(super) group_by_tag_set: Vec<&'a str>,

    /// A list of tag column names specified exclusively in the `SELECT` projection.
    pub(super) projection_tag_set: Vec<&'a str>,

    /// A list of booleans indicating whether matching elements in the
    /// `group_by_tag_set` are also projected in the query.
    pub(super) is_projected: Vec<bool>,
}

impl<'a> ProjectionInfo<'a> {
    /// Computes a `ProjectionInfo` from the specified `fields` and `group_by_tags`.
    pub(super) fn new(fields: &'a [Field], group_by_tags: &'a [&'a str]) -> Self {
        // Skip the `time` column
        let fields_no_time = &fields[1..];
        // always start with the time column
        let mut fields = vec![fields.first().cloned().unwrap()];

        let (group_by_tag_set, projection_tag_set, is_projected) = if group_by_tags.is_empty() {
            let tag_columns = find_tag_and_unknown_columns(fields_no_time)
                .sorted()
                .collect::<Vec<_>>();
            (vec![], tag_columns, vec![])
        } else {
            let mut tag_columns =
                find_tag_and_unknown_columns(fields_no_time).collect::<HashSet<_>>();

            // Find the list of tag keys specified in the `GROUP BY` clause, and
            // whether any of the tag keys are also projected in the SELECT list.
            let (tag_set, is_projected): (Vec<_>, Vec<_>) = group_by_tags
                .iter()
                .map(|s| (*s, tag_columns.contains(s)))
                .unzip();

            // Tags specified in the `GROUP BY` clause that are not already added to the
            // projection must be projected, so they can be used in the group key.
            //
            // At the end of the loop, the `tag_columns` set will contain the tag columns that
            // exist in the projection and not in the `GROUP BY`.
            fields.extend(
                tag_set
                    .iter()
                    .filter_map(|col| match tag_columns.remove(*col) {
                        true => None,
                        false => Some(Field {
                            expr: IQLExpr::VarRef(VarRef {
                                name: (*col).into(),
                                data_type: Some(VarRefDataType::Tag),
                            }),
                            name: col.to_string(),
                            data_type: Some(InfluxColumnType::Tag),
                        }),
                    }),
            );

            (
                tag_set,
                tag_columns.into_iter().sorted().collect::<Vec<_>>(),
                is_projected,
            )
        };

        fields.extend(fields_no_time.iter().cloned());

        Self {
            fields,
            group_by_tag_set,
            projection_tag_set,
            is_projected,
        }
    }
}

/// Find all the columns where the resolved data type
/// is a tag or is [`None`], which is unknown.
fn find_tag_and_unknown_columns(fields: &[Field]) -> impl Iterator<Item = &str> {
    fields.iter().filter_map(|f| match f.data_type {
        Some(InfluxColumnType::Tag) | None => Some(f.name.as_str()),
        _ => None,
    })
}
