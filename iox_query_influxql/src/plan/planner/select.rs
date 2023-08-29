use crate::error;
use crate::plan::ir::Field;
use arrow::datatypes::DataType;
use datafusion::common::{DFSchemaRef, Result};
use datafusion::logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder};
use datafusion_util::AsExpr;
use generated_types::influxdata::iox::querier::v1::influx_ql_metadata::TagKeyColumn;
use influxdb_influxql_parser::expression::{Call, Expr as IQLExpr, VarRef, VarRefDataType};
use influxdb_influxql_parser::identifier::Identifier;
use influxdb_influxql_parser::literal::Literal;
use itertools::Itertools;
use schema::{InfluxColumnType, INFLUXQL_MEASUREMENT_COLUMN_NAME};
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};

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
        .filter_map(|(index, f)| match &f.data_type {
            Some(InfluxColumnType::Tag) | None => Some((f.name.as_str(), index + START_INDEX)),
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

/// The selector function that has been specified for use with a selector
/// projection type.
#[derive(Debug)]
pub(super) enum Selector<'a> {
    Bottom {
        field_key: &'a Identifier,
        tag_keys: Vec<&'a Identifier>,
        n: i64,
    },
    First {
        field_key: &'a Identifier,
    },
    Last {
        field_key: &'a Identifier,
    },
    Max {
        field_key: &'a Identifier,
    },
    Min {
        field_key: &'a Identifier,
    },
    Percentile {
        field_key: &'a Identifier,
        n: f64,
    },
    Sample {
        field_key: &'a Identifier,
        n: i64,
    },
    Top {
        field_key: &'a Identifier,
        tag_keys: Vec<&'a Identifier>,
        n: i64,
    },
}

impl<'a> Selector<'a> {
    /// Find the selector function, with its location, in the specified field list.
    pub(super) fn find_enumerated(fields: &'a [Field]) -> Result<(usize, Self)> {
        fields
            .iter()
            .enumerate()
            .find_map(|(idx, f)| match &f.expr {
                IQLExpr::Call(c) => Some((idx, c)),
                _ => None,
            })
            .map(|(idx, c)| {
                Ok((
                    idx,
                    match c.name.as_str() {
                        "bottom" => Self::bottom(c),
                        "first" => Self::first(c),
                        "last" => Self::last(c),
                        "max" => Self::max(c),
                        "min" => Self::min(c),
                        "percentile" => Self::percentile(c),
                        "sample" => Self::sample(c),
                        "top" => Self::top(c),
                        name => error::internal(format!("unexpected selector function: {name}")),
                    }?,
                ))
            })
            .ok_or_else(|| error::map::internal("expected Call expression"))?
    }

    fn bottom(call: &'a Call) -> Result<Self> {
        let [field_key, tag_keys @ .., narg] = call.args.as_slice() else {
            return error::internal(format!(
                "invalid number of arguments for bottom: expected 2 or more, got {}",
                call.args.len()
            ));
        };
        let tag_keys: Result<Vec<_>> = tag_keys.iter().map(Self::identifier).collect();
        Ok(Self::Bottom {
            field_key: Self::identifier(field_key)?,
            tag_keys: tag_keys?,
            n: Self::literal_int(narg)?,
        })
    }

    fn first(call: &'a Call) -> Result<Self> {
        if call.args.len() != 1 {
            return error::internal(format!(
                "invalid number of arguments for first: expected 1, got {}",
                call.args.len()
            ));
        }
        Ok(Self::First {
            field_key: Self::identifier(call.args.get(0).unwrap())?,
        })
    }

    fn last(call: &'a Call) -> Result<Self> {
        if call.args.len() != 1 {
            return error::internal(format!(
                "invalid number of arguments for last: expected 1, got {}",
                call.args.len()
            ));
        }
        Ok(Self::Last {
            field_key: Self::identifier(call.args.get(0).unwrap())?,
        })
    }

    fn max(call: &'a Call) -> Result<Self> {
        if call.args.len() != 1 {
            return error::internal(format!(
                "invalid number of arguments for max: expected 1, got {}",
                call.args.len()
            ));
        }
        Ok(Self::Max {
            field_key: Self::identifier(call.args.get(0).unwrap())?,
        })
    }

    fn min(call: &'a Call) -> Result<Self> {
        if call.args.len() != 1 {
            return error::internal(format!(
                "invalid number of arguments for min: expected 1, got {}",
                call.args.len()
            ));
        }
        Ok(Self::Min {
            field_key: Self::identifier(call.args.get(0).unwrap())?,
        })
    }

    fn percentile(call: &'a Call) -> Result<Self> {
        if call.args.len() != 2 {
            return error::internal(format!(
                "invalid number of arguments for min: expected 1, got {}",
                call.args.len()
            ));
        }
        Ok(Self::Percentile {
            field_key: Self::identifier(call.args.get(0).unwrap())?,
            n: Self::literal_num(call.args.get(1).unwrap())?,
        })
    }

    fn sample(call: &'a Call) -> Result<Self> {
        if call.args.len() != 2 {
            return error::internal(format!(
                "invalid number of arguments for min: expected 1, got {}",
                call.args.len()
            ));
        }
        Ok(Self::Sample {
            field_key: Self::identifier(call.args.get(0).unwrap())?,
            n: Self::literal_int(call.args.get(1).unwrap())?,
        })
    }

    fn top(call: &'a Call) -> Result<Self> {
        let [field_key, tag_keys @ .., narg] = call.args.as_slice() else {
            return error::internal(format!(
                "invalid number of arguments for top: expected 2 or more, got {}",
                call.args.len()
            ));
        };
        let tag_keys: Result<Vec<_>> = tag_keys.iter().map(Self::identifier).collect();
        Ok(Self::Top {
            field_key: Self::identifier(field_key)?,
            tag_keys: tag_keys?,
            n: Self::literal_int(narg)?,
        })
    }

    fn identifier(expr: &'a IQLExpr) -> Result<&'a Identifier> {
        match expr {
            IQLExpr::VarRef(v) => Ok(&v.name),
            e => error::internal(format!("invalid column identifier: {}", e)),
        }
    }

    fn literal_int(expr: &'a IQLExpr) -> Result<i64> {
        match expr {
            IQLExpr::Literal(Literal::Integer(n)) => Ok(*n),
            e => error::internal(format!("invalid integer literal: {}", e)),
        }
    }

    fn literal_num(expr: &'a IQLExpr) -> Result<f64> {
        match expr {
            IQLExpr::Literal(Literal::Integer(n)) => Ok(*n as f64),
            IQLExpr::Literal(Literal::Float(n)) => Ok(*n),
            e => error::internal(format!("invalid integer literal: {}", e)),
        }
    }
}

impl<'a> Display for Selector<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            Self::Bottom {
                field_key,
                tag_keys,
                n,
            } => {
                write!(f, "bottom({field_key}")?;
                for tag_key in tag_keys {
                    write!(f, ", {tag_key}")?;
                }
                write!(f, ", {n})")
            }
            Self::First { field_key } => write!(f, "first({field_key})"),
            Self::Last { field_key } => write!(f, "last({field_key})"),
            Self::Max { field_key } => write!(f, "max({field_key})"),
            Self::Min { field_key } => write!(f, "min({field_key})"),
            Self::Percentile { field_key, n } => write!(f, "percentile({field_key}, {n})"),
            Self::Sample { field_key, n } => write!(f, "sample({field_key}, {n})"),
            Self::Top {
                field_key,
                tag_keys,
                n,
            } => {
                write!(f, "top({field_key}")?;
                for tag_key in tag_keys {
                    write!(f, ", {tag_key}")?;
                }
                write!(f, ", {n})")
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum SelectorWindowOrderBy<'a> {
    FieldAsc(&'a Identifier),
    FieldDesc(&'a Identifier),
}
