//! Defines data structures which represent an InfluxQL
//! statement after it has been processed

use crate::error;
use crate::plan::rewriter::ProjectionType;
use datafusion::common::Result;
use influxdb_influxql_parser::common::{
    LimitClause, MeasurementName, OffsetClause, OrderByClause, QualifiedMeasurementName,
    WhereClause,
};
use influxdb_influxql_parser::expression::{ConditionalExpression, Expr};
use influxdb_influxql_parser::select::{
    FieldList, FillClause, FromMeasurementClause, GroupByClause, MeasurementSelection,
    SelectStatement, TimeZoneClause,
};
use influxdb_influxql_parser::time_range::TimeRange;
use schema::{InfluxColumnType, Schema};
use std::collections::HashSet;
use std::fmt::{Display, Formatter};

use super::SchemaProvider;

/// A set of tag keys.
pub(super) type TagSet = HashSet<String>;

/// Represents a validated and normalized top-level [`SelectStatement`].
#[derive(Debug, Default, Clone)]
pub(super) struct SelectQuery {
    pub(super) select: Select,
}

#[derive(Debug, Default, Clone)]
pub(super) struct Select {
    /// The projection type of the selection.
    pub(super) projection_type: ProjectionType,

    /// The interval derived from the arguments to the `TIME` function
    /// when a `GROUP BY` clause is declared with `TIME`.
    pub(super) interval: Option<Interval>,

    /// The number of additional intervals that must be read
    /// for queries that group by time and use window functions such as
    /// `DIFFERENCE` or `DERIVATIVE`. This ensures data for the first
    /// window is available.
    ///
    /// See: <https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L50>
    pub(super) extra_intervals: usize,

    /// Projection clause of the selection.
    pub(super) fields: Vec<Field>,

    /// A list of data sources for the selection.
    pub(super) from: Vec<DataSource>,

    /// A conditional expression to filter the selection, excluding any predicates for the `time`
    /// column.
    pub(super) condition: Option<ConditionalExpression>,

    /// The time range derived from the `WHERE` clause of the `SELECT` statement.
    pub(super) time_range: TimeRange,

    /// The GROUP BY clause of the selection.
    pub(super) group_by: Option<GroupByClause>,

    /// The set of possible tags for the selection, by combining
    /// the tag sets of all inputs via the `FROM` clause.
    pub(super) tag_set: TagSet,

    /// The [fill] clause specifies the fill behaviour for the selection. If the value is [`None`],
    /// it is the same behavior as `fill(null)`.
    ///
    /// [fill]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#group-by-time-intervals-and-fill
    pub(super) fill: Option<FillClause>,

    /// Configures the ordering of the selection by time.
    pub(super) order_by: Option<OrderByClause>,

    /// A value to restrict the number of rows returned.
    pub(super) limit: Option<LimitClause>,

    /// A value to specify an offset to start retrieving rows.
    pub(super) offset: Option<OffsetClause>,

    /// The timezone for the query, specified as [`tz('<time zone>')`][time_zone_clause].
    ///
    /// [time_zone_clause]: https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#the-time-zone-clause
    pub(super) timezone: Option<chrono_tz::Tz>,
}

impl From<Select> for SelectStatement {
    fn from(value: Select) -> Self {
        Self {
            fields: FieldList::new(
                value
                    .fields
                    .into_iter()
                    .map(|c| influxdb_influxql_parser::select::Field {
                        expr: c.expr,
                        alias: Some(c.name.into()),
                    })
                    .collect(),
            ),
            from: FromMeasurementClause::new(
                value
                    .from
                    .into_iter()
                    .map(|tr| match tr {
                        DataSource::Table(name) => {
                            MeasurementSelection::Name(QualifiedMeasurementName {
                                database: None,
                                retention_policy: None,
                                name: MeasurementName::Name(name.as_str().into()),
                            })
                        }
                        DataSource::Subquery(q) => {
                            MeasurementSelection::Subquery(Box::new((*q).into()))
                        }
                    })
                    .collect(),
            ),
            condition: where_clause(value.condition, value.time_range),
            group_by: value.group_by,
            fill: value.fill,
            order_by: value.order_by,
            limit: value.limit,
            offset: value.offset,
            series_limit: None,
            series_offset: None,
            timezone: value.timezone.map(TimeZoneClause::new),
        }
    }
}

/// Combine the `condition` and `time_range` into a single `WHERE` predicate.
fn where_clause(
    condition: Option<ConditionalExpression>,
    time_range: TimeRange,
) -> Option<WhereClause> {
    let time_expr: Option<ConditionalExpression> = match (time_range.lower, time_range.upper) {
        (Some(lower), Some(upper)) if lower == upper => {
            Some(format!("time = {lower}").parse().unwrap())
        }
        (Some(lower), Some(upper)) => Some(
            format!("time >= {lower} AND time <= {upper}")
                .parse()
                .unwrap(),
        ),
        (Some(lower), None) => Some(format!("time >= {lower}").parse().unwrap()),
        (None, Some(upper)) => Some(format!("time <= {upper}").parse().unwrap()),
        (None, None) => None,
    };

    match (time_expr, condition) {
        (Some(lhs), Some(rhs)) => Some(WhereClause::new(lhs.and(rhs))),
        (Some(expr), None) | (None, Some(expr)) => Some(WhereClause::new(expr)),
        (None, None) => None,
    }
}

/// Represents a data source that is either a table or a subquery in a [`Select`] from clause.
#[derive(Debug, Clone)]
pub(super) enum DataSource {
    Table(String),
    Subquery(Box<Select>),
}

impl DataSource {
    pub(super) fn schema(&self, s: &dyn SchemaProvider) -> Result<DataSourceSchema<'_>> {
        match self {
            Self::Table(table_name) => s
                .table_schema(table_name)
                .map(DataSourceSchema::Table)
                .ok_or_else(|| error::map::internal("expected table")),
            Self::Subquery(q) => Ok(DataSourceSchema::Subquery(q)),
        }
    }
}

pub(super) enum DataSourceSchema<'a> {
    Table(Schema),
    Subquery(&'a Select),
}

impl<'a> DataSourceSchema<'a> {
    /// Returns `true` if the specified name is a tag field or a projection of a tag field if
    /// the `DataSource` is a subquery.
    pub(super) fn is_tag_field(&self, name: &str) -> bool {
        match self {
            DataSourceSchema::Table(s) => {
                matches!(s.field_type_by_name(name), Some(InfluxColumnType::Tag))
            }
            DataSourceSchema::Subquery(q) => q.tag_set.contains(name),
        }
    }

    /// Returns `true` if the specified name is a tag from the perspective of an outer
    /// query consuming the results of this subquery or table. If a subquery has aliases
    /// on its SELECT list, then those aliases are considered to be the names of the
    /// columns in the outer query.
    pub(super) fn is_projected_tag_field(&self, name: &str) -> bool {
        match self {
            DataSourceSchema::Table(_) => self.is_tag_field(name),
            DataSourceSchema::Subquery(q) => q
                .fields
                .iter()
                .any(|f| f.name == name && f.data_type == Some(InfluxColumnType::Tag)),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Field {
    pub(super) expr: Expr,
    pub(super) name: String,
    pub(super) data_type: Option<InfluxColumnType>,
}

impl Display for Field {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.expr, f)?;
        write!(f, " AS {}", self.name)
    }
}

/// Represents the interval duration and offset
/// derived from the `TIME` function when specified
/// in a `GROUP BY` clause.
#[derive(Debug, Clone, Copy)]
pub(super) struct Interval {
    /// The nanosecond duration of the interval
    pub duration: i64,

    /// The nanosecond offset of the interval.
    pub offset: Option<i64>,
}
