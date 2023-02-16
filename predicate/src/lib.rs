#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro
)]

pub mod delete_expr;
pub mod delete_predicate;
pub mod rpc_predicate;

use arrow::{
    array::{
        BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray, UInt64Array,
    },
    datatypes::SchemaRef,
};
use data_types::{InfluxDbType, TableSummary, TimestampRange};
use datafusion::{
    error::DataFusionError,
    logical_expr::{
        binary_expr,
        expr_visitor::{ExprVisitable, ExpressionVisitor, Recursion},
        utils::expr_to_columns,
        BinaryExpr,
    },
    optimizer::utils::split_conjunction,
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
    prelude::{col, lit_timestamp_nano, Expr},
};
use datafusion_util::{make_range_expr, nullable_schema, AsExpr};
use observability_deps::tracing::debug;
use rpc_predicate::VALUE_COLUMN_NAME;
use schema::TIME_COLUMN_NAME;
use std::{
    collections::{BTreeSet, HashSet},
    fmt,
    sync::Arc,
};

/// This `Predicate` represents the empty predicate (aka that evaluates to true for all rows).
pub const EMPTY_PREDICATE: Predicate = Predicate {
    field_columns: None,
    exprs: vec![],
    range: None,
    value_expr: vec![],
};

/// A unified Predicate structure for IOx that understands the
/// InfluxDB data model (e.g. Fields and Tags and timestamps) as well
/// as for arbitrary other predicates that are expressed by
/// DataFusion's [`Expr`] type.
///
/// Note that the InfluxDB data model (e.g. ParsedLine's)
/// distinguishes between some types of columns (tags and fields), and
/// likewise the semantics of this structure can express some types of
/// restrictions that only apply to certain types of columns.
///
/// Example:
/// ```
/// use predicate::Predicate;
/// use datafusion::prelude::{col, lit};
///
/// let p = Predicate::new()
///    .with_range(1, 100)
///    .with_expr(col("foo").eq(lit(42)));
///
/// assert_eq!(
///   p.to_string(),
///   "Predicate range: [1 - 100] exprs: [foo = Int32(42)]"
/// );
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd)]
pub struct Predicate {
    /// Optional field (aka "column") restriction. If present,
    /// restricts the results to only tables which have *at least one*
    /// of the fields in field_columns.
    pub field_columns: Option<BTreeSet<String>>,

    /// Optional timestamp range: only rows within this range are included in
    /// results. Other rows are excluded
    pub range: Option<TimestampRange>,

    /// Optional arbitrary predicates, represented as list of
    /// DataFusion expressions applied a logical conjunction (aka they
    /// are 'AND'ed together). Only rows that evaluate to TRUE for all
    /// these expressions should be returned. Other rows are excluded
    /// from the results.
    pub exprs: Vec<Expr>,

    /// Optional arbitrary predicates on the special `_value` column
    /// which represents the value of any column.
    ///
    /// These expressions are applied to `field_columns` projections
    /// in the form of `CASE` statement conditions.
    pub value_expr: Vec<ValueExpr>,
}

impl Predicate {
    pub fn new() -> Self {
        Default::default()
    }

    /// Return true if this predicate has any general purpose predicates
    pub fn has_exprs(&self) -> bool {
        !self.exprs.is_empty()
    }

    /// Return a DataFusion [`Expr`] predicate representing the
    /// combination of AND'ing all (`exprs`) and timestamp restriction
    /// in this Predicate.
    ///
    /// Returns None if there are no `Expr`'s restricting
    /// the data
    pub fn filter_expr(&self) -> Option<Expr> {
        let expr_iter = std::iter::once(self.make_timestamp_predicate_expr())
            // remove None
            .flatten()
            .chain(self.exprs.iter().cloned());

        // combine all items together with AND
        expr_iter.reduce(|accum, expr| accum.and(expr))
    }

    /// Return true if the field / column should be included in results
    pub fn should_include_field(&self, field_name: &str) -> bool {
        match &self.field_columns {
            None => true, // No field restriction on predicate
            Some(field_names) => field_names.contains(field_name),
        }
    }

    /// Creates a DataFusion predicate for appliying a timestamp range:
    ///
    /// `range.start <= time and time < range.end`
    fn make_timestamp_predicate_expr(&self) -> Option<Expr> {
        self.range
            .map(|range| make_range_expr(range.start(), range.end(), TIME_COLUMN_NAME))
    }

    /// Returns true if ths predicate evaluates to true for all rows
    pub fn is_empty(&self) -> bool {
        self == &EMPTY_PREDICATE
    }

    /// Return a negated DF logical expression for the given delete predicates
    pub fn negated_expr<S>(delete_predicates: &[S]) -> Option<Expr>
    where
        S: AsRef<Self>,
    {
        if delete_predicates.is_empty() {
            return None;
        }

        let pred = Self::default().with_delete_predicates(delete_predicates);

        // Make a conjunctive expression of the pred.exprs
        let mut val = None;
        for e in pred.exprs {
            match val {
                None => val = Some(e),
                Some(expr) => val = Some(expr.and(e)),
            }
        }

        val
    }

    /// Merge the given delete predicates into this select predicate.
    /// Since we want to eliminate data filtered by the delete predicates,
    /// they are first converted into their negated form: NOT(delete_predicate)
    /// then added/merged into the selection one
    pub fn with_delete_predicates<S>(mut self, delete_predicates: &[S]) -> Self
    where
        S: AsRef<Self>,
    {
        // Create a list of disjunctive negated expressions.
        // Example: there are two deletes as follows (note that time_range is stored separated in the Predicate
        //  but we need to put it together with the exprs here)
        //   . Delete_1: WHERE city != "Boston"  AND temp = 70  AND time_range in [10, 30)
        //   . Delete 2: WHERE state = "NY" AND route != "I90" AND time_range in [20, 50)
        // The negated list will be "NOT(Delete_1)", NOT(Delete_2)" which means
        //    NOT(city != "Boston"  AND temp = 70 AND time_range in [10, 30]),  NOT(state = "NY" AND route != "I90" AND time_range in [20, 50]) which means
        //   [NOT(city = Boston") OR NOT(temp = 70) OR NOT(time_range in [10, 30])], [NOT(state = "NY") OR NOT(route != "I90") OR NOT(time_range in [20, 50])]
        // Note that the "NOT(time_range in [20, 50])]" or "NOT(20 <= time <= 50)"" is replaced with "time < 20 OR time > 50"

        for pred in delete_predicates {
            let pred = pred.as_ref();

            let mut expr: Option<Expr> = None;

            // Time range
            if let Some(range) = pred.range {
                // time_expr =  NOT(start <= time_range <= end)
                // Equivalent to: (time < start OR time > end)
                let time_expr = col(TIME_COLUMN_NAME)
                    .lt(lit_timestamp_nano(range.start()))
                    .or(col(TIME_COLUMN_NAME).gt(lit_timestamp_nano(range.end())));

                match expr {
                    None => expr = Some(time_expr),
                    Some(e) => expr = Some(e.or(time_expr)),
                }
            }

            // Exprs
            for exp in &pred.exprs {
                match expr {
                    None => expr = Some(exp.clone().not()),
                    Some(e) => expr = Some(e.or(exp.clone().not())),
                }
            }

            // Push the negated expression of the delete predicate into the list exprs of the select predicate
            if let Some(e) = expr {
                self.exprs.push(e);
            }
        }
        self
    }

    /// Removes the timestamp range from this predicate, if the range
    /// is for the entire min/max valid range.
    ///
    /// This is used in certain cases to retain compatibility with the
    /// existing storage engine
    pub(crate) fn with_clear_timestamp_if_max_range(mut self) -> Self {
        self.range = self.range.take().and_then(|range| {
            // FIXME(lesam): This should properly be contains_all, but until
            // https://github.com/influxdata/idpe/issues/13094 is fixed we are more permissive
            // about what timestamp range we consider 'all time'
            if range.contains_nearly_all() {
                debug!("Cleared timestamp max-range");

                None
            } else {
                Some(range)
            }
        });

        self
    }

    /// Apply predicate to given table summary and avoid having to
    /// look at actual data.
    pub fn apply_to_table_summary(
        &self,
        table_summary: &TableSummary,
        schema: SchemaRef,
    ) -> PredicateMatch {
        let summary = SummaryWrapper {
            summary: table_summary,
        };

        // If we don't have statistics for a particular column, its
        // value will be null, so we need to ensure the schema we used
        // in pruning predicates allows for null.
        let schema = nullable_schema(schema);

        if let Some(expr) = self.filter_expr() {
            match PruningPredicate::try_new(expr.clone(), Arc::clone(&schema)) {
                Ok(pp) => {
                    match pp.prune(&summary) {
                        Ok(matched) => {
                            assert_eq!(matched.len(), 1);
                            if matched[0] {
                                // might match
                                return PredicateMatch::Unknown;
                            } else {
                                // does not match => since expressions are `AND`ed, we know that we will have zero matches
                                return PredicateMatch::Zero;
                            }
                        }
                        Err(e) => {
                            debug!(
                                %e,
                                %expr,
                                "cannot prune summary with PruningPredicate",
                            );
                            return PredicateMatch::Unknown;
                        }
                    }
                }
                Err(e) => {
                    debug!(
                        %e,
                        %expr,
                        "cannot create PruningPredicate from expression",
                    );
                    return PredicateMatch::Unknown;
                }
            }
        }

        PredicateMatch::Unknown
    }
}

struct SummaryWrapper<'a> {
    summary: &'a TableSummary,
}

impl<'a> PruningStatistics for SummaryWrapper<'a> {
    fn min_values(&self, column: &datafusion::prelude::Column) -> Option<arrow::array::ArrayRef> {
        let col = self.summary.column(&column.name)?;
        let stats = &col.stats;

        // special handling for timestamps
        if col.influxdb_type == InfluxDbType::Timestamp {
            let val = stats.as_i64()?;
            return Some(Arc::new(TimestampNanosecondArray::from(vec![val.min])));
        }

        let array = match stats {
            data_types::Statistics::I64(val) => Arc::new(Int64Array::from(vec![val.min])) as _,
            data_types::Statistics::U64(val) => Arc::new(UInt64Array::from(vec![val.min])) as _,
            data_types::Statistics::F64(val) => Arc::new(Float64Array::from(vec![val.min])) as _,
            data_types::Statistics::Bool(val) => Arc::new(BooleanArray::from(vec![val.min])) as _,
            data_types::Statistics::String(val) => {
                Arc::new(StringArray::from(vec![val.min.as_deref()])) as _
            }
        };

        Some(array)
    }

    fn max_values(&self, column: &datafusion::prelude::Column) -> Option<arrow::array::ArrayRef> {
        let col = self.summary.column(&column.name)?;
        let stats = &col.stats;

        // special handling for timestamps
        if col.influxdb_type == InfluxDbType::Timestamp {
            let val = stats.as_i64()?;
            return Some(Arc::new(TimestampNanosecondArray::from(vec![val.max])));
        }

        let array = match stats {
            data_types::Statistics::I64(val) => Arc::new(Int64Array::from(vec![val.max])) as _,
            data_types::Statistics::U64(val) => Arc::new(UInt64Array::from(vec![val.max])) as _,
            data_types::Statistics::F64(val) => Arc::new(Float64Array::from(vec![val.max])) as _,
            data_types::Statistics::Bool(val) => Arc::new(BooleanArray::from(vec![val.max])) as _,
            data_types::Statistics::String(val) => {
                Arc::new(StringArray::from(vec![val.max.as_deref()])) as _
            }
        };

        Some(array)
    }

    fn num_containers(&self) -> usize {
        // the summary represents a single virtual container
        1
    }

    fn null_counts(&self, column: &datafusion::prelude::Column) -> Option<arrow::array::ArrayRef> {
        let null_count = self.summary.column(&column.name)?.stats.null_count();

        Some(Arc::new(UInt64Array::from(vec![null_count])))
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn iter_to_str<S>(s: impl IntoIterator<Item = S>) -> String
        where
            S: ToString,
        {
            s.into_iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        }

        write!(f, "Predicate")?;

        if let Some(field_columns) = &self.field_columns {
            write!(f, " field_columns: {{{}}}", iter_to_str(field_columns))?;
        }

        if let Some(range) = &self.range {
            // TODO: could be nice to show this as actual timestamps (not just numbers)?
            write!(f, " range: [{} - {}]", range.start(), range.end())?;
        }

        if !self.exprs.is_empty() {
            write!(f, " exprs: [")?;
            for (i, expr) in self.exprs.iter().enumerate() {
                write!(f, "{expr}")?;
                if i < self.exprs.len() - 1 {
                    write!(f, ", ")?;
                }
            }
            write!(f, "]")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The result of evaluating a predicate on a set of rows
pub enum PredicateMatch {
    /// There is at least one row that matches the predicate that has
    /// at least one non null value in each field of the predicate
    AtLeastOneNonNullField,

    /// There are exactly zero rows that match the predicate
    Zero,

    /// There *may* be rows that match, OR there *may* be no rows that
    /// match
    Unknown,
}

impl Predicate {
    /// Sets the timestamp range
    pub fn with_range(mut self, start: i64, end: i64) -> Self {
        // Without more thought, redefining the timestamp range would
        // lose the old range. Asser that that cannot happen.
        assert!(
            self.range.is_none(),
            "Unexpected re-definition of timestamp range"
        );

        self.range = Some(TimestampRange::new(start, end));
        self
    }

    /// sets the optional timestamp range, if any
    pub fn with_maybe_timestamp_range(mut self, range: Option<TimestampRange>) -> Self {
        // Without more thought, redefining the timestamp range would
        // lose the old range. Asser that that cannot happen.
        assert!(
            range.is_none() || self.range.is_none(),
            "Unexpected re-definition of timestamp range"
        );
        self.range = range;
        self
    }

    /// Add an  exprestion "time >= retention_time"
    pub fn with_retention(mut self, retention_time: i64) -> Self {
        let expr = col(TIME_COLUMN_NAME).gt_eq(lit_timestamp_nano(retention_time));
        self.exprs.push(expr);
        self
    }

    /// Adds an expression to the list of general purpose predicates
    pub fn with_expr(self, expr: Expr) -> Self {
        self.with_exprs([expr])
    }

    /// Adds a ValueExpr to the list of value expressons
    pub fn with_value_expr(mut self, value_expr: ValueExpr) -> Self {
        self.value_expr.push(value_expr);
        self
    }

    /// Builds a regex matching expression from the provided column name and
    /// pattern. Values not matching the regex will be filtered out.
    pub fn with_regex_match_expr(self, column: &str, pattern: impl Into<String>) -> Self {
        let expr = query_functions::regex_match_expr(col(column), pattern.into());
        self.with_expr(expr)
    }

    /// Builds a regex "not matching" expression from the provided column name
    /// and pattern. Values *matching* the regex will be filtered out.
    pub fn with_regex_not_match_expr(self, column: &str, pattern: impl Into<String>) -> Self {
        let expr = query_functions::regex_not_match_expr(col(column), pattern.into());
        self.with_expr(expr)
    }

    /// Sets field_column restriction
    pub fn with_field_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        // We need to distinguish predicates like `column_name In
        // (foo, bar)` and `column_name = foo and column_name = bar` in order to handle
        // this
        if self.field_columns.is_some() {
            unimplemented!("Complex/Multi field predicates are not yet supported");
        }

        let column_names = columns
            .into_iter()
            .map(|s| s.into())
            .collect::<BTreeSet<_>>();

        self.field_columns = Some(column_names);
        self
    }

    /// Adds all expressions to the list of general purpose predicates
    pub fn with_exprs(mut self, filters: impl IntoIterator<Item = Expr>) -> Self {
        self.exprs.extend(filters.into_iter());
        self
    }

    /// Remove any clauses of this predicate that can not be run before deduplication.
    ///
    /// See <https://github.com/influxdata/influxdb_iox/issues/6066> for more details.
    ///
    /// Only expressions that are row-based and refer to primary key columns (and constants)
    /// can be evaluated prior to deduplication.
    ///
    /// If a predicate can filter out some but not all of the rows with
    /// the same primary key, it may filter out the row that should have been updated
    /// allowing the original through, producing incorrect results.
    ///
    /// Any predicate that operates solely on primary key columns will either pass or filter
    /// all rows with that primary key and thus is safe to push through.
    pub fn push_through_dedup(self, schema: &schema::Schema) -> Self {
        let pk: HashSet<_> = schema.primary_key().into_iter().collect();

        let exprs = self
            .exprs
            .iter()
            .flat_map(split_conjunction)
            .filter(|expr| {
                let mut columns = HashSet::default();
                if expr_to_columns(expr, &mut columns).is_err() {
                    // bail out, do NOT include this weird expression
                    return false;
                }

                // check if all columns are part of the primary key
                if !columns.into_iter().all(|c| pk.contains(c.name.as_str())) {
                    return false;
                }

                expr.accept(RowBasedVisitor::default())
                    .expect("never fails")
                    .row_based
            })
            .cloned()
            .collect();

        Self {
            // can always push time range through de-dup because it is a primary keys set operation
            range: self.range,
            exprs,
            field_columns: None,
            value_expr: vec![],
        }
    }
}

// Wrapper around `Expr::BinaryExpr` where left input is known to be
// single Column reference to the `_value` column
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd)]
pub struct ValueExpr {
    expr: Expr,
}

impl TryFrom<Expr> for ValueExpr {
    /// Returns the original Expr if conversion doesn't work
    type Error = Expr;

    /// tries to create a new ValueExpr. If `expr` follows the
    /// expected pattrn, returns Ok(Self). If not, returns Err(expr)
    fn try_from(expr: Expr) -> Result<Self, Self::Error> {
        if let Expr::BinaryExpr(BinaryExpr {
            left,
            op: _,
            right: _,
        }) = &expr
        {
            if let Expr::Column(inner) = left.as_ref() {
                if inner.name == VALUE_COLUMN_NAME {
                    return Ok(Self { expr });
                }
            }
        }
        Err(expr)
    }
}

impl ValueExpr {
    /// Returns a new [`Expr`] with the reference to the `_value`
    /// column replaced with the specified column name
    pub fn replace_col(&self, name: &str) -> Expr {
        if let Expr::BinaryExpr(BinaryExpr { left: _, op, right }) = &self.expr {
            binary_expr(name.as_expr(), *op, right.as_ref().clone())
        } else {
            unreachable!("Unexpected content in ValueExpr")
        }
    }
}

impl From<ValueExpr> for Expr {
    fn from(value_expr: ValueExpr) -> Self {
        value_expr.expr
    }
}

/// Recursively walk an expression tree, checking if the expression is row-based.
struct RowBasedVisitor {
    row_based: bool,
}

impl Default for RowBasedVisitor {
    fn default() -> Self {
        Self { row_based: true }
    }
}

impl ExpressionVisitor for RowBasedVisitor {
    fn pre_visit(mut self, expr: &Expr) -> Result<Recursion<Self>, DataFusionError> {
        match expr {
            Expr::Alias(_, _)
            | Expr::Between { .. }
            | Expr::BinaryExpr { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::Column(_)
            | Expr::Exists { .. }
            | Expr::GetIndexedField { .. }
            | Expr::ILike { .. }
            | Expr::InList { .. }
            | Expr::InSubquery { .. }
            | Expr::IsFalse(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotNull(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotUnknown(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsUnknown(_)
            | Expr::Like { .. }
            | Expr::Literal(_)
            | Expr::Negative(_)
            | Expr::Not(_)
            | Expr::Placeholder { .. }
            | Expr::QualifiedWildcard { .. }
            | Expr::ScalarFunction { .. }
            | Expr::ScalarSubquery(_)
            | Expr::ScalarUDF { .. }
            | Expr::ScalarVariable(_, _)
            | Expr::SimilarTo { .. }
            | Expr::Sort { .. }
            | Expr::TryCast { .. }
            | Expr::Wildcard => Ok(Recursion::Continue(self)),
            Expr::AggregateFunction { .. }
            | Expr::AggregateUDF { .. }
            | Expr::GroupingSet(_)
            | Expr::WindowFunction { .. } => {
                self.row_based = false;
                Ok(Recursion::Stop(self))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType as ArrowDataType;
    use data_types::{ColumnSummary, InfluxDbType, StatValues, MAX_NANO_TIME, MIN_NANO_TIME};
    use datafusion::prelude::{col, cube, lit};
    use schema::builder::SchemaBuilder;
    use test_helpers::maybe_start_logging;

    #[test]
    fn test_default_predicate_is_empty() {
        let p = Predicate::default();
        assert!(p.is_empty());
    }

    #[test]
    fn test_non_default_predicate_is_not_empty() {
        let p = Predicate::new().with_range(1, 100);

        assert!(!p.is_empty());
    }

    #[test]
    fn predicate_display_ts() {
        // TODO make this a doc example?
        let p = Predicate::new().with_range(1, 100);

        assert_eq!(p.to_string(), "Predicate range: [1 - 100]");
    }

    #[test]
    fn predicate_display_ts_and_expr() {
        let p = Predicate::new()
            .with_range(1, 100)
            .with_expr(col("foo").eq(lit(42)).and(col("bar").lt(lit(11))));

        assert_eq!(
            p.to_string(),
            "Predicate range: [1 - 100] exprs: [foo = Int32(42) AND bar < Int32(11)]"
        );
    }

    #[test]
    fn predicate_display_full() {
        let p = Predicate::new()
            .with_range(1, 100)
            .with_expr(col("foo").eq(lit(42)))
            .with_field_columns(vec!["f1", "f2"]);

        assert_eq!(
            p.to_string(),
            "Predicate field_columns: {f1, f2} range: [1 - 100] exprs: [foo = Int32(42)]"
        );
    }

    #[test]
    fn test_clear_timestamp_if_max_range_out_of_range() {
        let p = Predicate::new()
            .with_range(1, 100)
            .with_expr(col("foo").eq(lit(42)));

        let expected = p.clone();

        // no rewrite
        assert_eq!(p.with_clear_timestamp_if_max_range(), expected);
    }

    #[test]
    fn test_clear_timestamp_if_max_range_out_of_range_low() {
        let p = Predicate::new()
            .with_range(MIN_NANO_TIME, 100)
            .with_expr(col("foo").eq(lit(42)));

        let expected = p.clone();

        // no rewrite
        assert_eq!(p.with_clear_timestamp_if_max_range(), expected);
    }

    #[test]
    fn test_clear_timestamp_if_max_range_out_of_range_high() {
        let p = Predicate::new()
            .with_range(0, MAX_NANO_TIME + 1)
            .with_expr(col("foo").eq(lit(42)));

        let expected = p.clone();

        // no rewrite
        assert_eq!(p.with_clear_timestamp_if_max_range(), expected);
    }

    #[test]
    fn test_clear_timestamp_if_max_range_in_range() {
        let p = Predicate::new()
            .with_range(MIN_NANO_TIME, MAX_NANO_TIME + 1)
            .with_expr(col("foo").eq(lit(42)));

        let expected = Predicate::new().with_expr(col("foo").eq(lit(42)));
        // rewrite
        assert_eq!(p.with_clear_timestamp_if_max_range(), expected);
    }

    #[test]
    fn test_apply_to_table_summary() {
        maybe_start_logging();

        let p = Predicate::new()
            .with_range(100, 200)
            .with_expr(col("foo").eq(lit(42i64)))
            .with_expr(col("bar").eq(lit(42i64)))
            .with_expr(col(TIME_COLUMN_NAME).gt(lit_timestamp_nano(120i64)));

        let schema = SchemaBuilder::new()
            .field("foo", ArrowDataType::Int64)
            .unwrap()
            .field("bar", ArrowDataType::Int64)
            .unwrap()
            .timestamp()
            .build()
            .unwrap();

        let summary = TableSummary {
            columns: vec![ColumnSummary {
                name: "foo".to_owned(),
                influxdb_type: InfluxDbType::Field,
                stats: data_types::Statistics::I64(StatValues {
                    min: Some(10),
                    max: Some(20),
                    null_count: Some(0),
                    total_count: 1_000,
                    distinct_count: None,
                }),
            }],
        };
        assert_eq!(
            p.apply_to_table_summary(&summary, schema.as_arrow()),
            PredicateMatch::Zero,
        );

        let summary = TableSummary {
            columns: vec![ColumnSummary {
                name: "foo".to_owned(),
                influxdb_type: InfluxDbType::Field,
                stats: data_types::Statistics::I64(StatValues {
                    min: Some(10),
                    max: Some(50),
                    null_count: Some(0),
                    total_count: 1_000,
                    distinct_count: None,
                }),
            }],
        };
        assert_eq!(
            p.apply_to_table_summary(&summary, schema.as_arrow()),
            PredicateMatch::Unknown,
        );

        let summary = TableSummary {
            columns: vec![ColumnSummary {
                name: TIME_COLUMN_NAME.to_owned(),
                influxdb_type: InfluxDbType::Timestamp,
                stats: data_types::Statistics::I64(StatValues {
                    min: Some(115),
                    max: Some(115),
                    null_count: Some(0),
                    total_count: 1_000,
                    distinct_count: None,
                }),
            }],
        };
        assert_eq!(
            p.apply_to_table_summary(&summary, schema.as_arrow()),
            PredicateMatch::Zero,
        );

        let summary = TableSummary {
            columns: vec![ColumnSummary {
                name: TIME_COLUMN_NAME.to_owned(),
                influxdb_type: InfluxDbType::Timestamp,
                stats: data_types::Statistics::I64(StatValues {
                    min: Some(300),
                    max: Some(300),
                    null_count: Some(0),
                    total_count: 1_000,
                    distinct_count: None,
                }),
            }],
        };
        assert_eq!(
            p.apply_to_table_summary(&summary, schema.as_arrow()),
            PredicateMatch::Zero,
        );

        let summary = TableSummary {
            columns: vec![ColumnSummary {
                name: TIME_COLUMN_NAME.to_owned(),
                influxdb_type: InfluxDbType::Timestamp,
                stats: data_types::Statistics::I64(StatValues {
                    min: Some(150),
                    max: Some(300),
                    null_count: Some(0),
                    total_count: 1_000,
                    distinct_count: None,
                }),
            }],
        };
        assert_eq!(
            p.apply_to_table_summary(&summary, schema.as_arrow()),
            PredicateMatch::Unknown,
        )
    }

    /// Test that pruning works even when some expressions within the predicate cannot be evaluated by DataFusion
    #[test]
    fn test_apply_to_table_summary_partially_unsupported() {
        maybe_start_logging();

        let p = Predicate::new()
            .with_range(100, 200)
            .with_expr(col("foo").eq(lit(42i64)).not()); // NOT expressions are currently mostly unsupported by DataFusion

        let schema = SchemaBuilder::new()
            .field("foo", ArrowDataType::Int64)
            .unwrap()
            .timestamp()
            .build()
            .unwrap();

        let summary = TableSummary {
            columns: vec![
                ColumnSummary {
                    name: TIME_COLUMN_NAME.to_owned(),
                    influxdb_type: InfluxDbType::Timestamp,
                    stats: data_types::Statistics::I64(StatValues {
                        min: Some(10),
                        max: Some(20),
                        null_count: Some(0),
                        total_count: 1_000,
                        distinct_count: None,
                    }),
                },
                ColumnSummary {
                    name: "foo".to_owned(),
                    influxdb_type: InfluxDbType::Field,
                    stats: data_types::Statistics::I64(StatValues {
                        min: Some(10),
                        max: Some(20),
                        null_count: Some(0),
                        total_count: 1_000,
                        distinct_count: None,
                    }),
                },
            ],
        };
        assert_eq!(
            p.apply_to_table_summary(&summary, schema.as_arrow()),
            PredicateMatch::Zero,
        );
    }

    #[test]
    fn test_push_through_dedup() {
        let schema = SchemaBuilder::default()
            .tag("tag1")
            .tag("tag2")
            .field("field1", ArrowDataType::Float64)
            .unwrap()
            .field("field2", ArrowDataType::Float64)
            .unwrap()
            .timestamp()
            .build()
            .unwrap();

        // no-op predicate
        assert_eq!(
            Predicate {
                field_columns: None,
                range: None,
                exprs: vec![],
                value_expr: vec![],
            }
            .push_through_dedup(&schema),
            Predicate {
                field_columns: None,
                range: None,
                exprs: vec![],
                value_expr: vec![],
            },
        );

        // simple case
        assert_eq!(
            Predicate {
                field_columns: Some(BTreeSet::from([
                    String::from("tag1"),
                    String::from("field1"),
                    String::from("time"),
                ])),
                range: Some(TimestampRange::new(42, 1337)),
                exprs: vec![
                    col("tag1").eq(lit("foo")),
                    col("field1").eq(lit(1.0)), // filtered out
                    col("time").eq(lit(1)),
                ],
                value_expr: vec![ValueExpr::try_from(col("_value").eq(lit(1.0))).unwrap()],
            }
            .push_through_dedup(&schema),
            Predicate {
                field_columns: None,
                range: Some(TimestampRange::new(42, 1337)),
                exprs: vec![col("tag1").eq(lit("foo")), col("time").eq(lit(1)),],
                value_expr: vec![],
            },
        );

        // disassemble AND
        assert_eq!(
            Predicate {
                field_columns: None,
                range: None,
                exprs: vec![col("tag1")
                    .eq(lit("foo"))
                    .and(col("field1").eq(lit(1.0)))
                    .and(col("time").eq(lit(1))),],
                value_expr: vec![],
            }
            .push_through_dedup(&schema),
            Predicate {
                field_columns: None,
                range: None,
                exprs: vec![col("tag1").eq(lit("foo")), col("time").eq(lit(1)),],
                value_expr: vec![],
            },
        );

        // filter no-row operations
        assert_eq!(
            Predicate {
                field_columns: None,
                range: None,
                exprs: vec![
                    col("tag1").eq(lit("foo")),
                    cube(vec![col("time").eq(lit(1))]),
                ],
                value_expr: vec![],
            }
            .push_through_dedup(&schema),
            Predicate {
                field_columns: None,
                range: None,
                exprs: vec![col("tag1").eq(lit("foo"))],
                value_expr: vec![],
            },
        );

        // do NOT disassemble OR
        assert_eq!(
            Predicate {
                field_columns: None,
                range: None,
                exprs: vec![col("tag1")
                    .eq(lit("foo"))
                    .or(col("field1").eq(lit(1.0)))
                    .or(col("time").eq(lit(1))),],
                value_expr: vec![],
            }
            .push_through_dedup(&schema),
            Predicate {
                field_columns: None,
                range: None,
                exprs: vec![],
                value_expr: vec![],
            },
        );
    }
}
