#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod delete_expr;
pub mod delete_predicate;
pub mod rewrite;
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
    logical_expr::{binary_expr, utils::expr_to_columns},
    logical_plan::{col, lit_timestamp_nano, Expr, Operator},
    optimizer::utils::split_conjunction,
    physical_optimizer::pruning::{PruningPredicate, PruningStatistics},
};
use datafusion_util::{make_range_expr, nullable_schema};
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
/// use datafusion::logical_plan::{col, lit};
///
/// let p = Predicate::new()
///    .with_range(1, 100)
///    .with_expr(col("foo").eq(lit(42)));
///
/// assert_eq!(
///   p.to_string(),
///   "Predicate range: [1 - 100] exprs: [#foo = Int32(42)]"
/// );
/// ```
#[derive(Clone, Debug, Default, PartialEq, PartialOrd)]
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
    fn min_values(
        &self,
        column: &datafusion::logical_plan::Column,
    ) -> Option<arrow::array::ArrayRef> {
        let col = self.summary.column(&column.name)?;
        let stats = &col.stats;

        // special handling for timestamps
        if col.influxdb_type == Some(InfluxDbType::Timestamp) {
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

    fn max_values(
        &self,
        column: &datafusion::logical_plan::Column,
    ) -> Option<arrow::array::ArrayRef> {
        let col = self.summary.column(&column.name)?;
        let stats = &col.stats;

        // special handling for timestamps
        if col.influxdb_type == Some(InfluxDbType::Timestamp) {
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

    fn null_counts(
        &self,
        column: &datafusion::logical_plan::Column,
    ) -> Option<arrow::array::ArrayRef> {
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
                write!(f, "{}", expr)?;
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

    /// Adds an expression to the list of general purpose predicates
    pub fn with_expr(mut self, expr: Expr) -> Self {
        self.exprs.push(expr);
        self
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

    /// Adds only the expressions from `filters` that can be pushed down to
    /// execution engines.
    pub fn with_pushdown_exprs(mut self, filters: &[Expr]) -> Self {
        // For each expression of the filters, recursively split it, if it is is an AND conjunction
        // For example, expression (x AND y) will be split into a vector of 2 expressions [x, y]
        let mut exprs = vec![];
        filters
            .iter()
            .for_each(|expr| split_conjunction(expr, &mut exprs));

        // Only keep single_column and primitive binary expressions
        let mut pushdown_exprs: Vec<Expr> = vec![];
        let exprs_result = exprs
            .into_iter()
            .try_for_each::<_, Result<_, DataFusionError>>(|expr| {
                let mut columns = HashSet::new();
                expr_to_columns(expr, &mut columns)?;

                if columns.len() == 1 && Self::primitive_binary_expr(expr) {
                    pushdown_exprs.push(expr.clone());
                }
                Ok(())
            });

        match exprs_result {
            Ok(()) => {
                // Return the builder with only the pushdownable expressions on it.
                self.exprs.append(&mut pushdown_exprs);
            }
            Err(e) => {
                debug!("Error, {}, building push-down predicates for filters: {:#?}. No predicates are pushed down", e, filters);
            }
        }

        self
    }

    /// Return true if the given expression is in a primitive binary in the form: `column op constant`
    // and op must be a comparison one
    pub fn primitive_binary_expr(expr: &Expr) -> bool {
        match expr {
            Expr::BinaryExpr { left, op, right } => {
                matches!(
                    (&**left, &**right),
                    (Expr::Column(_), Expr::Literal(_)) | (Expr::Literal(_), Expr::Column(_))
                ) && matches!(
                    op,
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                )
            }
            _ => false,
        }
    }
}

// Wrapper around `Expr::BinaryExpr` where left input is known to be
// single Column reference to the `_value` column
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct ValueExpr {
    expr: Expr,
}

impl TryFrom<Expr> for ValueExpr {
    /// Returns the original Expr if conversion doesn't work
    type Error = Expr;

    /// tries to create a new ValueExpr. If `expr` follows the
    /// expected pattrn, returns Ok(Self). If not, returns Err(expr)
    fn try_from(expr: Expr) -> Result<Self, Self::Error> {
        if let Expr::BinaryExpr {
            left,
            op: _,
            right: _,
        } = &expr
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
        if let Expr::BinaryExpr { left: _, op, right } = &self.expr {
            binary_expr(col(name), *op, right.as_ref().clone())
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType as ArrowDataType;
    use data_types::{ColumnSummary, InfluxDbType, StatValues, MAX_NANO_TIME, MIN_NANO_TIME};
    use datafusion::logical_plan::{col, lit};
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
    fn test_pushdown_predicates() {
        let mut filters = vec![];

        // state = CA
        let expr1 = col("state").eq(lit("CA"));
        filters.push(expr1);

        // "price > 10"
        let expr2 = col("price").gt(lit(10));
        filters.push(expr2);

        // a < 10 AND b >= 50  --> will be split to [a < 10, b >= 50]
        let expr3 = col("a").lt(lit(10)).and(col("b").gt_eq(lit(50)));
        filters.push(expr3);

        // c != 3 OR d = 8  --> won't be pushed down
        let expr4 = col("c").not_eq(lit(3)).or(col("d").eq(lit(8)));
        filters.push(expr4);

        // e is null --> won't be pushed down
        let expr5 = col("e").is_null();
        filters.push(expr5);

        // f <= 60
        let expr6 = col("f").lt_eq(lit(60));
        filters.push(expr6);

        // g is not null --> won't be pushed down
        let expr7 = col("g").is_not_null();
        filters.push(expr7);

        // h + i  --> won't be pushed down
        let expr8 = col("h") + col("i");
        filters.push(expr8);

        // city = Boston
        let expr9 = col("city").eq(lit("Boston"));
        filters.push(expr9);

        // city != Braintree
        let expr9 = col("city").not_eq(lit("Braintree"));
        filters.push(expr9);

        // city != state --> won't be pushed down
        let expr10 = col("city").not_eq(col("state"));
        filters.push(expr10);

        // city = state --> won't be pushed down
        let expr11 = col("city").eq(col("state"));
        filters.push(expr11);

        // city_state = city + state --> won't be pushed down
        let expr12 = col("city_sate").eq(col("city") + col("state"));
        filters.push(expr12);

        // city = city + 5 --> won't be pushed down
        let expr13 = col("city").eq(col("city") + lit(5));
        filters.push(expr13);

        // city = city --> won't be pushed down
        let expr14 = col("city").eq(col("city"));
        filters.push(expr14);

        // city + 5 = city --> won't be pushed down
        let expr15 = (col("city") + lit(5)).eq(col("city"));
        filters.push(expr15);

        // 5 = city
        let expr16 = lit(5).eq(col("city"));
        filters.push(expr16);

        println!(" --------------- Filters: {:#?}", filters);

        // Expected pushdown predicates: [state = CA, price > 10, a < 10, b >= 50, f <= 60, city = Boston, city != Braintree, 5 = city]
        let predicate = Predicate::default().with_pushdown_exprs(&filters);

        println!(" ------------- Predicates: {:#?}", predicate);
        assert_eq!(predicate.exprs.len(), 8);
        assert_eq!(predicate.exprs[0], col("state").eq(lit("CA")));
        assert_eq!(predicate.exprs[1], col("price").gt(lit(10)));
        assert_eq!(predicate.exprs[2], col("a").lt(lit(10)));
        assert_eq!(predicate.exprs[3], col("b").gt_eq(lit(50)));
        assert_eq!(predicate.exprs[4], col("f").lt_eq(lit(60)));
        assert_eq!(predicate.exprs[5], col("city").eq(lit("Boston")));
        assert_eq!(predicate.exprs[6], col("city").not_eq(lit("Braintree")));
        assert_eq!(predicate.exprs[7], lit(5).eq(col("city")));
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
            "Predicate range: [1 - 100] exprs: [#foo = Int32(42) AND #bar < Int32(11)]"
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
            "Predicate field_columns: {f1, f2} range: [1 - 100] exprs: [#foo = Int32(42)]"
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
            .field("bar", ArrowDataType::Int64)
            .timestamp()
            .build()
            .unwrap();

        let summary = TableSummary {
            columns: vec![ColumnSummary {
                name: "foo".to_owned(),
                influxdb_type: Some(InfluxDbType::Field),
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
                influxdb_type: Some(InfluxDbType::Field),
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
                influxdb_type: Some(InfluxDbType::Timestamp),
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
                influxdb_type: Some(InfluxDbType::Timestamp),
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
                influxdb_type: Some(InfluxDbType::Timestamp),
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
            .timestamp()
            .build()
            .unwrap();

        let summary = TableSummary {
            columns: vec![
                ColumnSummary {
                    name: TIME_COLUMN_NAME.to_owned(),
                    influxdb_type: Some(InfluxDbType::Timestamp),
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
                    influxdb_type: Some(InfluxDbType::Field),
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
}
