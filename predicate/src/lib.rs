#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod delete_expr;
pub mod delete_predicate;
pub mod rpc_predicate;

use data_types::TimestampRange;
use datafusion::{
    common::tree_node::{TreeNodeVisitor, VisitRecursion},
    error::DataFusionError,
    logical_expr::{binary_expr, BinaryExpr},
    prelude::{col, lit_timestamp_nano, Expr},
};
use datafusion_util::{make_range_expr, AsExpr};
use observability_deps::tracing::debug;
use rpc_predicate::VALUE_COLUMN_NAME;
use schema::TIME_COLUMN_NAME;
use std::{collections::BTreeSet, fmt, ops::Not};

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

    /// Add an  exprestion "time > retention_time"
    pub fn with_retention(mut self, retention_time: i64) -> Self {
        let expr = col(TIME_COLUMN_NAME).gt(lit_timestamp_nano(retention_time));
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
    ) -> Result<Self, &'static str> {
        // We need to distinguish predicates like `column_name In
        // (foo, bar)` and `column_name = foo and column_name = bar` in order to handle
        // this
        if self.field_columns.is_some() {
            return Err("Complex/Multi field predicates are not yet supported");
        }

        let column_names = columns
            .into_iter()
            .map(|s| s.into())
            .collect::<BTreeSet<_>>();

        self.field_columns = Some(column_names);
        Ok(self)
    }

    /// Adds all expressions to the list of general purpose predicates
    pub fn with_exprs(mut self, filters: impl IntoIterator<Item = Expr>) -> Self {
        self.exprs.extend(filters);
        self
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

/// Recursively walk an expression tree, checking if the expression is
/// row-based.
///
/// A row-based function takes one row in and produces
/// one value as output.
///
/// Note that even though a predicate expression  like `col < 5` can be used to
/// filter rows, the expression itself is row-based (produces a single boolean).
///
/// Examples of non row based expressions are Aggregate and
/// Window function which produce different cardinality than their
/// input.
struct RowBasedVisitor {
    row_based: bool,
}

impl Default for RowBasedVisitor {
    fn default() -> Self {
        Self { row_based: true }
    }
}

impl TreeNodeVisitor for RowBasedVisitor {
    type N = Expr;

    fn pre_visit(&mut self, expr: &Expr) -> Result<VisitRecursion, DataFusionError> {
        match expr {
            Expr::Alias(_)
            | Expr::Between { .. }
            | Expr::BinaryExpr { .. }
            | Expr::Case { .. }
            | Expr::Cast { .. }
            | Expr::Column(_)
            | Expr::Exists { .. }
            | Expr::GetIndexedField { .. }
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
            | Expr::OuterReferenceColumn(_, _)
            | Expr::Placeholder { .. }
            | Expr::QualifiedWildcard { .. }
            | Expr::ScalarFunction { .. }
            | Expr::ScalarSubquery(_)
            | Expr::ScalarUDF { .. }
            | Expr::ScalarVariable(_, _)
            | Expr::SimilarTo { .. }
            | Expr::Sort { .. }
            | Expr::TryCast { .. }
            | Expr::Wildcard => Ok(VisitRecursion::Continue),
            Expr::AggregateFunction { .. }
            | Expr::AggregateUDF { .. }
            | Expr::GroupingSet(_)
            | Expr::WindowFunction { .. } => {
                self.row_based = false;
                Ok(VisitRecursion::Stop)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{MAX_NANO_TIME, MIN_NANO_TIME};
    use datafusion::prelude::{col, lit};

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
            .with_field_columns(vec!["f1", "f2"])
            .unwrap();

        assert_eq!(
            p.to_string(),
            "Predicate field_columns: {f1, f2} range: [1 - 100] exprs: [foo = Int32(42)]"
        );
    }

    #[test]
    fn predicate_multi_field_cols_not_supported() {
        let err = Predicate::new()
            .with_field_columns(vec!["f1", "f2"])
            .unwrap()
            .with_field_columns(vec!["f1", "f2"])
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Complex/Multi field predicates are not yet supported"
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
}
