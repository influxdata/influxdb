//! This module contains a unified Predicate structure for IOx qieries
//! that can select and filter Fields and Tags from the InfluxDB data
//! mode as well as for arbitrary other predicates that are expressed
//! by DataFusion's `Expr` type.

use std::{
    collections::{BTreeSet, HashSet},
    fmt,
};

use data_types::timestamp::TimestampRange;
use datafusion::{
    error::DataFusionError,
    logical_plan::{col, Expr, Operator},
    optimizer::utils,
};
use datafusion_util::{make_range_expr, AndExprBuilder};
use internal_types::schema::TIME_COLUMN_NAME;
use observability_deps::tracing::debug;

/// This `Predicate` represents the empty predicate (aka that
/// evaluates to true for all rows).
pub const EMPTY_PREDICATE: Predicate = Predicate {
    table_names: None,
    field_columns: None,
    exprs: vec![],
    range: None,
    partition_key: None,
};

#[derive(Debug, Clone, Copy)]
/// The result of evaluating a predicate on a set of rows
pub enum PredicateMatch {
    /// There is at least one row that matches the predicate
    AtLeastOne,

    /// There are exactly zero rows that match the predicate
    Zero,

    /// There *may* be rows that match, OR there *may* be no rows that
    /// match
    Unknown,
}

/// Represents a parsed predicate for evaluation by the
/// TSDatabase InfluxDB IOx query engine.
///
/// Note that the InfluxDB data model (e.g. ParsedLine's)
/// distinguishes between some types of columns (tags and fields), and
/// likewise the semantics of this structure can express some types of
/// restrictions that only apply to certain types of columns.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Predicate {
    /// Optional table restriction. If present, restricts the results
    /// to only tables whose names are in `table_names`
    pub table_names: Option<BTreeSet<String>>,

    /// Optional field restriction. If present, restricts the results to only
    /// tables which have *at least one* of the fields in field_columns.
    pub field_columns: Option<BTreeSet<String>>,

    /// Optional partition key filter
    pub partition_key: Option<String>,

    /// Optional timestamp range: only rows within this range are included in
    /// results. Other rows are excluded
    pub range: Option<TimestampRange>,

    /// Optional arbitrary predicates, represented as list of
    /// DataFusion expressions applied a logical conjunction (aka they
    /// are 'AND'ed together). Only rows that evaluate to TRUE for all
    /// these expressions should be returned. Other rows are excluded
    /// from the results.
    pub exprs: Vec<Expr>,
}

impl Predicate {
    /// Return true if this predicate has any general purpose predicates
    pub fn has_exprs(&self) -> bool {
        !self.exprs.is_empty()
    }

    /// Return a DataFusion `Expr` predicate representing the
    /// combination of all predicate (`exprs`) and timestamp
    /// restriction in this Predicate. Returns None if there are no
    /// `Expr`'s restricting the data
    pub fn filter_expr(&self) -> Option<Expr> {
        let mut builder =
            AndExprBuilder::default().append_opt(self.make_timestamp_predicate_expr());

        for expr in &self.exprs {
            builder = builder.append_expr(expr.clone());
        }

        builder.build()
    }

    /// Return true if results from this table should be included in
    /// results
    pub fn should_include_table(&self, table_name: &str) -> bool {
        match &self.table_names {
            None => true, // No table name restriction on predicate
            Some(table_names) => table_names.contains(table_name),
        }
    }

    /// Return true if the field should be included in results
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
            .map(|range| make_range_expr(range.start, range.end, TIME_COLUMN_NAME))
    }

    /// Returns true if ths predicate evaluates to true for all rows
    pub fn is_empty(&self) -> bool {
        self == &EMPTY_PREDICATE
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

        if let Some(table_names) = &self.table_names {
            write!(f, " table_names: {{{}}}", iter_to_str(table_names))?;
        }

        if let Some(field_columns) = &self.field_columns {
            write!(f, " field_columns: {{{}}}", iter_to_str(field_columns))?;
        }

        if let Some(partition_key) = &self.partition_key {
            write!(f, " partition_key: '{}'", partition_key)?;
        }

        if let Some(range) = &self.range {
            // TODO: could be nice to show this as actual timestamps (not just numbers)?
            write!(f, " range: [{} - {}]", range.start, range.end)?;
        }

        if !self.exprs.is_empty() {
            // Expr doesn't implement `Display` yet, so just the debug version
            // See https://github.com/apache/arrow-datafusion/issues/347
            let display_exprs = self.exprs.iter().map(|e| format!("{:?}", e));
            write!(f, " exprs: [{}]", iter_to_str(display_exprs))?;
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
/// Structure for building [`Predicate`]s
///
/// Example:
/// ```
/// use query::predicate::PredicateBuilder;
/// use datafusion::logical_plan::{col, lit};
///
/// let p = PredicateBuilder::new()
///    .timestamp_range(1, 100)
///    .add_expr(col("foo").eq(lit(42)))
///    .build();
///
/// assert_eq!(
///   p.to_string(),
///   "Predicate range: [1 - 100] exprs: [#foo Eq Int32(42)]"
/// );
/// ```
pub struct PredicateBuilder {
    inner: Predicate,
}

impl From<Predicate> for PredicateBuilder {
    fn from(inner: Predicate) -> Self {
        Self { inner }
    }
}

impl PredicateBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the timestamp range
    pub fn timestamp_range(mut self, start: i64, end: i64) -> Self {
        // Without more thought, redefining the timestamp range would
        // lose the old range. Asser that that cannot happen.
        assert!(
            self.inner.range.is_none(),
            "Unexpected re-definition of timestamp range"
        );
        self.inner.range = Some(TimestampRange { start, end });
        self
    }

    /// sets the optional timestamp range, if any
    pub fn timestamp_range_option(mut self, range: Option<TimestampRange>) -> Self {
        // Without more thought, redefining the timestamp range would
        // lose the old range. Asser that that cannot happen.
        assert!(
            range.is_none() || self.inner.range.is_none(),
            "Unexpected re-definition of timestamp range"
        );
        self.inner.range = range;
        self
    }

    /// Adds an expression to the list of general purpose predicates
    pub fn add_expr(mut self, expr: Expr) -> Self {
        self.inner.exprs.push(expr);
        self
    }

    /// Builds a regex matching expression from the provided column name and
    /// pattern. Values not matching the regex will be filtered out.
    pub fn build_regex_match_expr(self, column: &str, pattern: impl Into<String>) -> Self {
        self.regex_match_expr(column, pattern, true)
    }

    /// Builds a regex "not matching" expression from the provided column name
    /// and pattern. Values *matching* the regex will be filtered out.
    pub fn build_regex_not_match_expr(self, column: &str, pattern: impl Into<String>) -> Self {
        self.regex_match_expr(column, pattern, false)
    }

    fn regex_match_expr(mut self, column: &str, pattern: impl Into<String>, matches: bool) -> Self {
        let expr = crate::func::regex::regex_match_expr(col(column), pattern.into(), matches);
        self.inner.exprs.push(expr);
        self
    }

    /// Adds an optional table name restriction to the existing list
    pub fn table_option(self, table: Option<String>) -> Self {
        if let Some(table) = table {
            self.tables(vec![table])
        } else {
            self
        }
    }

    /// Set the table restriction to `table`
    pub fn table(self, table: impl Into<String>) -> Self {
        self.tables(vec![table.into()])
    }

    /// Sets table name restrictions from something that can iterate
    /// over items that can be converted into `Strings`
    pub fn tables<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        // We need to distinguish predicates like `table_name In
        // (foo, bar)` and `table_name = foo and table_name = bar` in order to handle
        // this
        assert!(
            self.inner.table_names.is_none(),
            "Multiple table predicate specification not yet supported"
        );

        let table_names: BTreeSet<String> = tables.into_iter().map(|s| s.into()).collect();

        self.inner.table_names = Some(table_names);
        self
    }

    /// Sets field_column restriction
    pub fn field_columns(mut self, columns: Vec<impl Into<String>>) -> Self {
        // We need to distinguish predicates like `column_name In
        // (foo, bar)` and `column_name = foo and column_name = bar` in order to handle
        // this
        if self.inner.field_columns.is_some() {
            unimplemented!("Complex/Multi field predicates are not yet supported");
        }

        let column_names = columns
            .into_iter()
            .map(|s| s.into())
            .collect::<BTreeSet<_>>();

        self.inner.field_columns = Some(column_names);
        self
    }

    /// Set the partition key restriction
    pub fn partition_key(mut self, partition_key: impl Into<String>) -> Self {
        assert!(
            self.inner.partition_key.is_none(),
            "multiple partition key predicates not suported"
        );
        self.inner.partition_key = Some(partition_key.into());
        self
    }

    /// Create a predicate, consuming this builder
    pub fn build(self) -> Predicate {
        self.inner
    }

    /// Adds only the expressions from `filters` that can be pushed down to
    /// execution engines.
    pub fn add_pushdown_exprs(mut self, filters: &[Expr]) -> Self {
        // For each expression of the filters, recursively split it, if it is is an AND conjunction
        // For example, expression (x AND y) will be split into a vector of 2 expressions [x, y]
        let mut exprs = vec![];
        filters
            .iter()
            .for_each(|expr| Self::split_members(expr, &mut exprs));

        // Only keep single_column and primitive binary expressions
        let mut pushdown_exprs: Vec<Expr> = vec![];
        let exprs_result = exprs
            .into_iter()
            .try_for_each::<_, Result<_, DataFusionError>>(|expr| {
                let mut columns = HashSet::new();
                utils::expr_to_column_names(&expr, &mut columns)?;

                if columns.len() == 1 && Self::primitive_binary_expr(&expr) {
                    pushdown_exprs.push(expr);
                }
                Ok(())
            });

        match exprs_result {
            Ok(()) => {
                // Return the builder with only the pushdownable expressions on it.
                self.inner.exprs.append(&mut pushdown_exprs);
            }
            Err(e) => {
                debug!("Error, {}, building push-down predicates for filters: {:#?}. No predicates are pushed down", e, filters);
            }
        }

        self
    }

    /// Recursively split all "AND" expressions into smaller one
    /// Example: "A AND B AND C" => [A, B, C]
    pub fn split_members(predicate: &Expr, predicates: &mut Vec<Expr>) {
        match predicate {
            Expr::BinaryExpr {
                right,
                op: Operator::And,
                left,
            } => {
                Self::split_members(left, predicates);
                Self::split_members(right, predicates);
            }
            other => predicates.push(other.clone()),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_plan::{col, lit};

    #[test]
    fn test_default_predicate_is_empty() {
        let p = Predicate::default();
        assert!(p.is_empty());
    }

    #[test]
    fn test_non_default_predicate_is_not_empty() {
        let p = PredicateBuilder::new().timestamp_range(1, 100).build();

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
        let predicate = PredicateBuilder::default()
            .add_pushdown_exprs(&filters)
            .build();

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
        let p = PredicateBuilder::new().timestamp_range(1, 100).build();

        assert_eq!(p.to_string(), "Predicate range: [1 - 100]");
    }

    #[test]
    fn predicate_display_ts_and_expr() {
        let p = PredicateBuilder::new()
            .timestamp_range(1, 100)
            .add_expr(col("foo").eq(lit(42)).and(col("bar").lt(lit(11))))
            .build();

        assert_eq!(
            p.to_string(),
            "Predicate range: [1 - 100] exprs: [#foo Eq Int32(42) And #bar Lt Int32(11)]"
        );
    }

    #[test]
    fn predicate_display_full() {
        let p = PredicateBuilder::new()
            .timestamp_range(1, 100)
            .add_expr(col("foo").eq(lit(42)))
            .table("my_table")
            .field_columns(vec!["f1", "f2"])
            .partition_key("the_key")
            .build();

        assert_eq!(p.to_string(), "Predicate table_names: {my_table} field_columns: {f1, f2} partition_key: 'the_key' range: [1 - 100] exprs: [#foo Eq Int32(42)]");
    }
}
