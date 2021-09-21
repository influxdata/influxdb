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
    logical_plan::{col, lit, Column, Expr, Operator},
    optimizer::utils,
    scalar::ScalarValue,
};
use datafusion_util::{make_range_expr, AndExprBuilder};
use internal_types::schema::TIME_COLUMN_NAME;
use observability_deps::tracing::debug;
use sqlparser::{
    ast::{BinaryOperator, Expr as SqlParserExpr, Ident, Statement, Value},
    dialect::GenericDialect,
    parser::Parser,
};

use snafu::Snafu;

use chrono::DateTime;

// Parse Delete Predicates
/// Parse Error
#[derive(Debug, Snafu)]
pub enum Error {
    /// Invalid time format
    #[snafu(display("Invalid timestamp: {}", value))]
    InvalidTimestamp { value: String },

    /// Invalid time range
    #[snafu(display("Invalid time range: ({}, {})", start, stop))]
    InvalidTimeRange { start: String, stop: String },

    /// Predicate syntax error
    #[snafu(display("Invalid predicate syntax: ({})", value))]
    InvalidSyntax { value: String },

    /// Predicate semantics error
    #[snafu(display("Invalid predicate semantics: ({})", value))]
    InvalidSemantics { value: String },

    /// Predicate include non supported expression
    #[snafu(display("Delete predicate must be conjunctive expressions of binary 'column_name = literal' or 'column_ame != literal': ({})", value))]
    NotSupportPredicate { value: String },
}

/// Result type for Parser Cient
pub type Result<T, E = Error> = std::result::Result<T, E>;

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

/// Represents a parsed predicate for evaluation by the InfluxDB IOx
/// query engine.
///
/// Note that the InfluxDB data model (e.g. ParsedLine's)
/// distinguishes between some types of columns (tags and fields), and
/// likewise the semantics of this structure can express some types of
/// restrictions that only apply to certain types of columns.
#[derive(Clone, Debug, Default, PartialEq, PartialOrd)]
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

    /// Return a negated DF logical expression for the given delete predicates
    pub fn negated_expr<S>(delete_predicates: &[S]) -> Option<Expr>
    where
        S: AsRef<Self>,
    {
        if delete_predicates.is_empty() {
            return None;
        }

        let mut pred = PredicateBuilder::default().build();
        pred.merge_delete_predicates(delete_predicates);

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
    pub fn merge_delete_predicates<S>(&mut self, delete_predicates: &[S])
    where
        S: AsRef<Self>,
    {
        // Create a list of disjunctive negated expressions.
        // Example: there are two deletes as follows (note that time_range is stored separated in the Predicate
        //  but we need to put it together with the exprs here)
        //   . Delete_1: WHERE city != "Boston"  AND temp = 70  AND time_range in [10, 30)
        //   . Delete 2: WHERE state = "NY" AND route != "I90" AND time_range in [20, 50)
        // The negated list will be "NOT(Delete_1)", NOT(Delete_2)" which means
        //    NOT(city != "Boston"  AND temp = 70 AND time_range in [10, 30)),  NOT(state = "NY" AND route != "I90" AND time_range in [20, 50)) which means
        //   [NOT(city = Boston") OR NOT(temp = 70) OR NOT(time_range in [10, 30))], [NOT(state = "NY") OR NOT(route != "I90") OR NOT(time_range in [20, 50))]
        // Note that the "NOT(time_range in [20, 50))]" or "NOT(20 <= time < 50)"" is replaced with "time < 20 OR time >= 50"

        for pred in delete_predicates {
            let pred = pred.as_ref();

            let mut expr: Option<Expr> = None;

            // Time range
            if let Some(range) = pred.range {
                // cast int to timestamp
                // NGA todo: add in DF a function timestamp_lit(i64_val) which does lit(ScalarValue::TimestampNanosecond(Some(i64_val))
                //  and use it here
                let ts_start = ScalarValue::TimestampNanosecond(Some(range.start));
                let ts_end = ScalarValue::TimestampNanosecond(Some(range.end));
                // time_expr =  NOT(start <= time_range < end)
                let time_expr = col(TIME_COLUMN_NAME)
                    .lt(lit(ts_start))
                    .or(col(TIME_COLUMN_NAME).gt_eq(lit(ts_end)));

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
/// use predicate::predicate::PredicateBuilder;
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
        let expr = crate::regex::regex_match_expr(col(column), pattern.into(), matches);
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
                utils::expr_to_columns(&expr, &mut columns)?;

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

/// Parser for Delete predicate and time range
#[derive(Debug, Clone)]
pub struct ParseDeletePredicate {
    pub start_time: i64,
    pub stop_time: i64,
    // conjunctive predicate of binary expressions of = or !=
    pub predicate: Vec<Expr>,
}

impl ParseDeletePredicate {
    /// Create a ParseDeletePredicate
    pub fn new(start_time: i64, stop_time: i64, predicate: Vec<Expr>) -> Self {
        Self {
            start_time,
            stop_time,
            predicate,
        }
    }

    /// Parse and convert the delete grpc API into ParseDeletePredicate to send to server
    pub fn try_new(table_name: &str, start: &str, stop: &str, predicate: &str) -> Result<Self> {
        // parse and check time range
        let (start_time, stop_time) = Self::parse_time_range(start, stop)?;

        // Parse the predicate
        let delete_exprs = Self::parse_predicate(table_name, predicate)?;

        Ok(Self::new(start_time, stop_time, delete_exprs))
    }

    /// Parse the predicate and convert it into datafusion expression
    /// A delete predicate is a conjunctive expression of many
    /// binary expressions of 'colum = constant' or 'column != constant'
    ///
    pub fn parse_predicate(table_name: &str, predicate: &str) -> Result<Vec<Expr>> {
        if predicate.is_empty() {
            return Ok(vec![]);
        }

        // Now add this predicate string into a DELETE SQL to user sqlparser to parse it
        // "DELETE FROM table_name WHERE predicate"
        let mut sql = "DELETE FROM ".to_string();
        sql.push_str(table_name);
        sql.push_str(" WHERE ");
        sql.push_str(predicate);

        // parse the delete sql
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql.as_str());
        match ast {
            Err(parse_err) => {
                let error_str = format!("{}, {}", predicate, parse_err);
                Err(Error::InvalidSyntax { value: error_str })
            }
            Ok(mut stmt) => {
                if stmt.len() != 1 {
                    return Err(Error::InvalidSemantics {
                        value: predicate.to_string(),
                    });
                }

                let stmt = stmt.pop();
                match stmt {
                    Some(Statement::Delete {
                        table_name: _,
                        selection: Some(expr),
                        ..
                    }) => {
                        // split this expr into smaller binary if any
                        let mut exprs = vec![];
                        let split = Self::split_members(table_name, &expr, &mut exprs);
                        if !split {
                            return Err(Error::NotSupportPredicate {
                                value: predicate.to_string(),
                            });
                        }
                        Ok(exprs)
                    }
                    _ => Err(Error::InvalidSemantics {
                        value: predicate.to_string(),
                    }),
                }
            }
        }
    }

    /// Recursively split all "AND" expressions into smaller ones
    /// Example: "A AND B AND C" => [A, B, C]
    /// Return false if not all of them are AND of binary expression of
    /// "column_name = literal" or "column_name != literal"
    ///
    /// The split expressions will be converted into data fusion expressions
    pub fn split_members(
        table_name: &str,
        predicate: &SqlParserExpr,
        predicates: &mut Vec<Expr>,
    ) -> bool {
        // The below code built to be compatible with
        // https://github.com/influxdata/influxdb/blob/master/predicate/parser_test.go
        match predicate {
            SqlParserExpr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                if !Self::split_members(table_name, left, predicates) {
                    return false;
                }
                if !Self::split_members(table_name, right, predicates) {
                    return false;
                }
            }
            SqlParserExpr::BinaryOp { left, op, right } => {
                // Verify Operator
                let op = match op {
                    BinaryOperator::Eq => Operator::Eq,
                    BinaryOperator::NotEq => Operator::NotEq,
                    _ => return false,
                };

                // verify if left is identifier (column name)
                let column = match &**left {
                    SqlParserExpr::Identifier(Ident {
                        value,
                        quote_style: _, // all quotes are ignored as done in idpe
                    }) => Expr::Column(Column {
                        relation: None,
                        name: value.to_string(),
                    }),
                    _ => return false, // not a column name
                };

                // verify if right is a literal or an identifier (e.g column name)
                let value = match &**right {
                    SqlParserExpr::Identifier(Ident {
                        value,
                        quote_style: _,
                    }) => lit(value.to_string()),
                    SqlParserExpr::Value(Value::DoubleQuotedString(value)) => {
                        lit(value.to_string())
                    }
                    SqlParserExpr::Value(Value::SingleQuotedString(value)) => {
                        lit(value.to_string())
                    }
                    SqlParserExpr::Value(Value::NationalStringLiteral(value)) => {
                        lit(value.to_string())
                    }
                    SqlParserExpr::Value(Value::HexStringLiteral(value)) => lit(value.to_string()),
                    SqlParserExpr::Value(Value::Number(v, _)) => match v.parse::<i64>() {
                        Ok(v) => lit(v),
                        Err(_) => lit(v.parse::<f64>().unwrap()),
                    },
                    SqlParserExpr::Value(Value::Boolean(v)) => lit(*v),
                    _ => return false, // not a literal
                };

                let expr = Expr::BinaryExpr {
                    left: Box::new(column),
                    op,
                    right: Box::new(value),
                };
                predicates.push(expr);
            }
            _ => return false,
        }

        true
    }

    /// Parse a time and return its time in nanosecond
    pub fn parse_time(input: &str) -> Result<i64> {
        // This input can be in timestamp form that end with Z such as 1970-01-01T00:00:00Z
        // See examples here https://docs.influxdata.com/influxdb/v2.0/reference/cli/influx/delete/#delete-all-points-within-a-specified-time-frame
        let datetime_result = DateTime::parse_from_rfc3339(input);
        match datetime_result {
            Ok(datetime) => Ok(datetime.timestamp_nanos()),
            Err(timestamp_err) => {
                // See if it is in nanosecond form
                let time_result = input.parse::<i64>();
                match time_result {
                    Ok(nano) => Ok(nano),
                    Err(nano_err) => {
                        // wrong format, return both error
                        let error_str = format!("{}, {}", timestamp_err, nano_err);
                        Err(Error::InvalidTimestamp { value: error_str })
                    }
                }
            }
        }
    }

    /// Parse a time range [start, stop]
    pub fn parse_time_range(start: &str, stop: &str) -> Result<(i64, i64)> {
        let start_time = Self::parse_time(start)?;
        let stop_time = Self::parse_time(stop)?;
        if start_time > stop_time {
            return Err(Error::InvalidTimeRange {
                start: start.to_string(),
                stop: stop.to_string(),
            });
        }

        Ok((start_time, stop_time))
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

    // The delete predicate
    #[test]
    fn test_time_range_valid() {
        let start = r#"100"#;
        let stop = r#"100"#;
        let result = ParseDeletePredicate::parse_time_range(start, stop).unwrap();
        let expected = (100, 100);
        assert_eq!(result, expected);

        let start = r#"100"#;
        let stop = r#"200"#;
        let result = ParseDeletePredicate::parse_time_range(start, stop).unwrap();
        let expected = (100, 200);
        assert_eq!(result, expected);

        let start = r#"1970-01-01T00:00:00Z"#; // this is nano 0
        let stop = r#"1970-01-01T00:00:00Z"#;
        let result = ParseDeletePredicate::parse_time_range(start, stop).unwrap();
        let expected = (0, 0);
        assert_eq!(result, expected);

        // let start = r#"1970-01-01T00:00:00Z"#;  // this is nano 0
        // let stop = r#"now()"#;  // -- Not working. Need to find a way to test this
        // let result = ParseDeletePredicate::parse_time_range(start, stop).unwrap();
        // let expected = (0, 0);
        // assert_eq!(result, expected);

        let start = r#"1970-01-01T00:00:00Z"#;
        let stop = r#"100"#;
        let result = ParseDeletePredicate::parse_time_range(start, stop).unwrap();
        let expected = (0, 100);
        assert_eq!(result, expected);

        let start = r#"1970-01-01T00:00:00Z"#;
        let stop = r#"1970-01-01T00:01:00Z"#;
        let result = ParseDeletePredicate::parse_time_range(start, stop).unwrap();
        let expected = (0, 60000000000);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_time_range_invalid() {
        let start = r#"100"#;
        let stop = r#"-100"#;
        let result = ParseDeletePredicate::parse_time_range(start, stop);
        assert!(result.is_err());

        let start = r#"100"#;
        let stop = r#"50"#;
        let result = ParseDeletePredicate::parse_time_range(start, stop);
        assert!(result.is_err());

        let start = r#"100"#;
        let stop = r#"1970-01-01T00:00:00Z"#;
        let result = ParseDeletePredicate::parse_time_range(start, stop);
        assert!(result.is_err());

        let start = r#"1971-09-01T00:00:10Z"#;
        let stop = r#"1971-09-01T00:00:05Z"#;
        let result = ParseDeletePredicate::parse_time_range(start, stop);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamp() {
        let input = r#"123"#;
        let time = ParseDeletePredicate::parse_time(input).unwrap();
        assert_eq!(time, 123);

        // must parse time
        let input = r#"1970-01-01T00:00:00Z"#;
        let time = ParseDeletePredicate::parse_time(input).unwrap();
        assert_eq!(time, 0);

        let input = r#"1971-02-01T15:30:21Z"#;
        let time = ParseDeletePredicate::parse_time(input).unwrap();
        assert_eq!(time, 34270221000000000);
    }

    #[test]
    fn test_parse_timestamp_negative() {
        let input = r#"-123"#;
        let time = ParseDeletePredicate::parse_time(input).unwrap();
        assert_eq!(time, -123);
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        let input = r#"123gdb"#;
        ParseDeletePredicate::parse_time(input).unwrap_err();

        let input = r#"1970-01-01T00:00:00"#;
        ParseDeletePredicate::parse_time(input).unwrap_err();

        // It turn out this is not invalid but return1 1971
        let input = r#"1971-02-01:30:21Z"#;
        ParseDeletePredicate::parse_time(input).unwrap_err();
    }

    #[test]
    fn test_parse_timestamp_out_of_range() {
        let input = r#"99999999999999999999999999999999"#;
        let time = ParseDeletePredicate::parse_time(input);
        assert!(time.is_err());
    }

    #[test]
    fn test_parse_predicate() {
        let table = "test";

        let pred = r#"city= Boston and cost !=100 and state != "MA" AND temp=87.5"#;
        let result = ParseDeletePredicate::parse_predicate(table, pred).unwrap();

        println!("{:#?}", result);

        let mut expected = vec![];
        let e = col("city").eq(lit("Boston"));
        expected.push(e);
        let val: i64 = 100;
        let e = col("cost").not_eq(lit(val));
        expected.push(e);
        let e = col("state").not_eq(lit("MA"));
        expected.push(e);
        let e = col("temp").eq(lit(87.5));
        expected.push(e);

        assert_eq!(result, expected)
    }

    #[test]
    fn test_parse_predicate_invalid() {
        let table = "test";

        let pred = r#"city= Boston Or cost !=100 and state != "MA""#; // OR
        let result = ParseDeletePredicate::parse_predicate(table, pred);
        assert!(result.is_err());

        let pred = r#"city= Boston and cost !=100+1 and state != "MA""#; // 100 + 1
        let result = ParseDeletePredicate::parse_predicate(table, pred);
        assert!(result.is_err());

        let pred = r#"cost > 100"#; // >
        let result = ParseDeletePredicate::parse_predicate(table, pred);
        assert!(result.is_err());

        let pred = r#"cost <= 100"#; // <
        let result = ParseDeletePredicate::parse_predicate(table, pred);
        assert!(result.is_err());

        let pred = r#"cost gt 100"#; // >
        let result = ParseDeletePredicate::parse_predicate(table, pred);
        assert!(result.is_err());

        let pred = r#"city = cost = 100"#; // >
        let result = ParseDeletePredicate::parse_predicate(table, pred);
        assert!(result.is_err());
    }

    #[test]
    fn test_full_delete_pred() {
        let table = "test";
        let start = r#"1970-01-01T00:00:00Z"#; // this is nano 0
        let stop = r#"200"#;
        let pred = r#"cost != 100"#;

        let result = ParseDeletePredicate::try_new(table, start, stop, pred).unwrap();
        assert_eq!(result.start_time, 0);
        assert_eq!(result.stop_time, 200);

        let mut expected = vec![];
        let num: i64 = 100;
        let e = col("cost").not_eq(lit(num));
        expected.push(e);
        assert_eq!(result.predicate, expected);
    }

    #[test]
    fn test_full_delete_pred_invalid_time_range() {
        let table = "test";
        let start = r#"100"#;
        let stop = r#"50"#;
        let pred = r#"cost != 100"#;

        let result = ParseDeletePredicate::try_new(table, start, stop, pred);
        assert!(result.is_err());
    }

    #[test]
    fn test_full_delete_pred_invalid_pred() {
        let table = "test";
        let start = r#"100"#;
        let stop = r#"200"#;
        let pred = r#"cost > 100"#;

        let result = ParseDeletePredicate::try_new(table, start, stop, pred);
        assert!(result.is_err());
    }
}
