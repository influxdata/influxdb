//! parse string that should be done at client before sending to server

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

use datafusion::{
    logical_plan::{Column, Expr, Operator},
    scalar::ScalarValue,
};

use sqlparser::{
    ast::{BinaryOperator, Expr as SqlParserExpr, Ident, Statement, Value},
    dialect::GenericDialect,
    parser::Parser,
};

use thiserror::Error;

//use serde::{Deserialize, Serialize};

use chrono::DateTime;

use crate::timestamp;

/// Parse Error
#[derive(Debug, Error)]
pub enum Error {
    /// Invalid time format
    #[error("Invalid timestamp: {}", .0)]
    InvalidTimestamp(String),

    /// Invalid time range
    #[error("Invalid time range: ({}, {})", .0, .1)]
    InvalidTimeRange(String, String),

    /// Predicate syntax error
    #[error("Invalid predicate syntax: ({})", .0)]
    InvalidSyntax(String),

    /// Predicate semantics error
    #[error("Invalid predicate semantics: ({})", .0)]
    InvalidSemantics(String),

    /// Predicate include non supported expression
    #[error("Delete predicate must be conjunctive expressions of binary 'column_name = literal' or 'column_ame != literal': ({})", .0)]
    NotSupportPredicate(String),
}

/// Result type for Parser Cient
pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    /// parse the delete predicate which is a conjunctive expression of many
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
                Err(Error::InvalidSyntax(error_str))
            }
            Ok(mut stmt) => {
                if stmt.len() != 1 {
                    return Err(Error::InvalidSemantics(predicate.to_string()));
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
                            return Err(Error::NotSupportPredicate(predicate.to_string()));
                        }
                        Ok(exprs)
                    }
                    _ => Err(Error::InvalidSemantics(predicate.to_string())),
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
        // Th below code built to be compatible with
        //  https://github.com/influxdata/idpe/blob/master/influxdbv2/predicate/parser_test.go
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
                        relation: Some(table_name.to_string()),
                        name: value.to_string(),
                    }),
                    _ => return false, // not a column name
                };

                // verify if right is a literal
                let value = match &**right {
                    SqlParserExpr::Identifier(Ident {
                        value,
                        quote_style: _,
                    }) => Expr::Literal(ScalarValue::Utf8(Some(value.to_string()))),
                    SqlParserExpr::Value(Value::DoubleQuotedString(value)) => {
                        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())))
                    }
                    SqlParserExpr::Value(Value::SingleQuotedString(value)) => {
                        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())))
                    }
                    SqlParserExpr::Value(Value::NationalStringLiteral(value)) => {
                        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())))
                    }
                    SqlParserExpr::Value(Value::HexStringLiteral(value)) => {
                        Expr::Literal(ScalarValue::Utf8(Some(value.to_string())))
                    }
                    SqlParserExpr::Value(Value::Number(v, _)) => {
                        Expr::Literal(ScalarValue::Float64(Some(v.parse().unwrap())))
                    }
                    // NGA todo: how to now this is an integer?
                    SqlParserExpr::Value(Value::Boolean(v)) => {
                        Expr::Literal(ScalarValue::Boolean(Some(*v)))
                    }
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
                let time_result = timestamp(input);
                match time_result {
                    Ok((_, nano)) => Ok(nano),
                    Err(nano_err) => {
                        // wrong format, return both error
                        let error_str = format!("{}, {}", timestamp_err, nano_err);
                        Err(Error::InvalidTimestamp(error_str))
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
            return Err(Error::InvalidTimeRange(start.to_string(), stop.to_string()));
        }

        Ok((start_time, stop_time))
    }
}

#[cfg(test)]
mod test {
    use datafusion::logical_plan::{col, lit};
    use sqlparser::test_utils::number;

    use super::*;

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
        // It turn out this is not invalid but return1 123
        let input = r#"123gdb"#;
        let time = ParseDeletePredicate::parse_time(input).unwrap();
        assert_eq!(time, 123);
        //assert!(time.is_err());

        // must parse time
        // It turn out this is not invalid but return1 1970
        let input = r#"1970-01-01T00:00:00"#;
        let time = ParseDeletePredicate::parse_time(input).unwrap();
        assert_eq!(time, 1970);
        //assert!(time.is_err());

        // It turn out this is not invalid but return1 1971
        let input = r#"1971-02-01:30:21Z"#;
        let time = ParseDeletePredicate::parse_time(input).unwrap();
        assert_eq!(time, 1971);
        //assert!(time.is_err());
    }

    #[test]
    fn test_parse_timestamp_out_of_range() {
        let input = r#"99999999999999999999999999999999"#;
        let time = ParseDeletePredicate::parse_time(input);
        assert!(time.is_err());
    }

    #[test]
    fn test_sqlparser() {
        // test how to use sqlparser
        let dialect = sqlparser::dialect::GenericDialect {};

        // string from IOx management API
        let iox_delete_predicate = r#"city = Boston and cost !=100 and state != "MA""#;

        // convert it to Delete SQL
        let mut sql = "DELETE FROM table_name WHERE ".to_string();
        sql.push_str(iox_delete_predicate);

        // parse the delete sql
        let mut ast = Parser::parse_sql(&dialect, sql.as_str()).unwrap();
        println!("AST: {:#?}", ast);

        // verify the parsed content
        assert_eq!(ast.len(), 1);
        let stmt = ast.pop().unwrap();
        match stmt {
            Statement::Delete {
                table_name: _,
                selection,
                ..
            } => {
                // Verify selection
                let results = selection.unwrap();

                // city = Boston
                let left = SqlParserExpr::BinaryOp {
                    left: Box::new(SqlParserExpr::Identifier(Ident::new("city"))),
                    op: BinaryOperator::Eq,
                    right: Box::new(SqlParserExpr::Identifier(Ident::new("Boston"))),
                };

                // cost !=100
                let right = SqlParserExpr::BinaryOp {
                    left: Box::new(SqlParserExpr::Identifier(Ident::new("cost"))),
                    op: BinaryOperator::NotEq,
                    right: Box::new(SqlParserExpr::Value(number("100"))),
                };

                // city = Boston and cost !=100
                let left = SqlParserExpr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                };

                // state != "MA"  -- Note the double quote
                let right = SqlParserExpr::BinaryOp {
                    left: Box::new(SqlParserExpr::Identifier(Ident::new("state"))),
                    op: BinaryOperator::NotEq,
                    right: Box::new(SqlParserExpr::Identifier(Ident::with_quote('"', "MA"))),
                };

                // city = Boston and cost !=100 and state != "MA"
                let expected = SqlParserExpr::BinaryOp {
                    left: Box::new(left),
                    op: BinaryOperator::And,
                    right: Box::new(right),
                };

                assert_eq!(results, expected);
            }
            _ => {
                panic!("Your sql is not a delete statement");
            }
        }
    }

    #[test]
    fn test_parse_predicate() {
        let table = "test";

        let pred = r#"city= Boston and cost !=100 and state != "MA""#;
        let result = ParseDeletePredicate::parse_predicate(table, pred).unwrap();

        println!("{:#?}", result);

        let mut expected = vec![];
        let e = col("test.city").eq(lit("Boston"));
        expected.push(e);
        let e = col("test.cost").not_eq(lit(100.0));
        expected.push(e);
        let e = col("test.state").not_eq(lit("MA"));
        expected.push(e);

        assert_eq!(result, expected)
    }

    #[test]
    fn test_parse_predicate_invalid() {
        let table = "test";

        let pred = r#"city= Boston Or cost !=100 and state != "MA""#; // OR
        let result = ParseDeletePredicate::parse_predicate(table, pred);
        assert!(result.is_err());

        let pred = r#"city= Boston and cost !=100+1 and state != "MA""#; // 1001
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
        let e = col("test.cost").not_eq(lit(100.0));
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
