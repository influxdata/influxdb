use crate::delete_expr::{df_to_expr, expr_to_df};
use chrono::DateTime;
use data_types::{DeleteExpr, DeletePredicate, TimestampRange};
use datafusion::{
    logical_expr::Operator,
    prelude::{binary_expr, lit, Column, Expr},
};
use snafu::Snafu;
use sqlparser::{
    ast::{BinaryOperator, Expr as SqlParserExpr, Ident, Statement, Value},
    dialect::GenericDialect,
    parser::Parser,
};

/// Parse Delete Predicates
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
    #[snafu(display("Delete predicate must be conjunctive expressions of binary 'column_name = literal' or 'column_name != literal': ({})", value))]
    NotSupportPredicate { value: String },
}

/// Result type for Parser Cient
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<DeletePredicate> for crate::Predicate {
    fn from(pred: DeletePredicate) -> Self {
        Self {
            field_columns: None,
            range: Some(pred.range),
            exprs: pred.exprs.into_iter().map(expr_to_df).collect(),
            value_expr: vec![],
        }
    }
}

/// Parse and convert the delete grpc API into ParseDeletePredicate to send to server
pub fn parse_delete_predicate(
    start_time: &str,
    stop_time: &str,
    predicate: &str,
) -> Result<DeletePredicate> {
    // parse and check time range
    let (start_time, stop_time) = parse_time_range(start_time, stop_time)?;

    // Parse the predicate
    let delete_exprs = parse_predicate(predicate)?;

    Ok(DeletePredicate {
        range: TimestampRange::new(start_time, stop_time),
        exprs: delete_exprs,
    })
}

/// Parse the predicate and convert it into datafusion expression
/// A delete predicate is a conjunctive expression of many
/// binary expressions of 'colum = constant' or 'column != constant'
///
fn parse_predicate(predicate: &str) -> Result<Vec<DeleteExpr>> {
    if predicate.is_empty() {
        return Ok(vec![]);
    }

    // "DELETE FROM table_name WHERE predicate"
    // Table name can be anything to have sqlparser work on the right sql syntax
    let mut sql = "DELETE FROM table_name WHERE ".to_string();
    sql.push_str(predicate);

    // parse the delete sql
    let dialect = GenericDialect {};
    let ast = Parser::parse_sql(&dialect, sql.as_str());
    match ast {
        Err(parse_err) => {
            let error_str = format!("{predicate}, {parse_err}");
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
                    selection: Some(expr),
                    ..
                }) => {
                    // split this expr into smaller binary if any
                    let mut exprs = vec![];
                    let split = split_members(&expr, &mut exprs);
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
fn split_members(predicate: &SqlParserExpr, predicates: &mut Vec<DeleteExpr>) -> bool {
    // The below code built to be compatible with
    // https://github.com/influxdata/influxdb/blob/master/predicate/parser_test.go
    match predicate {
        SqlParserExpr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            if !split_members(left, predicates) {
                return false;
            }
            if !split_members(right, predicates) {
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
                SqlParserExpr::Value(Value::DoubleQuotedString(value)) => lit(value.to_string()),
                SqlParserExpr::Value(Value::SingleQuotedString(value)) => lit(value.to_string()),
                SqlParserExpr::Value(Value::NationalStringLiteral(value)) => lit(value.to_string()),
                SqlParserExpr::Value(Value::HexStringLiteral(value)) => lit(value.to_string()),
                SqlParserExpr::Value(Value::Number(v, _)) => match v.parse::<i64>() {
                    Ok(v) => lit(v),
                    Err(_) => lit(v.parse::<f64>().unwrap()),
                },
                SqlParserExpr::Value(Value::Boolean(v)) => lit(*v),
                _ => return false, // not a literal
            };

            let expr = binary_expr(column, op, value);
            let expr: Result<DeleteExpr, _> = df_to_expr(expr);
            match expr {
                Ok(expr) => {
                    predicates.push(expr);
                }
                Err(_) => {
                    // cannot convert
                    return false;
                }
            }
        }
        _ => return false,
    }

    true
}

/// Parse a time and return its time in nanosecond
fn parse_time(input: &str) -> Result<i64> {
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
                    let error_str = format!("{timestamp_err}, {nano_err}");
                    Err(Error::InvalidTimestamp { value: error_str })
                }
            }
        }
    }
}

/// Parse a time range [start, stop]
fn parse_time_range(start: &str, stop: &str) -> Result<(i64, i64)> {
    let start_time = parse_time(start)?;
    let stop_time = parse_time(stop)?;
    if start_time > stop_time {
        return Err(Error::InvalidTimeRange {
            start: start.to_string(),
            stop: stop.to_string(),
        });
    }

    Ok((start_time, stop_time))
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::{Op, Scalar};

    #[test]
    fn test_time_range_valid() {
        let start = r#"100"#;
        let stop = r#"100"#;
        let result = parse_time_range(start, stop).unwrap();
        let expected = (100, 100);
        assert_eq!(result, expected);

        let start = r#"100"#;
        let stop = r#"200"#;
        let result = parse_time_range(start, stop).unwrap();
        let expected = (100, 200);
        assert_eq!(result, expected);

        let start = r#"1970-01-01T00:00:00Z"#; // this is nano 0
        let stop = r#"1970-01-01T00:00:00Z"#;
        let result = parse_time_range(start, stop).unwrap();
        let expected = (0, 0);
        assert_eq!(result, expected);

        // let start = r#"1970-01-01T00:00:00Z"#;  // this is nano 0
        // let stop = r#"now()"#;  // -- Not working. Need to find a way to test this
        // let result = ParseDeletePredicate::parse_time_range(start, stop).unwrap();
        // let expected = (0, 0);
        // assert_eq!(result, expected);

        let start = r#"1970-01-01T00:00:00Z"#;
        let stop = r#"100"#;
        let result = parse_time_range(start, stop).unwrap();
        let expected = (0, 100);
        assert_eq!(result, expected);

        let start = r#"1970-01-01T00:00:00Z"#;
        let stop = r#"1970-01-01T00:01:00Z"#;
        let result = parse_time_range(start, stop).unwrap();
        let expected = (0, 60000000000);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_time_range_invalid() {
        let start = r#"100"#;
        let stop = r#"-100"#;
        let result = parse_time_range(start, stop);
        assert!(result.is_err());

        let start = r#"100"#;
        let stop = r#"50"#;
        let result = parse_time_range(start, stop);
        assert!(result.is_err());

        let start = r#"100"#;
        let stop = r#"1970-01-01T00:00:00Z"#;
        let result = parse_time_range(start, stop);
        assert!(result.is_err());

        let start = r#"1971-09-01T00:00:10Z"#;
        let stop = r#"1971-09-01T00:00:05Z"#;
        let result = parse_time_range(start, stop);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_timestamp() {
        let input = r#"123"#;
        let time = parse_time(input).unwrap();
        assert_eq!(time, 123);

        // must parse time
        let input = r#"1970-01-01T00:00:00Z"#;
        let time = parse_time(input).unwrap();
        assert_eq!(time, 0);

        let input = r#"1971-02-01T15:30:21Z"#;
        let time = parse_time(input).unwrap();
        assert_eq!(time, 34270221000000000);
    }

    #[test]
    fn test_parse_timestamp_negative() {
        let input = r#"-123"#;
        let time = parse_time(input).unwrap();
        assert_eq!(time, -123);
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        let input = r#"123gdb"#;
        parse_time(input).unwrap_err();

        let input = r#"1970-01-01T00:00:00"#;
        parse_time(input).unwrap_err();

        // It turn out this is not invalid but return1 1971
        let input = r#"1971-02-01:30:21Z"#;
        parse_time(input).unwrap_err();
    }

    #[test]
    fn test_parse_timestamp_out_of_range() {
        let input = r#"99999999999999999999999999999999"#;
        let time = parse_time(input);
        assert!(time.is_err());
    }

    #[test]
    fn test_parse_predicate() {
        let pred = r#"city= Boston and cost !=100 and state != "MA" AND temp=87.5"#;
        let result = parse_predicate(pred).unwrap();

        println!("{result:#?}");

        let expected = vec![
            DeleteExpr::new(
                "city".to_string(),
                Op::Eq,
                Scalar::String("Boston".to_string()),
            ),
            DeleteExpr::new("cost".to_string(), Op::Ne, Scalar::I64(100)),
            DeleteExpr::new(
                "state".to_string(),
                Op::Ne,
                Scalar::String("MA".to_string()),
            ),
            DeleteExpr::new("temp".to_string(), Op::Eq, Scalar::F64((87.5).into())),
        ];

        assert_eq!(result, expected)
    }

    #[test]
    fn test_parse_predicate_invalid() {
        let pred = r#"city= Boston Or cost !=100 and state != "MA""#; // OR
        let result = parse_predicate(pred);
        assert!(result.is_err());

        let pred = r#"city= Boston and cost !=100+1 and state != "MA""#; // 100 + 1
        let result = parse_predicate(pred);
        assert!(result.is_err());

        let pred = r#"cost > 100"#; // >
        let result = parse_predicate(pred);
        assert!(result.is_err());

        let pred = r#"cost <= 100"#; // <
        let result = parse_predicate(pred);
        assert!(result.is_err());

        let pred = r#"cost gt 100"#; // >
        let result = parse_predicate(pred);
        assert!(result.is_err());

        let pred = r#"city = cost = 100"#; // >
        let result = parse_predicate(pred);
        assert!(result.is_err());
    }

    #[test]
    fn test_full_delete_pred() {
        let start = r#"1970-01-01T00:00:00Z"#; // this is nano 0
        let stop = r#"200"#;
        let pred = r#"cost != 100"#;

        let result = parse_delete_predicate(start, stop, pred).unwrap();
        assert_eq!(result.range.start(), 0);
        assert_eq!(result.range.end(), 200);

        let expected = vec![DeleteExpr::new(
            "cost".to_string(),
            Op::Ne,
            Scalar::I64(100),
        )];
        assert_eq!(result.exprs, expected);
    }

    #[test]
    fn test_full_delete_pred_invalid_time_range() {
        let start = r#"100"#;
        let stop = r#"50"#;
        let pred = r#"cost != 100"#;

        let result = parse_delete_predicate(start, stop, pred);
        assert!(result.is_err());
    }

    #[test]
    fn test_full_delete_pred_invalid_pred() {
        let start = r#"100"#;
        let stop = r#"200"#;
        let pred = r#"cost > 100"#;

        let result = parse_delete_predicate(start, stop, pred);
        assert!(result.is_err());
    }
}
