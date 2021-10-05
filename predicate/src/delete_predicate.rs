use std::convert::TryInto;

use chrono::DateTime;
use data_types::timestamp::TimestampRange;
use datafusion::logical_plan::{lit, Column, Expr, Operator};
use snafu::{ResultExt, Snafu};
use sqlparser::{
    ast::{BinaryOperator, Expr as SqlParserExpr, Ident, Statement, Value},
    dialect::GenericDialect,
    parser::Parser,
};

use crate::delete_expr::DeleteExpr;

const FLUX_TABLE: &str = "_measurement";

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

    #[snafu(display(r#"Unable to parse delete string '{}'"#, value))]
    DeleteInvalid {
        source: serde_json::Error,
        value: String,
    },

    #[snafu(display(
        r#"Invalid key which is either 'start', 'stop', or 'predicate': '{}'"#,
        value
    ))]
    DeleteKeywordInvalid { value: String },

    #[snafu(display(r#"Invalid timestamp or predicate value: '{}'"#, value))]
    DeleteValueInvalid { value: String },

    #[snafu(display(r#"Invalid JSON format of delete string '{}'"#, value))]
    DeleteObjectInvalid { value: String },

    #[snafu(display(r#"Invalid table name in delete '{}'"#, value))]
    DeleteTableInvalid { value: String },

    #[snafu(display(r#"Delete must include a start time and a stop time'{}'"#, value))]
    DeleteStartStopInvalid { value: String },
}

/// Result type for Parser Cient
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a parsed delete predicate for evaluation by the InfluxDB IOx
/// query engine.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeletePredicate {
    /// Only rows within this range are included in
    /// results. Other rows are excluded.
    pub range: TimestampRange,

    /// Optional arbitrary predicates, represented as list of
    /// expressions applied a logical conjunction (aka they
    /// are 'AND'ed together). Only rows that evaluate to TRUE for all
    /// these expressions should be returned. Other rows are excluded
    /// from the results.
    pub exprs: Vec<DeleteExpr>,
}

impl DeletePredicate {
    /// Parse and convert the delete grpc API into ParseDeletePredicate to send to server
    pub fn try_new(start: &str, stop: &str, predicate: &str) -> Result<Self> {
        // parse and check time range
        let (start_time, stop_time) = parse_time_range(start, stop)?;

        // Parse the predicate
        let delete_exprs = parse_predicate(predicate)?;

        Ok(Self {
            range: TimestampRange {
                start: start_time,
                end: stop_time,
            },
            exprs: delete_exprs,
        })
    }
}

impl From<DeletePredicate> for crate::predicate::Predicate {
    fn from(pred: DeletePredicate) -> Self {
        Self {
            table_names: None,
            field_columns: None,
            partition_key: None,
            range: Some(pred.range),
            exprs: pred.exprs.into_iter().map(|expr| expr.into()).collect(),
        }
    }
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

            let expr = Expr::BinaryExpr {
                left: Box::new(column),
                op,
                right: Box::new(value),
            };
            let expr: Result<DeleteExpr, _> = expr.try_into();
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
                    let error_str = format!("{}, {}", timestamp_err, nano_err);
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

// Note that this struct and its functions are used to parse FLUX DELETE,
// https://docs.influxdata.com/influxdb/v2.0/write-data/delete-data/, which happens before
// the parsing of timestamps and sql predicate. The examples below will show FLUX DELETE's syntax which is
// different from SQL syntax so we need this extra parsing step before invoking sqlparser to parse the
// sql-format predicates and timestamps
#[derive(Debug, Default, PartialEq, Clone)]
/// data of a parsed delete
pub struct ParsedDelete {
    /// Empty string, "", if no table specified
    pub table_name: String,
    pub start_time: String,
    pub stop_time: String,
    pub predicate: String,
}

/// Return parsed data of an influx delete:
/// A few input examples and their parsed results:
///   {"predicate":"_measurement=mytable AND host=\"Orient.local\"","start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}
///    => table_name="mytable", start_time="1970-01-01T00:00:00Z", end_time="2070-01-02T00:00:00Z", predicate="host=\"Orient.local\"""
///   {"predicate":"host=Orient.local and val != 50","start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}
///    => start_time="1970-01-01T00:00:00Z", end_time="2070-01-02T00:00:00Z", predicate="host=Orient.local and val != 50"
pub fn parse_delete(input: &str) -> Result<ParsedDelete> {
    let parsed_obj: serde_json::Value =
        serde_json::from_str(input).context(DeleteInvalid { value: input })?;
    let mut parsed_delete = ParsedDelete::default();

    if let serde_json::Value::Object(items) = parsed_obj {
        for item in items {
            // The value must be type String
            if let Some(val) = item.1.as_str() {
                match item.0.to_lowercase().as_str() {
                    "start" => parsed_delete.start_time = val.to_string(),
                    "stop" => parsed_delete.stop_time = val.to_string(),
                    "predicate" => parsed_delete.predicate = val.to_string(),
                    _ => {
                        return Err(Error::DeleteKeywordInvalid {
                            value: input.to_string(),
                        })
                    }
                }
            } else {
                return Err(Error::DeleteValueInvalid {
                    value: input.to_string(),
                });
            }
        }
    } else {
        return Err(Error::DeleteObjectInvalid {
            value: input.to_string(),
        });
    }

    // Start or stop is empty
    if parsed_delete.start_time.is_empty() || parsed_delete.stop_time.is_empty() {
        return Err(Error::DeleteStartStopInvalid {
            value: input.to_string(),
        });
    }

    // Extract table from the predicate if any
    if parsed_delete.predicate.contains(FLUX_TABLE) {
        // since predicate is a conjunctive expression, split them by "and"
        let predicate = parsed_delete
            .predicate
            .replace(" AND ", " and ")
            .replace(" ANd ", " and ")
            .replace(" And ", " and ")
            .replace(" AnD ", " and ");

        let split: Vec<&str> = predicate.split("and").collect();

        let mut predicate_no_table = "".to_string();
        for s in split {
            if s.contains(FLUX_TABLE) {
                // This should be in form "_measurement = <your_table_name>"
                // only <keep your_table_name> by replacing the rest with ""
                let table_name = s
                    .replace(FLUX_TABLE, "")
                    .replace("=", "")
                    .trim()
                    .to_string();
                // Do not support white spaces in table name
                if table_name.contains(' ') {
                    return Err(Error::DeleteTableInvalid {
                        value: input.to_string(),
                    });
                }
                parsed_delete.table_name = table_name;
            } else {
                // This is a normal column comparison, put it back to send to sqlparser later
                if !predicate_no_table.is_empty() {
                    predicate_no_table.push_str(" and ")
                }
                predicate_no_table.push_str(s.trim());
            }
        }
        parsed_delete.predicate = predicate_no_table;
    }

    Ok(parsed_delete)
}

#[cfg(test)]
mod tests {
    use crate::delete_expr::{Op, Scalar};

    use super::*;

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

        println!("{:#?}", result);

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

        let result = DeletePredicate::try_new(start, stop, pred).unwrap();
        assert_eq!(result.range.start, 0);
        assert_eq!(result.range.end, 200);

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

        let result = DeletePredicate::try_new(start, stop, pred);
        assert!(result.is_err());
    }

    #[test]
    fn test_full_delete_pred_invalid_pred() {
        let start = r#"100"#;
        let stop = r#"200"#;
        let pred = r#"cost > 100"#;

        let result = DeletePredicate::try_new(start, stop, pred);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_delete_full() {
        let delete_str = r#"{"predicate":"_measurement=mytable AND host=\"Orient.local\"","start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}"#;

        let expected = ParsedDelete {
            table_name: "mytable".to_string(),
            predicate: "host=\"Orient.local\"".to_string(),
            start_time: "1970-01-01T00:00:00Z".to_string(),
            stop_time: "2070-01-02T00:00:00Z".to_string(),
        };

        let result = parse_delete(delete_str).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_delete_no_table() {
        let delete_str = r#"{"start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z", "predicate":"host=\"Orient.local\""}"#;

        let expected = ParsedDelete {
            table_name: "".to_string(),
            predicate: "host=\"Orient.local\"".to_string(),
            start_time: "1970-01-01T00:00:00Z".to_string(),
            stop_time: "2070-01-02T00:00:00Z".to_string(),
        };

        let result = parse_delete(delete_str).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_delete_empty_predicate() {
        let delete_str =
            r#"{"start":"1970-01-01T00:00:00Z","predicate":"","stop":"2070-01-02T00:00:00Z"}"#;

        let expected = ParsedDelete {
            table_name: "".to_string(),
            predicate: "".to_string(),
            start_time: "1970-01-01T00:00:00Z".to_string(),
            stop_time: "2070-01-02T00:00:00Z".to_string(),
        };

        let result = parse_delete(delete_str).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_delete_no_predicate() {
        let delete_str = r#"{"start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}"#;

        let expected = ParsedDelete {
            table_name: "".to_string(),
            predicate: "".to_string(),
            start_time: "1970-01-01T00:00:00Z".to_string(),
            stop_time: "2070-01-02T00:00:00Z".to_string(),
        };

        let result = parse_delete(delete_str).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_delete_negative() {
        // invalid key
        let delete_str = r#"{"invalid":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}"#;
        let result = parse_delete(delete_str);
        let err = result.unwrap_err();
        assert!(err
            .to_string()
            .contains("Invalid key which is either 'start', 'stop', or 'predicate'"));

        // invalid timestamp value
        let delete_str = r#"{"start":123,"stop":"2070-01-02T00:00:00Z"}"#;
        let result = parse_delete(delete_str);
        let err = result.unwrap_err();
        assert!(err
            .to_string()
            .contains("Invalid timestamp or predicate value"));

        // invalid JSON
        let delete_str = r#"{"start":"1970-01-01T00:00:00Z",;"stop":"2070-01-02T00:00:00Z"}"#;
        let result = parse_delete(delete_str);
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Unable to parse delete string"));
    }
}
