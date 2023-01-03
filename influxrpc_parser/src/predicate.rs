//! This module has logic to translate a sub-set of DataFusion expressions into
//! RPC Nodes (predicates).
use generated_types::{
    node::Comparison as RPCComparison, node::Logical as RPCLogical, node::Type as RPCType,
    node::Value as RPCValue, Node as RPCNode, Predicate as RPCPredicate,
};

use snafu::{ResultExt, Snafu};
use sqlparser::{
    ast::{BinaryOperator as Operator, Expr, Ident, Value},
    parser::Parser,
    tokenizer::Tokenizer,
};

// String and byte representation of a measurement name in a predicate.
const MEASUREMENT_COLUMN_NAME: &str = "_measurement";
const TAG_KEY_FIELD: [u8; 1] = [255];

// String and byte representation of a field name in a prediacte.
const FIELD_COLUMN_NAME: &str = "_field";
const TAG_KEY_MEASUREMENT: [u8; 1] = [0];

/// Parse Error
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("unable to parse '{:?}' ", source))]
    ExprParseError {
        source: sqlparser::parser::ParserError,
    },

    #[snafu(display("unable to parse '{}' into numerical value", value))]
    NumericalParseError { value: String, msg: String },

    #[snafu(display("unexpected value '{:?}'", value))]
    UnexpectedValue { value: Value },

    #[snafu(display("unexpected expression type: '{:?}'", expr))]
    UnexpectedExprType { expr: Expr },

    #[snafu(display("unexpected operator: '{:?}'", op))]
    UnexpectedBinaryOperator { op: Operator },

    #[snafu(display(
        "unsupported identifier type: '{:?}'. Supported Types: field, tag",
        ident
    ))]
    UnsupportedIdentType { ident: String },
}

/// Result type for Parser Cient
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Parses and then converts a SQL expression to and InfluxRPC predicate node.
///
/// Expects expressions like the following:
///
///   * server = 'host' AND "temp"::field > 22
///   * "env"::tag = 'eu' OR "env"::tag = 'us' OR "env"::tag = 'asia'
///   * "host" = 'a' AND ("temp"::field > 100.3 OR "cpu"::field = 'cpu-1')
///   * "_measurement" = 'cpu'
///
/// Notes:
///
///   * Tag keys can be optionally surrounded in double quotes.
///   * Use the identifiers ::tag or ::field to explicitly denote whether the
///     expression is on a tag or a field.
///   * The omission of ::field indicates that the expression is on a tag.
///   * Numbers are parsed into integers where possible, but fall back to floats
///   * Use parentheses to denote precedence.
///   * _measurement and _field will be correctly converted into the binary format.
///
/// Unsupported:
///   * Regex operators are not yet supported.
///   * Unsigned integers cannot yet be supported because there is no current
///     way to denote them (we need to add a `u` suffix support).
///
pub fn expr_to_rpc_predicate(expr: &str) -> Result<RPCPredicate> {
    let dialect = sqlparser::dialect::PostgreSqlDialect {};
    let mut tokenizer = Tokenizer::new(&dialect, expr);
    let tokens = tokenizer.tokenize().unwrap();
    let mut parser = Parser::new(&dialect).with_tokens(tokens);

    Ok(RPCPredicate {
        root: Some(build_node(
            &parser.parse_expr().context(ExprParseSnafu)?,
            false,
        )?),
    })
}

// Builds an RPCNode given the value Expr and the converted children
fn build_node(expr: &Expr, strings_are_regex: bool) -> Result<RPCNode> {
    match expr {
        Expr::Nested(expr) => make_node(
            RPCType::ParenExpression,
            vec![build_node(expr, strings_are_regex)?],
            None,
        ),
        Expr::Cast { expr, data_type } => match data_type {
            sqlparser::ast::DataType::Custom(ident, _modifiers) => {
                if let Some(Ident { value, .. }) = ident.0.get(0) {
                    // See https://docs.influxdata.com/influxdb/v1.8/query_language/explore-data/#syntax
                    match value.as_str() {
                        "field" => {
                            // extract field key identifier
                            if let Expr::Identifier(field) = &**expr {
                                return make_leaf(
                                    RPCType::FieldRef,
                                    RPCValue::FieldRefValue(field.value.clone()),
                                );
                            }
                            return UnexpectedExprTypeSnafu {
                                expr: *expr.clone(),
                            }
                            .fail();
                        }
                        "tag" => {
                            // extract tag key identifier
                            if let Expr::Identifier(tag) = &**expr {
                                return make_leaf(
                                    RPCType::TagRef,
                                    RPCValue::TagRefValue(make_tag_name(tag.value.clone())),
                                );
                            }
                            return UnexpectedExprTypeSnafu {
                                expr: *expr.clone(),
                            }
                            .fail();
                        }
                        _ => {} // fall through
                    };
                }
                UnsupportedIdentTypeSnafu {
                    ident: ident.to_string(),
                }
                .fail()
            }
            _ => UnsupportedIdentTypeSnafu {
                ident: data_type.to_string(),
            }
            .fail(),
        },
        Expr::Identifier(c) => {
            // Identifiers with no casting syntax (no :: present) are treated
            // as tag keys.
            make_leaf(
                RPCType::TagRef,
                RPCValue::TagRefValue(make_tag_name(c.value.clone())),
            )
        }
        Expr::Value(v) => match v {
            Value::Boolean(b) => make_lit(RPCValue::BoolValue(*b)),
            Value::Number(n, _) => make_lit(parse_number(n)?),
            Value::DoubleQuotedString(v)
            | Value::SingleQuotedString(v)
            | Value::HexStringLiteral(v)
            | Value::NationalStringLiteral(v) => {
                if strings_are_regex {
                    make_lit(RPCValue::RegexValue(v.clone()))
                } else {
                    make_lit(RPCValue::StringValue(v.clone()))
                }
            }
            _ => UnexpectedValueSnafu {
                value: v.to_owned(),
            }
            .fail(),
        },
        Expr::BinaryOp { left, op, right } => {
            let strings_are_regex =
                matches!(op, Operator::PGRegexMatch | Operator::PGRegexNotMatch);

            build_binary_node(
                build_node(left, strings_are_regex)?,
                op.clone(),
                build_node(right, strings_are_regex)?,
            )
        }
        _ => UnexpectedExprTypeSnafu {
            expr: expr.to_owned(),
        }
        .fail(),
    }
}

fn parse_number(number: &str) -> Result<RPCValue, Error> {
    match number.parse::<i64>() {
        Ok(n) => Ok(RPCValue::IntValue(n)),
        Err(_) => {
            let f = number
                .parse::<f64>()
                .map_err(|e| Error::NumericalParseError {
                    value: number.to_owned(),
                    msg: e.to_string(),
                })?;
            Ok(RPCValue::FloatValue(f))
        }
    }
}

fn build_binary_node(left: RPCNode, op: Operator, right: RPCNode) -> Result<RPCNode> {
    match op {
        Operator::Eq => make_comparison_node(left, RPCComparison::Equal, right),
        Operator::NotEq => make_comparison_node(left, RPCComparison::NotEqual, right),
        Operator::Lt => make_comparison_node(left, RPCComparison::Lt, right),
        Operator::LtEq => make_comparison_node(left, RPCComparison::Lte, right),
        Operator::Gt => make_comparison_node(left, RPCComparison::Gt, right),
        Operator::GtEq => make_comparison_node(left, RPCComparison::Gte, right),
        Operator::PGRegexMatch => make_comparison_node(left, RPCComparison::Regex, right),
        Operator::PGRegexNotMatch => make_comparison_node(left, RPCComparison::NotRegex, right),
        // logical nodes
        Operator::And => make_logical_node(left, RPCLogical::And, right),
        Operator::Or => make_logical_node(left, RPCLogical::Or, right),
        _ => UnexpectedBinaryOperatorSnafu { op }.fail(),
    }
}

fn make_tag_name(tag_name: String) -> Vec<u8> {
    if tag_name == MEASUREMENT_COLUMN_NAME {
        return TAG_KEY_MEASUREMENT.to_vec();
    } else if tag_name == FIELD_COLUMN_NAME {
        return TAG_KEY_FIELD.to_vec();
    }
    tag_name.as_bytes().to_owned()
}

// Create an RPCNode.
fn make_node(
    node_type: RPCType,
    children: Vec<RPCNode>,
    value: Option<RPCValue>,
) -> Result<RPCNode> {
    Ok(RPCNode {
        node_type: node_type as i32,
        children,
        value,
    })
}

// Create a comparison node, e.g., server > "foo"
fn make_comparison_node(left: RPCNode, cmp: RPCComparison, right: RPCNode) -> Result<RPCNode> {
    make_node(
        RPCType::ComparisonExpression,
        vec![left, right],
        Some(RPCValue::Comparison(cmp as i32)),
    )
}

// Create a logical node, e.g., ("server" > 'foo') AND ("host" = 'bar')
fn make_logical_node(left: RPCNode, op: RPCLogical, right: RPCNode) -> Result<RPCNode> {
    make_node(
        RPCType::LogicalExpression,
        vec![left, right],
        Some(RPCValue::Logical(op as i32)),
    )
}

// Create a leaf node.
fn make_leaf(node_type: RPCType, value: RPCValue) -> Result<RPCNode> {
    make_node(node_type, vec![], Some(value))
}

// Creates an RPC literal leaf node
fn make_lit(value: RPCValue) -> Result<RPCNode> {
    make_leaf(RPCType::Literal, value)
}

#[cfg(test)]
mod test {
    use super::*;

    // helper functions to programmatically create RPC node.
    //

    fn rpc_op_from_str(cmp: &str) -> RPCComparison {
        match cmp {
            "=" => RPCComparison::Equal,
            "!=" => RPCComparison::NotEqual,
            ">" => RPCComparison::Gt,
            ">=" => RPCComparison::Gte,
            "<" => RPCComparison::Lt,
            "<=" => RPCComparison::Lte,
            "~" => RPCComparison::Regex,
            "!~" => RPCComparison::NotRegex,
            _ => panic!("invalid comparator string: {:?}", cmp),
        }
    }

    // Creates a simple tag comparison expression.
    //
    // Input should be a simple expression as a string, for example:
    //
    // server_a = foo
    // host != bar
    //
    // N.B, does not support spaces in tag keys or values.
    fn make_tag_expr(input: &str) -> RPCNode {
        let parts = input.split_whitespace().collect::<Vec<_>>();
        assert_eq!(parts.len(), 3, "invalid input string: {:?}", input);

        let comparison = rpc_op_from_str(parts[1]);
        let is_regex =
            (comparison == RPCComparison::Regex) || (comparison == RPCComparison::NotRegex);

        // remove quoting from literal - whilst we need the quoting to parse
        // the sql statement correctly, the RPCNode for the literal would not
        // have the quoting present.
        let literal = parts[2].replace(['\'', '"'], "");
        let literal = if is_regex {
            RPCValue::RegexValue(literal)
        } else {
            RPCValue::StringValue(literal)
        };
        RPCNode {
            node_type: RPCType::ComparisonExpression as i32,
            children: vec![
                RPCNode {
                    node_type: RPCType::TagRef as i32,
                    children: vec![],
                    value: Some(RPCValue::TagRefValue(parts[0].as_bytes().to_owned())),
                },
                RPCNode {
                    node_type: RPCType::Literal as i32,
                    children: vec![],
                    value: Some(literal),
                },
            ],
            value: Some(RPCValue::Comparison(comparison as i32)),
        }
    }

    // Creates a simple field comparison expression.
    fn make_field_expr(field_key: &str, op: &str, value: RPCValue) -> RPCNode {
        RPCNode {
            node_type: RPCType::ComparisonExpression as i32,
            children: vec![
                RPCNode {
                    node_type: RPCType::FieldRef as i32,
                    children: vec![],
                    value: Some(RPCValue::FieldRefValue(field_key.to_owned())),
                },
                RPCNode {
                    node_type: RPCType::Literal as i32,
                    children: vec![],
                    value: Some(value),
                },
            ],
            value: Some(RPCValue::Comparison(rpc_op_from_str(op) as i32)),
        }
    }

    macro_rules! make_field_expr_types {
        ($(($name:ident, $type:ty, $variant:ident),)*) => {
            $(
                fn $name(field_key: &str, op: &str, value: $type) -> RPCNode {
                    make_field_expr(field_key, op, RPCValue::$variant(value))
                }
            )*
        };
    }

    make_field_expr_types! {
        (make_field_expr_i64, i64, IntValue),
        (make_field_expr_f64, f64, FloatValue),
        (make_field_expr_bool, bool, BoolValue),
        (make_field_expr_str, String, StringValue),
    }

    fn make_sql_expr(input: &str) -> RPCNode {
        let parsed = expr_to_rpc_predicate(input).unwrap();
        parsed.root.unwrap()
    }

    #[test]
    // Test that simple sqlparser binary expressions are converted into the
    // correct tag comparison nodes
    fn test_from_sql_expr_tag_comparisons() {
        let ops = vec!["=", "!=", ">", ">=", "<", "<=", "~", "!~"];
        let exprs = ops
            .into_iter()
            .map(|op| format!("server {} 'abc'", op))
            .collect::<Vec<_>>();

        for expr_str in exprs {
            let expr = make_sql_expr(&expr_str);
            let exp_rpc_node = make_tag_expr(&expr_str);
            assert_eq!(expr, exp_rpc_node)
        }

        // Using the double quoted syntax for a tag key.
        let expr = make_sql_expr(r#""my,stuttering,tag,key" != 'foo'"#);
        let exp_rpc_node = make_tag_expr("my,stuttering,tag,key != 'foo'");
        assert_eq!(expr, exp_rpc_node);

        // Using the explicit ::tag syntax works.
        let expr = make_sql_expr("server::tag = 'foo'");
        let exp_rpc_node = make_tag_expr("server = 'foo'");
        assert_eq!(expr, exp_rpc_node);

        // Using the syntax "server"::tag also works.
        let expr = make_sql_expr(r#""server"::tag = 'foo'"#);
        let exp_rpc_node = make_tag_expr("server = 'foo'");
        assert_eq!(expr, exp_rpc_node);
    }

    #[test]
    fn test_from_sql_expr_invalid_tag_comparisons() {
        let expr = expr_to_rpc_predicate("server::foo = 'bar'");
        assert!(matches!(expr, Err(Error::UnsupportedIdentType { .. })));

        let expr = expr_to_rpc_predicate("22.32::field != 'abc'");
        assert!(matches!(expr, Err(Error::UnexpectedExprType { .. })));
    }

    #[test]
    fn test_from_sql_expr_special_key_comparison() {
        let expr = make_sql_expr("_measurement = 'cpu'");
        let exp_rpc_node = make_comparison_node(
            RPCNode {
                node_type: RPCType::TagRef as i32,
                children: vec![],
                value: Some(RPCValue::TagRefValue(TAG_KEY_MEASUREMENT.to_vec())),
            },
            RPCComparison::Equal,
            RPCNode {
                node_type: RPCType::Literal as i32,
                children: vec![],
                value: Some(RPCValue::StringValue("cpu".to_owned())),
            },
        )
        .unwrap();
        assert_eq!(expr, exp_rpc_node);

        let expr = make_sql_expr("_field = 'cpu'");
        let exp_rpc_node = make_comparison_node(
            RPCNode {
                node_type: RPCType::TagRef as i32,
                children: vec![],
                value: Some(RPCValue::TagRefValue(TAG_KEY_FIELD.to_vec())),
            },
            RPCComparison::Equal,
            RPCNode {
                node_type: RPCType::Literal as i32,
                children: vec![],
                value: Some(RPCValue::StringValue("cpu".to_owned())),
            },
        )
        .unwrap();
        assert_eq!(expr, exp_rpc_node);
    }

    #[test]
    // Test that simple sqlparser binary expressions are converted into the
    // correct tag comparison nodes
    fn test_from_sql_expr_field_comparisons() {
        let ops = vec!["=", "!=", ">", ">=", "<", "<="];

        for op in ops {
            let exprs = vec![
                (
                    make_sql_expr(&format!("server::field {} 100", op)),
                    make_field_expr_i64("server", op, 100_i64),
                ),
                (
                    make_sql_expr(&format!("server::field {} 100.0", op)),
                    make_field_expr_f64("server", op, 100.0),
                ),
                (
                    make_sql_expr(&format!("server::field {} true", op)),
                    make_field_expr_bool("server", op, true),
                ),
                (
                    make_sql_expr(&format!("server::field {} 'Mice'", op)),
                    make_field_expr_str("server", op, "Mice".to_owned()),
                ),
            ];

            for (expr, exp_rpc_node) in exprs {
                assert_eq!(expr, exp_rpc_node)
            }
        }
    }

    #[test]
    fn test_from_sql_expr_logical_node() {
        let expr = make_sql_expr("temp::field >= 22.9 AND env = 'us-west'");
        let exp_rpc_node = make_logical_node(
            make_field_expr_f64("temp", ">=", 22.9),
            RPCLogical::And,
            make_tag_expr("env = us-west"),
        )
        .unwrap();

        assert_eq!(expr, exp_rpc_node);

        let expr = make_sql_expr(r#" "server" = 'a' OR env = 'us-west'"#);
        let exp_rpc_node = make_logical_node(
            make_tag_expr(r#"server = a"#),
            RPCLogical::Or,
            make_tag_expr("env = us-west"),
        )
        .unwrap();

        assert_eq!(expr, exp_rpc_node);

        let expr = make_sql_expr("env = 'usa' OR env = 'eu' OR env = 'asia'");
        let exp_rpc_node = make_logical_node(
            make_logical_node(
                make_tag_expr("env = usa"),
                RPCLogical::Or,
                make_tag_expr("env = eu"),
            )
            .unwrap(),
            RPCLogical::Or,
            make_tag_expr("env = asia"),
        )
        .unwrap();

        assert_eq!(expr, exp_rpc_node);
    }

    #[test]
    fn test_from_sql_expr_nested() {
        let expr = make_sql_expr("env = 'usa' OR (env = 'eu' AND temp::field = 'on')");

        let left_rpc_expr = make_tag_expr("env = 'usa'");
        let right_rpc_expr = RPCNode {
            node_type: RPCType::ParenExpression as i32,
            children: vec![make_logical_node(
                make_tag_expr("env = eu"),
                RPCLogical::And,
                make_field_expr_str("temp", "=", "on".to_owned()),
            )
            .unwrap()],
            value: None,
        };
        let exp_rpc_node =
            make_logical_node(left_rpc_expr, RPCLogical::Or, right_rpc_expr).unwrap();
        assert_eq!(expr, exp_rpc_node);
    }
}
