//! This module has logic to translate a sub-set of DataFusion expressions into
//! RPC Nodes (predicates).
use generated_types::{
    node::Comparison as RPCComparison, node::Logical as RPCLogical, node::Type as RPCType,
    node::Value as RPCValue, Node as RPCNode,
};

use snafu::Snafu;
use sqlparser::ast::{BinaryOperator as Operator, Expr, Ident, Value};

const TAG_KEY_FIELD: [u8; 1] = [255];
const TAG_KEY_MEASUREMENT: [u8; 1] = [0];

/// Parse Error
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("unable to parse '{}' into numerical value", value))]
    ParseError { value: String, msg: String },

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

/// Converts a sub-set of Datafusion expressions into RPCNodes.
pub fn expr_to_rpc_node(expr: Expr) -> Result<RPCNode> {
    build_node(&expr)
}

// Builds an RPCNode given the value Expr and the converted children
fn build_node(expr: &Expr) -> Result<RPCNode> {
    match expr {
        Expr::Cast { expr, data_type } => match data_type {
            sqlparser::ast::DataType::Custom(ident) => {
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
            Value::DoubleQuotedString(v) => make_lit(RPCValue::StringValue(v.clone())),
            Value::SingleQuotedString(v) => make_lit(RPCValue::StringValue(v.clone())),
            Value::HexStringLiteral(v) => make_lit(RPCValue::StringValue(v.clone())),
            Value::NationalStringLiteral(v) => make_lit(RPCValue::StringValue(v.clone())),
            _ => UnexpectedValueSnafu {
                value: v.to_owned(),
            }
            .fail(),
        },
        Expr::BinaryOp { left, op, right } => {
            build_binary_node(build_node(left)?, op.clone(), build_node(right)?)
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
            let f = number.parse::<f64>().map_err(|e| Error::ParseError {
                value: number.to_string(),
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
        // logical nodes
        Operator::And => make_logical_node(left, RPCLogical::And, right),
        Operator::Or => make_logical_node(left, RPCLogical::Or, right),
        _ => UnexpectedBinaryOperatorSnafu { op }.fail(),
    }
}

fn make_tag_name(tag_name: String) -> Vec<u8> {
    if tag_name == crate::rpc_predicate::MEASUREMENT_COLUMN_NAME {
        return TAG_KEY_MEASUREMENT.to_vec();
    } else if tag_name == crate::rpc_predicate::FIELD_COLUMN_NAME {
        return TAG_KEY_FIELD.to_vec();
    }
    tag_name.as_bytes().to_owned()
}

// Create an RPCNode.
fn make_node(node_type: RPCType, children: Vec<RPCNode>, value: RPCValue) -> Result<RPCNode> {
    Ok(RPCNode {
        node_type: node_type as i32,
        children,
        value: Some(value),
    })
}

// Create a comparison node, e.g., server > "foo"
fn make_comparison_node(left: RPCNode, cmp: RPCComparison, right: RPCNode) -> Result<RPCNode> {
    make_node(
        RPCType::ComparisonExpression,
        vec![left, right],
        RPCValue::Comparison(cmp as i32),
    )
}

// Create a logical node, e.g., ("server" > 'foo') AND ("host" = 'bar')
fn make_logical_node(left: RPCNode, op: RPCLogical, right: RPCNode) -> Result<RPCNode> {
    make_node(
        RPCType::LogicalExpression,
        vec![left, right],
        RPCValue::Logical(op as i32),
    )
}

// Create a leaf node.
fn make_leaf(node_type: RPCType, value: RPCValue) -> Result<RPCNode> {
    make_node(node_type, vec![], value)
}

// Creates an RPC literal leaf node
fn make_lit(value: RPCValue) -> Result<RPCNode> {
    make_leaf(RPCType::Literal, value)
}

#[cfg(test)]
mod test {
    use sqlparser::{parser::Parser, tokenizer::Tokenizer};

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

        // remove quoting from literal - whilst we need the quoting to parse
        // the sql statement correctly, the RPCNode for the literal would not
        // have the quoting present.
        let literal = parts[2].replace("'", "").replace("\"", "");
        make_comparison_node(
            RPCNode {
                node_type: RPCType::TagRef as i32,
                children: vec![],
                value: Some(RPCValue::TagRefValue(parts[0].as_bytes().to_owned())),
            },
            rpc_op_from_str(parts[1]),
            RPCNode {
                node_type: RPCType::Literal as i32,
                children: vec![],
                value: Some(RPCValue::StringValue(literal)),
            },
        )
        .unwrap()
    }

    // Creates a simple field comparison expression.
    fn make_field_expr(field_key: &str, op: &str, value: RPCValue) -> RPCNode {
        make_comparison_node(
            RPCNode {
                node_type: RPCType::FieldRef as i32,
                children: vec![],
                value: Some(RPCValue::FieldRefValue(field_key.to_owned())),
            },
            rpc_op_from_str(op),
            RPCNode {
                node_type: RPCType::Literal as i32,
                children: vec![],
                value: Some(value),
            },
        )
        .unwrap()
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

    // Create a sqlparser Expr from an input string.
    fn make_sql_expr(input: &str) -> Expr {
        let dialect = sqlparser::dialect::GenericDialect {};
        let mut tokenizer = Tokenizer::new(&dialect, input);
        let tokens = tokenizer.tokenize().unwrap();
        let mut parser = Parser::new(tokens, &dialect);

        parser.parse_expr().unwrap()
    }

    #[test]
    // Test that simple sqlparser binary expressions are converted into the
    // correct tag comparison nodes
    fn test_from_sql_expr_tag_comparisons() {
        let ops = vec!["=", "!=", ">", ">=", "<", "<="];
        let exprs = ops
            .into_iter()
            .map(|op| format!("server {} 'abc'", op))
            .collect::<Vec<_>>();

        for expr_str in exprs {
            let exrp = make_sql_expr(&expr_str);
            let exp_rpc_node = make_tag_expr(&expr_str);
            assert_eq!(expr_to_rpc_node(exrp).unwrap(), exp_rpc_node)
        }

        // Using the double quoted syntax for a tag key.
        let expr = make_sql_expr(r#""my,stuttering,tag,key" != 'foo'"#);
        let exp_rpc_node = make_tag_expr("my,stuttering,tag,key != 'foo'");
        assert_eq!(expr_to_rpc_node(expr).unwrap(), exp_rpc_node);

        // Using the explicit ::tag syntax works.
        let exrp = make_sql_expr("server::tag = 'foo'");
        let exp_rpc_node = make_tag_expr("server = 'foo'");
        assert_eq!(expr_to_rpc_node(exrp).unwrap(), exp_rpc_node);

        // Using the syntax "server"::tag also works.
        let exrp = make_sql_expr("\"server\"::tag = 'foo'");
        let exp_rpc_node = make_tag_expr("server = 'foo'");
        assert_eq!(expr_to_rpc_node(exrp).unwrap(), exp_rpc_node);

        // Converting a binary expression with a regex matches is not
        // currently implemented.
        let expr = make_sql_expr("server ~ Abc");
        assert!(matches!(
            expr_to_rpc_node(expr),
            Err(Error::UnexpectedBinaryOperator { .. })
        ));
    }

    #[test]
    fn test_from_sql_expr_invalid_tag_comparisons() {
        let expr = make_sql_expr("server::foo = 'bar'");
        let got = expr_to_rpc_node(expr);
        assert!(matches!(got, Err(Error::UnsupportedIdentType { .. })));

        let expr = make_sql_expr("22.32::field != 'abc'");
        let got = expr_to_rpc_node(expr);
        assert!(matches!(got, Err(Error::UnexpectedExprType { .. })));
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
                    make_field_expr_str("server", op, "Mice".to_string()),
                ),
            ];

            for (expr, exp_rpc_node) in exprs {
                assert_eq!(expr_to_rpc_node(expr).unwrap(), exp_rpc_node)
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

        assert_eq!(expr_to_rpc_node(expr).unwrap(), exp_rpc_node);

        let expr = make_sql_expr(r#" "server" = 'a' OR env = 'us-west'"#);
        let exp_rpc_node = make_logical_node(
            make_tag_expr(r#"server = a"#),
            RPCLogical::Or,
            make_tag_expr("env = us-west"),
        )
        .unwrap();

        assert_eq!(expr_to_rpc_node(expr).unwrap(), exp_rpc_node);
    }
}
