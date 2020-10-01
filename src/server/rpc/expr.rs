//! This module has logic to translate gRPC `Predicate` nodes into
//! delorean_storage_interface::Predicates
use delorean_arrow::datafusion::{
    logical_plan::{Expr, Operator},
    scalar::ScalarValue,
};
use delorean_generated_types::{
    node::Comparison as RPCComparison, node::Logical as RPCLogical, node::Value as RPCValue,
    Node as RPCNode, Predicate as RPCPredicate,
};
use delorean_storage::Predicate as StoragePredicate;
use snafu::Snafu;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error creating predicate: Unexpected empty predicate: Node"))]
    EmptyPredicateNode {},

    #[snafu(display("Error creating predicate: Unexpected empty predicate: Value"))]
    EmptyPredicateValue {},

    #[snafu(display(
        "Error creating predicate: Regular expression predicates are not supported: {}",
        regexp
    ))]
    RegExpLiteralNotSupported { regexp: String },

    #[snafu(display("Error creating predicate: Regular expression predicates are not supported"))]
    RegExpNotSupported {},

    #[snafu(display(
        "Error creating predicate: Not Regular expression predicates are not supported"
    ))]
    NotRegExpNotSupported {},

    #[snafu(display("Error creating predicate: StartsWith comparisons not supported"))]
    StartsWithNotSupported {},

    #[snafu(display(
        "Error creating predicate: Unexpected children for predicate: {:?}",
        value
    ))]
    UnexpectedChildren { value: RPCValue },

    #[snafu(display("Error creating predicate: Unknown logical node type: {}", logical))]
    UnknownLogicalNode { logical: i32 },

    #[snafu(display(
        "Error creating predicate: Unknown comparison node type: {}",
        comparison
    ))]
    UnknownComparisonNode { comparison: i32 },

    #[snafu(display(
        "Error creating predicate: Unsupported number of children in binary operator {:?}: {} (must be 2)",
        op,
        num_children
    ))]
    UnsupportedNumberOfChildren { op: Operator, num_children: usize },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// converts the Node (predicate tree) into a datafusion based Expr
/// for evaluation
pub fn convert_predicate(predicate: Option<RPCPredicate>) -> Result<Option<StoragePredicate>> {
    match predicate {
        // no input predicate, is fine
        None => Ok(None),
        Some(predicate) => match predicate.root {
            None => EmptyPredicateNode {}.fail(),
            Some(node) => Ok(Some(StoragePredicate {
                expr: convert_node(node)?,
            })),
        },
    }
}

// converts a Node from the RPC layer into a datafusion logical expr
fn convert_node(node: RPCNode) -> Result<Expr> {
    let RPCNode { children, value } = node;
    let inputs = children
        .into_iter()
        .map(convert_node)
        .collect::<Result<Vec<_>>>()?;

    match value {
        // I don't really understand what a None value
        // means. So until we know they are needed error on
        // them here instead.
        None => EmptyPredicateValue {}.fail(),
        Some(value) => build_node(value, inputs),
    }
}

// Builds an Expr given the Value and the converted children
fn build_node(value: RPCValue, inputs: Vec<Expr>) -> Result<Expr> {
    // Only logical / comparison ops can have inputs.
    let can_have_children = match &value {
        RPCValue::Logical(_) | RPCValue::Comparison(_) => true,
        _ => false,
    };

    if !can_have_children && !inputs.is_empty() {
        return UnexpectedChildren { value }.fail();
    }

    match value {
        RPCValue::StringValue(s) => Ok(Expr::Literal(ScalarValue::Utf8(Some(s)))),
        RPCValue::BoolValue(b) => Ok(Expr::Literal(ScalarValue::Boolean(Some(b)))),
        RPCValue::IntValue(v) => Ok(Expr::Literal(ScalarValue::Int64(Some(v)))),
        RPCValue::UintValue(v) => Ok(Expr::Literal(ScalarValue::UInt64(Some(v)))),
        RPCValue::FloatValue(f) => Ok(Expr::Literal(ScalarValue::Float64(Some(f)))),
        RPCValue::RegexValue(regexp) => RegExpLiteralNotSupported { regexp }.fail(),
        RPCValue::TagRefValue(tag_name) => Ok(Expr::Column(tag_name)),
        RPCValue::FieldRefValue(field_name) => Ok(Expr::Column(field_name)),
        RPCValue::Logical(logical) => build_logical_node(logical, inputs),
        RPCValue::Comparison(comparison) => build_comparison_node(comparison, inputs),
    }
}

/// Creates an expr from a "Logical" Node
fn build_logical_node(logical: i32, inputs: Vec<Expr>) -> Result<Expr> {
    // This ideally could be a match, but I couldn't find a safe way
    // to match an i32 to RPCLogical except for ths

    if logical == RPCLogical::And as i32 {
        build_binary_expr(Operator::And, inputs)
    } else if logical == RPCLogical::Or as i32 {
        build_binary_expr(Operator::Or, inputs)
    } else {
        UnknownLogicalNode { logical }.fail()
    }
}

/// Creates an expr from a "Comparsion" Node
fn build_comparison_node(comparison: i32, inputs: Vec<Expr>) -> Result<Expr> {
    // again, this would ideally be a match but I couldn't figure out how to
    // match an i32 to the enum values

    if comparison == RPCComparison::Equal as i32 {
        build_binary_expr(Operator::Eq, inputs)
    } else if comparison == RPCComparison::NotEqual as i32 {
        build_binary_expr(Operator::NotEq, inputs)
    } else if comparison == RPCComparison::StartsWith as i32 {
        StartsWithNotSupported {}.fail()
    } else if comparison == RPCComparison::Regex as i32 {
        RegExpNotSupported {}.fail()
    } else if comparison == RPCComparison::NotRegex as i32 {
        NotRegExpNotSupported {}.fail()
    } else if comparison == RPCComparison::Lt as i32 {
        build_binary_expr(Operator::Lt, inputs)
    } else if comparison == RPCComparison::Lte as i32 {
        build_binary_expr(Operator::LtEq, inputs)
    } else if comparison == RPCComparison::Gt as i32 {
        build_binary_expr(Operator::Gt, inputs)
    } else if comparison == RPCComparison::Gte as i32 {
        build_binary_expr(Operator::GtEq, inputs)
    } else {
        UnknownComparisonNode { comparison }.fail()
    }
}

/// Creates a datafusion binary expression with the specified operator
fn build_binary_expr(op: Operator, inputs: Vec<Expr>) -> Result<Expr> {
    // convert input vector to options so we can "take" elements out of it
    let mut inputs = inputs.into_iter().map(Some).collect::<Vec<_>>();

    let num_children = inputs.len();
    match num_children {
        2 => Ok(Expr::BinaryExpr {
            left: Box::new(inputs[0].take().unwrap()),
            op,
            right: Box::new(inputs[1].take().unwrap()),
        }),
        _ => UnsupportedNumberOfChildren { op, num_children }.fail(),
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_convert_predicate_none() -> Result<()> {
        let res = convert_predicate(None);
        assert!(res.is_ok());
        assert!(res.unwrap().is_none());
        Ok(())
    }

    #[test]
    fn test_convert_predicate_empty() -> Result<()> {
        let rpc_predicate = RPCPredicate { root: None };

        let res = convert_predicate(Some(rpc_predicate));
        let expected_error = "Unexpected empty predicate: Node";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
        Ok(())
    }

    #[test]
    fn test_convert_predicate_good() -> Result<()> {
        // host > 5.0
        let field_ref = RPCNode {
            children: vec![],
            value: Some(RPCValue::FieldRefValue(String::from("host"))),
        };
        let iconst = RPCNode {
            children: vec![],
            value: Some(RPCValue::FloatValue(5.0)),
        };
        let comparison = RPCNode {
            children: vec![field_ref, iconst],
            value: Some(RPCValue::Comparison(RPCComparison::Gt as i32)),
        };

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let converted_expr = convert_predicate(Some(rpc_predicate))
            .expect("successfully converting predicate")
            .expect("Had non empty predicate");

        let expected_expr = Expr::BinaryExpr {
            left: Box::new(Expr::Column(String::from("host"))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Float64(Some(5.0)))),
        };

        // compare the expression using their string representations
        // as Expr can't be compared directly.
        let converted_expr = format!("{:?}", converted_expr.expr);
        let expected_expr = format!("{:?}", expected_expr);

        assert_eq!(
            expected_expr, converted_expr,
            "expected '{:#?}' doesn't match actual '{:#?}'",
            expected_expr, converted_expr
        );

        Ok(())
    }

    #[test]
    fn test_convert_predicate_no_children() -> Result<()> {
        let comparison = RPCNode {
            children: vec![],
            value: Some(RPCValue::Comparison(RPCComparison::Gt as i32)),
        };

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let res = convert_predicate(Some(rpc_predicate));
        let expected_error = "Error creating predicate: Unsupported number of children in binary operator Gt: 0 (must be 2)";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
        Ok(())
    }

    #[test]
    fn test_convert_predicate_comparison_bad_values() -> Result<()> {
        // Send in invalid input to simulate a bad actor
        let iconst = RPCNode {
            children: vec![],
            value: Some(RPCValue::FloatValue(5.0)),
        };

        let comparison = RPCNode {
            children: vec![iconst.clone(), iconst],
            value: Some(RPCValue::Comparison(42)), // 42 is not a valid comparison value
        };

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let res = convert_predicate(Some(rpc_predicate));
        let expected_error = "Error creating predicate: Unknown comparison node type: 42";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
        Ok(())
    }

    #[test]
    fn test_convert_predicate_logical_bad_values() -> Result<()> {
        // Send in invalid input to simulate a bad actor
        let iconst = RPCNode {
            children: vec![],
            value: Some(RPCValue::FloatValue(5.0)),
        };

        let comparison = RPCNode {
            children: vec![iconst.clone(), iconst],
            value: Some(RPCValue::Logical(42)), // 42 is not a valid logical value
        };

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let res = convert_predicate(Some(rpc_predicate));
        let expected_error = "Error creating predicate: Unknown logical node type: 42";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
        Ok(())
    }

    /// Return the dislay formay of the resulting error, or
    /// 'UNEXPECTED SUCCESS' if `res` is not an error.
    fn error_result_to_string<R>(res: Result<R>) -> String {
        match res {
            Ok(_) => "UNEXPECTED SUCCESS".into(),
            Err(e) => format!("{}", e),
        }
    }
}
