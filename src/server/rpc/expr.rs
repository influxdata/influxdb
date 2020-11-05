//! This module has logic to translate gRPC `Predicate` nodes into the
//! native storage system predicate form,  `storage::Predicates`

use std::convert::TryFrom;

use arrow_deps::datafusion::{
    logical_plan::{Expr, Operator},
    scalar::ScalarValue,
};
use generated_types::{
    node::Comparison as RPCComparison, node::Logical as RPCLogical, node::Value as RPCValue,
    Node as RPCNode, Predicate as RPCPredicate,
};
use snafu::{ResultExt, Snafu};
use storage::predicate::PredicateBuilder;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error creating predicate: Unexpected empty predicate: Node"))]
    EmptyPredicateNode {},

    #[snafu(display("Error creating predicate: Unexpected empty predicate: Value"))]
    EmptyPredicateValue {},

    #[snafu(display("Internal error: found measurement tag reference in unexpected location"))]
    InternalInvalidMeasurementReference {},

    #[snafu(display("Internal error: found field tag reference in unexpected location"))]
    InternalInvalidFieldReference {},

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

    #[snafu(display("Error converting tag_name to utf8: {}", source))]
    ConvertingTagName { source: std::string::FromUtf8Error },

    #[snafu(display("Error converting field_name to utf8: {}", source))]
    ConvertingFieldName { source: std::string::FromUtf8Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A trait for adding gRPC specific nodes to the generic predicate builder
pub trait AddRPCNode
where
    Self: Sized,
{
    fn rpc_predicate(self, predicate: Option<RPCPredicate>) -> Result<Self>;
}

impl AddRPCNode for PredicateBuilder {
    /// Adds the predicates represented by the Node (predicate tree)
    /// into predicates that can be evaluted by the storage system
    ///
    /// RPC predicates can have several different types of 'predicate' embedded in them.
    ///
    /// Predicates on tag value (where a tag is a column)
    ///
    /// Predicates on field value (where field is also a column)
    ///
    /// Predicates on 'measurement name' (encoded as tag_ref=\x00), aka select from a particular table
    ///
    /// Predicates on 'field name' (encoded as tag_ref=\xff), aka select only specific fields
    ///
    /// This code pulls apart the predicates, if any, into a StoragePredicate that breaks the predicate apart
    fn rpc_predicate(self, rpc_predicate: Option<RPCPredicate>) -> Result<Self> {
        match rpc_predicate {
            // no input predicate, is fine
            None => Ok(self),
            Some(rpc_predicate) => {
                match rpc_predicate.root {
                    None => EmptyPredicateNode {}.fail(),
                    Some(node) => {
                        // normalize so the rest of the passes can deal with fewer cases
                        let node = normalize_node(node)?;

                        // step one is to flatten any AND tree into a vector of conjucts
                        let conjuncts = flatten_ands(node, Vec::new())?;
                        conjuncts.into_iter().try_fold(self, convert_simple_node)
                    }
                }
            }
        }
    }
}

/// cleans up / normalizes the input in preparation for other
/// processing. Noramlizations performed:
///
/// 1. Flatten `None` value nodes with `children` of length 1 (semantically the
/// same as the child itself). Specifically, if the input is:
///
/// ```
/// Node {
///  value: None,
///  children: [child],
/// }
/// ```
///
/// Then the output is:
///
/// ```
/// child
/// ```
///
fn normalize_node(node: RPCNode) -> Result<RPCNode> {
    let RPCNode { children, value } = node;

    let mut normalized_children = children
        .into_iter()
        .map(normalize_node)
        .collect::<Result<Vec<_>>>()?;

    match (value, normalized_children.len()) {
        // Sometimes InfluxQL sends in a RPCNode with 1 child and no value
        // which seems some sort of wrapper -- unwrap this case
        (None, 1) => Ok(normalized_children.pop().unwrap()),
        // It is not clear what None means without exactly one child..
        (None, _) => EmptyPredicateValue {}.fail(),
        (Some(value), _) => {
            // performance any other normalizations needed
            Ok(RPCNode {
                children: normalized_children,
                value: Some(value),
            })
        }
    }
}

/// Converts the node and updates the `StoragePredicate` being built, as appropriate
///
/// It recognizes special predicate patterns and pulls them into
/// the fields on `StoragePredicate` for special processing. If no
/// patterns are matched, it falls back to a generic DataFusion Expr
fn convert_simple_node(builder: PredicateBuilder, node: RPCNode) -> Result<PredicateBuilder> {
    if let Ok(in_list) = InList::try_from(&node) {
        let InList { lhs, value_list } = in_list;

        // look for tag or measurement = <values>
        if let Some(RPCValue::TagRefValue(tag_name)) = lhs.value {
            if tag_name.is_measurement() {
                // add the table names as a predicate
                return Ok(builder.tables(value_list));
            } else if tag_name.is_field() {
                return Ok(builder.field_columns(value_list));
            }
        }
    }

    // If no special case applies, fall back to generic conversion
    let expr = convert_node_to_expr(node)?;

    Ok(builder.add_expr(expr))
}

/// converts a tree of (a AND (b AND c)) into [a, b, c]
fn flatten_ands(node: RPCNode, mut dst: Vec<RPCNode>) -> Result<Vec<RPCNode>> {
    // try to break it up, if possible
    if Some(RPCValue::Logical(RPCLogical::And as i32)) == node.value {
        let RPCNode { children, .. } = node;
        // try and add each child separately
        for child in children {
            dst = flatten_ands(child, dst)?;
        }
    } else {
        dst.push(node);
    }

    Ok(dst)
}

// Represents a predicate like <expr> IN (option1, option2, option3, ....)
//
// use `try_from_node1 to convert a tree like as ((expr = option1) OR (expr = option2)) or (expr = option3)) ...
// into such a form
#[derive(Debug)]
struct InList {
    lhs: RPCNode,
    value_list: Vec<String>,
}

impl TryFrom<&RPCNode> for InList {
    type Error = &'static str;

    /// If node represents an OR tree like (expr = option1) OR (expr=option2)... extracts
    /// an InList like expr IN (option1, option2)
    fn try_from(node: &RPCNode) -> Result<Self, &'static str> {
        InListBuilder::default().append(node)?.build()
    }
}

impl InList {
    fn new(lhs: RPCNode) -> Self {
        Self {
            lhs,
            value_list: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
struct InListBuilder {
    inner: Option<InList>,
}

impl InListBuilder {
    /// given we are converting and expression like (self) OR (rhs)
    ///
    /// attempts to flatten rhs into self
    ///
    /// For example, if we are at self OR (foo = 'bar') and self.lhs
    /// is foo, will add 'bar' to value_list
    fn append(self, node: &RPCNode) -> Result<Self, &'static str> {
        // lhs = rhs
        if Some(RPCValue::Comparison(RPCComparison::Equal as i32)) == node.value {
            assert_eq!(node.children.len(), 2);
            let lhs = &node.children[0];
            let rhs = &node.children[1];
            self.append_equal(lhs, rhs)
        }
        // lhs OR rhs
        else if Some(RPCValue::Logical(RPCLogical::Or as i32)) == node.value {
            assert_eq!(node.children.len(), 2);

            let lhs = &node.children[0];
            let rhs = &node.children[1];

            // recurse down both sides
            self.append(lhs).and_then(|s| s.append(rhs))
        } else {
            Err("Found something other than equal or OR")
        }
    }

    // append lhs = rhs expression, if possible, return None if not
    fn append_equal(mut self, lhs: &RPCNode, rhs: &RPCNode) -> Result<Self, &'static str> {
        let mut in_list = self
            .inner
            .take()
            .unwrap_or_else(|| InList::new(lhs.clone()));

        // lhs = rhs as String
        if let Some(RPCValue::StringValue(string_value)) = &rhs.value {
            if &in_list.lhs == lhs {
                in_list.value_list.push(string_value.clone());
                self.inner = Some(in_list);
                Ok(self)
            } else {
                Err("lhs did not match")
            }
        } else {
            Err("rhs wasn't a string")
        }
    }

    // consume self and return the built InList
    fn build(self) -> Result<InList, &'static str> {
        self.inner.ok_or("No sub expressions found")
    }
}

// encodes the magic special bytes that the storage gRPC layer uses to
// encode measurement name and field name as tag
pub trait SpecialTagKeys {
    /// Return true if this tag key actually refers to a measurement
    /// name (e.g. _measurement or _m)
    fn is_measurement(&self) -> bool;

    /// Return true if this tag key actually refers to a field
    /// name (e.g. _field or _f)
    fn is_field(&self) -> bool;
}

impl SpecialTagKeys for Vec<u8> {
    fn is_measurement(&self) -> bool {
        self.as_slice() == [0]
    }

    /// Return true if this tag key actually refers to a field
    /// name (e.g. _field or _f)
    fn is_field(&self) -> bool {
        self.as_slice() == [255]
    }
}

impl SpecialTagKeys for String {
    fn is_measurement(&self) -> bool {
        self.as_bytes() == [0]
    }

    /// Return true if this tag key actually refers to a field
    /// name (e.g. _field or _f)
    fn is_field(&self) -> bool {
        self.as_bytes() == [255]
    }
}

// converts a Node from the RPC layer into a datafusion logical expr
fn convert_node_to_expr(node: RPCNode) -> Result<Expr> {
    let RPCNode { children, value } = node;
    let inputs = children
        .into_iter()
        .map(convert_node_to_expr)
        .collect::<Result<Vec<_>>>()?;

    let value = value.expect("Normalization removed all None values");
    build_node(value, inputs)
}

fn make_tag_name(tag_name: Vec<u8>) -> Result<String> {
    // These should have been handled at a higher level -- if we get
    // here it is too late
    if tag_name.is_measurement() {
        InternalInvalidMeasurementReference.fail()
    } else if tag_name.is_field() {
        InternalInvalidFieldReference.fail()
    } else {
        String::from_utf8(tag_name).context(ConvertingTagName)
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
        RPCValue::TagRefValue(tag_name) => Ok(Expr::Column(make_tag_name(tag_name)?)),
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

    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn test_convert_predicate_none() {
        let predicate = PredicateBuilder::default()
            .rpc_predicate(None)
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
    }

    #[test]
    fn test_convert_predicate_empty() {
        let rpc_predicate = RPCPredicate { root: None };

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Unexpected empty predicate: Node";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    #[test]
    fn test_convert_predicate_good() {
        let (comparison, expected_expr) = make_host_comparison();

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .expect("successfully converting predicate")
            .build();

        assert_eq!(predicate.exprs.len(), 1);
        let converted_expr = &predicate.exprs[0];

        // compare the expression using their string representations
        // as Expr can't be compared directly.
        let converted_expr = format!("{:?}", converted_expr);
        let expected_expr = format!("{:?}", expected_expr);

        assert_eq!(
            expected_expr, converted_expr,
            "expected '{:#?}' doesn't match actual '{:#?}'",
            expected_expr, converted_expr
        );
    }

    #[test]
    fn test_convert_predicate_no_children() {
        let comparison = RPCNode {
            children: vec![],
            value: Some(RPCValue::Comparison(RPCComparison::Gt as i32)),
        };

        let rpc_predicate = RPCPredicate {
            root: Some(comparison),
        };

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Error creating predicate: Unsupported number of children in binary operator Gt: 0 (must be 2)";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    #[test]
    fn test_convert_predicate_comparison_bad_values() {
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

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Error creating predicate: Unknown comparison node type: 42";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    #[test]
    fn test_convert_predicate_logical_bad_values() {
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

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Error creating predicate: Unknown logical node type: 42";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    #[test]
    fn test_convert_predicate_field_selection() {
        let field_selection = make_field_ref_node("field1");

        let rpc_predicate = RPCPredicate {
            root: Some(field_selection),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
        assert!(predicate.table_names.is_none());
        assert_eq!(predicate.field_columns, Some(to_set(&["field1"])));
        assert!(predicate.range.is_none());
    }

    #[test]
    fn test_convert_predicate_field_selection_wrapped() {
        // test wrapping the whole predicate in a None value (aka what influxql does for some reason
        let field_selection = make_field_ref_node("field1");
        let wrapped = RPCNode {
            children: vec![field_selection],
            value: None,
        };

        let rpc_predicate = RPCPredicate {
            root: Some(wrapped),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
        assert!(predicate.table_names.is_none());
        assert_eq!(predicate.field_columns, Some(to_set(&["field1"])));
        assert!(predicate.range.is_none());
    }

    #[test]
    fn test_convert_predicate_multiple_field_selection() {
        let selection = make_or_node(make_field_ref_node("field1"), make_field_ref_node("field2"));
        let selection = make_or_node(selection, make_field_ref_node("field3"));

        let rpc_predicate = RPCPredicate {
            root: Some(selection),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
        assert!(predicate.table_names.is_none());
        assert_eq!(
            predicate.field_columns,
            Some(to_set(&["field1", "field2", "field3"]))
        );
        assert!(predicate.range.is_none());
    }

    // test multiple field restrictions and a general predicate
    #[test]
    fn test_convert_predicate_multiple_field_selection_and_predicate() {
        let (comparison, expected_expr) = make_host_comparison();

        let selection = make_or_node(make_field_ref_node("field1"), make_field_ref_node("field2"));

        let selection = make_and_node(selection, comparison);

        let rpc_predicate = RPCPredicate {
            root: Some(selection),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        // compare the expression using their string representations
        // as Expr can't be compared directly.
        assert_eq!(predicate.exprs.len(), 1);
        let converted_expr = format!("{:?}", predicate.exprs[0]);
        let expected_expr = format!("{:?}", expected_expr);

        assert_eq!(
            expected_expr, converted_expr,
            "expected '{:#?}' doesn't match actual '{:#?}'",
            expected_expr, converted_expr
        );
        assert!(predicate.table_names.is_none());

        assert_eq!(predicate.field_columns, Some(to_set(&["field1", "field2"])));
        assert!(predicate.range.is_none());
    }

    #[test]
    fn test_convert_predicate_measurement_selection() {
        let measurement_selection = make_measurement_ref_node("m1");

        let rpc_predicate = RPCPredicate {
            root: Some(measurement_selection),
        };

        let predicate = PredicateBuilder::default()
            .rpc_predicate(Some(rpc_predicate))
            .unwrap()
            .build();

        assert!(predicate.exprs.is_empty());
        assert_eq!(predicate.table_names, Some(to_set(&["m1"])));
        assert!(predicate.field_columns.is_none());
        assert!(predicate.range.is_none());
    }

    #[test]
    fn test_convert_predicate_unsupported_structure() {
        // Test (_f = "foo" and host > 5.0) OR (_m = "bar")
        // which is not something we know how to do

        let (comparison, _) = make_host_comparison();

        let unsupported = make_or_node(
            make_and_node(make_field_ref_node("foo"), comparison),
            make_measurement_ref_node("bar"),
        );

        let rpc_predicate = RPCPredicate {
            root: Some(unsupported),
        };

        let res = PredicateBuilder::default().rpc_predicate(Some(rpc_predicate));

        let expected_error = "Internal error: found field tag reference in unexpected location";
        let actual_error = error_result_to_string(res);
        assert!(
            actual_error.contains(expected_error),
            "expected '{}' not found in '{}'",
            expected_error,
            actual_error
        );
    }

    /// make a _f = 'field_name' type node
    fn make_field_ref_node(field_name: impl Into<String>) -> RPCNode {
        make_tag_ref_node(&[255], field_name)
    }

    /// make a _m = 'measurement_name' type node
    fn make_measurement_ref_node(field_name: impl Into<String>) -> RPCNode {
        make_tag_ref_node(&[0], field_name)
    }

    /// returns (RPCNode, and expected_expr for the "host > 5.0")
    fn make_host_comparison() -> (RPCNode, Expr) {
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

        let expected_expr = Expr::BinaryExpr {
            left: Box::new(Expr::Column(String::from("host"))),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Float64(Some(5.0)))),
        };

        (comparison, expected_expr)
    }

    fn make_tag_ref_node(tag_name: &[u8], field_name: impl Into<String>) -> RPCNode {
        let field_tag_ref_node = RPCNode {
            children: vec![],
            value: Some(RPCValue::TagRefValue(tag_name.to_vec())),
        };

        let string_node = RPCNode {
            children: vec![],
            value: Some(RPCValue::StringValue(field_name.into())),
        };

        RPCNode {
            children: vec![field_tag_ref_node, string_node],
            value: Some(RPCValue::Comparison(RPCComparison::Equal as i32)),
        }
    }

    /// make n1 OR n2
    fn make_or_node(n1: RPCNode, n2: RPCNode) -> RPCNode {
        RPCNode {
            children: vec![n1, n2],
            value: Some(RPCValue::Logical(RPCLogical::Or as i32)),
        }
    }

    /// make n1 AND n2
    fn make_and_node(n1: RPCNode, n2: RPCNode) -> RPCNode {
        RPCNode {
            children: vec![n1, n2],
            value: Some(RPCValue::Logical(RPCLogical::And as i32)),
        }
    }

    fn to_set(v: &[&str]) -> BTreeSet<String> {
        v.iter().map(|s| s.to_string()).collect::<BTreeSet<_>>()
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
