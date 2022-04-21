use generated_types::{
    node::{Comparison, Type as NodeType, Value},
    Node, Predicate,
};

/// Create a predicate representing tag_name=tag_value in the horrible gRPC
/// structs
pub(crate) fn make_tag_predicate(
    tag_name: impl Into<String>,
    tag_value: impl Into<String>,
) -> Predicate {
    Predicate {
        root: Some(Node {
            node_type: NodeType::ComparisonExpression as i32,
            children: vec![
                Node {
                    node_type: NodeType::TagRef as i32,
                    children: vec![],
                    value: Some(Value::TagRefValue(tag_name.into().into())),
                },
                Node {
                    node_type: NodeType::Literal as i32,
                    children: vec![],
                    value: Some(Value::StringValue(tag_value.into())),
                },
            ],
            value: Some(Value::Comparison(Comparison::Equal as _)),
        }),
    }
}
