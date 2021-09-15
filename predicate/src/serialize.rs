//! Code to serialize and deserialize certain expressions.
//!
//! Note that [Ballista] also provides a serialization using [Protocol Buffers 3]. However the protocol is meant as a
//! communication channel between workers and clients of Ballista, not for long term preservation. For IOx we need a
//! more stable solution. Luckily we only need to support a very small subset of expression.
//!
//! [Ballista]: https://github.com/apache/arrow-datafusion/blob/22fcb3d7a68a56afbe12eab9e7d98f7b8de33703/ballista/rust/core/proto/ballista.proto
//! [Protocol Buffers 3]: https://developers.google.com/protocol-buffers/docs/proto3
use std::{collections::BTreeSet, ops::Deref};

use data_types::timestamp::TimestampRange;
use datafusion::{
    logical_plan::{Column, Expr, Operator},
    scalar::ScalarValue,
};
use generated_types::influxdata::iox::catalog::v1 as proto;
use snafu::{OptionExt, Snafu};

use crate::predicate::Predicate;

#[derive(Debug, Snafu)]
pub enum SerializeError {
    #[snafu(display("unsupported expression: {:?}", expr))]
    UnsupportedExpression { expr: Expr },

    #[snafu(display("unsupported operants: left {:?}; right {:?}", left, right))]
    UnsupportedOperants { left: Expr, right: Expr },

    #[snafu(display("unsupported scalar value: {:?}", value))]
    UnsupportedScalarValue { value: ScalarValue },

    #[snafu(display("unsupported operator: {:?}", op))]
    UnsupportedOperator { op: Operator },
}

/// Serialize IOx [`Predicate`] to a protobuf object.
pub fn serialize(predicate: &Predicate) -> Result<proto::Predicate, SerializeError> {
    let proto_predicate = proto::Predicate {
        table_names: serialize_optional_string_set(&predicate.table_names),
        field_columns: serialize_optional_string_set(&predicate.field_columns),
        partition_key: serialize_optional_string(&predicate.partition_key),
        range: serialize_timestamp_range(&predicate.range),
        exprs: predicate
            .exprs
            .iter()
            .map(serialize_expr)
            .collect::<Result<Vec<proto::Expr>, SerializeError>>()?,
    };
    Ok(proto_predicate)
}

fn serialize_optional_string_set(
    set: &Option<BTreeSet<String>>,
) -> Option<proto::OptionalStringSet> {
    set.as_ref().map(|set| proto::OptionalStringSet {
        values: set.iter().cloned().collect(),
    })
}

fn serialize_optional_string(s: &Option<String>) -> Option<proto::OptionalString> {
    s.as_ref()
        .map(|s| proto::OptionalString { value: s.clone() })
}

fn serialize_timestamp_range(r: &Option<TimestampRange>) -> Option<proto::TimestampRange> {
    r.as_ref().map(|r| proto::TimestampRange {
        start: r.start,
        end: r.end,
    })
}

fn serialize_expr(expr: &Expr) -> Result<proto::Expr, SerializeError> {
    match expr {
        Expr::BinaryExpr { left, op, right } => {
            let (column, scalar) = match (left.deref(), right.deref()) {
                // The delete predicate parser currently only supports `<column><op><value>`, not `<value><op><column>`,
                // however this could can easily be extended to support the latter case as well.
                (Expr::Column(column), Expr::Literal(value)) => {
                    let column = column.name.clone();
                    let scalar = serialize_scalar_value(value)?;

                    (column, scalar)
                }
                (other_left, other_right) => {
                    return Err(SerializeError::UnsupportedOperants {
                        left: other_left.clone(),
                        right: other_right.clone(),
                    });
                }
            };

            let op = serialize_operator(op)?;

            let proto_expr = proto::Expr {
                column,
                op: op.into(),
                scalar: Some(scalar),
            };
            Ok(proto_expr)
        }
        other => Err(SerializeError::UnsupportedExpression {
            expr: other.clone(),
        }),
    }
}

fn serialize_scalar_value(value: &ScalarValue) -> Result<proto::Scalar, SerializeError> {
    // see https://github.com/apache/arrow-datafusion/blob/195b69995db8044ce283d72fb78eb6b74b8842f5/datafusion/src/sql/planner.rs#L274-L295
    match value {
        ScalarValue::Utf8(Some(value)) => Ok(proto::Scalar {
            value: Some(proto::scalar::Value::ValueString(value.clone())),
        }),
        ScalarValue::Int64(Some(value)) => Ok(proto::Scalar {
            value: Some(proto::scalar::Value::ValueI64(*value)),
        }),
        ScalarValue::Float64(Some(value)) => Ok(proto::Scalar {
            value: Some(proto::scalar::Value::ValueF64(*value)),
        }),
        ScalarValue::Boolean(Some(value)) => Ok(proto::Scalar {
            value: Some(proto::scalar::Value::ValueBool(*value)),
        }),
        other => Err(SerializeError::UnsupportedScalarValue {
            value: other.clone(),
        }),
    }
}

fn serialize_operator(op: &Operator) -> Result<proto::Op, SerializeError> {
    match op {
        Operator::Eq => Ok(proto::Op::Eq),
        Operator::NotEq => Ok(proto::Op::Ne),
        other => Err(SerializeError::UnsupportedOperator { op: *other }),
    }
}

#[derive(Debug, Snafu)]
pub enum DeserializeError {
    #[snafu(display("unspecified operator"))]
    UnspecifiedOperator,

    #[snafu(display("illegal operator enum value: {}", value))]
    IllegalOperatorEnumValue { value: i32 },

    #[snafu(display("missing scalar value"))]
    MissingScalarValue,
}

/// Deserialize IOx [`Predicate`] from a protobuf object.
pub fn deserialize(
    proto_predicate: &proto::Predicate,
    table_name: &str,
) -> Result<Predicate, DeserializeError> {
    let predicate = Predicate {
        table_names: deserialize_optional_string_set(&proto_predicate.table_names),
        field_columns: deserialize_optional_string_set(&proto_predicate.field_columns),
        partition_key: deserialize_optional_string(&proto_predicate.partition_key),
        range: deserialize_timestamp_range(&proto_predicate.range),
        exprs: proto_predicate
            .exprs
            .iter()
            .map(|expr| deserialize_expr(expr, table_name))
            .collect::<Result<Vec<Expr>, DeserializeError>>()?,
    };
    Ok(predicate)
}

fn deserialize_optional_string_set(
    set: &Option<proto::OptionalStringSet>,
) -> Option<BTreeSet<String>> {
    set.as_ref().map(|set| set.values.iter().cloned().collect())
}

fn deserialize_optional_string(s: &Option<proto::OptionalString>) -> Option<String> {
    s.as_ref().map(|s| s.value.clone())
}

fn deserialize_timestamp_range(r: &Option<proto::TimestampRange>) -> Option<TimestampRange> {
    r.as_ref().map(|r| TimestampRange {
        start: r.start,
        end: r.end,
    })
}

fn deserialize_expr(proto_expr: &proto::Expr, table_name: &str) -> Result<Expr, DeserializeError> {
    let column = Column {
        relation: Some(table_name.to_string()),
        name: proto_expr.column.clone(),
    };
    let op = deserialize_operator(&proto::Op::from_i32(proto_expr.op).context(
        IllegalOperatorEnumValue {
            value: proto_expr.op,
        },
    )?)?;
    let value = deserialize_scalar_value(&proto_expr.scalar)?;

    let expr = Expr::BinaryExpr {
        left: Box::new(Expr::Column(column)),
        op,
        right: Box::new(Expr::Literal(value)),
    };
    Ok(expr)
}

fn deserialize_scalar_value(
    value: &Option<proto::Scalar>,
) -> Result<ScalarValue, DeserializeError> {
    match value
        .as_ref()
        .context(MissingScalarValue)?
        .value
        .as_ref()
        .context(MissingScalarValue)?
    {
        proto::scalar::Value::ValueBool(value) => Ok(ScalarValue::Boolean(Some(*value))),
        proto::scalar::Value::ValueI64(value) => Ok(ScalarValue::Int64(Some(*value))),
        proto::scalar::Value::ValueU64(value) => Ok(ScalarValue::UInt64(Some(*value))),
        proto::scalar::Value::ValueF64(value) => Ok(ScalarValue::Float64(Some(*value))),
        proto::scalar::Value::ValueString(value) => Ok(ScalarValue::Utf8(Some(value.clone()))),
    }
}

fn deserialize_operator(op: &proto::Op) -> Result<Operator, DeserializeError> {
    match op {
        proto::Op::Unspecified => Err(DeserializeError::UnspecifiedOperator),
        proto::Op::Eq => Ok(Operator::Eq),
        proto::Op::Ne => Ok(Operator::NotEq),
    }
}

#[cfg(test)]
mod tests {
    use crate::predicate::{ParseDeletePredicate, PredicateBuilder};

    use super::*;

    #[test]
    fn test_roundtrip() {
        let table_name = "my_table";
        let predicate = delete_predicate(table_name);
        let proto = serialize(&predicate).unwrap();
        let recovered = deserialize(&proto, table_name).unwrap();
        assert_eq!(predicate, recovered);
    }

    fn delete_predicate(table_name: &str) -> Predicate {
        let start_time = "11";
        let stop_time = "22";
        let predicate = r#"city=Boston and cost!=100 and temp=87.5 and good=true"#;

        let parse_delete_pred =
            ParseDeletePredicate::try_new(table_name, start_time, stop_time, predicate).unwrap();

        let mut del_predicate_builder = PredicateBuilder::new()
            .table(table_name)
            .timestamp_range(parse_delete_pred.start_time, parse_delete_pred.stop_time);

        for expr in parse_delete_pred.predicate {
            del_predicate_builder = del_predicate_builder.add_expr(expr);
        }

        del_predicate_builder.build()
    }
}
