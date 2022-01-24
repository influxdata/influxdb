//! Code to serialize and deserialize certain expressions.
//!
//! Note that [Ballista] also provides a serialization using [Protocol Buffers 3]. However the protocol is meant as a
//! communication channel between workers and clients of Ballista, not for long term preservation. For IOx we need a
//! more stable solution. Luckily we only need to support a very small subset of expression.
//!
//! [Ballista]: https://github.com/apache/arrow-datafusion/blob/22fcb3d7a68a56afbe12eab9e7d98f7b8de33703/ballista/rust/core/proto/ballista.proto
//! [Protocol Buffers 3]: https://developers.google.com/protocol-buffers/docs/proto3

use crate::google::{FieldViolation, FromOptionalField, FromRepeatedField, OptionalField};
use crate::influxdata::iox::predicate::v1 as proto;
use crate::influxdata::iox::predicate::v1::scalar::Value;
use crate::influxdata::iox::predicate::v1::{Expr, Predicate};
use data_types::{
    delete_predicate::{DeleteExpr, DeletePredicate, Op, Scalar},
    timestamp::TimestampRange,
};

impl From<DeletePredicate> for proto::Predicate {
    fn from(predicate: DeletePredicate) -> Self {
        proto::Predicate {
            range: Some(proto::TimestampRange {
                start: predicate.range.start(),
                end: predicate.range.end(),
            }),
            exprs: predicate.exprs.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<proto::Predicate> for DeletePredicate {
    type Error = FieldViolation;

    fn try_from(value: Predicate) -> Result<Self, Self::Error> {
        let range = value.range.unwrap_field("range")?;

        Ok(Self {
            range: TimestampRange::new(range.start, range.end),
            exprs: value.exprs.repeated("exprs")?,
        })
    }
}

impl TryFrom<proto::Expr> for DeleteExpr {
    type Error = FieldViolation;

    fn try_from(value: Expr) -> Result<Self, Self::Error> {
        Ok(Self {
            column: value.column,
            op: proto::Op::from_i32(value.op).required("op")?,
            scalar: value.scalar.required("scalar")?,
        })
    }
}

impl From<DeleteExpr> for proto::Expr {
    fn from(expr: DeleteExpr) -> Self {
        Self {
            column: expr.column,
            op: proto::Op::from(expr.op).into(),
            scalar: Some(expr.scalar.into()),
        }
    }
}

impl TryFrom<proto::Scalar> for Scalar {
    type Error = FieldViolation;

    fn try_from(value: proto::Scalar) -> Result<Self, Self::Error> {
        Ok(value.value.unwrap_field("value")?.into())
    }
}

impl From<proto::scalar::Value> for Scalar {
    fn from(value: Value) -> Self {
        match value {
            Value::ValueBool(v) => Self::Bool(v),
            Value::ValueI64(v) => Self::I64(v),
            Value::ValueF64(v) => Self::F64(v.into()),
            Value::ValueString(v) => Self::String(v),
        }
    }
}

impl From<Scalar> for proto::Scalar {
    fn from(value: Scalar) -> Self {
        let value = match value {
            Scalar::Bool(v) => Value::ValueBool(v),
            Scalar::I64(v) => Value::ValueI64(v),
            Scalar::F64(v) => Value::ValueF64(v.0),
            Scalar::String(v) => Value::ValueString(v),
        };

        Self { value: Some(value) }
    }
}

impl TryFrom<proto::Op> for Op {
    type Error = FieldViolation;

    fn try_from(value: proto::Op) -> Result<Self, Self::Error> {
        match value {
            proto::Op::Unspecified => Err(FieldViolation::required("")),
            proto::Op::Eq => Ok(Self::Eq),
            proto::Op::Ne => Ok(Self::Ne),
        }
    }
}

impl From<Op> for proto::Op {
    fn from(value: Op) -> Self {
        match value {
            Op::Eq => Self::Eq,
            Op::Ne => Self::Ne,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let round_trip = |expr: DeleteExpr| {
            let serialized: proto::Expr = expr.clone().into();
            let deserialized: DeleteExpr = serialized.try_into().unwrap();
            assert_eq!(expr, deserialized);
        };

        round_trip(DeleteExpr {
            column: "foo".to_string(),
            op: Op::Eq,
            scalar: Scalar::Bool(true),
        });

        round_trip(DeleteExpr {
            column: "bar".to_string(),
            op: Op::Ne,
            scalar: Scalar::I64(-1),
        });
        round_trip(DeleteExpr {
            column: "baz".to_string(),
            op: Op::Eq,
            scalar: Scalar::F64((-1.1).into()),
        });
        round_trip(DeleteExpr {
            column: "col".to_string(),
            op: Op::Eq,
            scalar: Scalar::String("foo".to_string()),
        });
    }
}
