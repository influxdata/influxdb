//! Code to serialize and deserialize certain expressions.
//!
//! Note that [Ballista] also provides a serialization using [Protocol Buffers 3]. However the protocol is meant as a
//! communication channel between workers and clients of Ballista, not for long term preservation. For IOx we need a
//! more stable solution. Luckily we only need to support a very small subset of expression.
//!
//! [Ballista]: https://github.com/apache/arrow-datafusion/blob/22fcb3d7a68a56afbe12eab9e7d98f7b8de33703/ballista/rust/core/proto/ballista.proto
//! [Protocol Buffers 3]: https://developers.google.com/protocol-buffers/docs/proto3
use std::convert::TryInto;

use data_types::timestamp::TimestampRange;
use generated_types::influxdata::iox::catalog::v1 as proto;
use snafu::{ResultExt, Snafu};

use crate::{delete_expr::DeleteExpr, delete_predicate::DeletePredicate};

/// Serialize IOx [`DeletePredicate`] to a protobuf object.
pub fn serialize(predicate: &DeletePredicate) -> proto::Predicate {
    proto::Predicate {
        range: Some(proto::TimestampRange {
            start: predicate.range.start,
            end: predicate.range.end,
        }),
        exprs: predicate
            .exprs
            .iter()
            .map(|expr| expr.clone().into())
            .collect(),
    }
}

#[derive(Debug, Snafu)]
pub enum DeserializeError {
    #[snafu(display("timestamp range is missing"))]
    RangeMissing,

    #[snafu(display("cannot deserialize expr: {}", source))]
    CannotDeserializeExpr {
        source: crate::delete_expr::ProtoToExprError,
    },
}

/// Deserialize IOx [`DeletePredicate`] from a protobuf object.
pub fn deserialize(
    proto_predicate: &proto::Predicate,
) -> Result<DeletePredicate, DeserializeError> {
    let predicate = DeletePredicate {
        range: proto_predicate
            .range
            .as_ref()
            .map(|r| TimestampRange {
                start: r.start,
                end: r.end,
            })
            .ok_or(DeserializeError::RangeMissing)?,
        exprs: proto_predicate
            .exprs
            .iter()
            .map(|expr| {
                let expr: DeleteExpr = expr.clone().try_into().context(CannotDeserializeExpr)?;
                Ok(expr)
            })
            .collect::<Result<Vec<DeleteExpr>, DeserializeError>>()?,
    };
    Ok(predicate)
}

#[cfg(test)]
mod tests {
    use crate::delete_predicate::ParseDeletePredicate;

    use super::*;

    #[test]
    fn test_roundtrip() {
        let predicate = delete_predicate();
        let proto = serialize(&predicate);
        let recovered = deserialize(&proto).unwrap();
        assert_eq!(predicate, recovered);
    }

    fn delete_predicate() -> DeletePredicate {
        let start_time = "11";
        let stop_time = "22";
        let predicate = r#"city=Boston and cost!=100 and temp=87.5 and good=true"#;

        let parse_delete_pred =
            ParseDeletePredicate::try_new(start_time, stop_time, predicate).unwrap();

        parse_delete_pred.into()
    }
}
