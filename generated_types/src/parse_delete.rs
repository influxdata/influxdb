use crate::{
    google::{FieldViolation, FromFieldOpt},
    influxdata::iox::management::v1 as management,
};

use influxdb_line_protocol::delete_parser::{
    ProvidedDeleteBinaryExpr, ProvidedDeleteOp, ProvidedParseDelete,
};
use std::convert::{TryFrom, TryInto};

impl From<ProvidedDeleteOp> for management::DeleteOp {
    fn from(op: ProvidedDeleteOp) -> Self {
        match op {
            ProvidedDeleteOp::Eq => Self::Eq,
            ProvidedDeleteOp::NotEq => Self::NotEq,
        }
    }
}

impl TryFrom<management::DeleteOp> for ProvidedDeleteOp {
    type Error = FieldViolation;

    fn try_from(proto: management::DeleteOp) -> Result<Self, Self::Error> {
        match proto {
            management::DeleteOp::Eq => Ok(Self::Eq),
            management::DeleteOp::NotEq => Ok(Self::NotEq),
            management::DeleteOp::Unspecified => Err(FieldViolation::required("")),
        }
    }
}

impl From<ProvidedDeleteBinaryExpr> for management::DeleteBinaryExpr {
    fn from(bin_expr: ProvidedDeleteBinaryExpr) -> Self {
        let ProvidedDeleteBinaryExpr { column, op, value } = bin_expr;

        Self {
            column,
            op: management::DeleteOp::from(op).into(),
            value,
        }
    }
}

impl TryFrom<management::DeleteBinaryExpr> for ProvidedDeleteBinaryExpr {
    type Error = FieldViolation;

    fn try_from(proto: management::DeleteBinaryExpr) -> Result<Self, Self::Error> {
        let management::DeleteBinaryExpr { column, op, value } = proto;

        Ok(Self {
            column,
            op: management::DeleteOp::from_i32(op).required("op")?,
            value,
        })
    }
}

/// Conversion code to management API chunk structure
impl From<ProvidedParseDelete> for management::ParseDelete {
    fn from(parse_delete: ProvidedParseDelete) -> Self {
        let ProvidedParseDelete {
            start_time,
            stop_time,
            predicate,
        } = parse_delete;

        Self {
            start_time,
            stop_time,
            exprs: predicate.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<management::ParseDelete> for ProvidedParseDelete {
    type Error = FieldViolation;

    fn try_from(proto: management::ParseDelete) -> Result<Self, Self::Error> {
        let management::ParseDelete {
            start_time,
            stop_time,
            exprs,
        } = proto;

        let pred_result: Result<Vec<ProvidedDeleteBinaryExpr>, Self::Error> =
            exprs.into_iter().map(TryInto::try_into).collect();

        let pred = pred_result?;

        Ok(Self {
            start_time,
            stop_time,
            predicate: pred,
        })
    }
}
