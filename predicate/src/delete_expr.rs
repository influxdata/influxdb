use std::{
    convert::{TryFrom, TryInto},
    ops::Deref,
};

use generated_types::influxdata::iox::catalog::v1 as proto;
use ordered_float::OrderedFloat;
use snafu::{OptionExt, ResultExt, Snafu};

/// Single expression to be used as parts of a predicate.
///
/// Only very simple expression of the type `<column> <op> <scalar>` are supported.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeleteExpr {
    /// Column (w/o table name).
    column: String,

    /// Operator.
    op: Op,

    /// Scalar value.
    scalar: Scalar,
}

impl DeleteExpr {
    pub fn new(column: String, op: Op, scalar: Scalar) -> Self {
        Self { column, op, scalar }
    }

    /// Column (w/o table name).
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Operator.
    pub fn op(&self) -> Op {
        self.op
    }

    /// Scalar value.
    pub fn scalar(&self) -> &Scalar {
        &self.scalar
    }
}

impl std::fmt::Display for DeleteExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}{}", self.column(), self.op(), self.scalar())
    }
}

impl From<DeleteExpr> for datafusion::logical_plan::Expr {
    fn from(expr: DeleteExpr) -> Self {
        let column = datafusion::logical_plan::Column {
            relation: None,
            name: expr.column,
        };

        datafusion::logical_plan::Expr::BinaryExpr {
            left: Box::new(datafusion::logical_plan::Expr::Column(column)),
            op: expr.op.into(),
            right: Box::new(datafusion::logical_plan::Expr::Literal(expr.scalar.into())),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum ProtoToExprError {
    #[snafu(display("cannot deserialize operator: {}", source))]
    CannotDeserializeOperator {
        source: crate::delete_expr::ProtoToOpError,
    },

    #[snafu(display("illegal operator enum value: {}", value))]
    IllegalOperatorEnumValue { value: i32 },

    #[snafu(display("missing scalar"))]
    MissingScalar,

    #[snafu(display("cannot deserialize scalar: {}", source))]
    CannotDeserializeScalar {
        source: crate::delete_expr::ProtoToScalarError,
    },
}

impl TryFrom<proto::Expr> for DeleteExpr {
    type Error = ProtoToExprError;

    fn try_from(expr: proto::Expr) -> Result<Self, Self::Error> {
        let op = proto::Op::from_i32(expr.op)
            .context(IllegalOperatorEnumValue { value: expr.op })?
            .try_into()
            .context(CannotDeserializeOperator)?;

        let scalar = expr
            .clone()
            .scalar
            .context(MissingScalar)?
            .try_into()
            .context(CannotDeserializeScalar)?;

        Ok(DeleteExpr {
            column: expr.column,
            op,
            scalar,
        })
    }
}

#[derive(Debug, Snafu)]
pub enum DataFusionToExprError {
    #[snafu(display("unsupported expression: {:?}", expr))]
    UnsupportedExpression {
        expr: datafusion::logical_plan::Expr,
    },

    #[snafu(display("unsupported operants: left {:?}; right {:?}", left, right))]
    UnsupportedOperants {
        left: datafusion::logical_plan::Expr,
        right: datafusion::logical_plan::Expr,
    },

    #[snafu(display("cannot convert datafusion operator: {}", source))]
    CannotConvertDataFusionOperator {
        source: crate::delete_expr::DataFusionToOpError,
    },

    #[snafu(display("cannot convert datafusion scalar value: {}", source))]
    CannotConvertDataFusionScalarValue {
        source: crate::delete_expr::DataFusionToScalarError,
    },
}

impl TryFrom<datafusion::logical_plan::Expr> for DeleteExpr {
    type Error = DataFusionToExprError;

    fn try_from(expr: datafusion::logical_plan::Expr) -> Result<Self, Self::Error> {
        match expr {
            datafusion::logical_plan::Expr::BinaryExpr { left, op, right } => {
                let (column, scalar) = match (left.deref(), right.deref()) {
                    // The delete predicate parser currently only supports `<column><op><value>`, not `<value><op><column>`,
                    // however this could can easily be extended to support the latter case as well.
                    (
                        datafusion::logical_plan::Expr::Column(column),
                        datafusion::logical_plan::Expr::Literal(value),
                    ) => {
                        let column = column.name.clone();

                        let scalar: Scalar = value
                            .clone()
                            .try_into()
                            .context(CannotConvertDataFusionScalarValue)?;

                        (column, scalar)
                    }
                    (other_left, other_right) => {
                        return Err(DataFusionToExprError::UnsupportedOperants {
                            left: other_left.clone(),
                            right: other_right.clone(),
                        });
                    }
                };

                let op: Op = op.try_into().context(CannotConvertDataFusionOperator)?;

                Ok(DeleteExpr { column, op, scalar })
            }
            other => Err(DataFusionToExprError::UnsupportedExpression { expr: other }),
        }
    }
}

impl From<DeleteExpr> for proto::Expr {
    fn from(expr: DeleteExpr) -> Self {
        let op: proto::Op = expr.op.into();

        proto::Expr {
            column: expr.column,
            op: op.into(),
            scalar: Some(expr.scalar.into()),
        }
    }
}

/// Binary operator that can be evaluated on a column and a scalar value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Op {
    /// Strict equality (`=`).
    Eq,

    /// Inequality (`!=`).
    Ne,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Op::Eq => write!(f, "="),
            Op::Ne => write!(f, "!="),
        }
    }
}

impl From<Op> for datafusion::logical_plan::Operator {
    fn from(op: Op) -> Self {
        match op {
            Op::Eq => datafusion::logical_plan::Operator::Eq,
            Op::Ne => datafusion::logical_plan::Operator::NotEq,
        }
    }
}

#[derive(Debug, Snafu)]
pub enum DataFusionToOpError {
    #[snafu(display("unsupported operator: {:?}", op))]
    UnsupportedOperator {
        op: datafusion::logical_plan::Operator,
    },
}

impl TryFrom<datafusion::logical_plan::Operator> for Op {
    type Error = DataFusionToOpError;

    fn try_from(op: datafusion::logical_plan::Operator) -> Result<Self, Self::Error> {
        match op {
            datafusion::logical_plan::Operator::Eq => Ok(Op::Eq),
            datafusion::logical_plan::Operator::NotEq => Ok(Op::Ne),
            other => Err(DataFusionToOpError::UnsupportedOperator { op: other }),
        }
    }
}

impl From<Op> for proto::Op {
    fn from(op: Op) -> Self {
        match op {
            Op::Eq => proto::Op::Eq,
            Op::Ne => proto::Op::Ne,
        }
    }
}

#[derive(Debug, Snafu)]
pub enum ProtoToOpError {
    #[snafu(display("unspecified operator"))]
    UnspecifiedOperator,
}

impl TryFrom<proto::Op> for Op {
    type Error = ProtoToOpError;

    fn try_from(op: proto::Op) -> Result<Self, Self::Error> {
        match op {
            proto::Op::Unspecified => Err(ProtoToOpError::UnspecifiedOperator),
            proto::Op::Eq => Ok(Op::Eq),
            proto::Op::Ne => Ok(Op::Ne),
        }
    }
}

/// Scalar value of a certain type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scalar {
    Bool(bool),
    I64(i64),
    F64(OrderedFloat<f64>),
    String(String),
}

impl std::fmt::Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Bool(value) => value.fmt(f),
            Scalar::I64(value) => value.fmt(f),
            Scalar::F64(value) => value.fmt(f),
            Scalar::String(value) => write!(f, "'{}'", value),
        }
    }
}

impl From<Scalar> for datafusion::scalar::ScalarValue {
    fn from(scalar: Scalar) -> Self {
        match scalar {
            Scalar::Bool(value) => datafusion::scalar::ScalarValue::Boolean(Some(value)),
            Scalar::I64(value) => datafusion::scalar::ScalarValue::Int64(Some(value)),
            Scalar::F64(value) => datafusion::scalar::ScalarValue::Float64(Some(value.into())),
            Scalar::String(value) => datafusion::scalar::ScalarValue::Utf8(Some(value)),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum ProtoToScalarError {
    #[snafu(display("missing scalar value"))]
    MissingScalarValue,
}

impl TryFrom<proto::Scalar> for Scalar {
    type Error = ProtoToScalarError;

    fn try_from(scalar: proto::Scalar) -> Result<Self, Self::Error> {
        match scalar.value.context(MissingScalarValue)? {
            proto::scalar::Value::ValueBool(value) => Ok(Scalar::Bool(value)),
            proto::scalar::Value::ValueI64(value) => Ok(Scalar::I64(value)),
            proto::scalar::Value::ValueF64(value) => Ok(Scalar::F64(value.into())),
            proto::scalar::Value::ValueString(value) => Ok(Scalar::String(value)),
        }
    }
}

#[derive(Debug, Snafu)]
pub enum DataFusionToScalarError {
    #[snafu(display("unsupported scalar value: {:?}", value))]
    UnsupportedScalarValue {
        value: datafusion::scalar::ScalarValue,
    },
}

impl TryFrom<datafusion::scalar::ScalarValue> for Scalar {
    type Error = DataFusionToScalarError;

    fn try_from(scalar: datafusion::scalar::ScalarValue) -> Result<Self, Self::Error> {
        match scalar {
            datafusion::scalar::ScalarValue::Utf8(Some(value)) => Ok(Scalar::String(value)),
            datafusion::scalar::ScalarValue::Int64(Some(value)) => Ok(Scalar::I64(value)),
            datafusion::scalar::ScalarValue::Float64(Some(value)) => Ok(Scalar::F64(value.into())),
            datafusion::scalar::ScalarValue::Boolean(Some(value)) => Ok(Scalar::Bool(value)),
            other => Err(DataFusionToScalarError::UnsupportedScalarValue { value: other }),
        }
    }
}

impl From<Scalar> for proto::Scalar {
    fn from(scalar: Scalar) -> Self {
        match scalar {
            Scalar::Bool(value) => proto::Scalar {
                value: Some(proto::scalar::Value::ValueBool(value)),
            },
            Scalar::I64(value) => proto::Scalar {
                value: Some(proto::scalar::Value::ValueI64(value)),
            },
            Scalar::F64(value) => proto::Scalar {
                value: Some(proto::scalar::Value::ValueF64(value.into())),
            },
            Scalar::String(value) => proto::Scalar {
                value: Some(proto::scalar::Value::ValueString(value)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use test_helpers::assert_contains;

    use super::*;

    #[test]
    fn test_roundtrips() {
        assert_expr_works(
            DeleteExpr {
                column: "foo".to_string(),
                op: Op::Eq,
                scalar: Scalar::Bool(true),
            },
            "foo=true",
        );
        assert_expr_works(
            DeleteExpr {
                column: "bar".to_string(),
                op: Op::Ne,
                scalar: Scalar::I64(-1),
            },
            "bar!=-1",
        );
        assert_expr_works(
            DeleteExpr {
                column: "baz".to_string(),
                op: Op::Eq,
                scalar: Scalar::F64((-1.1).into()),
            },
            "baz=-1.1",
        );
        assert_expr_works(
            DeleteExpr {
                column: "col".to_string(),
                op: Op::Eq,
                scalar: Scalar::String("foo".to_string()),
            },
            "col='foo'",
        );
    }

    fn assert_expr_works(expr: DeleteExpr, display: &str) {
        let df_expr: datafusion::logical_plan::Expr = expr.clone().into();
        let expr2: DeleteExpr = df_expr.try_into().unwrap();
        assert_eq!(expr2, expr);

        let proto_expr: proto::Expr = expr.clone().into();
        let expr3: DeleteExpr = proto_expr.try_into().unwrap();
        assert_eq!(expr3, expr);

        assert_eq!(expr.to_string(), display);
    }

    #[test]
    fn test_unsupported_expression() {
        let expr = datafusion::logical_plan::Expr::Not(Box::new(
            datafusion::logical_plan::Expr::BinaryExpr {
                left: Box::new(datafusion::logical_plan::Expr::Column(
                    datafusion::logical_plan::Column {
                        relation: None,
                        name: "foo".to_string(),
                    },
                )),
                op: datafusion::logical_plan::Operator::Eq,
                right: Box::new(datafusion::logical_plan::Expr::Literal(
                    datafusion::scalar::ScalarValue::Utf8(Some("x".to_string())),
                )),
            },
        ));
        let res: Result<DeleteExpr, _> = expr.try_into();
        assert_contains!(res.unwrap_err().to_string(), "unsupported expression:");
    }

    #[test]
    fn test_unsupported_operants() {
        let expr = datafusion::logical_plan::Expr::BinaryExpr {
            left: Box::new(datafusion::logical_plan::Expr::Column(
                datafusion::logical_plan::Column {
                    relation: None,
                    name: "foo".to_string(),
                },
            )),
            op: datafusion::logical_plan::Operator::Eq,
            right: Box::new(datafusion::logical_plan::Expr::Column(
                datafusion::logical_plan::Column {
                    relation: None,
                    name: "bar".to_string(),
                },
            )),
        };
        let res: Result<DeleteExpr, _> = expr.try_into();
        assert_contains!(res.unwrap_err().to_string(), "unsupported operants:");
    }

    #[test]
    fn test_unsupported_scalar_value() {
        let scalar = datafusion::scalar::ScalarValue::List(
            Some(Box::new(vec![])),
            Box::new(arrow::datatypes::DataType::Float64),
        );
        let res: Result<Scalar, _> = scalar.try_into();
        assert_contains!(res.unwrap_err().to_string(), "unsupported scalar value:");
    }

    #[test]
    fn test_unsupported_scalar_value_in_expr() {
        let expr = datafusion::logical_plan::Expr::BinaryExpr {
            left: Box::new(datafusion::logical_plan::Expr::Column(
                datafusion::logical_plan::Column {
                    relation: None,
                    name: "foo".to_string(),
                },
            )),
            op: datafusion::logical_plan::Operator::Eq,
            right: Box::new(datafusion::logical_plan::Expr::Literal(
                datafusion::scalar::ScalarValue::List(
                    Some(Box::new(vec![])),
                    Box::new(arrow::datatypes::DataType::Float64),
                ),
            )),
        };
        let res: Result<DeleteExpr, _> = expr.try_into();
        assert_contains!(res.unwrap_err().to_string(), "unsupported scalar value:");
    }

    #[test]
    fn test_unsupported_operator() {
        let op = datafusion::logical_plan::Operator::Like;
        let res: Result<Op, _> = op.try_into();
        assert_contains!(res.unwrap_err().to_string(), "unsupported operator:");
    }

    #[test]
    fn test_unsupported_operator_in_expr() {
        let expr = datafusion::logical_plan::Expr::BinaryExpr {
            left: Box::new(datafusion::logical_plan::Expr::Column(
                datafusion::logical_plan::Column {
                    relation: None,
                    name: "foo".to_string(),
                },
            )),
            op: datafusion::logical_plan::Operator::Like,
            right: Box::new(datafusion::logical_plan::Expr::Literal(
                datafusion::scalar::ScalarValue::Utf8(Some("x".to_string())),
            )),
        };
        let res: Result<DeleteExpr, _> = expr.try_into();
        assert_contains!(res.unwrap_err().to_string(), "unsupported operator:");
    }
}
