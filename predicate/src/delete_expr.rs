use std::ops::Deref;

use data_types::delete_predicate::{DeleteExpr, Op, Scalar};
use snafu::{ResultExt, Snafu};

pub(crate) fn expr_to_df(expr: DeleteExpr) -> datafusion::logical_plan::Expr {
    use datafusion::logical_plan::Expr;

    let column = datafusion::logical_plan::Column {
        relation: None,
        name: expr.column,
    };

    Expr::BinaryExpr {
        left: Box::new(Expr::Column(column)),
        op: op_to_df(expr.op),
        right: Box::new(Expr::Literal(scalar_to_df(expr.scalar))),
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

pub(crate) fn df_to_expr(
    expr: datafusion::logical_plan::Expr,
) -> Result<DeleteExpr, DataFusionToExprError> {
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

                    let scalar = df_to_scalar(value.clone())
                        .context(CannotConvertDataFusionScalarValueSnafu)?;

                    (column, scalar)
                }
                (other_left, other_right) => {
                    return Err(DataFusionToExprError::UnsupportedOperants {
                        left: other_left.clone(),
                        right: other_right.clone(),
                    });
                }
            };

            let op = df_to_op(op).context(CannotConvertDataFusionOperatorSnafu)?;

            Ok(DeleteExpr { column, op, scalar })
        }
        other => Err(DataFusionToExprError::UnsupportedExpression { expr: other }),
    }
}

pub(crate) fn op_to_df(op: Op) -> datafusion::logical_plan::Operator {
    match op {
        Op::Eq => datafusion::logical_plan::Operator::Eq,
        Op::Ne => datafusion::logical_plan::Operator::NotEq,
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations)] // allow extensions
pub enum DataFusionToOpError {
    #[snafu(display("unsupported operator: {:?}", op))]
    UnsupportedOperator {
        op: datafusion::logical_plan::Operator,
    },
}

pub(crate) fn df_to_op(op: datafusion::logical_plan::Operator) -> Result<Op, DataFusionToOpError> {
    match op {
        datafusion::logical_plan::Operator::Eq => Ok(Op::Eq),
        datafusion::logical_plan::Operator::NotEq => Ok(Op::Ne),
        other => Err(DataFusionToOpError::UnsupportedOperator { op: other }),
    }
}

pub(crate) fn scalar_to_df(scalar: Scalar) -> datafusion::scalar::ScalarValue {
    use datafusion::scalar::ScalarValue;
    match scalar {
        Scalar::Bool(value) => ScalarValue::Boolean(Some(value)),
        Scalar::I64(value) => ScalarValue::Int64(Some(value)),
        Scalar::F64(value) => ScalarValue::Float64(Some(value.into())),
        Scalar::String(value) => ScalarValue::Utf8(Some(value)),
    }
}

#[derive(Debug, Snafu)]
pub enum DataFusionToScalarError {
    #[snafu(display("unsupported scalar value: {:?}", value))]
    UnsupportedScalarValue {
        value: datafusion::scalar::ScalarValue,
    },
}

pub(crate) fn df_to_scalar(
    scalar: datafusion::scalar::ScalarValue,
) -> Result<Scalar, DataFusionToScalarError> {
    use datafusion::scalar::ScalarValue;
    match scalar {
        ScalarValue::Utf8(Some(value)) => Ok(Scalar::String(value)),
        ScalarValue::Int64(Some(value)) => Ok(Scalar::I64(value)),
        ScalarValue::Float64(Some(value)) => Ok(Scalar::F64(value.into())),
        ScalarValue::Boolean(Some(value)) => Ok(Scalar::Bool(value)),
        other => Err(DataFusionToScalarError::UnsupportedScalarValue { value: other }),
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
            r#""foo"=true"#,
        );
        assert_expr_works(
            DeleteExpr {
                column: "bar".to_string(),
                op: Op::Ne,
                scalar: Scalar::I64(-1),
            },
            r#""bar"!=-1"#,
        );
        assert_expr_works(
            DeleteExpr {
                column: "baz".to_string(),
                op: Op::Eq,
                scalar: Scalar::F64((-1.1).into()),
            },
            r#""baz"=-1.1"#,
        );
        assert_expr_works(
            DeleteExpr {
                column: "col".to_string(),
                op: Op::Eq,
                scalar: Scalar::String("foo".to_string()),
            },
            r#""col"='foo'"#,
        );
    }

    fn assert_expr_works(expr: DeleteExpr, display: &str) {
        let df_expr = expr_to_df(expr.clone());
        let expr2 = df_to_expr(df_expr).unwrap();
        assert_eq!(expr2, expr);

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
        let res = df_to_expr(expr);
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
        let res = df_to_expr(expr);
        assert_contains!(res.unwrap_err().to_string(), "unsupported operants:");
    }

    #[test]
    fn test_unsupported_scalar_value() {
        let scalar = datafusion::scalar::ScalarValue::List(
            Some(Box::new(vec![])),
            Box::new(arrow::datatypes::DataType::Float64),
        );
        let res = df_to_scalar(scalar);
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
        let res = df_to_expr(expr);
        assert_contains!(res.unwrap_err().to_string(), "unsupported scalar value:");
    }

    #[test]
    fn test_unsupported_operator() {
        let res = df_to_op(datafusion::logical_plan::Operator::Like);
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
        let res = df_to_expr(expr);
        assert_contains!(res.unwrap_err().to_string(), "unsupported operator:");
    }
}
