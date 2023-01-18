use data_types::{DeleteExpr, Op, Scalar};
use datafusion::{
    logical_expr::BinaryExpr,
    prelude::{binary_expr, lit, Expr},
};
use snafu::{ResultExt, Snafu};
use std::ops::Deref;

pub(crate) fn expr_to_df(expr: DeleteExpr) -> Expr {
    let column = datafusion::prelude::Column {
        relation: None,
        name: expr.column,
    };

    binary_expr(
        Expr::Column(column),
        op_to_df(expr.op),
        lit(scalar_to_df(expr.scalar)),
    )
}

#[derive(Debug, Snafu)]
#[allow(clippy::large_enum_variant)]
pub enum DataFusionToExprError {
    #[snafu(display("unsupported expression: {:?}", expr))]
    UnsupportedExpression { expr: Expr },

    #[snafu(display("unsupported operants: left {:?}; right {:?}", left, right))]
    UnsupportedOperants { left: Expr, right: Expr },

    #[snafu(display("cannot convert datafusion operator: {}", source))]
    CannotConvertDataFusionOperator {
        source: crate::delete_expr::DataFusionToOpError,
    },

    #[snafu(display("cannot convert datafusion scalar value: {}", source))]
    CannotConvertDataFusionScalarValue {
        source: crate::delete_expr::DataFusionToScalarError,
    },
}

pub(crate) fn df_to_expr(expr: Expr) -> Result<DeleteExpr, DataFusionToExprError> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (column, scalar) = match (left.deref(), right.deref()) {
                // The delete predicate parser currently only supports `<column><op><value>`, not `<value><op><column>`,
                // however this could can easily be extended to support the latter case as well.
                (Expr::Column(column), Expr::Literal(value)) => {
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

pub(crate) fn op_to_df(op: Op) -> datafusion::logical_expr::Operator {
    match op {
        Op::Eq => datafusion::logical_expr::Operator::Eq,
        Op::Ne => datafusion::logical_expr::Operator::NotEq,
    }
}

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations)] // allow extensions
pub enum DataFusionToOpError {
    #[snafu(display("unsupported operator: {:?}", op))]
    UnsupportedOperator {
        op: datafusion::logical_expr::Operator,
    },
}

pub(crate) fn df_to_op(op: datafusion::logical_expr::Operator) -> Result<Op, DataFusionToOpError> {
    match op {
        datafusion::logical_expr::Operator::Eq => Ok(Op::Eq),
        datafusion::logical_expr::Operator::NotEq => Ok(Op::Ne),
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
    use arrow::datatypes::Field;
    use test_helpers::assert_contains;

    use super::*;
    use datafusion::prelude::col;

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
        let expr = (col("foo").eq(lit("x"))).not();
        let res = df_to_expr(expr);
        assert_contains!(res.unwrap_err().to_string(), "unsupported expression:");
    }

    #[test]
    fn test_unsupported_operants() {
        let expr = col("foo").eq(col("bar"));
        let res = df_to_expr(expr);
        assert_contains!(res.unwrap_err().to_string(), "unsupported operants:");
    }

    #[test]
    fn test_unsupported_scalar_value() {
        let scalar = datafusion::scalar::ScalarValue::List(
            Some(vec![]),
            Box::new(Field::new(
                "field",
                arrow::datatypes::DataType::Float64,
                true,
            )),
        );
        let res = df_to_scalar(scalar);
        assert_contains!(res.unwrap_err().to_string(), "unsupported scalar value:");
    }

    #[test]
    fn test_unsupported_scalar_value_in_expr() {
        let expr = col("foo").eq(lit(datafusion::scalar::ScalarValue::new_list(
            Some(vec![]),
            arrow::datatypes::DataType::Float64,
        )));
        let res = df_to_expr(expr);
        assert_contains!(res.unwrap_err().to_string(), "unsupported scalar value:");
    }

    #[test]
    fn test_unsupported_operator() {
        let res = df_to_op(datafusion::logical_expr::Operator::Lt);
        assert_contains!(res.unwrap_err().to_string(), "unsupported operator:");
    }

    #[test]
    fn test_unsupported_operator_in_expr() {
        let expr = col("foo").lt(lit("x"));
        let res = df_to_expr(expr);
        assert_contains!(res.unwrap_err().to_string(), "unsupported operator:");
    }
}
