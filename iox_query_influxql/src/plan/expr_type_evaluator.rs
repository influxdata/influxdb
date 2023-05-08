use crate::plan::field::field_by_name;
use crate::plan::field_mapper::map_type;
use crate::plan::ir::DataSource;
use crate::plan::{error, SchemaProvider};
use datafusion::common::Result;
use influxdb_influxql_parser::expression::{
    Binary, BinaryOperator, Call, Expr, VarRef, VarRefDataType,
};
use influxdb_influxql_parser::literal::Literal;
use influxdb_influxql_parser::select::Dimension;
use itertools::Itertools;

/// Evaluate the type of the specified expression.
///
/// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4796-L4797).
pub(super) fn evaluate_type(
    s: &dyn SchemaProvider,
    expr: &Expr,
    from: &[DataSource],
) -> Result<Option<VarRefDataType>> {
    TypeEvaluator::new(s, from).eval_type(expr)
}

/// Evaluate the type of the specified expression.
///
/// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4796-L4797).
pub(super) struct TypeEvaluator<'a> {
    s: &'a dyn SchemaProvider,
    from: &'a [DataSource],
}

impl<'a> TypeEvaluator<'a> {
    pub(super) fn new(s: &'a dyn SchemaProvider, from: &'a [DataSource]) -> Self {
        Self { from, s }
    }

    pub(super) fn eval_type(&self, expr: &Expr) -> Result<Option<VarRefDataType>> {
        Ok(match expr {
            Expr::VarRef(v) => self.eval_var_ref(v)?,
            Expr::Call(v) => self.eval_call(v)?,
            Expr::Binary(expr) => self.eval_binary_expr_type(expr)?,
            Expr::Nested(expr) => self.eval_type(expr)?,
            Expr::Literal(Literal::Float(_)) => Some(VarRefDataType::Float),
            Expr::Literal(Literal::Unsigned(_)) => Some(VarRefDataType::Unsigned),
            Expr::Literal(Literal::Integer(_)) => Some(VarRefDataType::Integer),
            Expr::Literal(Literal::String(_)) => Some(VarRefDataType::String),
            Expr::Literal(Literal::Boolean(_)) => Some(VarRefDataType::Boolean),
            // Remaining patterns are not valid field types
            Expr::BindParameter(_)
            | Expr::Distinct(_)
            | Expr::Wildcard(_)
            | Expr::Literal(Literal::Duration(_))
            | Expr::Literal(Literal::Regex(_))
            | Expr::Literal(Literal::Timestamp(_)) => None,
        })
    }

    fn eval_binary_expr_type(&self, expr: &Binary) -> Result<Option<VarRefDataType>> {
        let (lhs, op, rhs) = (
            self.eval_type(&expr.lhs)?,
            expr.op,
            self.eval_type(&expr.rhs)?,
        );

        // Deviation from InfluxQL OG, which fails if one operand is unsigned and the other is
        // an integer. This will let some additional queries succeed that would otherwise have
        // failed.
        //
        // In this case, we will let DataFusion handle automatic coercion, rather than fail.
        //
        // See: https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4729-L4730

        match (lhs, rhs) {
            (Some(dt), None) | (None, Some(dt)) => Ok(Some(dt)),
            (None, None) => Ok(None),
            (Some(lhs), Some(rhs)) => Ok(Some(binary_data_type(lhs, expr.op, rhs).ok_or_else(
                || {
                    error::map::query(format!(
                        "incompatible operands for operator {op}: {lhs} and {rhs}"
                    ))
                },
            )?)),
        }
    }

    /// Returns the type for the specified [`VarRef`].
    ///
    /// This function assumes that the expression has already been reduced.
    pub(super) fn eval_var_ref(&self, expr: &VarRef) -> Result<Option<VarRefDataType>> {
        Ok(match expr.data_type {
            Some(dt)
                if matches!(
                    dt,
                    VarRefDataType::Integer
                        | VarRefDataType::Unsigned
                        | VarRefDataType::Float
                        | VarRefDataType::String
                        | VarRefDataType::Boolean
                ) =>
            {
                Some(dt)
            }
            _ => {
                let mut data_type: Option<VarRefDataType> = None;
                for tr in self.from.iter() {
                    match tr {
                        DataSource::Table(name) => match (
                            data_type,
                            map_type(self.s, name.as_str(), expr.name.as_str())?,
                        ) {
                            (Some(existing), Some(res)) => {
                                if res < existing {
                                    data_type = Some(res)
                                }
                            }
                            (None, Some(res)) => data_type = Some(res),
                            _ => continue,
                        },
                        DataSource::Subquery(select) => {
                            // find the field by name
                            if let Some(field) = field_by_name(&select.fields, expr.name.as_str()) {
                                match (data_type, evaluate_type(self.s, &field.expr, &select.from)?)
                                {
                                    (Some(existing), Some(res)) => {
                                        if res < existing {
                                            data_type = Some(res)
                                        }
                                    }
                                    (None, Some(res)) => data_type = Some(res),
                                    _ => {}
                                }
                            };

                            if data_type.is_none() {
                                if let Some(group_by) = &select.group_by {
                                    if group_by.iter().any(|dim| {
                                        matches!(dim, Dimension::Tag(ident) if ident.as_str() == expr.name.as_str())
                                    }) {
                                        data_type = Some(VarRefDataType::Tag);
                                    }
                                }
                            }
                        }
                    }
                }

                data_type
            }
        })
    }

    /// Evaluate the datatype of the function identified by `name`.
    ///
    /// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4693)
    /// and [here](https://github.com/influxdata/influxdb/blob/37088e8f5330bec0f08a376b2cb945d02a296f4e/influxql/query/functions.go#L50).
    fn eval_call(&self, call: &Call) -> Result<Option<VarRefDataType>> {
        // Evaluate the data types of the arguments
        let arg_types: Vec<_> = call
            .args
            .iter()
            .map(|expr| self.eval_type(expr))
            .try_collect()?;

        Ok(match call.name.as_str() {
            // See: https://github.com/influxdata/influxdb/blob/e484c4d87193a475466c0285c018d16f168139e6/query/functions.go#L54-L60
            "mean" => Some(VarRefDataType::Float),
            "count" => Some(VarRefDataType::Integer),
            "min" | "max" | "sum" | "first" | "last" => match arg_types.first() {
                Some(v) => *v,
                None => None,
            },

            // See: https://github.com/influxdata/influxdb/blob/e484c4d87193a475466c0285c018d16f168139e6/query/functions.go#L80
            "median"
            | "integral"
            | "stddev"
            | "derivative"
            | "non_negative_derivative"
            | "moving_average"
            | "exponential_moving_average"
            | "double_exponential_moving_average"
            | "triple_exponential_moving_average"
            | "relative_strength_index"
            | "triple_exponential_derivative"
            | "kaufmans_efficiency_ratio"
            | "kaufmans_adaptive_moving_average"
            | "chande_momentum_oscillator"
            | "holt_winters"
            | "holt_winters_with_fit" => Some(VarRefDataType::Float),
            "elapsed" => Some(VarRefDataType::Integer),
            _ => None,
        })
    }
}

/// Determine the data type of the binary expression using the left and right operands and the operator
///
/// This logic is derived from [InfluxQL OG][og].
///
/// [og]: https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4192
fn binary_data_type(
    lhs: VarRefDataType,
    op: BinaryOperator,
    rhs: VarRefDataType,
) -> Option<VarRefDataType> {
    use BinaryOperator::*;
    use VarRefDataType::{Boolean, Float, Integer, Unsigned};

    match (lhs, op, rhs) {
        // Boolean only supports bitwise operators.
        //
        // See:
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4210
        (Boolean, BitwiseAnd | BitwiseOr | BitwiseXor, Boolean) => Some(Boolean),

        // A float for either operand is a float result, but only
        // support the +, -, * / and % operators.
        //
        // See:
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4228
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4285
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4411
        (Float, Add | Sub | Mul | Div | Mod, Float | Integer | Unsigned)
        | (Integer | Unsigned, Add | Sub | Mul | Div | Mod, Float) => Some(Float),

        // Integer and unsigned types support all operands and
        // the result is the same type if both operands are the same.
        //
        // See:
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4314
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4489
        (Integer, _, Integer) | (Unsigned, _, Unsigned) => Some(lhs),

        // If either side is unsigned, and the other is integer,
        // the result is unsigned for all operators.
        //
        // See:
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4358
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4440
        (Unsigned, _, Integer) | (Integer, _, Unsigned) => Some(Unsigned),

        // String or any other combination of operator and operands are invalid
        //
        // See:
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4562
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use crate::plan::expr_type_evaluator::{binary_data_type, evaluate_type};
    use crate::plan::rewriter::map_select;
    use crate::plan::test_utils::{parse_select, MockSchemaProvider};
    use assert_matches::assert_matches;
    use datafusion::common::DataFusionError;
    use influxdb_influxql_parser::expression::VarRefDataType;
    use itertools::iproduct;

    #[test]
    fn test_binary_data_type() {
        use influxdb_influxql_parser::expression::BinaryOperator::*;
        use VarRefDataType::{Boolean, Float, Integer, String, Tag, Timestamp, Unsigned};

        // Boolean ok
        for op in [BitwiseAnd, BitwiseOr, BitwiseXor] {
            assert_matches!(
                binary_data_type(Boolean, op, Boolean),
                Some(VarRefDataType::Boolean)
            );
        }

        // Boolean !ok
        for op in [Add, Sub, Div, Mul, Mod] {
            assert_matches!(binary_data_type(Boolean, op, Boolean), None);
        }

        // Float ok
        for (op, operand) in iproduct!([Add, Sub, Div, Mul, Mod], [Float, Integer, Unsigned]) {
            assert_matches!(binary_data_type(Float, op, operand), Some(Float));
            assert_matches!(binary_data_type(operand, op, Float), Some(Float));
        }

        // Float !ok
        for (op, operand) in iproduct!(
            [BitwiseAnd, BitwiseOr, BitwiseXor],
            [Float, Integer, Unsigned]
        ) {
            assert_matches!(binary_data_type(Float, op, operand), None);
            assert_matches!(binary_data_type(operand, op, Float), None);
        }

        // Integer op Integer | Unsigned op Unsigned
        for op in [Add, Sub, Div, Mul, Mod, BitwiseAnd, BitwiseOr, BitwiseXor] {
            assert_matches!(binary_data_type(Integer, op, Integer), Some(Integer));
            assert_matches!(binary_data_type(Unsigned, op, Unsigned), Some(Unsigned));
        }

        // Unsigned op Integer | Integer op Unsigned
        for op in [Add, Sub, Div, Mul, Mod, BitwiseAnd, BitwiseOr, BitwiseXor] {
            assert_matches!(binary_data_type(Integer, op, Unsigned), Some(Unsigned));
            assert_matches!(binary_data_type(Unsigned, op, Integer), Some(Unsigned));
        }

        // Fallible cases

        assert_matches!(binary_data_type(Tag, Add, Tag), None);
        assert_matches!(binary_data_type(String, Add, String), None);
        assert_matches!(binary_data_type(Timestamp, Add, Timestamp), None);
    }

    #[test]
    fn test_evaluate_type() {
        let namespace = MockSchemaProvider::default();

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT shared_field0 FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt =
            map_select(&namespace, &parse_select("SELECT shared_tag0 FROM temp_01")).unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Tag);

        // Unknown
        let stmt = map_select(&namespace, &parse_select("SELECT not_exists FROM temp_01")).unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from).unwrap();
        assert!(res.is_none());

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT shared_field0 FROM temp_02"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT shared_field0 FROM temp_02"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        // Same field across multiple measurements resolves to the highest precedence (float)
        let stmt = map_select(
            &namespace,
            &parse_select("SELECT shared_field0 FROM temp_01, temp_02"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // Explicit cast of integer field to float
        let stmt = map_select(
            &namespace,
            &parse_select("SELECT SUM(field_i64::float) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        //
        // Binary expressions
        //

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT field_f64 + field_i64 FROM all_types"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT field_bool | field_bool FROM all_types"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Boolean);

        // Fallible

        // Verify incompatible operators and operator error
        let stmt = map_select(
            &namespace,
            &parse_select("SELECT field_f64 & field_i64 FROM all_types"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from);
        assert_matches!(res, Err(DataFusionError::Plan(ref s)) if s == "incompatible operands for operator &: float and integer");

        // data types for functions
        let stmt = map_select(
            &namespace,
            &parse_select("SELECT SUM(field_f64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT SUM(field_i64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT SUM(field_u64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Unsigned);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT MIN(field_f64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT MAX(field_i64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT FIRST(field_str) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::String);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT LAST(field_str) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::String);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT MEAN(field_i64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT MEAN(field_u64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT COUNT(field_f64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT COUNT(field_i64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT COUNT(field_u64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT COUNT(field_str) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        // Float functions
        for call in [
            "median(field_i64)",
            "integral(field_i64)",
            "stddev(field_i64)",
            "derivative(field_i64)",
            "non_negative_derivative(field_i64)",
            "moving_average(field_i64, 2)",
            "exponential_moving_average(field_i64, 2)",
            "double_exponential_moving_average(field_i64, 2)",
            "triple_exponential_moving_average(field_i64, 2)",
            "relative_strength_index(field_i64, 2)",
            "triple_exponential_derivative(field_i64, 2)",
            "kaufmans_efficiency_ratio(field_i64, 2)",
            "kaufmans_adaptive_moving_average(field_i64, 2)",
            "chande_momentum_oscillator(field_i64, 2)",
        ] {
            let stmt = map_select(
                &namespace,
                &parse_select(&format!("SELECT {call} FROM temp_01")),
            )
            .unwrap();
            let field = stmt.fields.first().unwrap();
            let res = evaluate_type(&namespace, &field.expr, &stmt.from)
                .unwrap()
                .unwrap();
            assert_matches!(res, VarRefDataType::Float);
        }

        // holt_winters
        let stmt = map_select(
            &namespace,
            &parse_select(&format!(
                "SELECT holt_winters(mean(field_i64), 2, 3) FROM temp_01 GROUP BY TIME(10s)"
            )),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // holt_winters_with_fit
        let stmt = map_select(
            &namespace,
            &parse_select(&format!(
                "SELECT holt_winters_with_fit(mean(field_i64), 2, 3) FROM temp_01 GROUP BY TIME(10s)"
            )),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // Integer functions
        let stmt = map_select(
            &namespace,
            &parse_select("SELECT elapsed(field_i64) FROM temp_01"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        // subqueries

        let stmt = map_select(
            &namespace,
            &parse_select("SELECT inner FROM (SELECT field_f64 as inner FROM temp_01)"),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = map_select(
            &namespace,
            &parse_select(
                "SELECT inner FROM (SELECT shared_tag0, field_f64 as inner FROM temp_01)",
            ),
        )
        .unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = map_select(&namespace, &parse_select(
            "SELECT shared_tag0, inner FROM (SELECT shared_tag0, field_f64 as inner FROM temp_01)",
        )).unwrap();
        let field = stmt.fields.first().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Tag);
    }
}
