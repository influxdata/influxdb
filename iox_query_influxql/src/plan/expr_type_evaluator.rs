use crate::error;
use crate::plan::field::field_by_name;
use crate::plan::field_mapper::map_type;
use crate::plan::ir::DataSource;
use crate::plan::var_ref::influx_type_to_var_ref_data_type;
use crate::plan::SchemaProvider;
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
pub(super) struct TypeEvaluator<'a> {
    s: &'a dyn SchemaProvider,
    from: &'a [DataSource],
    /// Setting this to `true` will ensure scalar functions return errors for invalid data types.
    /// The default is false, to ensure compatibility with InfluxQL OG.
    call_type_is_strict: bool,
}

impl<'a> TypeEvaluator<'a> {
    /// Create a `TypeEvaluator` with behavior compatible with InfluxQL OG.
    ///
    /// This behavior includes limited evaluation of [`Call`] expressions, as described
    /// by [`TypeEvaluator::eval_scalar`].
    pub(super) fn new(s: &'a dyn SchemaProvider, from: &'a [DataSource]) -> Self {
        Self {
            from,
            s,
            call_type_is_strict: false,
        }
    }

    /// Create a `TypeEvaluator` with strict behavior.
    ///
    /// This behavior includes strict evaluation of [`Call`] expressions, that are
    /// not compatible with InfluxQL OG, but may be enabled in the future to improve
    /// the user experience.
    ///
    /// # NOTE
    ///
    /// This behaviour is unused in production, but may be enabled to improve the
    /// user experience of InfluxQL.
    #[cfg(test)]
    fn new_strict(s: &'a dyn SchemaProvider, from: &'a [DataSource]) -> Self {
        Self {
            from,
            s,
            call_type_is_strict: true,
        }
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
            (Some(lhs), Some(rhs)) => {
                Ok(Some(binary_data_type(lhs, op, rhs).ok_or_else(|| {
                    error::map::query(format!(
                        "incompatible operands for operator {op}: {lhs} and {rhs}"
                    ))
                })?))
            }
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
                        | VarRefDataType::Tag
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
                            map_type(self.s, name.as_str(), expr.name.as_str()),
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
                                match (data_type, influx_type_to_var_ref_data_type(field.data_type))
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
                                        matches!(dim, Dimension::VarRef(VarRef { name, ..}) if name.as_str() == expr.name.as_str())
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
            // These functions return the same type as their first argument
            "min" | "max" | "sum" | "first" | "last" | "distinct" => match arg_types.first() {
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

            name => self.eval_scalar(name, &arg_types)?,
        })
    }

    /// Evaluate the data type of a scalar function
    ///
    /// See: <https://github.com/influxdata/influxdb/blob/343ce4223810ecdbc7f4de68f2509a51b28f2c56/query/math.go#L24>
    ///
    /// 💥InfluxQL OG has a bug that it does not evaluate call types correctly, and returns
    /// the incorrect type by unconditionally using the first argument. It does not even call the
    /// mapper to evaluate scalar functions. We must replicate the InfluxQL OG behaviour,
    /// or queries will fail, that would ordinarily succeed.
    ///
    /// The bug may be traced through the OG source as follows.
    ///
    /// Prior to executing a `SELECT`, the following steps occur to validate all the field
    /// expression types.
    ///
    /// 1. Calls `validateTypes` to ensure all field data types are valid:
    ///    <https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/compile.go#L1186-L1187>
    ///
    /// 2. Uses a `MultiTypeMapper` to evaluate types, combining:
    ///
    ///    * a `FunctionTypeMapper` for sum, min, max, etc
    ///    * a `MathTypeMapper` for scalar functions like log, abs, etc
    ///
    ///    ⚠️NOTE: the order is important. `FunctionTypeMapper` is called first.
    ///
    ///    See: <https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L973-L976>
    ///
    /// 3. Call `EvalType` for each field:
    ///
    ///    See: <https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/select.go#L979>
    ///
    /// 4. For fields that have call expressions, the `evalCallExprType` function is ultimately called
    ///
    ///    See: <https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4697>
    ///
    /// 5. Because the `TypeMapper` is a `CallTypeMapper`, `evalCallExprType` eventually calls `CallType`:
    ///
    ///    See: <https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4715>
    ///
    /// 6. The `TypeMapper` is a `multiTypeMapper` and thus calls `CallType` for each instance. The first
    ///    inner call that returns no error and the `typ` is not `Unknown` will be returned to the caller
    ///
    ///    See: <https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4610-L4615>
    ///
    /// 7. Recall, the first `TypeMapper` is `FunctionTypeMapper`, so it's `CallType` is
    ///    called first.
    ///
    ///    🪳Here is the bug, which is that `FunctionTypeMapper::CallType` always returns
    ///      the type of the first argument:
    ///
    ///    See: <https://github.com/influxdata/influxdb/blob/f365bb7e3a9c5e227dbf66d84adf674d3d127176/query/functions.go#L98-L99>
    fn eval_scalar(
        &self,
        name: &str,
        arg_types: &[Option<VarRefDataType>],
    ) -> Result<Option<VarRefDataType>> {
        if self.call_type_is_strict {
            self.eval_scalar_strict(name, arg_types)
        } else {
            self.eval_scalar_compatible(arg_types)
        }
    }

    fn eval_scalar_compatible(
        &self,
        arg_types: &[Option<VarRefDataType>],
    ) -> Result<Option<VarRefDataType>> {
        Ok(arg_types.first().and_then(|v| *v))
    }

    fn eval_scalar_strict(
        &self,
        name: &str,
        arg_types: &[Option<VarRefDataType>],
    ) -> Result<Option<VarRefDataType>> {
        match name {
            // These functions require a single numeric as input and return a float
            name @ ("sin" | "cos" | "tan" | "atan" | "exp" | "log" | "ln" | "log2" | "log10"
            | "sqrt") => {
                match arg_types
                    .get(0)
                    .ok_or_else(|| error::map::query(format!("{name} expects 1 argument")))?
                {
                    Some(
                        VarRefDataType::Float | VarRefDataType::Integer | VarRefDataType::Unsigned,
                    )
                    | None => Ok(Some(VarRefDataType::Float)),
                    Some(arg0) => error::query(format!(
                        "invalid argument type for {name}: expected a number, got {arg0}"
                    )),
                }
            }

            // These functions require a single float as input and return a float
            name @ ("asin" | "acos") => {
                match arg_types
                    .get(0)
                    .ok_or_else(|| error::map::query(format!("{name} expects 1 argument")))?
                {
                    Some(VarRefDataType::Float) | None => Ok(Some(VarRefDataType::Float)),
                    Some(arg0) if self.call_type_is_strict => error::query(format!(
                        "invalid argument type for {name}: expected a float, got {arg0}"
                    )),
                    _ => Ok(None),
                }
            }

            // These functions require two numeric arguments and return a float
            name @ ("atan2" | "pow") => {
                let (Some(arg0), Some(arg1)) = (arg_types
                                                    .get(0), arg_types.get(1)) else {
                    return error::query(format!("{name} expects 2 arguments"))
                };

                match (arg0, arg1) {
                    (Some(
                        VarRefDataType::Float
                        | VarRefDataType::Integer
                        | VarRefDataType::Unsigned
                    ) | None, Some(
                        VarRefDataType::Float
                        | VarRefDataType::Integer
                        | VarRefDataType::Unsigned
                    ) | None) => Ok(Some(VarRefDataType::Float)),
                    (arg0, arg1) if self.call_type_is_strict => error::query(format!(
                        "invalid argument types for {name}: expected a number for both arguments, got ({arg0:?}, {arg1:?})"
                    )),
                    _ => Ok(None),
                }
            }

            // These functions return the same data type as their input
            name @ ("abs" | "floor" | "ceil" | "round") => {
                match arg_types
                    .get(0)
                    .cloned()
                    .ok_or_else(|| error::map::query(format!("{name} expects 1 argument")))?
                {
                    // Return the same data type as the input
                    dt @ Some(
                        VarRefDataType::Float | VarRefDataType::Integer | VarRefDataType::Unsigned,
                    ) => Ok(dt),
                    // If the input is unknown, default to float
                    None => Ok(Some(VarRefDataType::Float)),
                    Some(arg0) if self.call_type_is_strict => error::query(format!(
                        "invalid argument type for {name}: expected a number, got {arg0}"
                    )),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
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

        // Integers using the division operator are always float
        //
        // See:
        // * https://github.com/influxdata/influxql/blob/802555d6b3a35cd464a6d8afa2a6511002cf3c2c/ast.go#L4335-L4340
        // * https://github.com/influxdata/influxdb/blob/3372d3b878ebcba708dc9edfce7ea83cc8152393/query/cursor.go#L178
        (Integer, Div, Integer) => Some(Float),

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
    use super::*;
    use crate::plan::expr_type_evaluator::binary_data_type;
    use crate::plan::ir::DataSource;
    use crate::plan::test_utils::MockSchemaProvider;
    use assert_matches::assert_matches;
    use datafusion::common::DataFusionError;
    use influxdb_influxql_parser::expression::VarRefDataType;
    use influxdb_influxql_parser::select::Field;
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

        // division and integers are special
        assert_matches!(binary_data_type(Integer, Div, Integer), Some(Float));
        assert_matches!(binary_data_type(Unsigned, Div, Unsigned), Some(Unsigned));

        // Integer op Integer | Unsigned op Unsigned
        for op in [Add, Sub, Mul, Mod, BitwiseAnd, BitwiseOr, BitwiseXor] {
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

        fn evaluate_type(
            s: &dyn SchemaProvider,
            expr: &str,
            from: &[&str],
        ) -> Result<Option<VarRefDataType>> {
            let from = from
                .iter()
                .map(ToString::to_string)
                .map(DataSource::Table)
                .collect::<Vec<_>>();
            let Field { expr, .. } = expr.parse().unwrap();
            TypeEvaluator::new(s, &from).eval_type(&expr)
        }

        let res = evaluate_type(&namespace, "shared_field0", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let res = evaluate_type(&namespace, "shared_tag0", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Tag);

        // Unknown
        let res = evaluate_type(&namespace, "not_exists", &["temp_01"]).unwrap();
        assert!(res.is_none());

        let res = evaluate_type(&namespace, "shared_field0", &["temp_02"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let res = evaluate_type(&namespace, "shared_field0", &["temp_02"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        // Same field across multiple measurements resolves to the highest precedence (float)
        let res = evaluate_type(&namespace, "shared_field0", &["temp_01", "temp_02"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // Explicit cast of integer field to float
        let res = evaluate_type(&namespace, "SUM(field_i64::float)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        //
        // Binary expressions
        //

        let res = evaluate_type(&namespace, "field_f64 + field_i64", &["all_types"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let res = evaluate_type(&namespace, "field_bool | field_bool", &["all_types"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Boolean);

        // Fallible

        // Verify incompatible operators and operator error
        let res = evaluate_type(&namespace, "field_f64 & field_i64", &["all_types"]);
        assert_matches!(res, Err(DataFusionError::Plan(ref s)) if s == "incompatible operands for operator &: float and integer");

        // data types for functions
        let res = evaluate_type(&namespace, "SUM(field_f64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let res = evaluate_type(&namespace, "SUM(field_i64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let res = evaluate_type(&namespace, "SUM(field_u64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Unsigned);

        let res = evaluate_type(&namespace, "MIN(field_f64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let res = evaluate_type(&namespace, "MAX(field_i64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let res = evaluate_type(&namespace, "FIRST(field_str)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::String);

        let res = evaluate_type(&namespace, "LAST(field_str)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::String);

        let res = evaluate_type(&namespace, "DISTINCT(field_str)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::String);

        let res = evaluate_type(&namespace, "MEAN(field_i64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let res = evaluate_type(&namespace, "MEAN(field_u64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let res = evaluate_type(&namespace, "COUNT(field_f64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let res = evaluate_type(&namespace, "COUNT(field_i64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let res = evaluate_type(&namespace, "COUNT(field_u64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let res = evaluate_type(&namespace, "COUNT(field_str)", &["temp_01"])
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
            let res = evaluate_type(&namespace, call, &["temp_01"])
                .unwrap()
                .unwrap();
            assert_matches!(res, VarRefDataType::Float);
        }

        // holt_winters
        let res = evaluate_type(
            &namespace,
            "holt_winters(mean(field_i64), 2, 3)",
            &["temp_01"],
        )
        .unwrap()
        .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // holt_winters_with_fit
        let res = evaluate_type(
            &namespace,
            "holt_winters_with_fit(mean(field_i64), 2, 3)",
            &["temp_01"],
        )
        .unwrap()
        .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // Integer functions
        let res = evaluate_type(&namespace, "elapsed(field_i64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        // scalar functions

        // These require a single numeric input and return a float
        let res = evaluate_type(&namespace, "sin(field_f64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // These require a single float as input and return a float
        let res = evaluate_type(&namespace, "asin(field_f64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // These require two numeric arguments as input and return a float
        let res = evaluate_type(&namespace, "atan2(field_f64, 3)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // These require a numeric argument as input and return the same type
        let res = evaluate_type(&namespace, "abs(field_f64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);
        let res = evaluate_type(&namespace, "abs(field_i64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);
        let res = evaluate_type(&namespace, "abs(field_u64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Unsigned);
    }

    /// Validate InfluxQL OG compatible behavior for scalar functions
    #[test]
    fn test_evaluate_type_compat() {
        let namespace = MockSchemaProvider::default();

        fn evaluate_type(
            s: &dyn SchemaProvider,
            expr: &str,
            from: &[&str],
        ) -> Result<Option<VarRefDataType>> {
            let from = from
                .iter()
                .map(ToString::to_string)
                .map(DataSource::Table)
                .collect::<Vec<_>>();
            let Field { expr, .. } = expr.parse().unwrap();
            TypeEvaluator::new(s, &from).eval_type(&expr)
        }

        let res = evaluate_type(&namespace, "sin(field_i64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);
        let res = evaluate_type(&namespace, "sin(field_str)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::String);

        let res = evaluate_type(&namespace, "asin(field_i64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        // invalid number of arguments, still returns data type of first arg
        let res = evaluate_type(&namespace, "atan2(field_f64)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let res = evaluate_type(&namespace, "atan2(field_str, 3)", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::String);
        let res = evaluate_type(&namespace, "atan2(field_i64, 'str')", &["temp_01"])
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);
    }

    /// Validates `TypeEvaluator` when in strict mode.
    #[test]
    fn test_evaluate_type_strict() {
        let namespace = MockSchemaProvider::default();

        fn evaluate_type(
            s: &dyn SchemaProvider,
            expr: &str,
            from: &[&str],
        ) -> Result<Option<VarRefDataType>> {
            let from = from
                .iter()
                .map(ToString::to_string)
                .map(DataSource::Table)
                .collect::<Vec<_>>();
            let Field { expr, .. } = expr.parse().unwrap();
            TypeEvaluator::new_strict(s, &from).eval_type(&expr)
        }

        // In struct mode, these scalar functions should return an error when the arguments are an
        // invalid data type.

        evaluate_type(&namespace, "sin(field_str)", &["temp_01"]).unwrap_err();
        evaluate_type(&namespace, "asin(field_i64)", &["temp_01"]).unwrap_err();
        evaluate_type(&namespace, "atan2(field_f64)", &["temp_01"]).unwrap_err();
        evaluate_type(&namespace, "atan2(field_str, 3)", &["temp_01"]).unwrap_err();
        evaluate_type(&namespace, "atan2(field_i64, 'str')", &["temp_01"]).unwrap_err();
        evaluate_type(&namespace, "abs(field_str)", &["temp_01"]).unwrap_err();
    }
}
