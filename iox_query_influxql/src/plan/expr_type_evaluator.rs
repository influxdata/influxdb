use crate::plan::field::field_by_name;
use crate::plan::field_mapper::map_type;
use crate::plan::{error, SchemaProvider};
use datafusion::common::Result;
use influxdb_influxql_parser::common::{MeasurementName, QualifiedMeasurementName};
use influxdb_influxql_parser::expression::{Call, Expr, VarRef, VarRefDataType};
use influxdb_influxql_parser::literal::Literal;
use influxdb_influxql_parser::select::{Dimension, FromMeasurementClause, MeasurementSelection};
use itertools::Itertools;

/// Evaluate the type of the specified expression.
///
/// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4796-L4797).
pub(crate) fn evaluate_type(
    s: &dyn SchemaProvider,
    expr: &Expr,
    from: &FromMeasurementClause,
) -> Result<Option<VarRefDataType>> {
    TypeEvaluator::new(from, s).eval_type(expr)
}

struct TypeEvaluator<'a> {
    s: &'a dyn SchemaProvider,
    from: &'a FromMeasurementClause,
}

impl<'a> TypeEvaluator<'a> {
    fn new(from: &'a FromMeasurementClause, s: &'a dyn SchemaProvider) -> Self {
        Self { from, s }
    }

    fn eval_type(&self, expr: &Expr) -> Result<Option<VarRefDataType>> {
        Ok(match expr {
            Expr::VarRef(v) => self.eval_var_ref(v)?,
            Expr::Call(v) => self.eval_call(v)?,
            // NOTE: This is a deviation from https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4635,
            // as we'll let DataFusion determine the column type and if the types are compatible.
            Expr::Binary { .. } => None,
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

    /// Returns the type for the specified [`Expr`].
    /// This function assumes that the expression has already been reduced.
    fn eval_var_ref(&self, expr: &VarRef) -> Result<Option<VarRefDataType>> {
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
                for ms in self.from.iter() {
                    match ms {
                        MeasurementSelection::Name(QualifiedMeasurementName {
                            name: MeasurementName::Name(ident),
                            ..
                        }) => match (
                            data_type,
                            map_type(self.s, ident.as_str(), expr.name.as_str())?,
                        ) {
                            (Some(existing), Some(res)) => {
                                if res < existing {
                                    data_type = Some(res)
                                }
                            }
                            (None, Some(res)) => data_type = Some(res),
                            _ => continue,
                        },
                        MeasurementSelection::Subquery(select) => {
                            // find the field by name
                            if let Some(field) = field_by_name(select, expr.name.as_str()) {
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
                        _ => {
                            return error::internal("eval_var_ref: Unexpected MeasurementSelection")
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

#[cfg(test)]
mod test {
    use crate::plan::expr_type_evaluator::evaluate_type;
    use crate::plan::test_utils::{parse_select, MockSchemaProvider};
    use assert_matches::assert_matches;
    use influxdb_influxql_parser::expression::VarRefDataType;

    #[test]
    fn test_evaluate_type() {
        let namespace = MockSchemaProvider::default();

        let stmt = parse_select("SELECT shared_field0 FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = parse_select("SELECT shared_tag0 FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Tag);

        // Unknown
        let stmt = parse_select("SELECT not_exists FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from).unwrap();
        assert!(res.is_none());

        let stmt = parse_select("SELECT shared_field0 FROM temp_02");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = parse_select("SELECT shared_field0 FROM temp_02");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        // Same field across multiple measurements resolves to the highest precedence (float)
        let stmt = parse_select("SELECT shared_field0 FROM temp_01, temp_02");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // Explicit cast of integer field to float
        let stmt = parse_select("SELECT SUM(field_i64::float) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        // data types for functions
        let stmt = parse_select("SELECT SUM(field_f64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = parse_select("SELECT SUM(field_i64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = parse_select("SELECT SUM(field_u64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Unsigned);

        let stmt = parse_select("SELECT MIN(field_f64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = parse_select("SELECT MAX(field_i64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = parse_select("SELECT FIRST(field_str) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::String);

        let stmt = parse_select("SELECT LAST(field_str) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::String);

        let stmt = parse_select("SELECT MEAN(field_i64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = parse_select("SELECT MEAN(field_u64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = parse_select("SELECT COUNT(field_f64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = parse_select("SELECT COUNT(field_i64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = parse_select("SELECT COUNT(field_u64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        let stmt = parse_select("SELECT COUNT(field_str) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        // Float functions
        for name in [
            "median",
            "integral",
            "stddev",
            "derivative",
            "non_negative_derivative",
            "moving_average",
            "exponential_moving_average",
            "double_exponential_moving_average",
            "triple_exponential_moving_average",
            "relative_strength_index",
            "triple_exponential_derivative",
            "kaufmans_efficiency_ratio",
            "kaufmans_adaptive_moving_average",
            "chande_momentum_oscillator",
            "holt_winters",
            "holt_winters_with_fit",
        ] {
            let stmt = parse_select(&format!("SELECT {name}(field_i64) FROM temp_01"));
            let field = stmt.fields.head().unwrap();
            let res = evaluate_type(&namespace, &field.expr, &stmt.from)
                .unwrap()
                .unwrap();
            assert_matches!(res, VarRefDataType::Float);
        }

        // Integer functions
        let stmt = parse_select("SELECT elapsed(field_i64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

        // Invalid function
        let stmt = parse_select("SELECT not_valid(field_i64) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .is_none();
        assert!(res);

        // subqueries

        let stmt = parse_select("SELECT inner FROM (SELECT field_f64 as inner FROM temp_01)");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt =
            parse_select("SELECT inner FROM (SELECT shared_tag0, field_f64 as inner FROM temp_01)");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Float);

        let stmt = parse_select(
            "SELECT shared_tag0, inner FROM (SELECT shared_tag0, field_f64 as inner FROM temp_01)",
        );
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Tag);
    }
}
