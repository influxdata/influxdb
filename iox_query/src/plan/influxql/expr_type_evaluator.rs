use crate::plan::influxql::field::field_by_name;
use crate::plan::influxql::field_mapper::map_type;
use datafusion::common::{DataFusionError, Result};
use influxdb_influxql_parser::common::{MeasurementName, QualifiedMeasurementName};
use influxdb_influxql_parser::expression::{Expr, VarRefDataType};
use influxdb_influxql_parser::literal::Literal;
use influxdb_influxql_parser::select::{Dimension, FromMeasurementClause, MeasurementSelection};
use itertools::Itertools;
use predicate::rpc_predicate::QueryNamespaceMeta;

/// Evaluate the type of the specified expression.
///
/// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L4796-L4797).
pub(crate) fn evaluate_type(
    namespace: &dyn QueryNamespaceMeta,
    expr: &Expr,
    from: &FromMeasurementClause,
) -> Result<Option<VarRefDataType>> {
    TypeEvaluator::new(from, namespace).eval_type(expr)
}

struct TypeEvaluator<'a> {
    namespace: &'a dyn QueryNamespaceMeta,
    from: &'a FromMeasurementClause,
}

impl<'a> TypeEvaluator<'a> {
    fn new(from: &'a FromMeasurementClause, namespace: &'a dyn QueryNamespaceMeta) -> Self {
        Self { from, namespace }
    }

    fn eval_type(&self, expr: &Expr) -> Result<Option<VarRefDataType>> {
        Ok(match expr {
            Expr::VarRef { name, data_type } => self.eval_var_ref(name.as_str(), data_type)?,
            Expr::Call { name, args } => self.eval_call(name.as_str(), args)?,
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
    fn eval_var_ref(
        &self,
        name: &str,
        data_type: &Option<VarRefDataType>,
    ) -> Result<Option<VarRefDataType>> {
        Ok(match data_type {
            Some(dt)
                if matches!(
                    dt,
                    VarRefDataType::Integer
                        | VarRefDataType::Float
                        | VarRefDataType::String
                        | VarRefDataType::Boolean
                ) =>
            {
                Some(*dt)
            }
            _ => {
                let mut data_type: Option<VarRefDataType> = None;
                for ms in self.from.iter() {
                    match ms {
                        MeasurementSelection::Name(QualifiedMeasurementName {
                            name: MeasurementName::Name(ident),
                            ..
                        }) => match (data_type, map_type(self.namespace, ident.as_str(), name)?) {
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
                            if let Some(field) = field_by_name(select, name) {
                                match (
                                    data_type,
                                    evaluate_type(self.namespace, &field.expr, &select.from)?,
                                ) {
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
                                        matches!(dim, Dimension::Tag(ident) if ident.as_str() == name)
                                    }) {
                                        data_type = Some(VarRefDataType::Tag);
                                    }
                                }
                            }
                        }
                        _ => {
                            return Err(DataFusionError::Internal(
                                "eval_var_ref: Unexpected MeasurementSelection".to_string(),
                            ))
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
    fn eval_call(&self, name: &str, args: &[Expr]) -> Result<Option<VarRefDataType>> {
        // Evaluate the data types of the arguments
        let arg_types: Vec<_> = args.iter().map(|expr| self.eval_type(expr)).try_collect()?;

        Ok(match name.to_ascii_lowercase().as_str() {
            "mean" => Some(VarRefDataType::Float),
            "count" => Some(VarRefDataType::Integer),
            "min" | "max" | "sum" | "first" | "last" => match arg_types.first() {
                Some(v) => *v,
                None => None,
            },
            _ => None,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::plan::influxql::expr_type_evaluator::evaluate_type;
    use crate::plan::influxql::test_utils::{parse_select, MockNamespace};
    use assert_matches::assert_matches;
    use influxdb_influxql_parser::expression::VarRefDataType;

    #[test]
    fn test_evaluate_type() {
        let namespace = MockNamespace::default();

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

        let stmt = parse_select("SELECT COUNT(field_str) FROM temp_01");
        let field = stmt.fields.head().unwrap();
        let res = evaluate_type(&namespace, &field.expr, &stmt.from)
            .unwrap()
            .unwrap();
        assert_matches!(res, VarRefDataType::Integer);

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
