use crate::plan::ir::Field;
use influxdb_influxql_parser::expression::{Call, Expr, VarRef};
use influxdb_influxql_parser::visit::{Recursion, Visitable, Visitor};
use std::ops::Deref;

/// Returns the name of the field.
///
/// Prefers the alias if set, otherwise derives the name
/// from [Expr::VarRef] or [Expr::Call]. Finally, if neither
/// are available, falls back to an empty string.
///
/// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3326-L3328)
pub(crate) fn field_name(f: &influxdb_influxql_parser::select::Field) -> String {
    if let Some(alias) = &f.alias {
        return alias.deref().to_string();
    }

    let mut expr = &f.expr;
    loop {
        expr = match expr {
            Expr::Call(Call { name, .. }) => return name.clone(),
            Expr::Nested(nested) => nested,
            Expr::Binary { .. } => return binary_expr_name(&f.expr),
            Expr::Distinct(_) => return "distinct".to_string(),
            Expr::VarRef(VarRef { name, .. }) => return name.deref().into(),
            Expr::Wildcard(_) | Expr::BindParameter(_) | Expr::Literal(_) => return "".to_string(),
        };
    }
}

/// Returns the expression that matches the field name.
///
/// If the name matches one of the arguments to
/// "top" or "bottom", the variable reference inside of the function is returned.
///
/// Derive from [this implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L1725)
///
/// **NOTE**
///
/// This implementation duplicates the behavior of the original implementation, including skipping the
/// first argument. It is likely the original intended to skip the _last_ argument, which is the number
/// of rows.
pub(super) fn field_by_name<'a>(fields: &'a [Field], name: &str) -> Option<&'a Field> {
    fields.iter().find(|f| f.name == name || match &f.expr {
        Expr::Call(Call{ name: func_name, args }) if (func_name == "top"
            || func_name == "bottom")
            && args.len() > 2 =>
            args[1..].iter().any(|f| matches!(f, Expr::VarRef(VarRef{ name: field_name, .. }) if field_name.as_str() == name)),
        _ => false,
    })
}

struct BinaryExprNameVisitor<'a>(&'a mut Vec<String>);

impl<'a> Visitor for BinaryExprNameVisitor<'a> {
    type Error = ();

    fn pre_visit_var_ref(self, n: &VarRef) -> Result<Recursion<Self>, Self::Error> {
        self.0.push(n.name.to_string());
        Ok(Recursion::Continue(self))
    }

    fn pre_visit_call(self, n: &Call) -> Result<Recursion<Self>, Self::Error> {
        self.0.push(n.name.clone());
        Ok(Recursion::Stop(self))
    }
}

/// Returns the name of a binary expression by concatenating
/// the names of any [Expr::VarRef] and [Expr::Call] with underscores.
///
/// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3729-L3731)
fn binary_expr_name(expr: &Expr) -> String {
    let mut names = Vec::new();
    let vis = BinaryExprNameVisitor(&mut names);
    expr.accept(vis).unwrap(); // It is not expected to fail
    names.join("_")
}

#[cfg(test)]
mod test {
    use crate::plan::field::{field_by_name, field_name};
    use crate::plan::ir;
    use assert_matches::assert_matches;
    use influxdb_influxql_parser::select::Field;

    #[test]
    fn test_field_name() {
        let f: Field = "usage".parse().unwrap();
        assert_eq!(field_name(&f), "usage");

        let f: Field = "usage as u2".parse().unwrap();
        assert_eq!(field_name(&f), "u2");

        let f: Field = "(usage)".parse().unwrap();
        assert_eq!(field_name(&f), "usage");

        let f: Field = "COUNT(usage)".parse().unwrap();
        assert_eq!(field_name(&f), "count");

        let f: Field = "COUNT(usage) + SUM(usage_idle)".parse().unwrap();
        assert_eq!(field_name(&f), "count_sum");

        let f: Field = "1+2".parse().unwrap();
        assert_eq!(field_name(&f), "");

        let f: Field = "1 + usage".parse().unwrap();
        assert_eq!(field_name(&f), "usage");

        let f: Field = "/reg/".parse().unwrap();
        assert_eq!(field_name(&f), "");

        let f: Field = "DISTINCT usage".parse().unwrap();
        assert_eq!(field_name(&f), "distinct");

        let f: Field = "-usage".parse().unwrap();
        assert_eq!(field_name(&f), "usage");

        // Doesn't quote keyword
        let f: Field = "\"user\"".parse().unwrap();
        assert_eq!(field_name(&f), "user");
    }

    #[test]
    fn test_field_by_name() {
        fn parse_fields(exprs: Vec<&str>) -> Vec<ir::Field> {
            exprs
                .iter()
                .map(|s| {
                    let f: Field = s.parse().unwrap();
                    let name = field_name(&f);
                    let data_type = None;
                    ir::Field {
                        expr: f.expr,
                        name,
                        data_type,
                    }
                })
                .collect()
        }
        let stmt = parse_fields(vec!["usage", "idle"]);
        assert_eq!(
            format!("{}", field_by_name(&stmt, "usage").unwrap()),
            "usage AS usage"
        );

        let stmt = parse_fields(vec!["usage as foo", "usage"]);
        assert_eq!(
            format!("{}", field_by_name(&stmt, "foo").unwrap()),
            "usage AS foo"
        );

        let stmt = parse_fields(vec!["top(idle, usage, 5)", "usage"]);
        assert_eq!(
            format!("{}", field_by_name(&stmt, "usage").unwrap()),
            "top(idle, usage, 5) AS top"
        );

        let stmt = parse_fields(vec!["bottom(idle, usage, 5)", "usage"]);
        assert_eq!(
            format!("{}", field_by_name(&stmt, "usage").unwrap()),
            "bottom(idle, usage, 5) AS bottom"
        );

        // TOP is in uppercase, to ensure we can expect the function name to be
        // uniformly lowercase.
        let stmt = parse_fields(vec!["TOP(idle, usage, 5) as foo", "usage"]);
        assert_eq!(
            format!("{}", field_by_name(&stmt, "usage").unwrap()),
            "top(idle, usage, 5) AS foo"
        );
        assert_eq!(
            format!("{}", field_by_name(&stmt, "foo").unwrap()),
            "top(idle, usage, 5) AS foo"
        );

        // Not exists

        let stmt = parse_fields(vec!["usage", "idle"]);
        assert_matches!(field_by_name(&stmt, "bar"), None);

        // Does not match name by first argument to top or bottom, per
        // bug in original implementation.
        let stmt = parse_fields(vec!["top(foo, usage, 5)", "idle"]);
        assert_matches!(field_by_name(&stmt, "foo"), None);
        assert_eq!(
            format!("{}", field_by_name(&stmt, "idle").unwrap()),
            "idle AS idle"
        );
    }
}
