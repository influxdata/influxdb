use influxdb_influxql_parser::expression::Expr;
use influxdb_influxql_parser::select::{Field, SelectStatement};
use influxdb_influxql_parser::visit::{Recursion, Visitable, Visitor};
use std::ops::Deref;

/// Returns the name of the field.
///
/// Prefers the alias if set, otherwise derives the name
/// from [Expr::VarRef] or [Expr::Call]. Finally, if neither
/// are available, falls back to an empty string.
///
/// Derived from [Go implementation](https://github.com/influxdata/influxql/blob/1ba470371ec093d57a726b143fe6ccbacf1b452b/ast.go#L3326-L3328)
pub(crate) fn field_name(f: &Field) -> String {
    if let Some(alias) = &f.alias {
        return alias.to_string();
    }

    let mut expr = &f.expr;
    loop {
        expr = match expr {
            Expr::Call { name, .. } => return name.clone(),
            Expr::Nested(nested) => nested,
            Expr::Binary { .. } => return binary_expr_name(&f.expr),
            Expr::Distinct(_) => return "distinct".to_string(),
            Expr::VarRef { name, .. } => return name.deref().into(),
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
pub(crate) fn field_by_name(select: &SelectStatement, name: &str) -> Option<Field> {
    select.fields
        .iter()
        .find(|f| {
            field_name(f) == name || match &f.expr {
                Expr::Call { name: func_name, args } if (func_name.eq_ignore_ascii_case("top")
                    || func_name.eq_ignore_ascii_case("bottom"))
                    && args.len() > 2 =>
                    args[1..].iter().any(|f| matches!(f, Expr::VarRef { name: field_name, .. } if field_name.as_str() == name)),
                _ => false,
            }
        })
        .cloned()
}

struct BinaryExprNameVisitor<'a>(&'a mut Vec<String>);

impl<'a> Visitor for BinaryExprNameVisitor<'a> {
    type Error = ();

    fn pre_visit_expr(self, n: &Expr) -> Result<Recursion<Self>, Self::Error> {
        match n {
            Expr::Call { name, .. } => self.0.push(name.clone()),
            Expr::VarRef { name, .. } => self.0.push(name.to_string()),
            _ => {}
        };

        Ok(Recursion::Continue(self))
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
    use crate::plan::influxql::field::{field_by_name, field_name};
    use crate::plan::influxql::test_utils::{get_first_field, parse_select};
    use assert_matches::assert_matches;

    #[test]
    fn test_field_name() {
        let f = get_first_field("SELECT usage FROM cpu");
        assert_eq!(field_name(&f), "usage");

        let f = get_first_field("SELECT usage as u2 FROM cpu");
        assert_eq!(field_name(&f), "u2");

        let f = get_first_field("SELECT (usage) FROM cpu");
        assert_eq!(field_name(&f), "usage");

        let f = get_first_field("SELECT COUNT(usage) FROM cpu");
        assert_eq!(field_name(&f), "COUNT");

        let f = get_first_field("SELECT COUNT(usage) + SUM(usage_idle) FROM cpu");
        assert_eq!(field_name(&f), "COUNT_usage_SUM_usage_idle");

        let f = get_first_field("SELECT 1+2 FROM cpu");
        assert_eq!(field_name(&f), "");

        let f = get_first_field("SELECT 1 + usage FROM cpu");
        assert_eq!(field_name(&f), "usage");

        let f = get_first_field("SELECT /reg/ FROM cpu");
        assert_eq!(field_name(&f), "");

        let f = get_first_field("SELECT DISTINCT usage FROM cpu");
        assert_eq!(field_name(&f), "distinct");

        let f = get_first_field("SELECT -usage FROM cpu");
        assert_eq!(field_name(&f), "usage");

        // Doesn't quote keyword
        let f = get_first_field("SELECT \"user\" FROM cpu");
        assert_eq!(field_name(&f), "user");
    }

    #[test]
    fn test_field_by_name() {
        let stmt = parse_select("SELECT usage, idle FROM cpu");
        assert_eq!(
            format!("{}", field_by_name(&stmt, "usage").unwrap()),
            "usage"
        );

        let stmt = parse_select("SELECT usage as foo, usage FROM cpu");
        assert_eq!(
            format!("{}", field_by_name(&stmt, "foo").unwrap()),
            "usage AS foo"
        );

        let stmt = parse_select("SELECT top(idle, usage, 5), usage FROM cpu");
        assert_eq!(
            format!("{}", field_by_name(&stmt, "usage").unwrap()),
            "top(idle, usage, 5)"
        );

        let stmt = parse_select("SELECT bottom(idle, usage, 5), usage FROM cpu");
        assert_eq!(
            format!("{}", field_by_name(&stmt, "usage").unwrap()),
            "bottom(idle, usage, 5)"
        );

        let stmt = parse_select("SELECT top(idle, usage, 5) as foo, usage FROM cpu");
        assert_eq!(
            format!("{}", field_by_name(&stmt, "usage").unwrap()),
            "top(idle, usage, 5) AS foo"
        );
        assert_eq!(
            format!("{}", field_by_name(&stmt, "foo").unwrap()),
            "top(idle, usage, 5) AS foo"
        );

        // Not exists

        let stmt = parse_select("SELECT usage, idle FROM cpu");
        assert_matches!(field_by_name(&stmt, "bar"), None);

        // Does not match name by first argument to top or bottom, per
        // bug in original implementation.
        let stmt = parse_select("SELECT top(foo, usage, 5), idle FROM cpu");
        assert_matches!(field_by_name(&stmt, "foo"), None);
        assert_eq!(format!("{}", field_by_name(&stmt, "idle").unwrap()), "idle");
    }
}
