#![cfg(test)]

/// Constructs an [crate::expression::arithmetic::Expr::VarRef] expression.
#[macro_export]
macro_rules! var_ref {
    ($NAME: literal) => {
        $crate::expression::Expr::VarRef($crate::expression::VarRef {
            name: $NAME.into(),
            data_type: None,
        })
    };

    ($NAME: literal, $TYPE: ident) => {
        $crate::expression::Expr::VarRef($crate::expression::VarRef {
            name: $NAME.into(),
            data_type: Some($crate::expression::arithmetic::VarRefDataType::$TYPE),
        })
    };
}

/// Constructs a regular expression [crate::expression::arithmetic::Expr::Literal].
#[macro_export]
macro_rules! regex {
    ($EXPR: expr) => {
        $crate::expression::arithmetic::Expr::Literal(
            $crate::literal::Literal::Regex($EXPR.into()).into(),
        )
    };
}

/// Constructs a [crate::expression::arithmetic::Expr::BindParameter] expression.
#[macro_export]
macro_rules! param {
    ($EXPR: expr) => {
        $crate::expression::arithmetic::Expr::BindParameter(
            $crate::parameter::BindParameter::new($EXPR.into()).into(),
        )
    };
}

/// Constructs a [crate::expression::conditional::ConditionalExpression::Grouped] expression.
#[macro_export]
macro_rules! grouped {
    ($EXPR: expr) => {
        <$crate::expression::conditional::ConditionalExpression as std::convert::Into<
            Box<$crate::expression::conditional::ConditionalExpression>,
        >>::into($crate::expression::conditional::ConditionalExpression::Grouped($EXPR.into()))
    };
}

/// Constructs a [crate::expression::arithmetic::Expr::Nested] expression.
#[macro_export]
macro_rules! nested {
    ($EXPR: expr) => {
        <$crate::expression::arithmetic::Expr as std::convert::Into<
            Box<$crate::expression::arithmetic::Expr>,
        >>::into($crate::expression::arithmetic::Expr::Nested($EXPR.into()))
    };
}

/// Constructs a [crate::expression::arithmetic::Expr::Call] expression.
#[macro_export]
macro_rules! call {
    ($NAME:literal) => {
        $crate::expression::Expr::Call($crate::expression::Call {
            name: $NAME.into(),
            args: vec![],
        })
    };
    ($NAME:literal, $( $ARG:expr ),+) => {
        $crate::expression::Expr::Call($crate::expression::Call {
            name: $NAME.into(),
            args: vec![$( $ARG ),+],
        })
    };
}

/// Constructs a [crate::expression::arithmetic::Expr::Distinct] expression.
#[macro_export]
macro_rules! distinct {
    ($IDENT:literal) => {
        $crate::expression::arithmetic::Expr::Distinct($IDENT.into())
    };
}

/// Constructs a [crate::expression::arithmetic::Expr::Wildcard] expression.
#[macro_export]
macro_rules! wildcard {
    () => {
        $crate::expression::arithmetic::Expr::Wildcard(None)
    };
    (tag) => {
        $crate::expression::arithmetic::Expr::Wildcard(Some(
            $crate::expression::arithmetic::WildcardType::Tag,
        ))
    };
    (field) => {
        $crate::expression::arithmetic::Expr::Wildcard(Some(
            $crate::expression::arithmetic::WildcardType::Field,
        ))
    };
}

/// Constructs a [crate::expression::arithmetic::Expr::Binary] expression.
#[macro_export]
macro_rules! binary_op {
    ($LHS: expr, $OP: ident, $RHS: expr) => {
        $crate::expression::Expr::Binary($crate::expression::Binary {
            lhs: $LHS.into(),
            op: $crate::expression::BinaryOperator::$OP,
            rhs: $RHS.into(),
        })
    };
}

/// Constructs a [crate::expression::conditional::ConditionalExpression::Binary] expression.
#[macro_export]
macro_rules! cond_op {
    ($LHS: expr, $OP: ident, $RHS: expr) => {
        <$crate::expression::ConditionalExpression as std::convert::Into<
            Box<$crate::expression::ConditionalExpression>,
        >>::into($crate::expression::ConditionalExpression::Binary(
            $crate::expression::ConditionalBinary {
                lhs: $LHS.into(),
                op: $crate::expression::ConditionalOperator::$OP,
                rhs: $RHS.into(),
            },
        ))
    };
}
