use crate::timestamp::TimestampRange;
use std::{fmt::Write, num::FpCategory};

/// Represents a parsed delete predicate for evaluation by the InfluxDB IOx
/// query engine.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeletePredicate {
    /// Only rows within this range are included in
    /// results. Other rows are excluded.
    pub range: TimestampRange,

    /// Optional arbitrary predicates, represented as list of
    /// expressions applied a logical conjunction (aka they
    /// are 'AND'ed together). Only rows that evaluate to TRUE for all
    /// these expressions should be returned. Other rows are excluded
    /// from the results.
    pub exprs: Vec<DeleteExpr>,
}

impl DeletePredicate {
    /// Format expr to SQL string.
    pub fn expr_sql_string(&self) -> String {
        let mut out = String::new();
        for expr in &self.exprs {
            if !out.is_empty() {
                write!(&mut out, " AND ").expect("writing to a string shouldn't fail");
            }
            write!(&mut out, "{}", expr).expect("writing to a string shouldn't fail");
        }
        out
    }

    /// Return the approximate memory size of the predicate, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.exprs.iter().map(|expr| expr.size()).sum::<usize>()
    }
}

/// Single expression to be used as parts of a predicate.
///
/// Only very simple expression of the type `<column> <op> <scalar>` are supported.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeleteExpr {
    /// Column (w/o table name).
    pub column: String,

    /// Operator.
    pub op: Op,

    /// Scalar value.
    pub scalar: Scalar,
}

impl DeleteExpr {
    /// Create a new [`DeleteExpr`]
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

    /// Return the approximate memory size of the expression, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.column.capacity() + self.scalar.size()
    }
}

impl std::fmt::Display for DeleteExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            r#""{}"{}{}"#,
            self.column()
                .replace(r#"\"#, r#"\\"#)
                .replace(r#"""#, r#"\""#),
            self.op(),
            self.scalar(),
        )
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

/// Scalar value of a certain type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[allow(missing_docs)]
pub enum Scalar {
    Bool(bool),
    I64(i64),
    F64(ordered_float::OrderedFloat<f64>),
    String(String),
}

impl Scalar {
    /// Return the approximate memory size of the scalar, in bytes.
    ///
    /// This includes `Self`.
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>()
            + match &self {
                Self::Bool(_) | Self::I64(_) | Self::F64(_) => 0,
                Self::String(s) => s.capacity(),
            }
    }
}

impl std::fmt::Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Bool(value) => value.fmt(f),
            Scalar::I64(value) => value.fmt(f),
            Scalar::F64(value) => match value.classify() {
                FpCategory::Nan => write!(f, "'NaN'"),
                FpCategory::Infinite if *value.as_ref() < 0.0 => write!(f, "'-Infinity'"),
                FpCategory::Infinite => write!(f, "'Infinity'"),
                _ => write!(f, "{:?}", value.as_ref()),
            },
            Scalar::String(value) => {
                write!(
                    f,
                    "'{}'",
                    value.replace(r#"\"#, r#"\\"#).replace(r#"'"#, r#"\'"#),
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ordered_float::OrderedFloat;

    use super::*;

    #[test]
    fn test_expr_to_sql_no_expressions() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };
        assert_eq!(&pred.expr_sql_string(), "");
    }

    #[test]
    fn test_expr_to_sql_operators() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Ne,
                    scalar: Scalar::I64(2),
                },
            ],
        };
        assert_eq!(&pred.expr_sql_string(), r#""col1"=1 AND "col2"!=2"#);
    }

    #[test]
    fn test_expr_to_sql_column_escape() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col 1"),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                },
                DeleteExpr {
                    column: String::from(r#"col\2"#),
                    op: Op::Eq,
                    scalar: Scalar::I64(2),
                },
                DeleteExpr {
                    column: String::from(r#"col"3"#),
                    op: Op::Eq,
                    scalar: Scalar::I64(3),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col 1"=1 AND "col\\2"=2 AND "col\"3"=3"#
        );
    }

    #[test]
    fn test_expr_to_sql_bool() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::Bool(false),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::Bool(true),
                },
            ],
        };
        assert_eq!(&pred.expr_sql_string(), r#""col1"=false AND "col2"=true"#);
    }

    #[test]
    fn test_expr_to_sql_i64() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::I64(0),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::I64(-1),
                },
                DeleteExpr {
                    column: String::from("col3"),
                    op: Op::Eq,
                    scalar: Scalar::I64(1),
                },
                DeleteExpr {
                    column: String::from("col4"),
                    op: Op::Eq,
                    scalar: Scalar::I64(i64::MIN),
                },
                DeleteExpr {
                    column: String::from("col5"),
                    op: Op::Eq,
                    scalar: Scalar::I64(i64::MAX),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col1"=0 AND "col2"=-1 AND "col3"=1 AND "col4"=-9223372036854775808 AND "col5"=9223372036854775807"#
        );
    }

    #[test]
    fn test_expr_to_sql_f64() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(0.0)),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(-0.0)),
                },
                DeleteExpr {
                    column: String::from("col3"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(1.0)),
                },
                DeleteExpr {
                    column: String::from("col4"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(f64::INFINITY)),
                },
                DeleteExpr {
                    column: String::from("col5"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(f64::NEG_INFINITY)),
                },
                DeleteExpr {
                    column: String::from("col6"),
                    op: Op::Eq,
                    scalar: Scalar::F64(OrderedFloat::from(f64::NAN)),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col1"=0.0 AND "col2"=-0.0 AND "col3"=1.0 AND "col4"='Infinity' AND "col5"='-Infinity' AND "col6"='NaN'"#
        );
    }

    #[test]
    fn test_expr_to_sql_string() {
        let pred = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![
                DeleteExpr {
                    column: String::from("col1"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from("")),
                },
                DeleteExpr {
                    column: String::from("col2"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from("foo")),
                },
                DeleteExpr {
                    column: String::from("col3"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from(r#"fo\o"#)),
                },
                DeleteExpr {
                    column: String::from("col4"),
                    op: Op::Eq,
                    scalar: Scalar::String(String::from(r#"fo'o"#)),
                },
            ],
        };
        assert_eq!(
            &pred.expr_sql_string(),
            r#""col1"='' AND "col2"='foo' AND "col3"='fo\\o' AND "col4"='fo\'o'"#
        );
    }
}
