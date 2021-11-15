use crate::timestamp::TimestampRange;

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
}

impl std::fmt::Display for DeleteExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}{}", self.column(), self.op(), self.scalar())
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

impl std::fmt::Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Bool(value) => value.fmt(f),
            Scalar::I64(value) => value.fmt(f),
            Scalar::F64(value) => value.fmt(f),
            Scalar::String(value) => write!(f, "'{}'", value),
        }
    }
}
