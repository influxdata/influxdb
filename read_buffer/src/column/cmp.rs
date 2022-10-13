use std::{convert::TryFrom, fmt::Display};

/// Possible comparison operators
#[derive(Debug, PartialEq, Eq, Copy, Clone)]
#[allow(clippy::upper_case_acronyms)] // these look weird when not capitalized
pub enum Operator {
    Equal,
    NotEqual,
    GT,
    GTE,
    LT,
    LTE,
}

impl Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Equal => "=",
                Self::NotEqual => "!=",
                Self::GT => ">",
                Self::GTE => ">=",
                Self::LT => "<",
                Self::LTE => "<=",
            }
        )
    }
}

impl TryFrom<&str> for Operator {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "=" => Ok(Self::Equal),
            "!=" => Ok(Self::NotEqual),
            ">" => Ok(Self::GT),
            ">=" => Ok(Self::GTE),
            "<" => Ok(Self::LT),
            "<=" => Ok(Self::LTE),
            v => Err(format!("unknown operator {:?}", v)),
        }
    }
}

impl TryFrom<&datafusion::logical_expr::Operator> for Operator {
    type Error = String;

    fn try_from(op: &datafusion::logical_expr::Operator) -> Result<Self, Self::Error> {
        match op {
            datafusion::logical_expr::Operator::Eq => Ok(Self::Equal),
            datafusion::logical_expr::Operator::NotEq => Ok(Self::NotEqual),
            datafusion::logical_expr::Operator::Lt => Ok(Self::LT),
            datafusion::logical_expr::Operator::LtEq => Ok(Self::LTE),
            datafusion::logical_expr::Operator::Gt => Ok(Self::GT),
            datafusion::logical_expr::Operator::GtEq => Ok(Self::GTE),
            v => Err(format!("unsupported operator {:?}", v)),
        }
    }
}
