use std::{convert::TryFrom, fmt::Display};

/// Possible comparison operators
#[derive(Debug, PartialEq, Copy, Clone)]
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
        println!("--- Operator: {}", value);
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

impl TryFrom<&datafusion::logical_plan::Operator> for Operator {
    type Error = String;

    fn try_from(op: &datafusion::logical_plan::Operator) -> Result<Self, Self::Error> {
        println!("--- Operator: {}", op);
        match op {
            datafusion::logical_plan::Operator::Eq => Ok(Self::Equal),
            datafusion::logical_plan::Operator::NotEq => Ok(Self::NotEqual),
            datafusion::logical_plan::Operator::Lt => Ok(Self::LT),
            datafusion::logical_plan::Operator::LtEq => Ok(Self::LTE),
            datafusion::logical_plan::Operator::Gt => Ok(Self::GT),
            datafusion::logical_plan::Operator::GtEq => Ok(Self::GTE),
            v => Err(format!("unsupported operator {:?}", v)),
        }
    }
}
