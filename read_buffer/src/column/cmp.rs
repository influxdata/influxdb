use std::convert::TryFrom;

/// Possible comparison operators
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum Operator {
    Equal,
    NotEqual,
    GT,
    GTE,
    LT,
    LTE,
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

impl TryFrom<&arrow_deps::datafusion::logical_plan::Operator> for Operator {
    type Error = String;

    fn try_from(op: &arrow_deps::datafusion::logical_plan::Operator) -> Result<Self, Self::Error> {
        match op {
            arrow_deps::datafusion::logical_plan::Operator::Eq => Ok(Self::Equal),
            arrow_deps::datafusion::logical_plan::Operator::NotEq => Ok(Self::NotEqual),
            arrow_deps::datafusion::logical_plan::Operator::Lt => Ok(Self::LT),
            arrow_deps::datafusion::logical_plan::Operator::LtEq => Ok(Self::LTE),
            arrow_deps::datafusion::logical_plan::Operator::Gt => Ok(Self::GT),
            arrow_deps::datafusion::logical_plan::Operator::GtEq => Ok(Self::GTE),
            v => Err(format!("unsupported operator {:?}", v)),
        }
    }
}
