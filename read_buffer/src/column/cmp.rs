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
            _ => Err("unknown operator".to_owned()),
        }
    }
}
