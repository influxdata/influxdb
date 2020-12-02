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
