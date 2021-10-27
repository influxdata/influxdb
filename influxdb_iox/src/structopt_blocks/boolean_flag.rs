/// Boolean flag that works with environment variables.
///
/// Workaround for <https://github.com/TeXitoi/structopt/issues/428>
#[derive(Debug, Clone, Copy)]
pub enum BooleanFlag {
    True,
    False,
}

impl std::str::FromStr for BooleanFlag {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "yes" | "y" | "true" | "t" | "1" => Ok(Self::True),
            "no" | "n" | "false" | "f" | "0" => Ok(Self::False),
            _ => Err(format!(
                "Invalid boolean flag '{}'. Valid options: yes, no, y, n, true, false, t, f, 1, 0",
                s
            )),
        }
    }
}

impl From<BooleanFlag> for bool {
    fn from(yes_no: BooleanFlag) -> Self {
        matches!(yes_no, BooleanFlag::True)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parsing() {
        assert!(bool::from(BooleanFlag::from_str("yes").unwrap()));
        assert!(bool::from(BooleanFlag::from_str("Yes").unwrap()));
        assert!(bool::from(BooleanFlag::from_str("YES").unwrap()));

        assert!(!bool::from(BooleanFlag::from_str("No").unwrap()));
        assert!(!bool::from(BooleanFlag::from_str("FaLse").unwrap()));

        BooleanFlag::from_str("foo").unwrap_err();
    }
}
