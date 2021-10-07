use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    convert::TryFrom,
    fmt,
    num::{NonZeroU32, ParseIntError},
    str::FromStr,
};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct ServerId(NonZeroU32);

impl ServerId {
    pub fn new(id: NonZeroU32) -> Self {
        Self(id)
    }

    pub fn get(&self) -> NonZeroU32 {
        self.0
    }

    pub fn get_u32(&self) -> u32 {
        self.0.get()
    }
}

impl FromStr for ServerId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let value: u32 = value.parse().context(UnableToParse { value })?;
        Self::try_from(value)
    }
}

impl TryFrom<u32> for ServerId {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        NonZeroU32::new(value)
            .map(Self)
            .context(ValueMayNotBeZero)
            .map_err(Into::into)
    }
}

impl fmt::Display for ServerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Snafu)]
pub struct Error(InnerError);

#[derive(Debug, Snafu)]
enum InnerError {
    #[snafu(display("The server ID may not be zero"))]
    ValueMayNotBeZero,

    #[snafu(display("Could not parse {} as a non-zero 32-bit unsigned number", value))]
    UnableToParse {
        source: ParseIntError,
        value: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cannot_be_zero() {
        assert!(matches!(
            ServerId::try_from(0),
            Err(Error(InnerError::ValueMayNotBeZero))
        ));
    }

    #[test]
    fn can_be_nonzero() {
        let value = 2;
        let server_id = ServerId::try_from(value).unwrap();
        assert_eq!(server_id.get_u32(), value);
    }

    #[test]
    fn can_be_parsed_from_a_string() {
        assert!(matches!(
            "0".parse::<ServerId>(),
            Err(Error(InnerError::ValueMayNotBeZero)),
        ));
        assert!(matches!(
            "moo".parse::<ServerId>(),
            Err(Error(InnerError::UnableToParse { source: _, value })) if value == "moo",
        ));

        let server_id = "1337".parse::<ServerId>().unwrap();
        assert_eq!(server_id.get_u32(), 1337);
    }

    #[test]
    fn can_be_displayed() {
        let server_id = ServerId::try_from(42).unwrap();
        assert_eq!("42", format!("{}", server_id));
    }
}
