// ID handling code ported from https://github.com/influxdata/influxdb/blob/047e195/id.go for
// interoperability purposes.

use serde::{de::Error as _, Deserialize, Deserializer};
use snafu::{ensure, OptionExt, ResultExt, Snafu};
use std::{
    convert::{TryFrom, TryInto},
    fmt,
    num::{NonZeroU64, ParseIntError},
    str::FromStr,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("ID cannot be zero"))]
    IDCannotBeZero,

    #[snafu(display("ID must have a length of {} bytes, was {} bytes: '{}'", ID_LENGTH, hex.len(), hex))]
    IDLengthIncorrect { hex: String },

    #[snafu(display("Invalid ID: {}", source))]
    InvalidID { source: ParseIntError },
}

/// ID_LENGTH is the exact length a string (or a byte slice representing it)
/// must have in order to be decoded into a valid ID.
const ID_LENGTH: usize = 16;

/// ID is a unique identifier.
///
/// Its zero value is not a valid ID.
#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub struct ID(NonZeroU64);

impl TryFrom<u64> for ID {
    type Error = Error;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Ok(Self(NonZeroU64::new(value).context(IDCannotBeZero)?))
    }
}

impl From<ID> for u64 {
    fn from(value: ID) -> Self {
        value.0.get()
    }
}

impl<'de> Deserialize<'de> for ID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        s.try_into().map_err(D::Error::custom)
    }
}

impl TryFrom<&str> for ID {
    type Error = Error;

    fn try_from(hex: &str) -> Result<Self, Self::Error> {
        ensure!(hex.len() == ID_LENGTH, IDLengthIncorrect { hex });

        u64::from_str_radix(hex, 16)
            .context(InvalidID)
            .and_then(|value| value.try_into())
    }
}

impl FromStr for ID {
    type Err = Error;

    fn from_str(hex: &str) -> Result<Self, Self::Err> {
        Self::try_from(hex)
    }
}

impl fmt::Display for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl fmt::Debug for ID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self, f)
    }
}

impl From<ID> for String {
    fn from(value: ID) -> Self {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::convert::TryInto;

    #[test]
    fn test_id_from_string() {
        let success_cases = [
            (
                "ffffffffffffffff",
                ID(NonZeroU64::new(18_446_744_073_709_551_615).unwrap()),
            ),
            (
                "020f755c3c082000",
                ID(NonZeroU64::new(148_466_351_731_122_176).unwrap()),
            ),
            (
                "0000111100001111",
                ID(NonZeroU64::new(18_764_712_120_593).unwrap()),
            ),
        ];

        for &(input, expected_output) in &success_cases {
            let actual_output = input.try_into().unwrap();
            assert_eq!(expected_output, actual_output, "input was `{}`", input);
        }

        let failure_cases = [
            ("0000000000000000", "ID cannot be zero"),
            (
                "gggggggggggggggg",
                "Invalid ID: invalid digit found in string",
            ),
            (
                "abc",
                "ID must have a length of 16 bytes, was 3 bytes: 'abc'",
            ),
            (
                "abcdabcdabcdabcd0",
                "ID must have a length of 16 bytes, was 17 bytes: 'abcdabcdabcdabcd0'",
            ),
        ];

        for &(input, expected_output) in &failure_cases {
            let actual_output: Result<ID, Error> = input.try_into();
            let actual_output: Error = actual_output.unwrap_err();
            let actual_output = actual_output.to_string();
            assert_eq!(expected_output, actual_output, "input was `{}`", input);
        }
    }

    #[test]
    fn test_id_to_string() {
        let cases = [
            (ID(NonZeroU64::new(0x1234).unwrap()), "0000000000001234"),
            (
                ID(NonZeroU64::new(18_446_744_073_709_551_615).unwrap()),
                "ffffffffffffffff",
            ),
            (
                ID(NonZeroU64::new(148_466_351_731_122_176).unwrap()),
                "020f755c3c082000",
            ),
            (
                ID(NonZeroU64::new(18_764_712_120_593).unwrap()),
                "0000111100001111",
            ),
        ];

        for &(input, expected_output) in &cases {
            let actual_output = input.to_string();
            assert_eq!(expected_output, actual_output);
        }
    }

    #[test]
    fn test_deserialize_then_to_string() {
        let i: ID = "0000111100001111".parse().unwrap();
        assert_eq!(ID(NonZeroU64::new(18_764_712_120_593).unwrap()), i);

        #[derive(Deserialize)]
        struct WriteInfo {
            org: ID,
        }

        let query = "org=0000111100001111";
        let write_info: WriteInfo = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(
            ID(NonZeroU64::new(18_764_712_120_593).unwrap()),
            write_info.org
        );
        assert_eq!("0000111100001111", write_info.org.to_string());
    }
}
