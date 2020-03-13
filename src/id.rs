// ID handling code ported from https://github.com/influxdata/influxdb/blob/047e195/id.go for
// interoperability purposes.

use serde::{de::Error, Deserialize, Deserializer};

use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::str::FromStr;

/// ID_LENGTH is the exact length a string (or a byte slice representing it) must have in order to
/// be decoded into a valid ID.
const ID_LENGTH: usize = 16;

/// Id is a unique identifier.
///
/// Its zero value is not a valid ID.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub struct Id(u64);

impl From<u64> for Id {
    fn from(value: u64) -> Id {
        Id(value)
    }
}

impl From<Id> for u64 {
    fn from(value: Id) -> u64 {
        value.0
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        s.try_into().map_err(D::Error::custom)
    }
}

impl TryFrom<&str> for Id {
    type Error = &'static str;

    fn try_from(hex: &str) -> Result<Self, Self::Error> {
        if hex.len() != ID_LENGTH {
            return Err("id must have a length of 16 bytes");
        }

        u64::from_str_radix(hex, 16)
            .map_err(|_| "invalid ID")
            .and_then(|value| {
                if value == 0 {
                    Err("invalid ID")
                } else {
                    Ok(Id(value))
                }
            })
    }
}

impl FromStr for Id {
    type Err = &'static str;

    fn from_str(hex: &str) -> Result<Self, Self::Err> {
        Id::try_from(hex)
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::convert::TryInto;

    #[test]
    fn test_id_from_string() {
        let cases = [
            ("0000000000000000", Err("invalid ID")),
            ("ffffffffffffffff", Ok(Id(18_446_744_073_709_551_615))),
            ("020f755c3c082000", Ok(Id(148_466_351_731_122_176))),
            ("gggggggggggggggg", Err("invalid ID")),
            ("0000111100001111", Ok(Id(18_764_712_120_593))),
            ("abc", Err("id must have a length of 16 bytes")),
            (
                "abcdabcdabcdabcd0",
                Err("id must have a length of 16 bytes"),
            ),
        ];

        for &(input, expected_output) in &cases {
            let actual_output = input.try_into();
            assert_eq!(expected_output, actual_output);
        }
    }

    #[test]
    fn test_id_to_string() {
        let cases = [
            (Id(0x1234), "0000000000001234"),
            (Id(18_446_744_073_709_551_615), "ffffffffffffffff"),
            (Id(148_466_351_731_122_176), "020f755c3c082000"),
            (Id(18_764_712_120_593), "0000111100001111"),
        ];

        for &(input, expected_output) in &cases {
            let actual_output = input.to_string();
            assert_eq!(expected_output, actual_output);
        }
    }

    #[test]
    fn test_deserialize_then_to_string() {
        let i: Id = "0000111100001111".parse().unwrap();
        assert_eq!(Id(18_764_712_120_593), i);

        #[derive(Deserialize)]
        struct WriteInfo {
            org: Id,
        }

        let query = "org=0000111100001111";
        let write_info: WriteInfo = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(Id(18_764_712_120_593), write_info.org);
        assert_eq!("0000111100001111", write_info.org.to_string());
    }
}
