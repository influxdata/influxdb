// ID handling code ported from https://github.com/influxdata/influxdb/blob/047e195/id.go for
// interoperability purposes.

use serde::Deserialize;

use std::convert::TryFrom;
use std::fmt;

/// ID_LENGTH is the exact length a string (or a byte slice representing it) must have in order to
/// be decoded into a valid ID.
const ID_LENGTH: usize = 16;

/// Id is a unique identifier.
///
/// Its zero value is not a valid ID.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Deserialize)]
pub struct Id(u64);

impl From<u64> for Id {
    fn from(value: u64) -> Id {
        Id(value)
    }
}

// Converting from `u32` might not be needed in the final implementation; it's currently used for
// convenience with the current RocksDB storage.
impl From<u32> for Id {
    fn from(value: u32) -> Id {
        Id(value as u64)
    }
}

impl From<Id> for u64 {
    fn from(value: Id) -> u64 {
        value.0
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

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn test_id_from_string() {
        let cases = [
            ("0000000000000000", Err("invalid ID")),
            ("ffffffffffffffff", Ok(Id(18_446_744_073_709_551_615))),
            ("020f755c3c082000", Ok(Id(148_466_351_731_122_176))),
            ("gggggggggggggggg", Err("invalid ID")),
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
        ];

        for &(input, expected_output) in &cases {
            let actual_output = input.to_string();
            assert_eq!(expected_output, actual_output);
        }
    }
}
