#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop
)]

pub mod encoders;
pub mod mapper;
pub mod reader;

use std::convert::TryFrom;
use std::error;
use std::fmt;
use std::io;

#[derive(Clone, Debug)]
pub struct ParsedTSMKey {
    pub measurement: String,
    pub tagset: Vec<(String, String)>,
    pub field_key: String,
}

/// parse_tsm_key parses from the series key the measurement, field key and tag
/// set.
///
/// It does not provide access to the org and bucket ids on the key, these can
/// be accessed via org_id() and bucket_id() respectively.
///
/// TODO: handle escapes in the series key for , = and \t
///
fn parse_tsm_key(mut key: Vec<u8>) -> Result<ParsedTSMKey, TSMError> {
    // skip over org id, bucket id, comma, null byte (measurement) and =
    // The next n-1 bytes are the measurement name, where the nᵗʰ byte is a `,`.
    key = key.drain(8 + 8 + 1 + 1 + 1..).collect::<Vec<u8>>();
    let mut i = 0;
    // TODO(edd): can we make this work with take_while?
    while i != key.len() {
        if key[i] == b',' {
            break;
        }
        i += 1;
    }

    let mut rem_key = key.drain(i..).collect::<Vec<u8>>();
    let measurement = String::from_utf8(key).map_err(|e| TSMError {
        description: e.to_string(),
    })?;

    let mut tagset = Vec::<(String, String)>::with_capacity(10);
    let mut reading_key = true;
    let mut key = String::with_capacity(100);
    let mut value = String::with_capacity(100);

    // skip the comma separating measurement tag
    for byte in rem_key.drain(1..) {
        match byte {
            44 => {
                // ,
                reading_key = true;
                tagset.push((key, value));
                key = String::with_capacity(250);
                value = String::with_capacity(250);
            }
            61 => {
                // =
                reading_key = false;
            }
            _ => {
                if reading_key {
                    key.push(byte as char);
                } else {
                    value.push(byte as char);
                }
            }
        }
    }

    // fields are stored on the series keys in TSM indexes as follows:
    //
    // <field_key><4-byte delimiter><field_key>
    //
    // so we can trim the parsed value.
    let field_trim_length = (value.len() - 4) / 2;
    let (field, _) = value.split_at(field_trim_length);
    Ok(ParsedTSMKey {
        measurement,
        tagset,
        field_key: field.to_string(),
    })
}

#[derive(Clone, Debug, Copy)]
pub enum BlockType {
    Float,
    Integer,
    Bool,
    Str,
    Unsigned,
}

impl TryFrom<u8> for BlockType {
    type Error = TSMError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Float),
            1 => Ok(Self::Integer),
            2 => Ok(Self::Bool),
            3 => Ok(Self::Str),
            4 => Ok(Self::Unsigned),
            _ => Err(TSMError {
                description: format!("{:?} is invalid block type", value),
            }),
        }
    }
}

/// `Block` holds information about location and time range of a block of data.
#[derive(Debug, Copy, Clone)]
#[allow(dead_code)]
pub struct Block {
    pub min_time: i64,
    pub max_time: i64,
    pub offset: u64,
    pub size: u32,
}

// MAX_BLOCK_VALUES is the maximum number of values a TSM block can store.
const MAX_BLOCK_VALUES: usize = 1000;

/// `BlockData` describes the various types of block data that can be held within
/// a TSM file.
#[derive(Debug)]
pub enum BlockData {
    Float { ts: Vec<i64>, values: Vec<f64> },
    Integer { ts: Vec<i64>, values: Vec<i64> },
    Bool { ts: Vec<i64>, values: Vec<bool> },
    Str { ts: Vec<i64>, values: Vec<Vec<u8>> },
    Unsigned { ts: Vec<i64>, values: Vec<u64> },
}

impl BlockData {
    pub fn is_empty(&self) -> bool {
        match &self {
            BlockData::Float { ts, values: _ } => ts.is_empty(),
            BlockData::Integer { ts, values: _ } => ts.is_empty(),
            BlockData::Bool { ts, values: _ } => ts.is_empty(),
            BlockData::Str { ts, values: _ } => ts.is_empty(),
            BlockData::Unsigned { ts, values: _ } => ts.is_empty(),
        }
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
/// `InfluxID` represents an InfluxDB ID used in InfluxDB 2.x to represent
/// organization and bucket identifiers.
pub struct InfluxID(u64);

#[allow(dead_code)]
impl InfluxID {
    fn new_str(s: &str) -> Result<InfluxID, TSMError> {
        let v = u64::from_str_radix(s, 16).map_err(|e| TSMError {
            description: e.to_string(),
        })?;
        Ok(InfluxID(v))
    }

    fn from_be_bytes(bytes: [u8; 8]) -> InfluxID {
        InfluxID(u64::from_be_bytes(bytes))
    }
}

impl std::fmt::Display for InfluxID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        write!(f, "{:016x}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct TSMError {
    pub description: String,
}

impl fmt::Display for TSMError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for TSMError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl From<io::Error> for TSMError {
    fn from(e: io::Error) -> Self {
        Self {
            description: format!("TODO - io error: {} ({:?})", e, e),
        }
    }
}

impl From<std::str::Utf8Error> for TSMError {
    fn from(e: std::str::Utf8Error) -> Self {
        Self {
            description: format!("TODO - utf8 error: {} ({:?})", e, e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tsm_key() {
        //<org_id bucket_id>,\x00=http_api_request_duration_seconds,handler=platform,method=POST,path=/api/v2/setup,status=2XX,user_agent=Firefox,\xff=sum#!~#sum
        let buf = vec![
            "05C19117091A100005C19117091A10012C003D68747470",
            "5F6170695F726571756573745F6475726174696F6E5F73",
            "65636F6E64732C68616E646C65723D706C6174666F726D",
            "2C6D6574686F643D504F53542C706174683D2F6170692F",
            "76322F73657475702C7374617475733D3258582C757365",
            "725F6167656E743D46697265666F782CFF3D73756D2321",
            "7E2373756D",
        ]
        .join("");
        let tsm_key = hex::decode(buf).unwrap();

        let parsed_key = super::parse_tsm_key(tsm_key).unwrap();
        assert_eq!(
            parsed_key.measurement,
            String::from("http_api_request_duration_seconds")
        );

        let exp_tagset = vec![
            (String::from("handler"), String::from("platform")),
            (String::from("method"), String::from("POST")),
            (String::from("path"), String::from("/api/v2/setup")),
            (String::from("status"), String::from("2XX")),
            (String::from("user_agent"), String::from("Firefox")),
        ];
        assert_eq!(parsed_key.tagset, exp_tagset);
        assert_eq!(parsed_key.field_key, String::from("sum"));
    }

    #[test]
    fn influx_id() {
        let id = InfluxID::new_str("20aa9b0").unwrap();
        assert_eq!(id, InfluxID(34_253_232));
        assert_eq!(format!("{}", id), "00000000020aa9b0");
    }
}
