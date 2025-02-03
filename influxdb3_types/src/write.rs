use serde::{Deserialize, Serialize};

/// The precision of the timestamp
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Precision {
    Auto,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Default for Precision {
    fn default() -> Self {
        Self::Auto
    }
}

impl From<iox_http::write::Precision> for Precision {
    fn from(legacy: iox_http::write::Precision) -> Self {
        match legacy {
            iox_http::write::Precision::Second => Precision::Second,
            iox_http::write::Precision::Millisecond => Precision::Millisecond,
            iox_http::write::Precision::Microsecond => Precision::Microsecond,
            iox_http::write::Precision::Nanosecond => Precision::Nanosecond,
        }
    }
}

impl std::str::FromStr for Precision {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let p = match s {
            "s" => Self::Second,
            "ms" => Self::Millisecond,
            "us" => Self::Microsecond,
            "ns" => Self::Nanosecond,
            _ => return Err(format!("unrecognized precision unit: {s}")),
        };
        Ok(p)
    }
}
