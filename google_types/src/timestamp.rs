use crate::protobuf::Timestamp;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::convert::{TryFrom, TryInto};

impl TryFrom<Timestamp> for chrono::DateTime<Utc> {
    type Error = std::num::TryFromIntError;
    fn try_from(value: Timestamp) -> Result<Self, Self::Error> {
        let Timestamp { seconds, nanos } = value;

        let dt = NaiveDateTime::from_timestamp(seconds, nanos.try_into()?);
        Ok(DateTime::<Utc>::from_utc(dt, Utc))
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(value: DateTime<Utc>) -> Self {
        Self {
            seconds: value.timestamp(),
            nanos: value.timestamp_subsec_nanos() as i32,
        }
    }
}

impl Serialize for Timestamp {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let t: DateTime<Utc> = self.clone().try_into().map_err(serde::ser::Error::custom)?;
        serializer.serialize_str(t.to_rfc3339().as_str())
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        let d = DateTime::parse_from_rfc3339(s).map_err(serde::de::Error::custom)?;
        let d: DateTime<Utc> = d.into();
        Ok(d.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{FixedOffset, TimeZone};
    use serde::de::value::{BorrowedStrDeserializer, Error};

    #[test]
    fn test_date() {
        let datetime = FixedOffset::east(5 * 3600)
            .ymd(2016, 11, 8)
            .and_hms(21, 7, 9);
        let encoded = datetime.to_rfc3339();
        assert_eq!(&encoded, "2016-11-08T21:07:09+05:00");

        let utc: DateTime<Utc> = datetime.into();
        let utc_encoded = utc.to_rfc3339();
        assert_eq!(&utc_encoded, "2016-11-08T16:07:09+00:00");

        let deserializer = BorrowedStrDeserializer::<'_, Error>::new(&encoded);
        let a: Timestamp = Timestamp::deserialize(deserializer).unwrap();
        assert_eq!(a.seconds, utc.timestamp());
        assert_eq!(a.nanos, utc.timestamp_subsec_nanos() as i32);

        let encoded = serde_json::to_string(&a).unwrap();
        assert_eq!(encoded, format!("\"{}\"", utc_encoded));
    }
}
