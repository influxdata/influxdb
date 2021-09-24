use crate::protobuf::Duration;
use serde::{Deserialize, Serialize, Serializer};
use std::convert::{TryFrom, TryInto};

impl TryFrom<Duration> for std::time::Duration {
    type Error = std::num::TryFromIntError;

    fn try_from(value: Duration) -> Result<Self, Self::Error> {
        Ok(std::time::Duration::new(
            value.seconds.try_into()?,
            value.nanos.try_into()?,
        ))
    }
}

impl From<std::time::Duration> for Duration {
    fn from(value: std::time::Duration) -> Self {
        Self {
            seconds: value.as_secs() as _,
            nanos: value.subsec_nanos() as _,
        }
    }
}

impl Serialize for Duration {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if self.seconds != 0 && self.nanos != 0 && (self.nanos < 0) != (self.seconds < 0) {
            return Err(serde::ser::Error::custom("Duration has inconsistent signs"));
        }

        let mut s = if self.seconds == 0 {
            if self.nanos < 0 {
                "-0".to_string()
            } else {
                "0".to_string()
            }
        } else {
            self.seconds.to_string()
        };

        if self.nanos != 0 {
            s.push('.');
            let f = match split_nanos(self.nanos.abs() as u32) {
                (millis, 0, 0) => format!("{:03}", millis),
                (millis, micros, 0) => format!("{:03}{:03}", millis, micros),
                (millis, micros, nanos) => format!("{:03}{:03}{:03}", millis, micros, nanos),
            };
            s.push_str(&f);
        }

        s.push('s');
        serializer.serialize_str(&s)
    }
}

impl<'de> serde::Deserialize<'de> for Duration {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = Deserialize::deserialize(deserializer)?;
        let s = s
            .strip_suffix('s')
            .ok_or_else(|| serde::de::Error::custom("missing 's' suffix"))?;
        let secs: f64 = s.parse().map_err(serde::de::Error::custom)?;

        if secs < 0. {
            let negated = std::time::Duration::from_secs_f64(-secs);
            Ok(Self {
                seconds: -(negated.as_secs() as i64),
                nanos: -(negated.subsec_nanos() as i32),
            })
        } else {
            Ok(std::time::Duration::from_secs_f64(secs).into())
        }
    }
}

/// Splits nanoseconds into whole milliseconds, microseconds, and nanoseconds
fn split_nanos(mut nanos: u32) -> (u32, u32, u32) {
    let millis = nanos / 1_000_000;
    nanos -= millis * 1_000_000;
    let micros = nanos / 1_000;
    nanos -= micros * 1_000;
    (millis, micros, nanos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duration() {
        let verify = |duration: &Duration, expected: &str| {
            assert_eq!(serde_json::to_string(duration).unwrap().as_str(), expected);
            assert_eq!(
                &serde_json::from_str::<Duration>(expected).unwrap(),
                duration
            )
        };

        let duration = Duration {
            seconds: 0,
            nanos: 0,
        };
        verify(&duration, "\"0s\"");

        let duration = Duration {
            seconds: 0,
            nanos: 123,
        };
        verify(&duration, "\"0.000000123s\"");

        let duration = Duration {
            seconds: 0,
            nanos: 123456,
        };
        verify(&duration, "\"0.000123456s\"");

        let duration = Duration {
            seconds: 0,
            nanos: 123456789,
        };
        verify(&duration, "\"0.123456789s\"");

        let duration = Duration {
            seconds: 0,
            nanos: -67088,
        };
        verify(&duration, "\"-0.000067088s\"");

        let duration = Duration {
            seconds: 121,
            nanos: 3454,
        };
        verify(&duration, "\"121.000003454s\"");

        let duration = Duration {
            seconds: -90,
            nanos: -2456301,
        };
        verify(&duration, "\"-90.002456301s\"");

        let duration = Duration {
            seconds: -90,
            nanos: 234,
        };
        serde_json::to_string(&duration).unwrap_err();

        let duration = Duration {
            seconds: 90,
            nanos: -234,
        };
        serde_json::to_string(&duration).unwrap_err();
    }
}
