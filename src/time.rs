use crate::Error;

use std::time::SystemTime;
use std::time::{Duration, UNIX_EPOCH};

// TODO: because we're using SystemTime as our base time object, we only support times after
//       unix epoch. We should fix this so we can represent a wider range of dates & times.

#[derive(Debug, PartialEq, Clone, Copy)]
pub struct RelativeDuration {
    pub subtract: bool,
    pub duration: Duration,
}

impl RelativeDuration {
    pub fn from_time(&self, t: std::time::SystemTime) -> Result<std::time::SystemTime, Error> {
        if self.subtract {
            match t.checked_sub(self.duration) {
                Some(t) => Ok(t),
                None => Err(Error {
                    description: "unable to subtract duration from time".to_string(),
                }),
            }
        } else {
            match t.checked_add(self.duration) {
                Some(t) => Ok(t),
                None => Err(Error {
                    description: "unable to add duration from time".to_string(),
                }),
            }
        }
    }
}

// TODO: update this so that we're not indexing into the string. Should be iterating with chars
pub fn parse_duration(s: &str) -> Result<RelativeDuration, Error> {
    if s.len() < 2 {
        return Err(Error {
            description: "duration must have at least two characters".to_string(),
        });
    }

    let i;
    let start = if s.starts_with('-') { 1 } else { 0 };

    match s[start..].chars().position(|c| !c.is_digit(10)) {
        Some(p) => i = p + start,
        None => {
            return Err(Error {
                description: "duration must end with a valid unit like s, m, ms, us, ns"
                    .to_string(),
            })
        }
    }

    if i == 0 {
        return Err(Error {
            description: format!(
                "unable to parse duration {} because of invalid first character",
                s
            ),
        });
    }

    let magnitude = match s[start..i].parse::<u64>() {
        Ok(n) => n,
        Err(e) => {
            return Err(Error {
                description: e.to_string(),
            })
        }
    };

    let duration = match &s[i..] {
        "s" => Duration::from_secs(magnitude),
        "m" => Duration::from_secs(magnitude * 60),
        unknown => {
            return Err(Error {
                description: format!("unhandled duration '{}'", unknown),
            })
        }
    };

    Ok(RelativeDuration {
        subtract: start == 1,
        duration,
    })
}

pub fn time_as_i64_nanos(t: &SystemTime) -> i64 {
    let d = t
        .duration_since(UNIX_EPOCH)
        .expect("unable to support times before 1970-01-01T00:00:00");
    let s = d.as_secs() as i64;
    s * 1_000_000_000 + d.subsec_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_durations() {
        assert_eq!(
            parse_duration("1m").unwrap(),
            RelativeDuration {
                subtract: false,
                duration: Duration::from_secs(60)
            }
        );

        assert_eq!(
            parse_duration("-20s").unwrap(),
            RelativeDuration {
                subtract: true,
                duration: Duration::from_secs(20)
            }
        );

        assert_eq!(
            parse_duration("10d"),
            Err(Error {
                description: "unhandled duration 'd'".to_string()
            }),
        );

        assert_eq!(
            parse_duration("a23"),
            Err(Error {
                description: "unable to parse duration a23 because of invalid first character"
                    .to_string()
            })
        );

        assert_eq!(
            parse_duration("3"),
            Err(Error {
                description: "duration must have at least two characters".to_string()
            })
        );
    }
}
