use delorean_line_parser::{self, FieldValue, ParsedLine};
use either::Either;
use snafu::Snafu;
use std::{
    convert::TryFrom,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Unable to parse the line protocol: {}"#, source))]
    #[snafu(context(false))]
    LineProtocolParserFailed { source: delorean_line_parser::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Represents a single typed point of timeseries data
///
/// A `Point<T>` consists of a series identifier, a timestamp, and a value.
///
/// The series identifier is a string that concatenates the
/// measurement name, tag name=value pairs and field name. These tags
/// are unique and sorted.
///
/// For example, a `Point<T>` containing an `f64` value representing
/// `cpu,host=A,region=west usage_system=64.2 1590488773254420000` could
/// be represented as a `Point<T>` like this:
///
/// ```
/// use delorean::line_parser::Point;
///
/// let p = Point {
///     series: "cpu,host=A,region=west\tusage_system".to_string(),
///     series_id: None,
///     value: 64.2,
///     time: 1590488773254420000,
/// };
/// ```
#[derive(Debug, PartialEq, Clone)]
pub struct Point<T> {
    pub series: String,
    pub series_id: Option<u64>,
    pub time: i64,
    pub value: T,
}

impl<T> Point<T> {
    pub fn index_pairs(&self) -> Vec<Pair> {
        index_pairs(&self.series)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum PointType {
    I64(Point<i64>),
    F64(Point<f64>),
}

impl PointType {
    pub fn new_i64(series: String, value: i64, time: i64) -> Self {
        Self::I64(Point {
            series,
            series_id: None,
            value,
            time,
        })
    }

    pub fn new_f64(series: String, value: f64, time: i64) -> Self {
        Self::F64(Point {
            series,
            series_id: None,
            value,
            time,
        })
    }

    pub fn series(&self) -> &String {
        match self {
            Self::I64(p) => &p.series,
            Self::F64(p) => &p.series,
        }
    }

    pub fn time(&self) -> i64 {
        match self {
            Self::I64(p) => p.time,
            Self::F64(p) => p.time,
        }
    }

    pub fn set_time(&mut self, t: i64) {
        match self {
            Self::I64(p) => p.time = t,
            Self::F64(p) => p.time = t,
        }
    }

    pub fn series_id(&self) -> Option<u64> {
        match self {
            Self::I64(p) => p.series_id,
            Self::F64(p) => p.series_id,
        }
    }

    pub fn set_series_id(&mut self, id: u64) {
        match self {
            Self::I64(p) => p.series_id = Some(id),
            Self::F64(p) => p.series_id = Some(id),
        }
    }

    pub fn i64_value(&self) -> Option<i64> {
        match self {
            Self::I64(p) => Some(p.value),
            _ => None,
        }
    }

    pub fn f64_value(&self) -> Option<f64> {
        match self {
            Self::F64(p) => Some(p.value),
            _ => None,
        }
    }

    pub fn index_pairs(&self) -> Vec<Pair> {
        match self {
            Self::I64(p) => p.index_pairs(),
            Self::F64(p) => p.index_pairs(),
        }
    }
}

// TODO: handle escapes in the line protocol for , = and \t
/// index_pairs parses the series key into key value pairs for insertion into the index. In
/// cases where this series is already in the database, this parse step can be skipped entirely.
/// The measurement is represented as a _m key and field as _f.
pub fn index_pairs(key: &str) -> Vec<Pair> {
    let chars = key.chars();
    let mut pairs = vec![];
    let mut key = "_m".to_string();
    let mut value = String::with_capacity(250);
    let mut reading_key = false;

    for ch in chars {
        match ch {
            ',' => {
                reading_key = true;
                pairs.push(Pair { key, value });
                key = String::with_capacity(250);
                value = String::with_capacity(250);
            }
            '=' => {
                reading_key = false;
            }
            '\t' => {
                reading_key = false;
                pairs.push(Pair { key, value });
                key = "_f".to_string();
                value = String::with_capacity(250);
            }
            _ => {
                if reading_key {
                    key.push(ch);
                } else {
                    value.push(ch);
                }
            }
        }
    }
    pairs.push(Pair { key, value });

    pairs
}

// TODO: Could `Pair` hold `Cow` strings?
#[derive(Debug, PartialEq)]
pub struct Pair {
    pub key: String,
    pub value: String,
}

// TODO: Return an error for invalid inputs
pub fn parse(input: &str) -> Result<Vec<PointType>> {
    let since_the_epoch = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards");
    let now_ns = i64::try_from(since_the_epoch.as_nanos()).expect("Time does not fit");

    parse_full(input, now_ns)
}

fn parse_full(input: &str, now_ns: i64) -> Result<Vec<PointType>> {
    delorean_line_parser::parse_lines(input)
        .flat_map(|parsed_line| match parsed_line {
            Ok(parsed_line) => match line_to_points(parsed_line, now_ns) {
                Ok(i) => Either::Left(i.map(Ok)),
                Err(e) => Either::Right(std::iter::once(Err(e))),
            },
            Err(e) => Either::Right(std::iter::once(Err(e.into()))),
        })
        .collect()
}

fn line_to_points(
    parsed_line: ParsedLine<'_>,
    now: i64,
) -> Result<impl Iterator<Item = PointType> + '_> {
    let ParsedLine {
        series,
        field_set,
        timestamp,
    } = parsed_line;

    let series_base = series.generate_base()?;
    let timestamp = timestamp.unwrap_or(now);

    Ok(field_set.into_iter().map(move |(field_key, field_value)| {
        let series = format!("{}\t{}", series_base, field_key);

        match field_value {
            FieldValue::I64(value) => PointType::new_i64(series, value, timestamp),
            FieldValue::F64(value) => PointType::new_f64(series, value, timestamp),
            FieldValue::String(_) => unimplemented!("String support for points"),
            FieldValue::Boolean(_) => unimplemented!("Boolean support for points"),
        }
    }))
}

#[cfg(test)]
mod test {
    use super::*;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    #[test]
    fn parse_without_a_timestamp_uses_the_default() -> Result {
        let input = r#"foo value1=1i"#;
        let vals = parse_full(input, 555)?;

        assert_eq!(vals[0].series(), "foo\tvalue1");
        assert_eq!(vals[0].time(), 555);
        assert_eq!(vals[0].i64_value().unwrap(), 1);

        Ok(())
    }

    #[test]
    fn index_pairs() {
        let p = Point {
            series: "cpu,host=A,region=west\tusage_system".to_string(),
            series_id: None,
            value: 0,
            time: 0,
        };
        let pairs = p.index_pairs();
        assert_eq!(
            pairs,
            vec![
                Pair {
                    key: "_m".to_string(),
                    value: "cpu".to_string()
                },
                Pair {
                    key: "host".to_string(),
                    value: "A".to_string()
                },
                Pair {
                    key: "region".to_string(),
                    value: "west".to_string()
                },
                Pair {
                    key: "_f".to_string(),
                    value: "usage_system".to_string()
                },
            ]
        );
    }
}
