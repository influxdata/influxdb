use std::str::Chars;
use std::{error, fmt};

#[derive(Debug, PartialEq, Clone)]
pub struct Point<T> {
    pub series: String,
    pub series_id: Option<u64>,
    pub time: i64,
    pub value: T,
}

impl<T> Point<T> {
    pub fn index_pairs(&self) -> Result<Vec<Pair>, ParseError> {
        index_pairs(&self.series)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum PointType {
    I64(Point<i64>),
    F64(Point<f64>),
}

impl PointType {
    pub fn new_i64(series: String, value: i64, time: i64) -> PointType {
        PointType::I64(Point {
            series,
            series_id: None,
            value,
            time,
        })
    }

    pub fn new_f64(series: String, value: f64, time: i64) -> PointType {
        PointType::F64(Point {
            series,
            series_id: None,
            value,
            time,
        })
    }

    pub fn series(&self) -> &String {
        match self {
            PointType::I64(p) => &p.series,
            PointType::F64(p) => &p.series,
        }
    }

    pub fn time(&self) -> i64 {
        match self {
            PointType::I64(p) => p.time,
            PointType::F64(p) => p.time,
        }
    }

    pub fn set_time(&mut self, t: i64) {
        match self {
            PointType::I64(p) => p.time = t,
            PointType::F64(p) => p.time = t,
        }
    }

    pub fn series_id(&self) -> Option<u64> {
        match self {
            PointType::I64(p) => p.series_id,
            PointType::F64(p) => p.series_id,
        }
    }

    pub fn set_series_id(&mut self, id: u64) {
        match self {
            PointType::I64(p) => p.series_id = Some(id),
            PointType::F64(p) => p.series_id = Some(id),
        }
    }

    pub fn i64_value(&self) -> Option<i64> {
        match self {
            PointType::I64(p) => Some(p.value),
            _ => None,
        }
    }

    pub fn f64_value(&self) -> Option<f64> {
        match self {
            PointType::F64(p) => Some(p.value),
            _ => None,
        }
    }

    pub fn index_pairs(&self) -> Result<Vec<Pair>, ParseError> {
        match self {
            PointType::I64(p) => p.index_pairs(),
            PointType::F64(p) => p.index_pairs(),
        }
    }
}

// TODO: handle escapes in the line protocol for , = and \t
/// index_pairs parses the series key into key value pairs for insertion into the index. In
/// cases where this series is already in the database, this parse step can be skipped entirely.
/// The measurement is represented as a _m key and field as _f.
pub fn index_pairs(key: &str) -> Result<Vec<Pair>, ParseError> {
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

    Ok(pairs)
}

#[derive(Debug, PartialEq)]
pub struct Pair {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct ParseError {
    description: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for ParseError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

// TODO: have parse return an error for invalid inputs
pub fn parse(input: &str) -> Vec<PointType> {
    let mut points: Vec<PointType> = Vec::with_capacity(10000);
    let lines = input.lines();
    for line in lines {
        read_line(line, &mut points)
    }
    points
}

fn read_line(line: &str, points: &mut Vec<PointType>) {
    let mut points = points;
    let mut chars = line.chars();
    let mut series = String::with_capacity(1000);
    while let Some(ch) = chars.next() {
        match ch {
            ' ' => read_fields(&series, &mut chars, &mut points),
            _ => series.push(ch),
        }
    }
}

fn read_fields(measurement_tags: &str, chars: &mut Chars<'_>, points: &mut Vec<PointType>) {
    let mut chars = chars;
    let mut points = points;
    let mut field_name = String::with_capacity(100);

    let mut point_offset = points.len();

    while let Some(ch) = chars.next() {
        match ch {
            '=' => {
                let should_break =
                    !read_value(&measurement_tags, field_name, &mut chars, &mut points);
                field_name = String::with_capacity(100);
                if should_break {
                    break;
                }
            }
            _ => field_name.push(ch),
        }
    }

    // read the time
    for ch in chars {
        field_name.push(ch);
    }
    let time = field_name.parse::<i64>().unwrap();

    while point_offset < points.len() {
        points[point_offset].set_time(time);
        point_offset += 1;
    }
}

// read_value reads the value from the chars and returns true if there are more fields and values to be read
fn read_value(
    measurement_tags: &str,
    field_name: String,
    chars: &mut Chars<'_>,
    points: &mut Vec<PointType>,
) -> bool {
    let mut value = String::new();

    for ch in chars {
        match ch {
            ' ' | ',' => {
                let series = measurement_tags.to_string() + "\t" + &field_name;

                // if the last character of the value is an i then it's an integer, otherwise it's
                // a float (at least until we support the other data types
                let point = if value.ends_with('i') {
                    let val = value[..value.len() - 1].parse::<i64>().unwrap();
                    PointType::new_i64(series, val, 0)
                } else {
                    let val = value.parse::<f64>().unwrap();
                    PointType::new_f64(series, val, 0)
                };
                points.push(point);

                if ch == ' ' {
                    return false;
                }

                return true;
            }
            _ => value.push(ch),
        }
    }

    false
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::approximately_equal;

    #[test]
    fn parse_single_field_integer() {
        let input = "foo asdf=23i 1234";
        let vals = parse(input);

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert_eq!(vals[0].i64_value().unwrap(), 23);
    }

    #[test]
    fn parse_single_field_float_no_decimal() {
        let input = "foo asdf=44 546";
        let vals = parse(input);

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 546);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 44.0));
    }

    #[test]
    fn parse_single_field_float_with_decimal() {
        let input = "foo asdf=3.74 123";
        let vals = parse(input);

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 123);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 3.74));
    }

    #[test]
    fn parse_two_fields_integer() {
        let input = "foo asdf=23i,bar=5i 1234";
        let vals = parse(input);

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert_eq!(vals[0].i64_value().unwrap(), 23);

        assert_eq!(vals[1].series(), "foo\tbar");
        assert_eq!(vals[1].time(), 1234);
        assert_eq!(vals[1].i64_value().unwrap(), 5);
    }

    #[test]
    fn parse_two_fields_float() {
        let input = "foo asdf=23.1,bar=5 1234";
        let vals = parse(input);

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 23.1));

        assert_eq!(vals[1].series(), "foo\tbar");
        assert_eq!(vals[1].time(), 1234);
        assert!(approximately_equal(vals[1].f64_value().unwrap(), 5.0));
    }

    #[test]
    fn parse_mixed_float_and_integer() {
        let input = "foo asdf=23.1,bar=5i 1234";
        let vals = parse(input);

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 23.1));

        assert_eq!(vals[1].series(), "foo\tbar");
        assert_eq!(vals[1].time(), 1234);
        assert_eq!(vals[1].i64_value().unwrap(), 5);
    }

    #[test]
    fn parse_tag_set_included_in_series() {
        let input = "foo,tag1=1,tag2=2 value=1 123";
        let vals = parse(input);

        assert_eq!(vals[0].series(), "foo,tag1=1,tag2=2\tvalue");
    }

    #[test]
    fn index_pairs() {
        let p = Point {
            series: "cpu,host=A,region=west\tusage_system".to_string(),
            series_id: None,
            value: 0,
            time: 0,
        };
        let pairs = p.index_pairs().unwrap();
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
