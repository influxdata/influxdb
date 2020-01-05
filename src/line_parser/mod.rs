use std::str::Chars;
use std::{error, fmt};
use std::fs::read;
use actix_web::ResponseError;
use actix_web::http::StatusCode;

#[derive(Debug, PartialEq, Clone)]
pub struct Point {
    pub series: String,
    pub time: i64,
    pub value: i64,
}

impl Point {
    pub fn index_pairs(&self) -> Result<Vec<Pair>, ParseError> {
        index_pairs(&self.series)
    }
}

// TODO: handle escapes in the line protocol for , = and \t
/// index_pairs parses the series key into key value pairs for insertion into the index. In
/// cases where this series is already in the database, this parse step can be skipped entirely.
/// The measurement is represented as a _m key and field as _f.
pub fn index_pairs(key: &str) -> Result<Vec<Pair>, ParseError> {
    let mut chars = key.chars();
    let mut pairs = vec![];
    let mut key = "_m".to_string();
    let mut value = String::with_capacity(250);
    let mut reading_key = false;

    while let Some(ch) = chars.next() {
        match ch {
            ',' => {
                reading_key = true;
                pairs.push(Pair{key, value});
                key = String::with_capacity(250);
                value = String::with_capacity(250);
            },
            '=' => {
                reading_key = false;
            },
            '\t' => {
                reading_key = false;
                pairs.push(Pair{key, value});
                key = "_f".to_string();
                value = String::with_capacity(250);
            },
            _ => {
                if reading_key {
                    key.push(ch);
                } else {
                    value.push(ch);
                }
            }
        }
    }
    pairs.push(Pair{key, value});

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
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for ParseError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

impl ResponseError for ParseError {
    fn status_code(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}

// TODO: have parse return an error for invalid inputs
pub fn parse(input: &str) -> Vec<Point> {
    let mut points = Vec::with_capacity(10000);
    let mut lines= input.lines();

    for line in lines {
        read_line(line, &mut points)
    }

    return points;
}

fn read_line(line: &str, points: &mut Vec<Point>) {
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

fn read_fields(measurement_tags: &str, chars: &mut Chars, points: &mut Vec<Point>) {
    let mut chars = chars;
    let mut points = points;
    let mut field_name = String::with_capacity(100);

    let mut point_offset = points.len();

    while let Some(ch) = chars.next() {
        match ch {
            '=' => {
                read_value(&measurement_tags, field_name, &mut chars, &mut points);
                field_name = String::with_capacity(100);
            },
            _ => field_name.push(ch),
        }
    }

    let time = field_name.parse::<i64>().unwrap();

    while point_offset < points.len() {
        points[point_offset].time = time;
        point_offset += 1;
    }
}

fn read_value(measurement_tags: &str, field_name: String, chars: &mut Chars, points: &mut Vec<Point>) {
    let mut value = String::new();

    while let Some(ch) = chars.next() {
        match ch {
            ' ' => {
                let val = value.parse::<i64>().unwrap();
                points.push(Point{series: measurement_tags.to_string() + "\t" + &field_name, value: val, time: 0});
                return;
            },
            _ => value.push(ch),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_single_field() {
        let input = "foo asdf=23 1234";

        let vals = parse(input);
        assert_eq!(vals[0].series, "foo\tasdf");
        assert_eq!(vals[0].time, 1234);
        assert_eq!(vals[0].value, 23);
    }

    #[test]
    fn parse_two_fields() {
        let input = "foo asdf=23 bar=5 1234";

        let vals = parse(input);
        assert_eq!(vals[0].series, "foo\tasdf");
        assert_eq!(vals[0].time, 1234);
        assert_eq!(vals[0].value, 23);

        assert_eq!(vals[1].series, "foo\tbar");
        assert_eq!(vals[1].time, 1234);
        assert_eq!(vals[1].value, 5);
    }

    #[test]
    fn index_pairs() {
        let p = Point{series: "cpu,host=A,region=west\tusage_system".to_string(), value: 0, time: 0};
        let pairs = p.index_pairs().unwrap();
        assert_eq!(pairs, vec![
            Pair{key: "_m".to_string(), value: "cpu".to_string()},
            Pair{key: "host".to_string(), value: "A".to_string()},
            Pair{key: "region".to_string(), value: "west".to_string()},
            Pair{key: "_f".to_string(), value: "usage_system".to_string()},
        ]);
    }
}