use either::Either;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::digit1,
    combinator::{map, opt, recognize},
    multi::separated_list,
    sequence::{preceded, separated_pair, terminated, tuple},
    IResult,
};
use snafu::Snafu;
use std::{borrow::Cow, collections::BTreeMap};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Must not contain duplicate tags, but "{}" was repeated"#, tag_key))]
    DuplicateTag { tag_key: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

    pub fn index_pairs(&self) -> Vec<Pair> {
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

#[derive(Debug)]
struct ParsedLine<'a> {
    measurement: Cow<'a, str>,
    tag_set: Option<Vec<(&'a str, &'a str)>>,
    field_set: Vec<(&'a str, FieldValue)>,
    timestamp: Option<i64>,
}

#[derive(Debug)]
enum FieldValue {
    I64(i64),
    F64(f64),
}

// TODO: Return an error for invalid inputs
pub fn parse(input: &str) -> Result<Vec<PointType>> {
    input
        .lines()
        .flat_map(|line| match parse_line(line) {
            Ok((_remaining, parsed_line)) => match line_to_points(parsed_line) {
                Ok(i) => Either::Left(i.map(Ok)),
                Err(e) => Either::Right(std::iter::once(Err(e))),
            },
            Err(e) => panic!("TODO: Failed to parse: {}", e),
        })
        .collect()
}

fn line_to_points(parsed_line: ParsedLine<'_>) -> Result<impl Iterator<Item = PointType> + '_> {
    let ParsedLine {
        measurement,
        tag_set,
        field_set,
        timestamp,
    } = parsed_line;

    let mut unique_sorted_tag_set = BTreeMap::new();
    for (tag_key, tag_value) in tag_set.unwrap_or_default() {
        if unique_sorted_tag_set.insert(tag_key, tag_value).is_some() {
            return DuplicateTag { tag_key }.fail();
        }
    }
    let tag_set = unique_sorted_tag_set;

    let timestamp = timestamp.expect("TODO: default timestamp not supported");

    let mut series_base = measurement.into_owned();
    for (tag_key, tag_value) in tag_set {
        use std::fmt::Write;
        write!(&mut series_base, ",{}={}", tag_key, tag_value).expect("Could not append string");
    }
    let series_base = series_base;

    Ok(field_set.into_iter().map(move |(field_key, field_value)| {
        let series = format!("{}\t{}", series_base, field_key);

        match field_value {
            FieldValue::I64(value) => PointType::new_i64(series, value, timestamp),
            FieldValue::F64(value) => PointType::new_f64(series, value, timestamp),
        }
    }))
}

fn parse_line(i: &str) -> IResult<&str, ParsedLine<'_>> {
    let tag_set = preceded(tag(","), tag_set);
    let field_set = preceded(tag(" "), field_set);
    let timestamp = preceded(tag(" "), timestamp);

    let line = tuple((measurement, opt(tag_set), field_set, opt(timestamp)));

    map(line, |(measurement, tag_set, field_set, timestamp)| {
        ParsedLine {
            measurement,
            tag_set,
            field_set,
            timestamp,
        }
    })(i)
}

fn measurement(i: &str) -> IResult<&str, Cow<'_, str>> {
    let normal_char = take_while1(|c| c != ' ' && c != ',' && c != '\\');

    let space = map(tag(" "), |_| " ");
    let comma = map(tag(","), |_| ",");
    let backslash = map(tag("\\"), |_| "\\");

    let escaped = alt((space, comma, backslash));

    escape_or_fallback(normal_char, '\\', escaped)(i)
}

fn tag_set(i: &str) -> IResult<&str, Vec<(&str, &str)>> {
    let tag_key = take_while1(|c| c != '=');
    let tag_value = take_while1(|c| c != ',' && c != ' ');
    let one_tag = separated_pair(tag_key, tag("="), tag_value);
    separated_list(tag(","), one_tag)(i)
}

fn field_set(i: &str) -> IResult<&str, Vec<(&str, FieldValue)>> {
    let field_key = take_while1(|c| c != '=');
    let one_field = separated_pair(field_key, tag("="), field_value);
    separated_list(tag(","), one_field)(i)
}

fn field_value(i: &str) -> IResult<&str, FieldValue> {
    let int = map(terminated(digit1, tag("i")), |v: &str| {
        FieldValue::I64(v.parse().expect("TODO: Unsupported"))
    });

    let float_no_decimal = map(digit1, |v: &str| {
        FieldValue::F64(v.parse().expect("TODO: Unsupported"))
    });

    let float_with_decimal = map(
        recognize(separated_pair(digit1, tag("."), digit1)),
        |v: &str| FieldValue::F64(v.parse().expect("TODO: Unsupported")),
    );

    alt((float_with_decimal, int, float_no_decimal))(i)
}

fn timestamp(i: &str) -> IResult<&str, i64> {
    map(digit1, |f: &str| {
        f.parse().expect("TODO: parsing timestamp failed")
    })(i)
}

/// Parse an unescaped piece of text, interspersed with
/// potentially-escaped characters. If the character *isn't* escaped,
/// treat it as a literal character.
fn escape_or_fallback<'a, Error>(
    normal: impl Fn(&'a str) -> IResult<&'a str, &'a str, Error>,
    escape_char: char,
    escaped: impl Fn(&'a str) -> IResult<&'a str, &'a str, Error>,
) -> impl Fn(&'a str) -> IResult<&'a str, Cow<'a, str>, Error>
where
    Error: nom::error::ParseError<&'a str>,
{
    move |i| {
        let mut result = Cow::from("");
        let mut head = i;

        if let Ok((remaining, parsed)) = normal(head) {
            result = Cow::from(parsed);
            head = remaining;
        }

        loop {
            match normal(head) {
                Ok((remaining, parsed)) => {
                    result.to_mut().push_str(parsed);
                    head = remaining;
                }
                Err(nom::Err::Error(_)) => {
                    // FUTURE: https://doc.rust-lang.org/std/primitive.str.html#method.strip_prefix
                    if head.starts_with(escape_char) {
                        let after = &head[escape_char.len_utf8()..];

                        match escaped(after) {
                            Ok((remaining, parsed)) => {
                                result.to_mut().push_str(parsed);
                                head = remaining;
                            }
                            Err(nom::Err::Error(_)) => {
                                result.to_mut().push(escape_char);
                                head = after;
                            }
                            Err(e) => return Err(e),
                        }
                    } else {
                        // have we parsed *anything*?
                        if head == i {
                            return Err(nom::Err::Error(Error::from_error_kind(
                                head,
                                nom::error::ErrorKind::EscapedTransform,
                            )));
                        } else {
                            return Ok((head, result));
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::tests::approximately_equal;

    type Error = Box<dyn std::error::Error>;
    type Result<T = (), E = Error> = std::result::Result<T, E>;

    #[test]
    fn parse_single_field_integer() -> Result {
        let input = "foo asdf=23i 1234";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert_eq!(vals[0].i64_value().unwrap(), 23);

        Ok(())
    }

    #[test]
    fn parse_single_field_float_no_decimal() -> Result {
        let input = "foo asdf=44 546";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 546);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 44.0));

        Ok(())
    }

    #[test]
    fn parse_single_field_float_with_decimal() -> Result {
        let input = "foo asdf=3.74 123";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 123);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 3.74));

        Ok(())
    }

    #[test]
    fn parse_two_fields_integer() -> Result {
        let input = "foo asdf=23i,bar=5i 1234";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert_eq!(vals[0].i64_value().unwrap(), 23);

        assert_eq!(vals[1].series(), "foo\tbar");
        assert_eq!(vals[1].time(), 1234);
        assert_eq!(vals[1].i64_value().unwrap(), 5);

        Ok(())
    }

    #[test]
    fn parse_two_fields_float() -> Result {
        let input = "foo asdf=23.1,bar=5 1234";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 23.1));

        assert_eq!(vals[1].series(), "foo\tbar");
        assert_eq!(vals[1].time(), 1234);
        assert!(approximately_equal(vals[1].f64_value().unwrap(), 5.0));

        Ok(())
    }

    #[test]
    fn parse_mixed_float_and_integer() -> Result {
        let input = "foo asdf=23.1,bar=5i 1234";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo\tasdf");
        assert_eq!(vals[0].time(), 1234);
        assert!(approximately_equal(vals[0].f64_value().unwrap(), 23.1));

        assert_eq!(vals[1].series(), "foo\tbar");
        assert_eq!(vals[1].time(), 1234);
        assert_eq!(vals[1].i64_value().unwrap(), 5);

        Ok(())
    }

    #[test]
    fn parse_tag_set_included_in_series() -> Result {
        let input = "foo,tag1=1,tag2=2 value=1 123";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo,tag1=1,tag2=2\tvalue");

        Ok(())
    }

    #[test]
    fn parse_tag_set_unsorted() -> Result {
        let input = "foo,tag2=2,tag1=1 value=1 123";
        let vals = parse(input)?;

        assert_eq!(vals[0].series(), "foo,tag1=1,tag2=2\tvalue");

        Ok(())
    }

    #[test]
    fn parse_tag_set_duplicate_tags() -> Result {
        let input = "foo,tag=1,tag=2 value=1 123";
        let err = parse(input).expect_err("Parsing duplicate tags should fail");

        assert_eq!(
            err.to_string(),
            r#"Must not contain duplicate tags, but "tag" was repeated"#
        );

        Ok(())
    }

    macro_rules! assert_fully_parsed {
        ($parse_result:expr, $output:expr $(,)?) => {{
            let (remaining, parsed) = $parse_result?;

            assert!(
                remaining.is_empty(),
                "Some input remained to be parsed: {:?}",
                remaining,
            );
            assert_eq!(parsed, $output, "Did not parse the expected output");

            Ok(())
        }};
    }

    #[test]
    fn measurement_allows_escaping_comma() -> Result {
        assert_fully_parsed!(measurement(r#"wea\,ther"#), r#"wea,ther"#)
    }

    #[test]
    fn measurement_allows_escaping_space() -> Result {
        assert_fully_parsed!(measurement(r#"wea\ ther"#), r#"wea ther"#)
    }

    #[test]
    fn measurement_allows_escaping_backslash() -> Result {
        assert_fully_parsed!(measurement(r#"\\wea\\ther\\"#), r#"\wea\ther\"#)
    }

    #[test]
    fn measurement_allows_backslash_with_unknown_escape() -> Result {
        assert_fully_parsed!(measurement(r#"\wea\ther\"#), r#"\wea\ther\"#)
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
