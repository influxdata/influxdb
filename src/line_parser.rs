use either::Either;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while1},
    character::complete::digit1,
    combinator::{map, opt, recognize},
    sequence::{preceded, separated_pair, terminated, tuple},
    IResult,
};
use smallvec::SmallVec;
use snafu::Snafu;
use std::{borrow::Cow, collections::btree_map::Entry, collections::BTreeMap, fmt};

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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct EscapedStr<'a>(SmallVec<[&'a str; 1]>);

impl fmt::Display for EscapedStr<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for p in &self.0 {
            p.fmt(f)?;
        }
        Ok(())
    }
}

impl<'a> EscapedStr<'a> {
    fn is_escaped(&self) -> bool {
        self.0.len() > 1
    }
}

impl From<EscapedStr<'_>> for String {
    fn from(other: EscapedStr<'_>) -> Self {
        other.to_string()
    }
}

impl PartialEq<&str> for EscapedStr<'_> {
    fn eq(&self, other: &&str) -> bool {
        let mut head = *other;
        for p in &self.0 {
            if head.starts_with(p) {
                head = &head[p.len()..];
            } else {
                return false;
            }
        }
        head.is_empty()
    }
}

type TagSet<'a> = SmallVec<[(EscapedStr<'a>, EscapedStr<'a>); 8]>;
type FieldSet<'a> = SmallVec<[(EscapedStr<'a>, FieldValue); 4]>;

#[derive(Debug)]
struct ParsedLine<'a> {
    series: Series<'a>,
    field_set: FieldSet<'a>,
    timestamp: Option<i64>,
}

#[derive(Debug)]
struct Series<'a> {
    raw_input: &'a str,
    measurement: EscapedStr<'a>,
    tag_set: Option<TagSet<'a>>,
}

impl<'a> Series<'a> {
    pub fn generate_base(self) -> Result<Cow<'a, str>> {
        match (!self.is_escaped(), self.is_sorted_and_unique()) {
            (true, true) => Ok(self.raw_input.into()),
            (_, true) => self.generate_base_with_escaping().map(Into::into),
            (_, _) => self
                .generate_base_with_escaping_sorting_deduplicating()
                .map(Into::into),
        }
    }

    fn generate_base_with_escaping(self) -> Result<String> {
        let mut series_base = self.measurement.to_string();
        for (tag_key, tag_value) in self.tag_set.unwrap_or_default() {
            use std::fmt::Write;
            write!(&mut series_base, ",{}={}", tag_key, tag_value)
                .expect("Could not append string");
        }
        Ok(series_base)
    }

    fn generate_base_with_escaping_sorting_deduplicating(self) -> Result<String> {
        let mut unique_sorted_tag_set = BTreeMap::new();
        for (tag_key, tag_value) in self.tag_set.unwrap_or_default() {
            match unique_sorted_tag_set.entry(tag_key) {
                Entry::Vacant(e) => {
                    e.insert(tag_value);
                }
                Entry::Occupied(e) => {
                    let (tag_key, _) = e.remove_entry();
                    return DuplicateTag { tag_key }.fail();
                }
            }
        }

        let mut series_base = self.measurement.to_string();
        for (tag_key, tag_value) in unique_sorted_tag_set {
            use std::fmt::Write;
            write!(&mut series_base, ",{}={}", tag_key, tag_value)
                .expect("Could not append string");
        }

        Ok(series_base)
    }

    fn is_escaped(&self) -> bool {
        self.measurement.is_escaped() || {
            match &self.tag_set {
                None => false,
                Some(tag_set) => tag_set
                    .iter()
                    .any(|(tag_key, tag_value)| tag_key.is_escaped() || tag_value.is_escaped()),
            }
        }
    }

    fn is_sorted_and_unique(&self) -> bool {
        match &self.tag_set {
            None => true,
            Some(tag_set) => {
                let mut i = tag_set.iter().zip(tag_set.iter().skip(1));
                i.all(|((last_tag_key, _), (this_tag_key, _))| last_tag_key < this_tag_key)
            }
        }
    }
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
        series,
        field_set,
        timestamp,
    } = parsed_line;

    let series_base = series.generate_base()?;
    let timestamp = timestamp.expect("TODO: default timestamp not supported");

    Ok(field_set.into_iter().map(move |(field_key, field_value)| {
        let series = format!("{}\t{}", series_base, field_key);

        match field_value {
            FieldValue::I64(value) => PointType::new_i64(series, value, timestamp),
            FieldValue::F64(value) => PointType::new_f64(series, value, timestamp),
        }
    }))
}

fn parse_line(i: &str) -> IResult<&str, ParsedLine<'_>> {
    let field_set = preceded(tag(" "), field_set);
    let timestamp = preceded(tag(" "), timestamp);

    let line = tuple((series, field_set, opt(timestamp)));

    map(line, |(series, field_set, timestamp)| ParsedLine {
        series,
        field_set,
        timestamp,
    })(i)
}

fn series(i: &str) -> IResult<&str, Series<'_>> {
    let tag_set = preceded(tag(","), tag_set);
    let series = tuple((measurement, opt(tag_set)));

    let series_and_raw_input = parse_and_recognize(series);

    map(
        series_and_raw_input,
        |(raw_input, (measurement, tag_set))| Series {
            raw_input,
            measurement,
            tag_set,
        },
    )(i)
}

fn measurement(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| c != ' ' && c != ',' && c != '\\');

    let space = map(tag(" "), |_| " ");
    let comma = map(tag(","), |_| ",");
    let backslash = map(tag("\\"), |_| "\\");

    let escaped = alt((space, comma, backslash));

    escape_or_fallback(normal_char, "\\", escaped)(i)
}

fn tag_set(i: &str) -> IResult<&str, TagSet<'_>> {
    let one_tag = separated_pair(tag_key, tag("="), tag_value);
    parameterized_separated_list(tag(","), one_tag, SmallVec::new, |v, i| v.push(i))(i)
}

fn tag_key(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| c != '=' && c != '\\');

    escaped_value(normal_char)(i)
}

fn tag_value(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| c != ',' && c != ' ' && c != '\\');
    escaped_value(normal_char)(i)
}

fn field_set(i: &str) -> IResult<&str, FieldSet<'_>> {
    let one_field = separated_pair(field_key, tag("="), field_value);
    parameterized_separated_list(tag(","), one_field, SmallVec::new, |v, i| v.push(i))(i)
}

fn field_key(i: &str) -> IResult<&str, EscapedStr<'_>> {
    let normal_char = take_while1(|c| c != '=' && c != '\\');
    escaped_value(normal_char)(i)
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

/// While not all of these escape characters are required to be
/// escaped, we support the client escaping them proactively to
/// provide a common experience.
fn escaped_value<'a, Error>(
    normal: impl Fn(&'a str) -> IResult<&'a str, &'a str, Error>,
) -> impl FnOnce(&'a str) -> IResult<&'a str, EscapedStr<'a>, Error>
where
    Error: nom::error::ParseError<&'a str>,
{
    move |i| {
        let backslash = map(tag("\\"), |_| "\\");
        let comma = map(tag(","), |_| ",");
        let equal = map(tag("="), |_| "=");
        let space = map(tag(" "), |_| " ");

        let escaped = alt((backslash, comma, equal, space));

        escape_or_fallback(normal, "\\", escaped)(i)
    }
}

/// Parse an unescaped piece of text, interspersed with
/// potentially-escaped characters. If the character *isn't* escaped,
/// treat it as a literal character.
fn escape_or_fallback<'a, Error>(
    normal: impl Fn(&'a str) -> IResult<&'a str, &'a str, Error>,
    escape_char: &'static str,
    escaped: impl Fn(&'a str) -> IResult<&'a str, &'a str, Error>,
) -> impl Fn(&'a str) -> IResult<&'a str, EscapedStr<'a>, Error>
where
    Error: nom::error::ParseError<&'a str>,
{
    move |i| {
        let mut result = SmallVec::new();
        let mut head = i;

        loop {
            match normal(head) {
                Ok((remaining, parsed)) => {
                    result.push(parsed);
                    head = remaining;
                }
                Err(nom::Err::Error(_)) => {
                    // FUTURE: https://doc.rust-lang.org/std/primitive.str.html#method.strip_prefix
                    if head.starts_with(escape_char) {
                        let after = &head[escape_char.len()..];

                        match escaped(after) {
                            Ok((remaining, parsed)) => {
                                result.push(parsed);
                                head = remaining;
                            }
                            Err(nom::Err::Error(_)) => {
                                result.push(escape_char);
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
                            return Ok((head, EscapedStr(result)));
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// This is a copied version of nom's `separated_list` that allows
/// parameterizing the created collection via closures.
pub fn parameterized_separated_list<I, O, O2, E, F, G, Ret>(
    sep: G,
    f: F,
    cre: impl FnOnce() -> Ret,
    mut add: impl FnMut(&mut Ret, O),
) -> impl FnOnce(I) -> IResult<I, Ret, E>
where
    I: Clone + PartialEq,
    F: Fn(I) -> IResult<I, O, E>,
    G: Fn(I) -> IResult<I, O2, E>,
    E: nom::error::ParseError<I>,
{
    move |mut i: I| {
        let mut res = cre();

        match f(i.clone()) {
            Err(nom::Err::Error(_)) => return Ok((i, res)),
            Err(e) => return Err(e),
            Ok((i1, o)) => {
                if i1 == i {
                    return Err(nom::Err::Error(E::from_error_kind(
                        i1,
                        nom::error::ErrorKind::SeparatedList,
                    )));
                }

                add(&mut res, o);
                i = i1;
            }
        }

        loop {
            match sep(i.clone()) {
                Err(nom::Err::Error(_)) => return Ok((i, res)),
                Err(e) => return Err(e),
                Ok((i1, _)) => {
                    if i1 == i {
                        return Err(nom::Err::Error(E::from_error_kind(
                            i1,
                            nom::error::ErrorKind::SeparatedList,
                        )));
                    }

                    match f(i1.clone()) {
                        Err(nom::Err::Error(_)) => return Ok((i, res)),
                        Err(e) => return Err(e),
                        Ok((i2, o)) => {
                            if i2 == i {
                                return Err(nom::Err::Error(E::from_error_kind(
                                    i2,
                                    nom::error::ErrorKind::SeparatedList,
                                )));
                            }

                            add(&mut res, o);
                            i = i2;
                        }
                    }
                }
            }
        }
    }
}

/// This is a copied version of nom's `recognize` that runs the parser
/// **and** returns the entire matched input.
pub fn parse_and_recognize<
    I: Clone + nom::Offset + nom::Slice<std::ops::RangeTo<usize>>,
    O,
    E: nom::error::ParseError<I>,
    F,
>(
    parser: F,
) -> impl Fn(I) -> IResult<I, (I, O), E>
where
    F: Fn(I) -> IResult<I, O, E>,
{
    move |input: I| {
        let i = input.clone();
        match parser(i) {
            Ok((i, o)) => {
                let index = input.offset(&i);
                Ok((i, (input.slice(..index), o)))
            }
            Err(e) => Err(e),
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
    fn tag_key_allows_escaping_comma() -> Result {
        assert_fully_parsed!(tag_key(r#"wea\,ther"#), r#"wea,ther"#)
    }

    #[test]
    fn tag_key_allows_escaping_equal() -> Result {
        assert_fully_parsed!(tag_key(r#"wea\=ther"#), r#"wea=ther"#)
    }

    #[test]
    fn tag_key_allows_escaping_space() -> Result {
        assert_fully_parsed!(tag_key(r#"wea\ ther"#), r#"wea ther"#)
    }

    #[test]
    fn tag_key_allows_escaping_backslash() -> Result {
        assert_fully_parsed!(tag_key(r#"\\wea\\ther\\"#), r#"\wea\ther\"#)
    }

    #[test]
    fn tag_key_allows_backslash_with_unknown_escape() -> Result {
        assert_fully_parsed!(tag_key(r#"\wea\ther\"#), r#"\wea\ther\"#)
    }

    #[test]
    fn tag_value_allows_escaping_comma() -> Result {
        assert_fully_parsed!(tag_value(r#"wea\,ther"#), r#"wea,ther"#)
    }

    #[test]
    fn tag_value_allows_escaping_equal() -> Result {
        assert_fully_parsed!(tag_value(r#"wea\=ther"#), r#"wea=ther"#)
    }

    #[test]
    fn tag_value_allows_escaping_space() -> Result {
        assert_fully_parsed!(tag_value(r#"wea\ ther"#), r#"wea ther"#)
    }

    #[test]
    fn tag_value_allows_escaping_backslash() -> Result {
        assert_fully_parsed!(tag_value(r#"\\wea\\ther\\"#), r#"\wea\ther\"#)
    }

    #[test]
    fn tag_value_allows_backslash_with_unknown_escape() -> Result {
        assert_fully_parsed!(tag_value(r#"\wea\ther\"#), r#"\wea\ther\"#)
    }

    #[test]
    fn field_key_allows_escaping_comma() -> Result {
        assert_fully_parsed!(field_key(r#"wea\,ther"#), r#"wea,ther"#)
    }

    #[test]
    fn field_key_allows_escaping_equal() -> Result {
        assert_fully_parsed!(field_key(r#"wea\=ther"#), r#"wea=ther"#)
    }

    #[test]
    fn field_key_allows_escaping_space() -> Result {
        assert_fully_parsed!(field_key(r#"wea\ ther"#), r#"wea ther"#)
    }

    #[test]
    fn field_key_allows_escaping_backslash() -> Result {
        assert_fully_parsed!(field_key(r#"\\wea\\ther\\"#), r#"\wea\ther\"#)
    }

    #[test]
    fn field_key_allows_backslash_with_unknown_escape() -> Result {
        assert_fully_parsed!(field_key(r#"\wea\ther\"#), r#"\wea\ther\"#)
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
