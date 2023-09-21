use super::*;
use snafu::{OptionExt, ResultExt, Snafu};

#[derive(Clone, Debug)]
pub struct ParsedTsmKey {
    pub org_id: InfluxId,
    pub bucket_id: InfluxId,
    pub measurement: String,
    pub tagset: Vec<(String, String)>,
    pub field_key: String,
}

/// Public error type that wraps the underlying data parsing error
/// with the actual key value being parsed.
#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum Error {
    #[snafu(display(r#"Error while parsing tsm tag key '{}': {}"#, key, source))]
    ParsingTsmKey { key: String, source: DataError },
}

#[derive(Debug, Snafu, PartialEq, Eq)]
pub enum DataError {
    #[snafu(display(r#"Key length too short"#))]
    KeyTooShort {},

    #[snafu(display(r#"No measurement found (expected to find in tag field \x00)"#))]
    NoMeasurement {},

    #[snafu(display(r#"No field key (expected to find in tag field \xff)"#))]
    NoFieldKey {},

    #[snafu(display(
        r#"Found new measurement '{}' after the first '{}'"#,
        new_measurement,
        old_measurement
    ))]
    MultipleMeasurements {
        new_measurement: String,
        old_measurement: String,
    },

    #[snafu(display(
        r#"Found new field key '{}' after the first '{}'"#,
        new_field,
        old_field
    ))]
    MultipleFields {
        new_field: String,
        old_field: String,
    },

    #[snafu(display(r#"Error parsing field key: {}"#, details))]
    ParsingFieldKey { details: String },

    #[snafu(display(r#"Error parsing tsm tag key: {}"#, description))]
    ParsingTsmTagKey { description: String },

    #[snafu(display(
        r#"Error parsing tsm tag value for key '{}': {}"#,
        tag_key,
        description
    ))]
    ParsingTsmTagValue {
        tag_key: String,
        description: String,
    },

    #[snafu(display(r#"Error parsing tsm field key: {}"#, description))]
    ParsingTsmFieldKey { description: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Parses the the measurement, field key and tag set from a TSM index key
///
/// It does not provide access to the org and bucket IDs on the key; these can be accessed via
/// `org_id()` and `bucket_id()` respectively.
///
/// Loosely based on [points.go](https://github.com/influxdata/influxdb/blob/751d70a213e5fdae837eda13d7ecb37763e69abb/models/points.go#L462)
///
/// The format looks roughly like:
///
/// ```text
/// <org_id bucket_id>,\x00=<measurement>,<tag_keys_str>,\xff=<field_key_str>#!
/// ~#<field_key_str>
/// ```
///
/// For example:
///
/// ```text
/// <org_id bucket_id>,\x00=http_api_request_duration_seconds,status=2XX,\
/// xff=sum#!~#sum
///
///    measurement = "http_api_request"
///    tags = [("status", "2XX")]
///    field = "sum"
/// ```
pub fn parse_tsm_key(key: &[u8]) -> Result<ParsedTsmKey, Error> {
    // Wrap in an internal function to translate error types and add key context
    parse_tsm_key_internal(key).context(ParsingTsmKeySnafu {
        key: String::from_utf8_lossy(key),
    })
}

fn parse_tsm_key_internal(key: &[u8]) -> Result<ParsedTsmKey, DataError> {
    // Get the org and bucket id from the first section of the key.
    let mut rem_key = key.iter().copied();
    let org_id = parse_id(&mut rem_key)?;
    let bucket_id = parse_id(&mut rem_key)?;

    // Now fetch the measurement and tags, starting after the org, bucket and a comma.
    rem_key.next(); // Skip the comma

    let mut tagset = Vec::with_capacity(10);
    let mut measurement = None;
    let mut field_key = None;

    loop {
        let tag_key = parse_tsm_tag_key(&mut rem_key)?;

        let has_more_tags = match tag_key {
            KeyType::Tag(tag_key) => {
                let (has_more_tags, tag_value) = parse_tsm_tag_value(&tag_key, &mut rem_key)?;
                tagset.push((tag_key, tag_value));
                has_more_tags
            }

            KeyType::Measurement => {
                let (has_more_tags, tag_value) = parse_tsm_tag_value("Measurement", &mut rem_key)?;
                match measurement {
                    Some(measurement) => {
                        return MultipleMeasurementsSnafu {
                            new_measurement: tag_value,
                            old_measurement: measurement,
                        }
                        .fail()
                    }
                    None => {
                        measurement = Some(tag_value);
                        has_more_tags
                    }
                }
            }
            KeyType::Field => {
                // since `parse_tsm_field_key_value` consumes the rest of the iterator, it
                // is some kind of logic error if we already have a field key
                assert!(field_key.is_none(), "second field key found while parsing");

                let parsed_value = parse_tsm_field_key_value(&mut rem_key)?;
                assert!(
                    rem_key.next().is_none(),
                    "parsing field key value did not consume remaining index entry"
                );

                field_key = Some(parsed_value);
                false
            }
        };

        if !has_more_tags {
            break;
        }
    }

    Ok(ParsedTsmKey {
        org_id,
        bucket_id,
        measurement: measurement.context(NoMeasurementSnafu)?,
        tagset,
        field_key: field_key.context(NoFieldKeySnafu)?,
    })
}

// Parses an influx id from the byte sequence. IDs are generally just 8 bytes, but we escape
// certain characters ('\', ' ' and '='), so we unescape them as part of this process.
// The iterator will consume all bytes that are part of the id.
fn parse_id(key: impl Iterator<Item = u8>) -> Result<InfluxId, DataError> {
    let mut id: [u8; 8] = [0; 8];

    let mut i = 0;
    let mut escaped = false;
    for x in key {
        if x == b'\\' && !escaped {
            escaped = true;
            continue;
        }

        id[i] = x;
        if i >= 7 {
            return Ok(InfluxId::from_be_bytes(id));
        }
        i += 1;
        escaped = false;
    }

    Err(DataError::KeyTooShort {})
}

/// Parses the field value stored in a TSM field key into a field name.
/// fields are stored on the series keys in TSM indexes as follows:
///
/// <field_key><4-byte delimiter><field_key>
///
/// Example: sum#!~#sum means 'sum' field key
///
/// It also turns out that the data after the delimiter does not necessarily
/// escape the data.
///
/// So for example, the following is a valid field key value (for the
/// field named "Code Cache"):
///
/// {\xff>=Code\ Cache#!~#Code Cache
fn parse_tsm_field_key_value(rem_key: impl Iterator<Item = u8>) -> Result<String, DataError> {
    #[derive(Debug)]
    enum State {
        Data,
        Escape, //
        Key1,   // saw #
        Key2,   // saw #!
        Key3,   // saw #!~
        Done,
    }

    let mut field_name = String::with_capacity(100);
    let mut state = State::Data;

    // return the next byte if its value is not a valid unless escaped
    fn check_next_byte(byte: u8) -> Result<u8, DataError> {
        match byte {
            b'=' => ParsingTsmFieldKeySnafu {
                description: "invalid unescaped '='",
            }
            .fail(),
            // An unescaped space is an invalid tag value.
            b' ' => ParsingTsmFieldKeySnafu {
                description: "invalid unescaped ' '",
            }
            .fail(),
            b',' => ParsingTsmFieldKeySnafu {
                description: "invalid unescaped ','",
            }
            .fail(),
            _ => Ok(byte),
        }
    }

    // Determines what hte next state is when in the middle of parsing a
    // delimiter.
    fn next_key_state(
        byte: u8,
        next_delim_byte: u8,
        next_delim_state: State,
        delim_so_far: &str,
        field_name: &mut String,
    ) -> Result<State, DataError> {
        // If the next_delim_byte is the next part of the delimiter
        if byte == next_delim_byte {
            Ok(next_delim_state)
        }
        // otherwise it was data that happened to be the same first
        // few bytes as delimiter. Add the part of the delimiter seen
        // so far and go back to data
        else {
            field_name.push_str(delim_so_far);
            // start of delimiter again
            match byte {
                b'#' => Ok(State::Key1),
                b'\\' => Ok(State::Escape),
                _ => {
                    field_name.push(check_next_byte(byte)? as char);
                    Ok(State::Data)
                }
            }
        }
    }

    // loop over input byte by byte and once we are at the end of the field key,
    // consume the rest of the key stream (ignoring all remaining characters)
    for byte in rem_key {
        match state {
            State::Data => match byte {
                b'#' => state = State::Key1,
                b'\\' => state = State::Escape,
                _ => field_name.push(check_next_byte(byte)? as char),
            },
            State::Escape => {
                field_name.push(byte as char);
                state = State::Data
            }
            State::Key1 => state = next_key_state(byte, b'!', State::Key2, "#", &mut field_name)?,
            State::Key2 => state = next_key_state(byte, b'~', State::Key3, "#!", &mut field_name)?,
            State::Key3 => state = next_key_state(byte, b'#', State::Done, "#!~", &mut field_name)?,
            State::Done => {} // ignore all data after delimiter
        };
    }

    match state {
        State::Done if !field_name.is_empty() => Ok(field_name),
        State::Done => ParsingFieldKeySnafu {
            details: "field key too short",
        }
        .fail(),
        _ => ParsingFieldKeySnafu {
            details: format!(
                "Delimiter not found before end of stream reached. \
                                  Still in state {state:?}"
            ),
        }
        .fail(),
    }
}

#[derive(Debug, PartialEq)]

/// Represents the 'type' of the tag.
///
/// This is used to represent the
/// the way the 'measurement name' and the `field name` are stored in
/// TSM OSS 2.0 files, which is different than where line protocol has the
/// measurement and field names.
///
/// Specifically, the measurement name and field names are stored as
/// 'tag's with the special keys \x00 and \xff, respectively.
enum KeyType {
    Tag(String),
    /// the measurement name is encoded in the tsm key as the value of a
    /// special tag key '\x00'.
    ///
    /// For example,the tsm key
    /// "\x00=foo" has the measurement name "foo"
    Measurement,
    /// the field name is encoded in the tsm key as the value of a
    /// special tag key '\xff'.
    ///
    /// For example,the tsm key
    /// "user_agent=Firefox,\xff=sum#!~#sum" has a 'user_agent` tag
    /// key with value Firefix and a field named 'sum')
    Field,
}

impl From<&KeyType> for String {
    fn from(item: &KeyType) -> Self {
        match item {
            KeyType::Tag(s) => s.clone(),
            KeyType::Measurement => "<measurement>".to_string(),
            KeyType::Field => "<field>".to_string(),
        }
    }
}

/// Parses bytes from the `rem_key` input stream until the end of the
/// next key value (=). Consumes the '='
fn parse_tsm_tag_key(rem_key: impl Iterator<Item = u8>) -> Result<KeyType, DataError> {
    enum State {
        Data,
        Measurement,
        Field,
        Escape,
    }

    let mut state = State::Data;
    let mut key = String::with_capacity(250);

    // Examine each character in the tag key until we hit an unescaped
    // equals (the tag value), or we hit an error (i.e., unescaped
    // space or comma).
    for byte in rem_key {
        match state {
            State::Data => match byte {
                b'\x00' => {
                    state = State::Measurement;
                }
                b'\xff' => {
                    state = State::Field;
                }
                b'=' => return Ok(KeyType::Tag(key)),
                b',' => {
                    return ParsingTsmTagKeySnafu {
                        description: "unescaped comma",
                    }
                    .fail();
                }
                b' ' => {
                    return ParsingTsmTagKeySnafu {
                        description: "unescaped space",
                    }
                    .fail();
                }
                b'\\' => state = State::Escape,
                _ => key.push(byte as char),
            },
            State::Measurement => match byte {
                b'=' => {
                    return Ok(KeyType::Measurement);
                }
                _ => {
                    return ParsingTsmTagKeySnafu {
                        description: "extra data after special 0x00",
                    }
                    .fail();
                }
            },
            State::Field => match byte {
                b'=' => {
                    return Ok(KeyType::Field);
                }
                _ => {
                    return ParsingTsmTagKeySnafu {
                        description: "extra data after special 0xff",
                    }
                    .fail();
                }
            },
            State::Escape => {
                state = State::Data;
                key.push(byte as char);
            }
        }
    }

    ParsingTsmTagKeySnafu {
        description: "unexpected end of data",
    }
    .fail()
}

/// Parses bytes from the `rem_key` input stream until the end of a
/// tag value
///
/// Returns a tuple `(has_more_tags, tag_value)`
///
/// Examples:
///
/// "val1,tag2=val --> Ok((true, "val1")));
/// "val1" --> Ok((False, "val1")));
fn parse_tsm_tag_value(
    tag_key: &str,
    rem_key: impl Iterator<Item = u8>,
) -> Result<(bool, String), DataError> {
    #[derive(Debug)]
    enum State {
        Start,
        Data,
        Escape,
    }

    let mut state = State::Start;
    let mut tag_value = String::with_capacity(100);

    // Examine each character in the tag value until we hit an unescaped
    // comma (move onto next tag key), or we error out.
    for byte in rem_key {
        match state {
            State::Start => {
                match byte {
                    // An unescaped equals sign is an invalid tag value.
                    // cpu,tag={'=', 'fo=o'}
                    b'=' => {
                        return ParsingTsmTagValueSnafu {
                            tag_key,
                            description: "invalid unescaped '='",
                        }
                        .fail()
                    }
                    // An unescaped space is an invalid tag value.
                    b' ' => {
                        return ParsingTsmTagValueSnafu {
                            tag_key,
                            description: "invalid unescaped ' '",
                        }
                        .fail()
                    }
                    b',' => {
                        return ParsingTsmTagValueSnafu {
                            tag_key,
                            description: "missing tag value",
                        }
                        .fail()
                    }
                    b'\\' => state = State::Escape,
                    _ => {
                        state = State::Data;
                        tag_value.push(byte as char);
                    }
                }
            }
            State::Data => {
                match byte {
                    // An unescaped equals sign is an invalid tag value.
                    // cpu,tag={'=', 'fo=o'}
                    b'=' => {
                        return ParsingTsmTagValueSnafu {
                            tag_key,
                            description: "invalid unescaped '='",
                        }
                        .fail()
                    }
                    // An unescaped space is an invalid tag value.
                    b' ' => {
                        return ParsingTsmTagValueSnafu {
                            tag_key,
                            description: "invalid unescaped ' '",
                        }
                        .fail()
                    }
                    // cpu,tag=foo,
                    b',' => return Ok((true, tag_value)),
                    // start of escape value
                    b'\\' => state = State::Escape,
                    _ => {
                        tag_value.push(byte as char);
                    }
                }
            }
            State::Escape => {
                tag_value.push(byte as char);
                state = State::Data;
            }
        }
    }

    // Tag value cannot be empty.
    match state {
        State::Start => ParsingTsmTagValueSnafu {
            tag_key,
            description: "missing tag value",
        }
        .fail(),
        State::Escape => ParsingTsmTagValueSnafu {
            tag_key,
            description: "tag value ends in escape",
        }
        .fail(),
        _ => Ok((false, tag_value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_id_good() {
        // Simple with no escaping
        let mut key = b"\x00\x00\x00\x00\x00\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x02,\x00=cpu"
            .iter()
            .copied();
        let org_id = parse_id(&mut key).expect("unable to parse id");
        assert_eq!(org_id, InfluxId(1));

        let bucket_id = parse_id(&mut key).expect("unable to parse id");
        assert_eq!(bucket_id, InfluxId(2));

        // Check that the iterator has been left at the comma
        let rem: Vec<u8> = key.collect();
        assert_eq!(rem, b",\x00=cpu");
    }

    #[test]
    fn test_parse_id_escaped() {
        // ID with escaped characters: we escape space (\x20), comma (\x2c) and backslash (\x5c)
        let mut key = b"\x00\x5c\x20\x5c\x5c\x5c\x2c\x01\x5c\x2c\x03\x04,\x00=cpu"
            .iter()
            .copied();
        let unescaped: [u8; 8] = hex::decode("00205c2c012c0304").unwrap().try_into().unwrap();

        let id = parse_id(&mut key).expect("unable to parse id");
        assert_eq!(id, InfluxId::from_be_bytes(unescaped));

        // Check that the iterator has been left at the next byte
        let rem: Vec<u8> = key.collect();
        assert_eq!(rem, b",\x00=cpu");
    }

    #[test]
    fn test_parse_tsm_field_key_value() {
        // test the operation of parse_tsm_field_key_value
        do_test_parse_tsm_field_key_value_good("sum#!~#sum", "sum");
        do_test_parse_tsm_field_key_value_bad("#!~#", "field key too short");

        do_test_parse_tsm_field_key_value_good("foo#!~#fpp", "foo");
        do_test_parse_tsm_field_key_value_good("foo#!~#", "foo");

        // escaped values
        do_test_parse_tsm_field_key_value_good(r"foo\ bar#!~#foo bar", "foo bar");
        do_test_parse_tsm_field_key_value_good(r"foo\,bar#!~#foo,bar", "foo,bar");

        // unescaped values
        do_test_parse_tsm_field_key_value_bad("foo bar#!~#foo bar", "invalid unescaped ' '");
        do_test_parse_tsm_field_key_value_bad("foo,bar#!~#foo,bar", "invalid unescaped ','");

        do_test_parse_tsm_field_key_value_good("foo##!~#foo", "foo#");
        do_test_parse_tsm_field_key_value_good("fo#o#!~#foo", "fo#o");

        // partial delimiters
        do_test_parse_tsm_field_key_value_good("foo#!#!~#foo", "foo#!");
        do_test_parse_tsm_field_key_value_good("fo#!o#!~#foo", "fo#!o");
        do_test_parse_tsm_field_key_value_good(r"fo#!\ o#!~#foo", "fo#! o");
        do_test_parse_tsm_field_key_value_good(r"fo#!\,o#!~#foo", "fo#!,o");
        do_test_parse_tsm_field_key_value_good(r"fo#!\=o#!~#foo", "fo#!=o");

        do_test_parse_tsm_field_key_value_good("foo#!~o#!~#foo", "foo#!~o");
        do_test_parse_tsm_field_key_value_good("fo#!~o#!~#foo", "fo#!~o");
        do_test_parse_tsm_field_key_value_good(r"fo#!~\ #!~#foo", "fo#!~ ");

        do_test_parse_tsm_field_key_value_good("foo#!~#!~#foo", "foo"); // matches!
        do_test_parse_tsm_field_key_value_good("fo#!~o#!~#foo", "fo#!~o");
        do_test_parse_tsm_field_key_value_good(r"fo#!~\ #!~#foo", "fo#!~ ");

        // test partial delimiters
        do_test_parse_tsm_field_key_value_bad(
            "foo",
            "Delimiter not found before end of stream reached",
        );
        do_test_parse_tsm_field_key_value_bad(
            "foo#",
            "Delimiter not found before end of stream reached",
        );
        do_test_parse_tsm_field_key_value_bad(
            "foo#!",
            "Delimiter not found before end of stream reached",
        );
        do_test_parse_tsm_field_key_value_bad(
            "foo#!~",
            "Delimiter not found before end of stream reached",
        );

        // test unescaped ' ', '=' and ',' before and after the delimiters
        do_test_parse_tsm_field_key_value_bad("foo bar#!~#foo bar", "invalid unescaped ' '");
        do_test_parse_tsm_field_key_value_bad("foo,bar#!~#foo,bar", "invalid unescaped ','");
        do_test_parse_tsm_field_key_value_bad("foo=bar#!~#foo=bar", "invalid unescaped '='");
        // but escaped before the delimiter is fine
        do_test_parse_tsm_field_key_value_good(r"foo\ bar#!~#foo bar", "foo bar");
        do_test_parse_tsm_field_key_value_good(r"foo\,bar#!~#foo,bar", "foo,bar");
        do_test_parse_tsm_field_key_value_good(r"foo\=bar#!~#foo=bar", "foo=bar");
    }

    #[test]
    fn test_parse_tsm_tag_key() {
        do_test_parse_tsm_tag_key_error("", "", "unexpected end of data");
        do_test_parse_tsm_tag_key_good("foo=bar", "bar", KeyType::Tag("foo".into()));
        do_test_parse_tsm_tag_key_good("foo=", "", KeyType::Tag("foo".into()));
        do_test_parse_tsm_tag_key_error("foo", "", "unexpected end of data");
        do_test_parse_tsm_tag_key_error("foo,=bar", "=bar", "unescaped comma");
        do_test_parse_tsm_tag_key_error("foo =bar", "=bar", "unescaped space");

        do_test_parse_tsm_tag_key_good(r"\ foo=bar", "bar", KeyType::Tag(" foo".into()));
        do_test_parse_tsm_tag_key_good(r"\=foo=bar", "bar", KeyType::Tag("=foo".into()));
        do_test_parse_tsm_tag_key_good(r"\,foo=bar", "bar", KeyType::Tag(",foo".into()));
        do_test_parse_tsm_tag_key_good(r"\foo=bar", "bar", KeyType::Tag("foo".into()));
        do_test_parse_tsm_tag_key_good(r"\\foo=bar", "bar", KeyType::Tag(r"\foo".into()));

        do_test_parse_tsm_tag_key_good(r"f\ oo=bar", "bar", KeyType::Tag("f oo".into()));
        do_test_parse_tsm_tag_key_good(r"f\=oo=bar", "bar", KeyType::Tag("f=oo".into()));
        do_test_parse_tsm_tag_key_good(r"f\,oo=bar", "bar", KeyType::Tag("f,oo".into()));
        do_test_parse_tsm_tag_key_good(r"f\oo=bar", "bar", KeyType::Tag("foo".into()));
    }

    #[test]
    fn test_parse_tsm_tag_value() {
        do_test_parse_tsm_tag_value_error("", "", "missing tag value");
        do_test_parse_tsm_tag_value_good(
            "val1,tag2=val2 value=1",
            "tag2=val2 value=1",
            (true, "val1".into()),
        );
        do_test_parse_tsm_tag_value_good("val1", "", (false, "val1".into()));
        do_test_parse_tsm_tag_value_good(r"\ val1", "", (false, " val1".into()));
        do_test_parse_tsm_tag_value_good(r"val\ 1", "", (false, "val 1".into()));
        do_test_parse_tsm_tag_value_good(r"val1\ ", "", (false, "val1 ".into()));
        do_test_parse_tsm_tag_value_error(r"val1\", "", "tag value ends in escape");
        do_test_parse_tsm_tag_value_error(r"=b", "b", "invalid unescaped '='");
        do_test_parse_tsm_tag_value_error(r"f=b", "b", "invalid unescaped '='");
        do_test_parse_tsm_tag_value_error(r" v", "v", "invalid unescaped ' '");
        do_test_parse_tsm_tag_value_error(r"v ", "", "invalid unescaped ' '");
    }

    // create key in this form:
    //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>
    fn make_tsm_key_prefix(measurement: &str, tag_keys_str: &str) -> Vec<u8> {
        let mut key = Vec::new();

        let org = b"12345678";
        let bucket = b"87654321";

        // 8 bytes of ORG
        key.extend_from_slice(org);

        // 8 bytes of BUCKET
        key.extend_from_slice(bucket);

        key.push(b',');

        // 2 bytes: special measurement tag key \x00=
        key.push(b'\x00');
        key.push(b'=');
        key.extend_from_slice(measurement.as_bytes());

        key.push(b',');
        key.extend_from_slice(tag_keys_str.as_bytes());

        key
    }

    // add this to the key: ,\xff=<field_key_str>#!~#<field_key_str>
    fn add_field_key(mut key: Vec<u8>, field_key_str: &str) -> Vec<u8> {
        key.push(b',');
        key.push(b'\xff');
        key.push(b'=');
        key.extend_from_slice(field_key_str.as_bytes());
        key.extend_from_slice(b"#!~#");
        key.extend_from_slice(field_key_str.as_bytes());
        key
    }

    #[test]
    fn parse_tsm_key_good() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>,\xff=<field_key_str>#!~#
        //<org_id <field_key_str>
        let mut key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");
        key = add_field_key(key, "f");

        let org_id = InfluxId::from_be_bytes(*b"12345678");
        let bucket_id = InfluxId::from_be_bytes(*b"87654321");

        let parsed_key = super::parse_tsm_key(&key).unwrap();
        assert_eq!(parsed_key.org_id, org_id);
        assert_eq!(parsed_key.bucket_id, bucket_id);
        assert_eq!(parsed_key.measurement, String::from("m"));
        let exp_tagset = vec![
            (String::from("tag1"), String::from("val1")),
            (String::from("tag2"), String::from("val2")),
        ];
        assert_eq!(parsed_key.tagset, exp_tagset);
        assert_eq!(parsed_key.field_key, String::from("f"));
    }

    #[test]
    fn parse_tsm_key_too_short() {
        let key = b"1234567887654";
        let err_str = parse_tsm_key(&key[..])
            .expect_err("expect parsing error")
            .to_string();

        assert!(
            err_str
                .contains("Error while parsing tsm tag key '1234567887654': Key length too short"),
            "{}",
            err_str
        );
    }

    #[test]
    fn parse_tsm_error_has_key() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>
        let key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");

        let err_str = parse_tsm_key(&key)
            .expect_err("expect parsing error")
            .to_string();
        // expect that a representation of the actual TSM key is in the error message
        assert!(
            err_str.contains(
                "Error while parsing tsm tag key '1234567887654321,\x00=m,tag1=val1,tag2=val2':"
            ),
            "{}",
            err_str
        );
    }

    #[test]
    fn parse_tsm_key_no_field() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>
        let key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");

        let err_str = parse_tsm_key(&key)
            .expect_err("expect parsing error")
            .to_string();
        assert!(
            err_str.contains("No field key (expected to find in tag field \\xff)"),
            "{}",
            err_str
        );
    }

    #[test]
    fn parse_tsm_key_two_fields() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>\xff=<field-key_str>#!~#
        //<org_id <field_key_str>\xff=<field-key_str>#!~#<field_key_str>
        let mut key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");
        key = add_field_key(key, "f");
        key = add_field_key(key, "f2");

        // Now we just ignore all content after the field key
        let parsed_key = parse_tsm_key(&key).expect("parsed");
        assert_eq!(
            parsed_key.field_key,
            "f",
            "while parsing {}",
            String::from_utf8_lossy(&key)
        );
    }

    #[test]
    fn test_parse_tsm_key() {
        //<org_id bucket_id>,\x00=http_api_request_duration_seconds,handler=platform,
        //<org_id method=POST,path=/api/v2/setup,status=2XX,user_agent=Firefox,\xff=sum#
        //<org_id !~#sum
        let buf = "05C19117091A100005C19117091A10012C003D68747470\
             5F6170695F726571756573745F6475726174696F6E5F73\
             65636F6E64732C68616E646C65723D706C6174666F726D\
             2C6D6574686F643D504F53542C706174683D2F6170692F\
             76322F73657475702C7374617475733D3258582C757365\
             725F6167656E743D46697265666F782CFF3D73756D2321\
             7E2373756D";
        let tsm_key = hex::decode(buf).unwrap();

        let parsed_key = super::parse_tsm_key(&tsm_key).unwrap();
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
    fn parse_tsm_key_escaped() {
        //<org_id bucket_id>,\x00=query_log,env=prod01-eu-central-1,error=memory\
        //<org_id allocation\ limit\ reached:\ limit\ 740000000\ bytes\,\ allocated:\
        //<org_id 739849088\,\ wanted:\ 6946816;\ memory\ allocation\ limit\ reached:\
        //<org_id limit\ 740000000\ bytes\,\ allocated:\ 739849088\,\ wanted:\
        //<org_id 6946816,errorCode=invalid,errorType=user,
        //<org_id host=queryd-algow-rw-76d68d5968-fzgwr,
        //<org_id hostname=queryd-algow-rw-76d68d5968-fzgwr,nodename=ip-10-153-10-221.
        //<org_id eu-central-1.compute.internal,orgID=0b6e852e272ffdd9,
        //<org_id ot_trace_sampled=false,role=queryd-algow-rw,source=hackney,\
        //<org_id xff=responseSize#!~#responseSize
        let buf = "844910ECE80BE8BC3C0BD4C89186CA892C\
             003D71756572795F6C6F672C656E763D70726F6430312D65752D63656E747261\
             6C2D312C6572726F723D6D656D6F72795C20616C6C6F636174696F6E5C206C69\
             6D69745C20726561636865643A5C206C696D69745C203734303030303030305C\
             2062797465735C2C5C20616C6C6F63617465643A5C203733393834393038385C2\
             C5C2077616E7465643A5C20363934363831363B5C206D656D6F72795C20616C6C\
             6F636174696F6E5C206C696D69745C20726561636865643A5C206C696D69745C2\
             03734303030303030305C2062797465735C2C5C20616C6C6F63617465643A5C20\
             3733393834393038385C2C5C2077616E7465643A5C20363934363831362C65727\
             26F72436F64653D696E76616C69642C6572726F72547970653D757365722C686F\
             73743D7175657279642D616C676F772D72772D373664363864353936382D667A6\
             777722C686F73746E616D653D7175657279642D616C676F772D72772D37366436\
             3864353936382D667A6777722C6E6F64656E616D653D69702D31302D3135332D3\
             1302D3232312E65752D63656E7472616C2D312E636F6D707574652E696E746572\
             6E616C2C6F726749443D306236653835326532373266666464392C6F745F74726\
             163655F73616D706C65643D66616C73652C726F6C653D7175657279642D616C67\
             6F772D72772C736F757263653D6861636B6E65792CFF3D726573706F6E7365536\
             97A6523217E23726573706F6E736553697A65";
        let tsm_key = hex::decode(buf).unwrap();

        let parsed_key = super::parse_tsm_key(&tsm_key).unwrap();
        assert_eq!(parsed_key.measurement, String::from("query_log"));

        let exp_tagset = vec![
            (String::from("env"), String::from("prod01-eu-central-1")),
            (String::from("error"), String::from("memory allocation limit reached: limit 740000000 bytes, allocated: 739849088, wanted: 6946816; memory allocation limit reached: limit 740000000 bytes, allocated: 739849088, wanted: 6946816")),
            (String::from("errorCode"), String::from("invalid")),
            (String::from("errorType"), String::from("user")),
            (String::from("host"), String::from("queryd-algow-rw-76d68d5968-fzgwr")),
            (String::from("hostname"), String::from("queryd-algow-rw-76d68d5968-fzgwr")),
            (String::from("nodename"), String::from("ip-10-153-10-221.eu-central-1.compute.internal")),
            (String::from("orgID"), String::from("0b6e852e272ffdd9")),
            (String::from("ot_trace_sampled"), String::from("false")),
            (String::from("role"), String::from("queryd-algow-rw")),
            (String::from("source"), String::from("hackney")),

        ];
        assert_eq!(parsed_key.tagset, exp_tagset);
        assert_eq!(parsed_key.field_key, String::from("responseSize"));
    }

    fn do_test_parse_tsm_field_key_value_good(input: &str, expected_field_key: &str) {
        let mut iter = input.bytes();
        let result = parse_tsm_field_key_value(&mut iter);
        match result {
            Ok(field_key) => {
                assert_eq!(
                    field_key, expected_field_key,
                    "Unexpected field key parsing '{input}'"
                );
            }
            Err(e) => panic!(
                "Unexpected error while parsing field key '{input}', got '{e}', expected '{expected_field_key}'"
            ),
        }
    }

    fn do_test_parse_tsm_field_key_value_bad(input: &str, expected_error: &str) {
        let mut iter = input.bytes();
        let result = parse_tsm_field_key_value(&mut iter);
        match result {
            Ok(field_key) => {
                panic!(
                    "Unexpected success parsing field key '{input}'. \
                        Expected error '{expected_error}', got  '{field_key}'"
                );
            }
            Err(err) => {
                let err_str = err.to_string();
                assert!(
                    err_str.contains(expected_error),
                    "Did not find expected error while parsing '{input}'. \
                     Expected '{expected_error}' but actual error was '{err_str}'"
                );
            }
        }
    }

    fn do_test_parse_tsm_tag_key_good(
        input: &str,
        expected_remaining_input: &str,
        expected_tag_key: KeyType,
    ) {
        let mut iter = input.bytes();

        let result = parse_tsm_tag_key(&mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_key) => {
                assert_eq!(tag_key, expected_tag_key, "while parsing input '{input}'");
            }
            Err(err) => {
                panic!(
                    "Got error '{err}', expected parsed tag key: '{expected_tag_key:?}' while parsing '{input}'"
                );
            }
        }
        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{input}'"
        );
    }

    fn do_test_parse_tsm_tag_key_error(
        input: &str,
        expected_remaining_input: &str,
        expected_error: &str,
    ) {
        let mut iter = input.bytes();

        let result = parse_tsm_tag_key(&mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_key) => {
                panic!(
                    "Got parsed key {tag_key:?}, expected failure {expected_error} while parsing input '{input}'"
                );
            }
            Err(err) => {
                let err_str = err.to_string();
                assert!(
                    err_str.contains(expected_error),
                    "Did not find expected error '{expected_error}' in actual error '{err_str}'"
                );
            }
        }
        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{input}'"
        );
    }

    fn do_test_parse_tsm_tag_value_good(
        input: &str,
        expected_remaining_input: &str,
        expected_tag_value: (bool, String),
    ) {
        let mut iter = input.bytes();

        let result = parse_tsm_tag_value("Unknown", &mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_value) => {
                assert_eq!(
                    tag_value, expected_tag_value,
                    "while parsing input '{input}'"
                );
            }
            Err(err) => {
                panic!(
                    "Got error '{err}', expected parsed tag_value: '{expected_tag_value:?}' while parsing input '{input}"
                );
            }
        }

        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{input}'"
        );
    }

    fn do_test_parse_tsm_tag_value_error(
        input: &str,
        expected_remaining_input: &str,
        expected_error: &str,
    ) {
        let mut iter = input.bytes();

        let result = parse_tsm_tag_value("Unknown", &mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_value) => {
                panic!(
                    "Got parsed tag_value {tag_value:?}, expected failure {expected_error} while parsing input '{input}'"
                );
            }
            Err(err) => {
                let err_str = err.to_string();
                assert!(
                    err_str.contains(expected_error),
                    "Did not find expected error '{expected_error}' in actual error '{err_str}'"
                );
            }
        }

        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{input}'"
        );
    }
}
