use crate::TSMError;
use snafu::Snafu;

#[derive(Clone, Debug)]
pub struct ParsedTSMKey {
    pub measurement: String,
    pub tagset: Vec<(String, String)>,
    pub field_key: String,
}

#[derive(Debug, Snafu, PartialEq)]
pub enum Error {
    #[snafu(display(r#"Error parsing tsm tag key: {}"#, description))]
    ParsingTSMKey { description: String },

    #[snafu(display(r#"Error parsing tsm field key: {}"#, description))]
    ParsingTSMFieldKey { description: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// parses the the measurement, field key and tag
/// set from a tsm index key
///
/// It does not provide access to the org and bucket ids on the key, these can
/// be accessed via org_id() and bucket_id() respectively.
///
/// Loosely based on [points.go](https://github.com/influxdata/influxdb/blob/751d70a213e5fdae837eda13d7ecb37763e69abb/models/points.go#L462)
///
/// The format looks roughly like:
///
/// <org_id bucket_id>,\x00=<measurement>,<tag_keys_str>,\xff=<field_key_str>#!~#<field_key_str>
///
/// For example:
/// <org_id bucket_id>,\x00=http_api_request_duration_seconds,status=2XX,\xff=sum#!~#sum
///
///    measurement = "http_api_request"
///    tags = [("status", "2XX")]
///    field = "sum"
pub fn parse_tsm_key(key: &[u8]) -> Result<ParsedTSMKey, TSMError> {
    // skip over org id, bucket id, comma
    // The next n-1 bytes are the measurement name, where the nᵗʰ byte is a `,`.
    let mut rem_key = key.into_iter().copied().skip(8 + 8 + 1);

    let mut tagset = Vec::with_capacity(10);
    let mut measurement = None;
    let mut field_key = None;

    loop {
        let key = parse_tsm_tag_key(&mut rem_key).map_err(|e| TSMError {
            description: e.to_string(),
        })?;
        let (has_more_tags, tag_value) =
            parse_tsm_tag_value(&mut rem_key).map_err(|e| TSMError {
                description: e.to_string(),
            })?;

        match key {
            KeyType::Tag(tag_key) => {
                tagset.push((tag_key, tag_value));
            }

            KeyType::Measurement => match measurement {
                Some(measurement) => {
                    return Err(TSMError {
                        description: format!(
                            "found second measurement '{}' after first '{}'",
                            tag_value, measurement
                        ),
                    });
                }
                None => measurement = Some(tag_value),
            },

            KeyType::Field => match field_key {
                Some(field_key) => {
                    return Err(TSMError {
                        description: format!(
                            "found second field key '{}' after first '{}'",
                            tag_value, field_key
                        ),
                    });
                }
                None => {
                    field_key = Some(parse_tsm_field_key(&tag_value).map_err(|e| TSMError {
                        description: e.to_string(),
                    })?);
                }
            },
        };

        if !has_more_tags {
            break;
        }
    }

    Ok(ParsedTSMKey {
        measurement: measurement.ok_or(TSMError {
            description: "No measurement name found in index".into(),
        })?,
        tagset,
        field_key: field_key.ok_or(TSMError {
            description: "No field key found in index".into(),
        })?,
    })
}

/// Parses the field value stored in a TSM key into a field name.
/// fields are stored on the series keys in TSM indexes as follows:
///
/// <field_key><4-byte delimiter><field_key>
///
/// Example: sum#!~#sum means 'sum' field key
fn parse_tsm_field_key(value: &str) -> Result<String> {
    const DELIM: &str = "#!~#";

    if value.len() < 6 {
        return ParsingTSMFieldKey {
            description: "field key too short",
        }
        .fail();
    }

    let field_trim_length = (value.len() - 4) / 2;
    let (field, _) = value.split_at(field_trim_length);

    // Expect exactly <field><delim><field>
    if value.find(field) != Some(0)
        || value[field.len()..].find(DELIM) != Some(0)
        || value[field.len() + DELIM.len()..].find(field) != Some(0)
    {
        return ParsingTSMFieldKey {
            description: format!(
                "Invalid field key format '{}', expected '{}{}{}'",
                value, field, DELIM, field
            ),
        }
        .fail();
    }

    Ok(field.into())
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

/// Parses bytes from the `rem_key` input stream until the end of the
/// next key value (=). Consumes the '='
fn parse_tsm_tag_key<T>(rem_key: &mut T) -> Result<KeyType>
where
    T: Iterator<Item = u8>,
{
    enum State {
        Data,
        Measurement,
        Field,
        Escape,
    };

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
                    return ParsingTSMKey {
                        description: "unescaped comma",
                    }
                    .fail();
                }
                b' ' => {
                    return ParsingTSMKey {
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
                    return ParsingTSMKey {
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
                    return ParsingTSMKey {
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

    ParsingTSMKey {
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
fn parse_tsm_tag_value<T>(rem_key: &mut T) -> Result<(bool, String)>
where
    T: Iterator<Item = u8>,
{
    #[derive(Debug)]
    enum State {
        Start,
        Data,
        Escaped,
    };

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
                        return ParsingTSMKey {
                            description: "invalid unescaped '='",
                        }
                        .fail()
                    }
                    // An unescaped space is an invalid tag value.
                    b' ' => {
                        return ParsingTSMKey {
                            description: "invalid unescaped ' '",
                        }
                        .fail()
                    }
                    b',' => {
                        return ParsingTSMKey {
                            description: "missing tag value",
                        }
                        .fail()
                    }
                    b'\\' => state = State::Escaped,
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
                        return ParsingTSMKey {
                            description: "invalid unescaped '='",
                        }
                        .fail()
                    }
                    // An unescaped space is an invalid tag value.
                    b' ' => {
                        return ParsingTSMKey {
                            description: "invalid unescaped ' '",
                        }
                        .fail()
                    }
                    // cpu,tag=foo,
                    b',' => return Ok((true, tag_value)),
                    // start of escape value
                    b'\\' => state = State::Escaped,
                    _ => {
                        tag_value.push(byte as char);
                    }
                }
            }
            State::Escaped => {
                tag_value.push(byte as char);
                state = State::Data;
            }
        }
    }

    // Tag value cannot be empty.
    match state {
        State::Start => ParsingTSMKey {
            description: "missing tag value",
        }
        .fail(),
        State::Escaped => ParsingTSMKey {
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
    fn test_parse_tsm_field_key() {
        // test the operation of parse_tsm_field_key
        assert_eq!(parse_tsm_field_key("sum#!~#sum"), Ok("sum".into()));
        assert_eq!(
            parse_tsm_field_key("#!~#"),
            Err(Error::ParsingTSMFieldKey {
                description: "field key too short".into()
            })
        );
        assert_eq!(
            parse_tsm_field_key("foo#!~#fpp"),
            Err(Error::ParsingTSMFieldKey {
                description: "Invalid field key format \'foo#!~#fpp\', expected \'foo#!~#foo\'"
                    .into()
            })
        );
        assert_eq!(
            parse_tsm_field_key("foo#!~#naaa"),
            Err(Error::ParsingTSMFieldKey {
                description: "Invalid field key format \'foo#!~#naaa\', expected \'foo#!~#foo\'"
                    .into()
            })
        );
        assert_eq!(
            parse_tsm_field_key("foo#!~#"),
            Err(Error::ParsingTSMFieldKey {
                description: "Invalid field key format \'foo#!~#\', expected \'f#!~#f\'".into()
            })
        );
        assert_eq!(
            parse_tsm_field_key("foo####foo"),
            Err(Error::ParsingTSMFieldKey {
                description: "Invalid field key format \'foo####foo\', expected \'foo#!~#foo\'"
                    .into()
            })
        );
    }

    #[test]
    fn test_parse_tsm_tag_key() {
        do_test_parse_tsm_tag_key(
            "",
            "",
            Err(Error::ParsingTSMKey {
                description: "unexpected end of data".into(),
            }),
        );
        do_test_parse_tsm_tag_key("foo=bar", "bar", Ok(KeyType::Tag("foo".into())));
        do_test_parse_tsm_tag_key("foo=", "", Ok(KeyType::Tag("foo".into())));
        do_test_parse_tsm_tag_key(
            "foo",
            "",
            Err(Error::ParsingTSMKey {
                description: "unexpected end of data".into(),
            }),
        );
        do_test_parse_tsm_tag_key(
            "foo,=bar",
            "=bar",
            Err(Error::ParsingTSMKey {
                description: "unescaped comma".into(),
            }),
        );
        do_test_parse_tsm_tag_key(
            "foo =bar",
            "=bar",
            Err(Error::ParsingTSMKey {
                description: "unescaped space".into(),
            }),
        );

        do_test_parse_tsm_tag_key(r"\ foo=bar", "bar", Ok(KeyType::Tag(" foo".into())));
        do_test_parse_tsm_tag_key(r"\=foo=bar", "bar", Ok(KeyType::Tag("=foo".into())));
        do_test_parse_tsm_tag_key(r"\,foo=bar", "bar", Ok(KeyType::Tag(",foo".into())));
        do_test_parse_tsm_tag_key(r"\foo=bar", "bar", Ok(KeyType::Tag("foo".into())));
        do_test_parse_tsm_tag_key(r"\\foo=bar", "bar", Ok(KeyType::Tag(r"\foo".into())));

        do_test_parse_tsm_tag_key(r"f\ oo=bar", "bar", Ok(KeyType::Tag("f oo".into())));
        do_test_parse_tsm_tag_key(r"f\=oo=bar", "bar", Ok(KeyType::Tag("f=oo".into())));
        do_test_parse_tsm_tag_key(r"f\,oo=bar", "bar", Ok(KeyType::Tag("f,oo".into())));
        do_test_parse_tsm_tag_key(r"f\oo=bar", "bar", Ok(KeyType::Tag("foo".into())));
    }

    #[test]
    fn test_parse_tsm_tag_value() {
        do_test_parse_tsm_tag_value(
            "",
            "",
            Err(Error::ParsingTSMKey {
                description: "missing tag value".into(),
            }),
        );
        do_test_parse_tsm_tag_value(
            "val1,tag2=val2 value=1",
            "tag2=val2 value=1",
            Ok((true, "val1".into())),
        );
        do_test_parse_tsm_tag_value("val1", "", Ok((false, "val1".into())));
        do_test_parse_tsm_tag_value(r"\ val1", "", Ok((false, " val1".into())));
        do_test_parse_tsm_tag_value(r"val\ 1", "", Ok((false, "val 1".into())));
        do_test_parse_tsm_tag_value(r"val1\ ", "", Ok((false, "val1 ".into())));
        do_test_parse_tsm_tag_value(
            r"val1\",
            "",
            Err(Error::ParsingTSMKey {
                description: "tag value ends in escape".into(),
            }),
        );
        do_test_parse_tsm_tag_value(
            r"=b",
            "b",
            Err(Error::ParsingTSMKey {
                description: "invalid unescaped '='".into(),
            }),
        );
        do_test_parse_tsm_tag_value(
            r"f=b",
            "b",
            Err(Error::ParsingTSMKey {
                description: "invalid unescaped '='".into(),
            }),
        );
        do_test_parse_tsm_tag_value(
            r" v",
            "v",
            Err(Error::ParsingTSMKey {
                description: "invalid unescaped ' '".into(),
            }),
        );
        do_test_parse_tsm_tag_value(
            r"v ",
            "",
            Err(Error::ParsingTSMKey {
                description: "invalid unescaped ' '".into(),
            }),
        );
    }

    // create key in this form:
    //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>
    fn make_tsm_key_prefix(measurement: &str, tag_keys_str: &str) -> Vec<u8> {
        let mut key = Vec::new();
        //Vec<u8> = hex::decode("05C19117091A100005C19117091A10012C003D").expect("decoded");

        const ORG: &str = "12345678";
        const BUCKET: &str = "87654321";

        // 8 bytes of ORG
        for b in ORG.as_bytes() {
            key.push(*b)
        }
        // 8 bytes of BUCKET
        for b in BUCKET.as_bytes() {
            key.push(*b)
        }
        key.push(b',');
        // 2 bytes: special measurement tag key \x00=
        key.push(b'\x00');
        key.push(b'=');

        // measurement
        for b in measurement.as_bytes() {
            key.push(*b);
        }
        key.push(b',');
        for b in tag_keys_str.as_bytes() {
            key.push(*b)
        }
        key
    }

    // add this to the key: ,\xff=<field_key_str>#!~#<field_key_str>
    fn add_field_key(mut key: Vec<u8>, field_key_str: &str) -> Vec<u8> {
        key.push(b',');
        key.push(b'\xff');
        key.push(b'=');
        for b in field_key_str.as_bytes() {
            key.push(*b)
        }
        for b in b"#!~#" {
            key.push(*b)
        }
        for b in field_key_str.as_bytes() {
            key.push(*b)
        }
        key
    }

    #[test]
    fn parse_tsm_key_good() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>,\xff=<field_key_str>#!~#<field_key_str>
        let mut key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");
        key = add_field_key(key, "f");

        let parsed_key = super::parse_tsm_key(&key).unwrap();
        assert_eq!(parsed_key.measurement, String::from("m"));
        let exp_tagset = vec![
            (String::from("tag1"), String::from("val1")),
            (String::from("tag2"), String::from("val2")),
        ];
        assert_eq!(parsed_key.tagset, exp_tagset);
        assert_eq!(parsed_key.field_key, String::from("f"));
    }

    #[test]
    fn parse_tsm_key_no_field() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>
        let key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");

        assert_eq!(
            super::parse_tsm_key(&key)
                .err()
                .expect("expect parsing error"),
            TSMError {
                description: "No field key found in index".into()
            }
        );
    }

    #[test]
    fn parse_tsm_key_two_fields() {
        //<org_id bucket_id>,\x00=<measurement>,<tag_keys_str>\xff=<field-key_str>#!~#<field_key_str>\xff=<field-key_str>#!~#<field_key_str>
        let mut key = make_tsm_key_prefix("m", "tag1=val1,tag2=val2");
        key = add_field_key(key, "f");
        key = add_field_key(key, "f2");

        assert_eq!(
            super::parse_tsm_key(&key)
                .err()
                .expect("expect parsing error"),
            TSMError {
                description: "found second field key \'f2#!~#f2\' after first \'f\'".into()
            }
        );
    }

    #[test]
    fn parse_tsm_key() {
        //<org_id bucket_id>,\x00=http_api_request_duration_seconds,handler=platform,method=POST,path=/api/v2/setup,status=2XX,user_agent=Firefox,\xff=sum#!~#sum
        let buf = vec![
            "05C19117091A100005C19117091A10012C003D68747470",
            "5F6170695F726571756573745F6475726174696F6E5F73",
            "65636F6E64732C68616E646C65723D706C6174666F726D",
            "2C6D6574686F643D504F53542C706174683D2F6170692F",
            "76322F73657475702C7374617475733D3258582C757365",
            "725F6167656E743D46697265666F782CFF3D73756D2321",
            "7E2373756D",
        ]
        .join("");
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
        //<org_id bucket_id>,\x00=query_log,env=prod01-eu-central-1,error=memory\ allocation\ limit\ reached:\ limit\ 740000000\ bytes\,\ allocated:\ 739849088\,\ wanted:\ 6946816;\ memory\ allocation\ limit\ reached:\ limit\ 740000000\ bytes\,\ allocated:\ 739849088\,\ wanted:\ 6946816,errorCode=invalid,errorType=user,host=queryd-algow-rw-76d68d5968-fzgwr,hostname=queryd-algow-rw-76d68d5968-fzgwr,nodename=ip-10-153-10-221.eu-central-1.compute.internal,orgID=0b6e852e272ffdd9,ot_trace_sampled=false,role=queryd-algow-rw,source=hackney,\xff=responseSize#!~#responseSize
        let buf = vec![
            "844910ECE80BE8BC3C0BD4C89186CA892C",
            "003D71756572795F6C6F672C656E763D70726F6430312D65752D63656E747261",
            "6C2D312C6572726F723D6D656D6F72795C20616C6C6F636174696F6E5C206C69",
            "6D69745C20726561636865643A5C206C696D69745C203734303030303030305C",
            "2062797465735C2C5C20616C6C6F63617465643A5C203733393834393038385C2",
            "C5C2077616E7465643A5C20363934363831363B5C206D656D6F72795C20616C6C",
            "6F636174696F6E5C206C696D69745C20726561636865643A5C206C696D69745C2",
            "03734303030303030305C2062797465735C2C5C20616C6C6F63617465643A5C20",
            "3733393834393038385C2C5C2077616E7465643A5C20363934363831362C65727",
            "26F72436F64653D696E76616C69642C6572726F72547970653D757365722C686F",
            "73743D7175657279642D616C676F772D72772D373664363864353936382D667A6",
            "777722C686F73746E616D653D7175657279642D616C676F772D72772D37366436",
            "3864353936382D667A6777722C6E6F64656E616D653D69702D31302D3135332D3",
            "1302D3232312E65752D63656E7472616C2D312E636F6D707574652E696E746572",
            "6E616C2C6F726749443D306236653835326532373266666464392C6F745F74726",
            "163655F73616D706C65643D66616C73652C726F6C653D7175657279642D616C67",
            "6F772D72772C736F757263653D6861636B6E65792CFF3D726573706F6E7365536",
            "97A6523217E23726573706F6E736553697A65",
        ]
        .join("");
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

    fn do_test_parse_tsm_tag_key(
        input: &str,
        expected_remaining_input: &str,
        expected_result: Result<KeyType>,
    ) {
        let input = String::from(input);
        let mut iter = input.bytes();

        let result = parse_tsm_tag_key(&mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_key) => match expected_result {
                Ok(expected_tag_key) => {
                    assert_eq!(tag_key, expected_tag_key, "while parsing input '{}'", input)
                }
                Err(expected_err) => assert!(
                    false,
                    "Got parsed key {:?}, expected failure {} while parsing input '{}'",
                    tag_key, expected_err, input
                ),
            },
            Err(err) => match expected_result {
                Ok(expected_tag_key) => assert!(
                    false,
                    "Got error '{}', expected parsed tag key: '{:?}' while parsing '{}'",
                    err, expected_tag_key, input
                ),
                Err(expected_err) => {
                    assert_eq!(err, expected_err, "while parsing input '{}'", input)
                }
            },
        }
        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{}'",
            input
        );
    }

    fn do_test_parse_tsm_tag_value(
        input: &str,
        expected_remaining_input: &str,
        expected_result: Result<(bool, String)>,
    ) {
        let input = String::from(input);
        let mut iter = input.bytes();

        let result = parse_tsm_tag_value(&mut iter);
        let remaining_input =
            String::from_utf8(iter.collect()).expect("can not find remaining input");

        match result {
            Ok(tag_value) => match expected_result {
                Ok(expected_tag_value) => assert_eq!(
                    tag_value, expected_tag_value,
                    "while parsing input '{}'",
                    input
                ),
                Err(expected_err) => assert!(
                    false,
                    "Got parsed tag_value {:?}, expected failure {} while parsing input '{}'",
                    tag_value, expected_err, input
                ),
            },
            Err(err) => match expected_result {
                Ok(expected_tag_value) => assert!(
                    false,
                    "Got error '{}', expected parsed tag_value: '{:?}' while parsing input '{}",
                    err, expected_tag_value, input
                ),
                Err(expected_err) => {
                    assert_eq!(err, expected_err, "while parsing input '{}'", input)
                }
            },
        }
        assert_eq!(
            remaining_input, expected_remaining_input,
            "remaining input was not correct while parsing input '{}'",
            input
        );
    }
}
