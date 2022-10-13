use snafu::{ResultExt, Snafu};

/// Parse Delete Predicates
/// Parse Error
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(r#"Unable to parse delete string '{}'"#, value))]
    Invalid {
        source: serde_json::Error,
        value: String,
    },

    #[snafu(display(
        r#"Invalid key which is either 'start', 'stop', or 'predicate': '{}'"#,
        value
    ))]
    KeywordInvalid { value: String },

    #[snafu(display(r#"Invalid timestamp or predicate value: '{}'"#, value))]
    ValueInvalid { value: String },

    #[snafu(display(r#"Invalid JSON format of delete string '{}'"#, value))]
    ObjectInvalid { value: String },

    #[snafu(display(r#"Invalid table name in delete '{}'"#, value))]
    TableInvalid { value: String },

    #[snafu(display(r#"Delete must include a start time and a stop time'{}'"#, value))]
    StartStopInvalid { value: String },
}

/// Result type for Parser Cient
pub(crate) type Result<T, E = Error> = std::result::Result<T, E>;

const FLUX_TABLE: &str = "_measurement";

/// Data of a parsed delete
///
/// Note that this struct and its functions are used to parse FLUX DELETE,
/// <https://docs.influxdata.com/influxdb/v2.0/write-data/delete-data/>, which happens before
/// the parsing of timestamps and sql predicate. The examples below will show FLUX DELETE's syntax which is
/// different from SQL syntax so we need this extra parsing step before invoking sqlparser to parse the
/// sql-format predicates and timestamps
///
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub(crate) struct HttpDeleteRequest {
    /// Empty string, "", if no table specified
    pub(crate) table_name: String,
    pub(crate) start_time: String,
    pub(crate) stop_time: String,
    pub(crate) predicate: String,
}

/// Return parsed data of an influx delete:
/// A few input examples and their parsed results:
///   {"predicate":"_measurement=mytable AND host=\"Orient.local\"","start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}
///    => table_name="mytable", start_time="1970-01-01T00:00:00Z", end_time="2070-01-02T00:00:00Z", predicate="host=\"Orient.local\"""
///   {"predicate":"host=Orient.local and val != 50","start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}
///    => start_time="1970-01-01T00:00:00Z", end_time="2070-01-02T00:00:00Z", predicate="host=Orient.local and val != 50"
///
pub(crate) fn parse_http_delete_request(input: &str) -> Result<HttpDeleteRequest> {
    let parsed_obj: serde_json::Value =
        serde_json::from_str(input).context(InvalidSnafu { value: input })?;
    let mut parsed_delete = HttpDeleteRequest::default();

    if let serde_json::Value::Object(items) = parsed_obj {
        for item in items {
            // The value must be type String
            if let Some(val) = item.1.as_str() {
                match item.0.to_lowercase().as_str() {
                    "start" => parsed_delete.start_time = val.to_string(),
                    "stop" => parsed_delete.stop_time = val.to_string(),
                    "predicate" => parsed_delete.predicate = val.to_string(),
                    _ => {
                        return Err(Error::KeywordInvalid {
                            value: input.to_string(),
                        })
                    }
                }
            } else {
                return Err(Error::ValueInvalid {
                    value: input.to_string(),
                });
            }
        }
    } else {
        return Err(Error::ObjectInvalid {
            value: input.to_string(),
        });
    }

    // Start or stop is empty
    if parsed_delete.start_time.is_empty() || parsed_delete.stop_time.is_empty() {
        return Err(Error::StartStopInvalid {
            value: input.to_string(),
        });
    }

    // Extract table from the predicate if any
    if parsed_delete.predicate.contains(FLUX_TABLE) {
        // since predicate is a conjunctive expression, split them by "and"
        let predicate = parsed_delete
            .predicate
            .replace(" AND ", " and ")
            .replace(" ANd ", " and ")
            .replace(" And ", " and ")
            .replace(" AnD ", " and ");

        let split: Vec<&str> = predicate.split("and").collect();

        let mut predicate_no_table = "".to_string();
        for s in split {
            if s.contains(FLUX_TABLE) {
                // This should be in form "_measurement = <your_table_name>"
                // only <keep your_table_name> by replacing the rest with ""
                let table_name = s
                    .replace(FLUX_TABLE, "")
                    .replace('=', "")
                    .trim()
                    .to_string();
                // Do not support white spaces in table name
                if table_name.contains(' ') {
                    return Err(Error::TableInvalid {
                        value: input.to_string(),
                    });
                }
                parsed_delete.table_name = table_name;
            } else {
                // This is a normal column comparison, put it back to send to sqlparser later
                if !predicate_no_table.is_empty() {
                    predicate_no_table.push_str(" and ")
                }
                predicate_no_table.push_str(s.trim());
            }
        }
        parsed_delete.predicate = predicate_no_table;
    }

    Ok(parsed_delete)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_http_delete_full() {
        let delete_str = r#"{"predicate":"_measurement=mytable AND host=\"Orient.local\"","start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}"#;

        let expected = HttpDeleteRequest {
            table_name: "mytable".to_string(),
            predicate: "host=\"Orient.local\"".to_string(),
            start_time: "1970-01-01T00:00:00Z".to_string(),
            stop_time: "2070-01-02T00:00:00Z".to_string(),
        };

        let result = parse_http_delete_request(delete_str).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_http_delete_no_table() {
        let delete_str = r#"{"start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z", "predicate":"host=\"Orient.local\""}"#;

        let expected = HttpDeleteRequest {
            table_name: "".to_string(),
            predicate: "host=\"Orient.local\"".to_string(),
            start_time: "1970-01-01T00:00:00Z".to_string(),
            stop_time: "2070-01-02T00:00:00Z".to_string(),
        };

        let result = parse_http_delete_request(delete_str).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_http_delete_empty_predicate() {
        let delete_str =
            r#"{"start":"1970-01-01T00:00:00Z","predicate":"","stop":"2070-01-02T00:00:00Z"}"#;

        let expected = HttpDeleteRequest {
            table_name: "".to_string(),
            predicate: "".to_string(),
            start_time: "1970-01-01T00:00:00Z".to_string(),
            stop_time: "2070-01-02T00:00:00Z".to_string(),
        };

        let result = parse_http_delete_request(delete_str).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_http_delete_no_predicate() {
        let delete_str = r#"{"start":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}"#;

        let expected = HttpDeleteRequest {
            table_name: "".to_string(),
            predicate: "".to_string(),
            start_time: "1970-01-01T00:00:00Z".to_string(),
            stop_time: "2070-01-02T00:00:00Z".to_string(),
        };

        let result = parse_http_delete_request(delete_str).unwrap();
        assert_eq!(expected, result);
    }

    #[test]
    fn test_parse_http_delete_negative() {
        // invalid key
        let delete_str = r#"{"invalid":"1970-01-01T00:00:00Z","stop":"2070-01-02T00:00:00Z"}"#;
        let result = parse_http_delete_request(delete_str);
        let err = result.unwrap_err();
        assert!(err
            .to_string()
            .contains("Invalid key which is either 'start', 'stop', or 'predicate'"));

        // invalid timestamp value
        let delete_str = r#"{"start":123,"stop":"2070-01-02T00:00:00Z"}"#;
        let result = parse_http_delete_request(delete_str);
        let err = result.unwrap_err();
        assert!(err
            .to_string()
            .contains("Invalid timestamp or predicate value"));

        // invalid JSON
        let delete_str = r#"{"start":"1970-01-01T00:00:00Z",;"stop":"2070-01-02T00:00:00Z"}"#;
        let result = parse_http_delete_request(delete_str);
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Unable to parse delete string"));
    }
}
