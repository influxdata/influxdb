//! Ticket handling for the native IOx Flight API

use arrow_flight::Ticket;
use bytes::Bytes;
use flightsql::FlightSQLCommand;
use generated_types::google::protobuf::Any;
use generated_types::influxdata::iox::querier::v1 as proto;
use generated_types::influxdata::iox::querier::v1::read_info::QueryType;
use observability_deps::tracing::trace;
use prost::Message;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::fmt::{Debug, Display, Formatter};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid ticket"))]
    Invalid,
    #[snafu(display("Invalid ticket content: {}", msg))]
    InvalidContent { msg: String },
    #[snafu(display("Invalid Flight SQL ticket: {}", source))]
    FlightSQL { source: flightsql::Error },
    #[snafu(display("Invalid Protobuf: {}", source))]
    Decode { source: prost::DecodeError },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// AnyError is an internal error that contains the result of attempting
/// to decode a protobuf "Any" message. This is separate from Error so
/// that an error resulting from attempting to decode the value can be
/// embedded as a source.
#[derive(Debug, Snafu)]
enum AnyError {
    #[snafu(display("Invalid Protobuf: {}", source))]
    DecodeAny { source: prost::DecodeError },
    #[snafu(display("Unknown type_url: {}", type_url))]
    UnknownTypeURL { type_url: String },
    #[snafu(display("Invalid value: {}", source))]
    InvalidValue { source: Error },
}

/// Request structure of the "opaque" tickets used for IOx Arrow
/// Flight DoGet endpoint.
///
/// This structure encapsulates the deserialization and serializion
/// logic for these requests.  The protocol is described in more
/// detail on [`FlightService`](crate::FlightService).
///
/// # Ticket Format
///
/// Tickets are encoded in one of two formats:
///
/// 1. Protobuf: as a [ReadInfo](proto::ReadInfo) wrapped as a "Any"
/// message and encoded using binary encoding
///
/// 2. JSON: formatted as below.
///
/// ## Known clients use the JSON encoding
///
/// - <https://github.com/influxdata/influxdb-iox-client-go/commit/2e7a3b0bd47caab7f1a31a1bbe0ff54aa9486b7b>
/// - <https://github.com/influxdata/influxdb-iox-client-go/commit/52f1a1b8d5bb8cc8dc2fe825f4da630ad0b9167c>
///
/// ## Example JSON Ticket format
///
/// This runs the SQL "SELECT 1" in database `my_db`
///
/// ```json
/// {
///   "database": "my_db",
///   "sql_query": "SELECT 1;"
/// }
/// ```
///
/// This is the same as the example above, but has an explicit query language
///
/// ```json
/// {
///   "database": "my_db",
///   "sql_query": "SELECT 1;"
///   "query_type": "sql"
/// }
/// ```
///
/// This runs the 'SHOW DATABASES' InfluxQL command (the `sql_query` field name is misleading)
///
/// ```json
/// {
///   "database": "my_db",
///   "sql_query": "SHOW DATABASES;"
///   "query_type": "influxql"
/// }
/// ```
#[derive(Debug, PartialEq, Clone)]
pub struct IoxGetRequest {
    database: String,
    query: RunQuery,
    is_debug: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub enum RunQuery {
    /// Unparameterized SQL query
    Sql(String),
    /// InfluxQL
    InfluxQL(String),
    /// Execute a FlightSQL command. The payload is an encoded
    /// FlightSQL Command*. message that was received at the
    /// get_flight_info endpoint
    FlightSQL(FlightSQLCommand),
}

impl RunQuery {
    pub fn variant(&self) -> &'static str {
        match self {
            Self::Sql(_) => "sql",
            Self::InfluxQL(_) => "influxql",
            Self::FlightSQL(_) => "flightsql",
        }
    }
}

impl Display for RunQuery {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Sql(s) => Display::fmt(s, f),
            Self::InfluxQL(s) => Display::fmt(s, f),
            Self::FlightSQL(s) => Display::fmt(s, f),
        }
    }
}

impl IoxGetRequest {
    const READ_INFO_TYPE_URL: &str = "type.googleapis.com/influxdata.iox.querier.v1.ReadInfo";

    /// Create a new request to run the specified query
    pub fn new(database: impl Into<String>, query: RunQuery, is_debug: bool) -> Self {
        Self {
            database: database.into(),
            query,
            is_debug,
        }
    }

    /// try to decode a ReadInfo structure from a Token
    pub fn try_decode(ticket: Ticket) -> Result<Self> {
        // decode ticket
        IoxGetRequest::decode_protobuf_any(ticket.ticket.clone())
            .or_else(|e| {
                match e {
                    // If the ticket decoded as an Any with a type_url that was recognised
                    // don't attempt to fall back to ReadInfo it will almost certainly
                    // succeed, but with invalid parameters.
                    AnyError::InvalidValue { source } => Err(source),
                    e => {
                        trace!(%e, "Error decoding ticket as Any, trying as ReadInfo");
                        IoxGetRequest::decode_protobuf(ticket.ticket.clone())
                    }
                }
            })
            .or_else(|e| {
                trace!(%e, ticket=%String::from_utf8_lossy(&ticket.ticket),
                       "Error decoding ticket as ProtoBuf, trying as JSON");
                IoxGetRequest::decode_json(ticket.ticket.clone())
            })
            .map_err(|e| {
                trace!(%e, "Error decoding ticket as JSON");
                Error::Invalid
            })
    }

    /// Encode the request as a protobuf Ticket
    pub fn try_encode(self) -> Result<Ticket> {
        let Self {
            database,
            query,
            is_debug,
        } = self;

        let read_info = match query {
            RunQuery::Sql(sql_query) => proto::ReadInfo {
                database,
                sql_query,
                query_type: QueryType::Sql.into(),
                flightsql_command: vec![],
                is_debug,
            },
            RunQuery::InfluxQL(influxql) => proto::ReadInfo {
                database,
                // field name is misleading
                sql_query: influxql,
                query_type: QueryType::InfluxQl.into(),
                flightsql_command: vec![],
                is_debug,
            },
            RunQuery::FlightSQL(flightsql_command) => proto::ReadInfo {
                database,
                sql_query: "".into(),
                query_type: QueryType::FlightSqlMessage.into(),
                flightsql_command: flightsql_command
                    .try_encode()
                    .context(FlightSQLSnafu)?
                    .into(),
                is_debug,
            },
        };

        let any = Any {
            type_url: Self::READ_INFO_TYPE_URL.to_string(),
            value: read_info.encode_to_vec().into(),
        };
        let ticket = any.encode_to_vec();

        Ok(Ticket {
            ticket: ticket.into(),
        })
    }

    /// See comments on [`IoxGetRequest`] for details of this format
    fn decode_json(ticket: Bytes) -> Result<Self, String> {
        let json_str = String::from_utf8(ticket.to_vec()).map_err(|_| "Not UTF8".to_string())?;

        /// This represents ths JSON fields
        #[derive(Deserialize, Debug)]
        struct ReadInfoJson {
            #[serde(alias = "namespace_name", alias = "bucket", alias = "bucket-name")]
            database: String,
            sql_query: String,
            // If query type is not supplied, defaults to SQL
            query_type: Option<String>,
            #[serde(default = "Default::default")]
            is_debug: bool,
        }

        let ReadInfoJson {
            database,
            sql_query,
            query_type,
            is_debug,
        } = serde_json::from_str(&json_str).map_err(|e| format!("JSON parse error: {e}"))?;

        let query = if let Some(query_type) = query_type {
            match query_type.as_str() {
                "sql" => RunQuery::Sql(sql_query),
                "influxql" => RunQuery::InfluxQL(sql_query),
                _ => {
                    return Err(format!(
                        "unknown query type. Expected 'sql' or 'influxql', got {query_type}'"
                    ))
                }
            }
        } else {
            // default to SQL
            RunQuery::Sql(sql_query)
        };

        Ok(Self {
            database,
            query,
            is_debug,
        })
    }

    /// Decode a ReadInfo ticket wrapped in a protobuf Any message.
    fn decode_protobuf_any(ticket: Bytes) -> Result<Self, AnyError> {
        let any = Any::decode(ticket).context(DecodeAnySnafu)?;
        if any.type_url == Self::READ_INFO_TYPE_URL {
            Self::decode_protobuf(any.value).context(InvalidValueSnafu)
        } else {
            UnknownTypeURLSnafu {
                type_url: any.type_url,
            }
            .fail()
        }
    }

    /// See comments on [`IoxGetRequest`] for details of this format
    fn decode_protobuf(ticket: Bytes) -> Result<Self, Error> {
        let read_info = proto::ReadInfo::decode(ticket).context(DecodeSnafu)?;

        let query_type = read_info.query_type();
        let proto::ReadInfo {
            database,
            sql_query,
            query_type: _,
            flightsql_command,
            is_debug,
        } = read_info;

        Ok(Self {
            database,
            query: match query_type {
                QueryType::Unspecified | QueryType::Sql => {
                    if !flightsql_command.is_empty() {
                        return InvalidContentSnafu {
                            msg: "QueryType::Sql contained non empty flightsql_command",
                        }
                        .fail();
                    }
                    RunQuery::Sql(sql_query)
                }
                QueryType::InfluxQl => {
                    if !flightsql_command.is_empty() {
                        return InvalidContentSnafu {
                            msg: "QueryType::InfluxQl contained non empty flightsql_command",
                        }
                        .fail();
                    }
                    RunQuery::InfluxQL(sql_query)
                }
                QueryType::FlightSqlMessage => {
                    if !sql_query.is_empty() {
                        return InvalidContentSnafu {
                            msg: "QueryType::FlightSqlMessage contained non empty sql_query",
                        }
                        .fail();
                    }
                    let cmd = FlightSQLCommand::try_decode(flightsql_command.into())
                        .context(FlightSQLSnafu)?;
                    RunQuery::FlightSQL(cmd)
                }
            },
            is_debug,
        })
    }

    pub fn database(&self) -> &str {
        self.database.as_ref()
    }

    pub fn query(&self) -> &RunQuery {
        &self.query
    }

    pub fn is_debug(&self) -> bool {
        self.is_debug
    }
}

#[cfg(test)]
mod tests {
    use arrow_flight::sql::CommandStatementQuery;
    use assert_matches::assert_matches;
    use generated_types::influxdata::iox::querier::v1::read_info::QueryType;

    use super::*;

    #[test]
    fn json_ticket_decoding_compatibility() {
        // The Go clients still use JSON tickets. See:
        //
        // - <https://github.com/influxdata/influxdb-iox-client-go/commit/2e7a3b0bd47caab7f1a31a1bbe0ff54aa9486b7b>
        // - <https://github.com/influxdata/influxdb-iox-client-go/commit/52f1a1b8d5bb8cc8dc2fe825f4da630ad0b9167c
        //
        // Do not change this test without having first changed what the Go clients are sending!
        let ticket = make_json_ticket(r#"{"database": "my_db", "sql_query": "SELECT 1;"}"#);
        let ri = IoxGetRequest::try_decode(ticket).unwrap();

        assert_eq!(ri.database, "my_db");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1;"));
    }

    #[test]
    fn json_ticket_decoding() {
        struct TestCase {
            json: &'static str,
            expected: IoxGetRequest,
        }

        impl TestCase {
            fn new_sql(json: &'static str, expected_database: &str, query: &str) -> Self {
                Self {
                    json,
                    expected: IoxGetRequest {
                        database: String::from(expected_database),
                        query: RunQuery::Sql(String::from(query)),
                        is_debug: false,
                    },
                }
            }

            fn new_influxql(json: &'static str, expected_database: &str, query: &str) -> Self {
                Self {
                    json,
                    expected: IoxGetRequest {
                        database: String::from(expected_database),
                        query: RunQuery::InfluxQL(String::from(query)),
                        is_debug: false,
                    },
                }
            }
        }

        let cases = vec![
            // implict `query_type`
            TestCase::new_sql(
                r#"{"database": "my_db", "sql_query": "SELECT 1;"}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_sql(
                r#"{"namespace_name": "my_db", "sql_query": "SELECT 1;"}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_sql(
                r#"{"bucket": "my_db", "sql_query": "SELECT 1;"}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_sql(
                r#"{"bucket-name": "my_db", "sql_query": "SELECT 1;"}"#,
                "my_db",
                "SELECT 1;",
            ),
            // explicit query type, sql
            TestCase::new_sql(
                r#"{"database": "my_db", "sql_query": "SELECT 1;", "query_type": "sql"}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_sql(
                r#"{"namespace_name": "my_db", "sql_query": "SELECT 1;", "query_type": "sql"}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_sql(
                r#"{"bucket": "my_db", "sql_query": "SELECT 1;", "query_type": "sql"}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_sql(
                r#"{"bucket-name": "my_db", "sql_query": "SELECT 1;", "query_type": "sql"}"#,
                "my_db",
                "SELECT 1;",
            ),
            // explicit query type null
            TestCase::new_sql(
                r#"{"database": "my_db", "sql_query": "SELECT 1;", "query_type": null}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_sql(
                r#"{"namespace_name": "my_db", "sql_query": "SELECT 1;", "query_type": null}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_sql(
                r#"{"bucket": "my_db", "sql_query": "SELECT 1;", "query_type": null}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_sql(
                r#"{"bucket-name": "my_db", "sql_query": "SELECT 1;", "query_type": null}"#,
                "my_db",
                "SELECT 1;",
            ),
            // explicit query type, influxql
            TestCase::new_influxql(
                r#"{"database": "my_db", "sql_query": "SELECT 1;", "query_type": "influxql"}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_influxql(
                r#"{"namespace_name": "my_db", "sql_query": "SELECT 1;", "query_type": "influxql"}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_influxql(
                r#"{"bucket": "my_db", "sql_query": "SELECT 1;", "query_type": "influxql"}"#,
                "my_db",
                "SELECT 1;",
            ),
            TestCase::new_influxql(
                r#"{"bucket-name": "my_db", "sql_query": "SELECT 1;", "query_type": "influxql"}"#,
                "my_db",
                "SELECT 1;",
            ),
            // explicit query type, influxql on metadata
            TestCase::new_influxql(
                r#"{"database": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": "influxql"}"#,
                "my_otherdb",
                "SHOW DATABASES;",
            ),
            TestCase::new_influxql(
                r#"{"namespace_name": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": "influxql"}"#,
                "my_otherdb",
                "SHOW DATABASES;",
            ),
            TestCase::new_influxql(
                r#"{"bucket": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": "influxql"}"#,
                "my_otherdb",
                "SHOW DATABASES;",
            ),
            TestCase::new_influxql(
                r#"{"bucket-name": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": "influxql"}"#,
                "my_otherdb",
                "SHOW DATABASES;",
            ),
            // explicit query type, sql on metadata
            TestCase::new_sql(
                r#"{"database": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": "sql"}"#,
                "my_otherdb",
                "SHOW DATABASES;",
            ),
            TestCase::new_sql(
                r#"{"namespace_name": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": "sql"}"#,
                "my_otherdb",
                "SHOW DATABASES;",
            ),
            TestCase::new_sql(
                r#"{"bucket": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": "sql"}"#,
                "my_otherdb",
                "SHOW DATABASES;",
            ),
            TestCase::new_sql(
                r#"{"bucket-name": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": "sql"}"#,
                "my_otherdb",
                "SHOW DATABASES;",
            ),
        ];

        for TestCase { json, expected } in cases {
            println!("Test:\nInput:\n{json}\nExpected:\n{expected:?}");
            let ticket = make_json_ticket(json);

            let ri = IoxGetRequest::try_decode(ticket).unwrap();
            assert_eq!(ri, expected);
        }
    }

    #[test]
    fn json_ticket_decoding_invalid_json() {
        // invalid json (database name rather than namespace name)
        let ticket = make_json_ticket(r#"{"database_name": "my_db", "sql_query": "SELECT 1;"}"#);
        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn json_ticket_decoding_invalid_query_type() {
        // invalid query_type
        let ticket = make_json_ticket(
            r#"{"namespace_name": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": "flight"}"#,
        );
        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn json_ticket_decoding_empty_query_type() {
        // invalid query_type ""
        let ticket = make_json_ticket(
            r#"{"namespace_name": "my_otherdb", "sql_query": "SHOW DATABASES;", "query_type": ""}"#,
        );
        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn proto_ticket_decoding_unspecified() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::Unspecified.into(),
            flightsql_command: vec![],
            is_debug: false,
        });

        // Reverts to default (unspecified) for invalid query_type enumeration, and thus SQL
        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.database, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn proto_ticket_decoding_sql() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::Sql.into(),
            flightsql_command: vec![],
            is_debug: false,
        });

        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.database, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn proto_ticket_decoding_influxql() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::InfluxQl.into(),
            flightsql_command: vec![],
            is_debug: false,
        });

        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.database, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::InfluxQL(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn proto_ticket_decoding_too_new() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".into(),
            query_type: 42, // not a known query type
            flightsql_command: vec![],
            is_debug: false,
        });

        // Reverts to default (unspecified) for invalid query_type enumeration, and thus SQL
        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.database, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn proto_ticket_decoding_sql_too_many_fields() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::Sql.into(),
            // can't have both sql_query and flightsql
            flightsql_command: vec![1, 2, 3],
            is_debug: false,
        });

        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn proto_ticket_decoding_influxql_too_many_fields() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::InfluxQl.into(),
            // can't have both sql_query and flightsql
            flightsql_command: vec![1, 2, 3],
            is_debug: false,
        });

        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn proto_ticket_decoding_flightsql_too_many_fields() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::FlightSqlMessage.into(),
            // can't have both sql_query and flightsql
            flightsql_command: vec![1, 2, 3],
            is_debug: false,
        });

        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn proto_ticket_decoding_error() {
        let ticket = Ticket {
            ticket: b"invalid ticket".to_vec().into(),
        };

        // Reverts to default (unspecified) for invalid query_type enumeration, and thus SQL
        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn any_ticket_decoding_unspecified() {
        let ticket = make_any_wrapped_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::Unspecified.into(),
            flightsql_command: vec![],
            is_debug: false,
        });

        // Reverts to default (unspecified) for invalid query_type enumeration, and thus SQL
        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.database, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn any_ticket_decoding_sql() {
        let ticket = make_any_wrapped_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::Sql.into(),
            flightsql_command: vec![],
            is_debug: false,
        });

        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.database, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn any_ticket_decoding_influxql() {
        let ticket = make_any_wrapped_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::InfluxQl.into(),
            flightsql_command: vec![],
            is_debug: false,
        });

        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.database, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::InfluxQL(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn any_ticket_decoding_too_new() {
        let ticket = make_any_wrapped_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".into(),
            query_type: 42, // not a known query type
            flightsql_command: vec![],
            is_debug: false,
        });

        // Reverts to default (unspecified) for invalid query_type enumeration, and thus SQL
        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.database, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn any_ticket_decoding_sql_too_many_fields() {
        let ticket = make_any_wrapped_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::Sql.into(),
            // can't have both sql_query and flightsql
            flightsql_command: vec![1, 2, 3],
            is_debug: false,
        });

        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn any_ticket_decoding_influxql_too_many_fields() {
        let ticket = make_any_wrapped_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::InfluxQl.into(),
            // can't have both sql_query and flightsql
            flightsql_command: vec![1, 2, 3],
            is_debug: false,
        });

        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn any_ticket_decoding_flightsql_too_many_fields() {
        let ticket = make_any_wrapped_proto_ticket(&proto::ReadInfo {
            database: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::FlightSqlMessage.into(),
            // can't have both sql_query and flightsql
            flightsql_command: vec![1, 2, 3],
            is_debug: false,
        });

        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn any_ticket_decoding_error() {
        let ticket = Ticket {
            ticket: b"invalid ticket".to_vec().into(),
        };

        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn round_trip_sql() {
        let request = IoxGetRequest {
            database: "foo_blarg".into(),
            query: RunQuery::Sql("select * from bar".into()),
            is_debug: false,
        };

        let ticket = request.clone().try_encode().expect("encoding failed");

        let roundtripped = IoxGetRequest::try_decode(ticket).expect("decode failed");

        assert_eq!(request, roundtripped)
    }

    #[test]
    fn round_trip_sql_is_debug() {
        let request = IoxGetRequest {
            database: "foo_blarg".into(),
            query: RunQuery::Sql("select * from bar".into()),
            is_debug: true,
        };

        let ticket = request.clone().try_encode().expect("encoding failed");

        let roundtripped = IoxGetRequest::try_decode(ticket).expect("decode failed");

        assert_eq!(request, roundtripped)
    }

    #[test]
    fn round_trip_influxql() {
        let request = IoxGetRequest {
            database: "foo_blarg".into(),
            query: RunQuery::InfluxQL("select * from bar".into()),
            is_debug: false,
        };

        let ticket = request.clone().try_encode().expect("encoding failed");

        let roundtripped = IoxGetRequest::try_decode(ticket).expect("decode failed");

        assert_eq!(request, roundtripped)
    }

    #[test]
    fn round_trip_flightsql() {
        let cmd = FlightSQLCommand::CommandStatementQuery(CommandStatementQuery {
            query: "select * from foo".into(),
            transaction_id: None,
        });

        let request = IoxGetRequest {
            database: "foo_blarg".into(),
            query: RunQuery::FlightSQL(cmd),
            is_debug: false,
        };

        let ticket = request.clone().try_encode().expect("encoding failed");

        let roundtripped = IoxGetRequest::try_decode(ticket).expect("decode failed");

        assert_eq!(request, roundtripped)
    }

    fn make_any_wrapped_proto_ticket(read_info: &proto::ReadInfo) -> Ticket {
        let any = Any {
            type_url: IoxGetRequest::READ_INFO_TYPE_URL.to_string(),
            value: read_info.encode_to_vec().into(),
        };
        Ticket {
            ticket: any.encode_to_vec().into(),
        }
    }

    fn make_proto_ticket(read_info: &proto::ReadInfo) -> Ticket {
        Ticket {
            ticket: read_info.encode_to_vec().into(),
        }
    }

    fn make_json_ticket(json: &str) -> Ticket {
        Ticket {
            ticket: json.as_bytes().to_vec().into(),
        }
    }
}
