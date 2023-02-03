//! Ticket handling for the native IOx Flight API

use arrow_flight::Ticket;
use bytes::Bytes;
use flightsql::FlightSQLCommand;
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

/// This is the structure of the opaque tickets` used for requests to
/// IOx Flight DoGet endpoint
///
/// This structure encapsulates the deserialization and serializion
/// logic for these requests
#[derive(Debug, PartialEq, Clone)]
pub struct IoxGetRequest {
    namespace_name: String,
    query: RunQuery,
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
    /// Create a new request to run the specified query
    pub fn new(namespace_name: impl Into<String>, query: RunQuery) -> Self {
        Self {
            namespace_name: namespace_name.into(),
            query,
        }
    }

    /// try to decode a ReadInfo structure from a Token
    pub fn try_decode(ticket: Ticket) -> Result<Self> {
        // decode ticket
        IoxGetRequest::decode_protobuf(ticket.ticket.clone())
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
            namespace_name,
            query,
        } = self;

        let read_info = match query {
            RunQuery::Sql(sql_query) => proto::ReadInfo {
                namespace_name,
                sql_query,
                query_type: QueryType::Sql.into(),
                flightsql_command: vec![],
            },
            RunQuery::InfluxQL(influxql) => proto::ReadInfo {
                namespace_name,
                // field name is misleading
                sql_query: influxql,
                query_type: QueryType::InfluxQl.into(),
                flightsql_command: vec![],
            },
            RunQuery::FlightSQL(flightsql_command) => proto::ReadInfo {
                namespace_name,
                sql_query: "".into(),
                query_type: QueryType::FlightSqlMessage.into(),
                flightsql_command: flightsql_command
                    .try_encode()
                    .context(FlightSQLSnafu)?
                    .into(),
            },
        };

        let ticket = read_info.encode_to_vec();

        Ok(Ticket {
            ticket: ticket.into(),
        })
    }

    /// The Go clients still use an older form of ticket encoding, JSON tickets
    ///
    /// - <https://github.com/influxdata/influxdb-iox-client-go/commit/2e7a3b0bd47caab7f1a31a1bbe0ff54aa9486b7b>
    /// - <https://github.com/influxdata/influxdb-iox-client-go/commit/52f1a1b8d5bb8cc8dc2fe825f4da630ad0b9167c>
    ///
    /// Go clients are unable to execute InfluxQL queries until the JSON structure is updated
    /// accordingly.
    fn decode_json(ticket: Bytes) -> Result<Self, String> {
        let json_str = String::from_utf8(ticket.to_vec()).map_err(|_| "Not UTF8".to_string())?;

        #[derive(Deserialize, Debug)]
        struct ReadInfoJson {
            namespace_name: String,
            sql_query: String,
        }

        let ReadInfoJson {
            namespace_name,
            sql_query,
        } = serde_json::from_str(&json_str).map_err(|e| format!("JSON parse error: {e}"))?;

        Ok(Self {
            namespace_name,
            /// Old JSON format is always SQL
            query: RunQuery::Sql(sql_query),
        })
    }

    fn decode_protobuf(ticket: Bytes) -> Result<Self, Error> {
        let read_info = proto::ReadInfo::decode(ticket).context(DecodeSnafu)?;

        let query_type = read_info.query_type();
        let proto::ReadInfo {
            namespace_name,
            sql_query,
            query_type: _,
            flightsql_command,
        } = read_info;

        Ok(Self {
            namespace_name,
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
        })
    }

    pub fn namespace_name(&self) -> &str {
        self.namespace_name.as_ref()
    }

    pub fn query(&self) -> &RunQuery {
        &self.query
    }
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use generated_types::influxdata::iox::querier::v1::read_info::QueryType;

    use super::*;

    #[test]
    fn json_ticket_decoding() {
        // The Go clients still use JSON tickets. See:
        //
        // - <https://github.com/influxdata/influxdb-iox-client-go/commit/2e7a3b0bd47caab7f1a31a1bbe0ff54aa9486b7b>
        // - <https://github.com/influxdata/influxdb-iox-client-go/commit/52f1a1b8d5bb8cc8dc2fe825f4da630ad0b9167c
        //
        // Do not change this test without having first changed what the Go clients are sending!
        let ticket = make_json_ticket(r#"{"namespace_name": "my_db", "sql_query": "SELECT 1;"}"#);
        let ri = IoxGetRequest::try_decode(ticket).unwrap();

        assert_eq!(ri.namespace_name, "my_db");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1;"));
    }

    #[test]
    fn json_ticket_decoding_error() {
        // invalid json (database name rather than namespace name)
        let ticket = make_json_ticket(r#"{"database_name": "my_db", "sql_query": "SELECT 1;"}"#);
        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn proto_ticket_decoding_unspecified() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            namespace_name: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::Unspecified.into(),
            flightsql_command: vec![],
        });

        // Reverts to default (unspecified) for invalid query_type enumeration, and thus SQL
        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.namespace_name, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn proto_ticket_decoding_sql() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            namespace_name: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::Sql.into(),
            flightsql_command: vec![],
        });

        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.namespace_name, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn proto_ticket_decoding_influxql() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            namespace_name: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::InfluxQl.into(),
            flightsql_command: vec![],
        });

        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.namespace_name, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::InfluxQL(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn proto_ticket_decoding_too_new() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            namespace_name: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".into(),
            query_type: 42, // not a known query type
            flightsql_command: vec![],
        });

        // Reverts to default (unspecified) for invalid query_type enumeration, and thus SQL
        let ri = IoxGetRequest::try_decode(ticket).unwrap();
        assert_eq!(ri.namespace_name, "<foo>_<bar>");
        assert_matches!(ri.query, RunQuery::Sql(query) => assert_eq!(query, "SELECT 1"));
    }

    #[test]
    fn proto_ticket_decoding_sql_too_many_fields() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            namespace_name: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::Sql.into(),
            // can't have both sql_query and flightsql
            flightsql_command: vec![1, 2, 3],
        });

        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn proto_ticket_decoding_influxql_too_many_fields() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            namespace_name: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::InfluxQl.into(),
            // can't have both sql_query and flightsql
            flightsql_command: vec![1, 2, 3],
        });

        let e = IoxGetRequest::try_decode(ticket).unwrap_err();
        assert_matches!(e, Error::Invalid);
    }

    #[test]
    fn proto_ticket_decoding_flightsql_too_many_fields() {
        let ticket = make_proto_ticket(&proto::ReadInfo {
            namespace_name: "<foo>_<bar>".to_string(),
            sql_query: "SELECT 1".to_string(),
            query_type: QueryType::FlightSqlMessage.into(),
            // can't have both sql_query and flightsql
            flightsql_command: vec![1, 2, 3],
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
    fn round_trip_sql() {
        let request = IoxGetRequest {
            namespace_name: "foo_blarg".into(),
            query: RunQuery::Sql("select * from bar".into()),
        };

        let ticket = request.clone().try_encode().expect("encoding failed");

        let roundtripped = IoxGetRequest::try_decode(ticket).expect("decode failed");

        assert_eq!(request, roundtripped)
    }

    #[test]
    fn round_trip_influxql() {
        let request = IoxGetRequest {
            namespace_name: "foo_blarg".into(),
            query: RunQuery::InfluxQL("select * from bar".into()),
        };

        let ticket = request.clone().try_encode().expect("encoding failed");

        let roundtripped = IoxGetRequest::try_decode(ticket).expect("decode failed");

        assert_eq!(request, roundtripped)
    }

    #[test]
    fn round_trip_flightsql() {
        let cmd = FlightSQLCommand::CommandStatementQuery("select * from foo".into());

        let request = IoxGetRequest {
            namespace_name: "foo_blarg".into(),
            query: RunQuery::FlightSQL(cmd),
        };

        let ticket = request.clone().try_encode().expect("encoding failed");

        let roundtripped = IoxGetRequest::try_decode(ticket).expect("decode failed");

        assert_eq!(request, roundtripped)
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
