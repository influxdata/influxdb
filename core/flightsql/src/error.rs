//! FlightSQL errors
use std::string::FromUtf8Error;

use arrow::error::ArrowError;
use arrow_flight::error::FlightError;
use datafusion::error::DataFusionError;
use prost::DecodeError;
use snafu::Snafu;

#[derive(Debug, Snafu)]
// allow Snafu 's to be used in the crate
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Invalid protobuf: {}", source))]
    #[snafu(context(false))]
    Decode { source: DecodeError },

    #[snafu(display("Invalid argument: {}", description))]
    InvalidArgument { description: String },

    #[snafu(display(
        "Invalid PreparedStatement handle. Expected protobuf Any or UTF-8 query string\nnot protobuf Any: {}\nand not UTF-8: {}",
        proto_source,
        source
    ))]
    InvalidHandle {
        proto_source: DecodeError,
        source: FromUtf8Error,
    },

    #[snafu(display(
        "Expected stream of FlightData but found none when executing method {} for command {:?}.",
        method,
        cmd
    ))]
    NoFlightData { cmd: String, method: &'static str },

    #[snafu(display("Invalid Any type URL. Expected '{}' found '{}'", expected, actual))]
    InvalidTypeUrl { expected: String, actual: String },

    #[snafu(display("Invalid parameters in prepared statement: {}", source))]
    #[snafu(context(false))]
    InvalidPreparedStatementParams { source: iox_query_params::Error },

    #[snafu(display("{}", source))]
    #[snafu(context(false))]
    Flight { source: FlightError },

    #[snafu(display("{}", source))]
    #[snafu(context(false))]
    DataFusion {
        #[snafu(source(from(DataFusionError, Box::new)))]
        source: Box<DataFusionError>,
    },

    #[snafu(display("{}", source))]
    #[snafu(context(false))]
    Arrow { source: ArrowError },

    #[snafu(display("Unsupported FlightSQL message type: {}", description))]
    UnsupportedMessageType { description: String },

    #[snafu(display("Protocol error. Method {} does not expect '{:?}'", method, cmd))]
    Protocol { cmd: String, method: &'static str },

    #[snafu(display(
        "Unable to infer the type of parameter '{}' in prepared statement",
        parameter_name
    ))]
    UnknownParameterType { parameter_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for DataFusionError {
    fn from(value: Error) -> Self {
        match value {
            Error::DataFusion { source } => *source,
            Error::Arrow { source } => Self::ArrowError(Box::new(source), None),
            value => Self::External(Box::new(value)),
        }
    }
}
