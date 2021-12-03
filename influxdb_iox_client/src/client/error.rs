use generated_types::google::{AlreadyExists, FieldViolation, NotFound, PreconditionViolation};
use std::fmt::Debug;
use thiserror::Error;
use tonic::{Code, Status};

/// A generic opaque error
pub type StdError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// A gRPC error payload with optional [details](https://cloud.google.com/apis/design/errors#error_details)
#[derive(Error, Debug, Clone)]
#[error("{message}")]
pub struct ServerError<D> {
    /// A human readable error message
    pub message: String,
    /// An optional machine-readable error
    pub details: Option<D>,
}

fn parse_status<D: ServerErrorDetails>(status: tonic::Status) -> ServerError<D> {
    ServerError {
        message: status.message().to_string(),
        details: D::try_decode(&status),
    }
}

trait ServerErrorDetails: Sized {
    fn try_decode(data: &tonic::Status) -> Option<Self>;
}

impl ServerErrorDetails for () {
    fn try_decode(_: &tonic::Status) -> Option<Self> {
        None
    }
}

impl ServerErrorDetails for FieldViolation {
    fn try_decode(status: &tonic::Status) -> Option<Self> {
        generated_types::google::decode_field_violation(status).next()
    }
}

impl ServerErrorDetails for AlreadyExists {
    fn try_decode(status: &tonic::Status) -> Option<Self> {
        generated_types::google::decode_already_exists(status).next()
    }
}

impl ServerErrorDetails for NotFound {
    fn try_decode(status: &tonic::Status) -> Option<Self> {
        generated_types::google::decode_not_found(status).next()
    }
}

impl ServerErrorDetails for PreconditionViolation {
    fn try_decode(status: &tonic::Status) -> Option<Self> {
        generated_types::google::decode_precondition_violation(status).next()
    }
}

/// The errors returned by this client
#[derive(Error, Debug)]
#[allow(missing_docs)]
pub enum Error {
    #[error("The operation was cancelled: {0}")]
    Cancelled(ServerError<()>),

    #[error("Unknown server error: {0}")]
    Unknown(ServerError<()>),

    #[error("Client specified an invalid argument: {0}")]
    InvalidArgument(ServerError<FieldViolation>),

    #[error("Deadline expired before operation could complete: {0}")]
    DeadlineExceeded(ServerError<()>),

    #[error("Some requested entity was not found: {0}")]
    NotFound(ServerError<NotFound>),

    #[error("Some entity that we attempted to create already exists: {0}")]
    AlreadyExists(ServerError<AlreadyExists>),

    #[error("The caller does not have permission to execute the specified operation: {0}")]
    PermissionDenied(ServerError<()>),

    #[error("Some resource has been exhausted: {0}")]
    ResourceExhausted(ServerError<()>),

    #[error("The system is not in a state required for the operation's execution: {0}")]
    FailedPrecondition(ServerError<PreconditionViolation>),

    #[error("The operation was aborted: {0}")]
    Aborted(ServerError<()>),

    #[error("Operation was attempted past the valid range: {0}")]
    OutOfRange(ServerError<()>),

    #[error("Operation is not implemented or supported: {0}")]
    Unimplemented(ServerError<()>),

    #[error("Internal error: {0}")]
    Internal(ServerError<()>),

    #[error("The service is currently unavailable: {0}")]
    Unavailable(ServerError<()>),

    #[error("Unrecoverable data loss or corruption: {0}")]
    DataLoss(ServerError<()>),

    #[error("The request does not have valid authentication credentials: {0}")]
    Unauthenticated(ServerError<()>),

    #[error("Received an invalid response from the server: {0}")]
    InvalidResponse(#[from] FieldViolation),

    #[error("An unexpected error occurred in the client library: {0}")]
    Client(StdError),
}

impl From<tonic::Status> for Error {
    fn from(s: Status) -> Self {
        match s.code() {
            Code::Ok => Self::Client("status is not an error".into()),
            Code::Cancelled => Self::Cancelled(parse_status(s)),
            Code::Unknown => Self::Unknown(parse_status(s)),
            Code::InvalidArgument => Self::InvalidArgument(parse_status(s)),
            Code::DeadlineExceeded => Self::DeadlineExceeded(parse_status(s)),
            Code::NotFound => Self::NotFound(parse_status(s)),
            Code::AlreadyExists => Self::AlreadyExists(parse_status(s)),
            Code::PermissionDenied => Self::PermissionDenied(parse_status(s)),
            Code::ResourceExhausted => Self::ResourceExhausted(parse_status(s)),
            Code::FailedPrecondition => Self::FailedPrecondition(parse_status(s)),
            Code::Aborted => Self::Aborted(parse_status(s)),
            Code::OutOfRange => Self::OutOfRange(parse_status(s)),
            Code::Unimplemented => Self::Unimplemented(parse_status(s)),
            Code::Internal => Self::Internal(parse_status(s)),
            Code::Unavailable => Self::Unavailable(parse_status(s)),
            Code::DataLoss => Self::DataLoss(parse_status(s)),
            Code::Unauthenticated => Self::Unauthenticated(parse_status(s)),
        }
    }
}
