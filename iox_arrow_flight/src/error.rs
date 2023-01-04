use std::error::Error;

/// Many different operations in the `arrow` crate return this error type.
#[derive(Debug)]
pub enum FlightError {
    /// Returned when functionality is not yet available.
    NotYetImplemented(String),
    /// Error from the underlying tonic library
    Tonic(tonic::Status),
    /// Some unexpected message was received
    ProtocolError(String),
    /// An error occured during decoding
    DecodeError(String),
    ExternalError(Box<dyn Error + Send + Sync>),
}

impl FlightError {
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::ProtocolError(message.into())
    }

    /// Wraps an external error in an `ArrowError`.
    pub fn from_external_error(error: Box<dyn Error + Send + Sync>) -> Self {
        Self::ExternalError(error)
    }
}

impl std::fmt::Display for FlightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO better format / error
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for FlightError {}

impl From<tonic::Status> for FlightError {
    fn from(status: tonic::Status) -> Self {
        Self::Tonic(status)
    }
}

pub type Result<T> = std::result::Result<T, FlightError>;
