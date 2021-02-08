use thiserror::Error;

/// Error responses when querying an IOx database using the Arrow Flight gRPC
/// API.
#[derive(Debug, Error)]
pub enum GrpcQueryError {
    /// An error occurred while serializing the query.
    #[error(transparent)]
    QuerySerializeError(#[from] serde_json::Error),

    /// There were no FlightData messages returned when we expected to get one
    /// containing a Schema.
    #[error("no FlightData containing a Schema returned")]
    NoSchema,

    /// An error involving an Arrow operation occurred.
    #[error(transparent)]
    ArrowError(#[from] arrow_deps::arrow::error::ArrowError),

    /// The data contained invalid Flatbuffers.
    #[error("Invalid Flatbuffer: `{0}`")]
    InvalidFlatbuffer(String),

    /// The message header said it was a dictionary batch, but interpreting the
    /// message as a dictionary batch returned `None`. Indicates malformed
    /// Flight data from the server.
    #[error("Message with header of type dictionary batch could not return a dictionary batch")]
    CouldNotGetDictionaryBatch,

    /// An unknown server error occurred. Contains the `tonic::Status` returned
    /// from the server.
    #[error(transparent)]
    GrpcError(#[from] tonic::Status),
}
