use snafu::Snafu;

/// Error responses returned when attempting to create a new IOx database.
#[derive(Debug, Snafu)]
pub enum CreateDatabaseError {
    /// The database contains an invalid character.
    #[snafu(display("database name {} contains an invalid character", name))]
    InvalidName {
        /// The database name provided
        name: String,
    },

    /// A generic request/response error.
    #[snafu(display("{}", source))]
    Unknown {
        /// The underlying error.
        source: super::RequestError,
    },
}

impl From<super::RequestError> for CreateDatabaseError {
    fn from(v: super::RequestError) -> Self {
        CreateDatabaseError::Unknown { source: v }
    }
}
