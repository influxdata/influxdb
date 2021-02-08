use thiserror::Error;

/// Error responses when creating a new IOx database.
#[derive(Debug, Error)]
pub enum ClientError {
    /// The database name contains an invalid character.
    #[error("the database name contains an invalid character")]
    InvalidDatabaseName,
}
