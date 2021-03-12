use generated_types::google::{InternalError, NotFound, PreconditionViolation};
use tracing::error;

/// map common `server::Error` errors  to the appropriate tonic Status
pub fn default_server_error_handler(error: server::Error) -> tonic::Status {
    use server::Error;

    match error {
        Error::IdNotSet => PreconditionViolation {
            category: "Writer ID".to_string(),
            subject: "influxdata.com/iox".to_string(),
            description: "Writer ID must be set".to_string(),
        }
        .into(),
        Error::DatabaseNotFound { db_name } => NotFound {
            resource_type: "database".to_string(),
            resource_name: db_name,
            ..Default::default()
        }
        .into(),
        error => {
            error!(?error, "Unexpected error");
            InternalError {}.into()
        }
    }
}

/// map common `server::db::Error` errors  to the appropriate tonic Status
pub fn default_db_error_handler(error: server::db::Error) -> tonic::Status {
    use server::db::Error;
    match error {
        Error::DatabaseNotReadable {} => PreconditionViolation {
            category: "database".to_string(),
            subject: "influxdata.com/iox".to_string(),
            description: "Cannot read from database: no mutable buffer configured".to_string(),
        }
        .into(),
        Error::DatatbaseNotWriteable {} => PreconditionViolation {
            category: "database".to_string(),
            subject: "influxdata.com/iox".to_string(),
            description: "Cannot write to database: no mutable buffer configured".to_string(),
        }
        .into(),
        error => {
            error!(?error, "Unexpected error");
            InternalError {}.into()
        }
    }
}
