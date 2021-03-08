use generated_types::google::{InternalError, NotFound, PreconditionViolation};
use tracing::error;

use server::Error;

/// map common server errors  to the appropriate tonic Status
pub fn default_error_handler(error: Error) -> tonic::Status {
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
