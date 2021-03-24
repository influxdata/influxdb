use generated_types::google::{FieldViolation, InternalError, NotFound, PreconditionViolation};
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
        Error::InvalidDatabaseName { source } => FieldViolation {
            field: "db_name".into(),
            description: source.to_string(),
        }
        .into(),
        error => {
            error!(?error, "Unexpected error");
            InternalError {}.into()
        }
    }
}

/// map common `catalog::Error` errors to the appropriate tonic Status
pub fn default_catalog_error_handler(error: catalog::Error) -> tonic::Status {
    use catalog::Error;
    match error {
        Error::UnknownPartition { partition_key } => NotFound {
            resource_type: "partition".to_string(),
            resource_name: partition_key,
            ..Default::default()
        }
        .into(),
        Error::UnknownChunk {
            partition_key,
            chunk_id,
        } => NotFound {
            resource_type: "chunk".to_string(),
            resource_name: format!("{}:{}", partition_key, chunk_id),
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
        Error::RollingOverChunk { source, .. } => default_catalog_error_handler(source),
        error => {
            error!(?error, "Unexpected error");
            InternalError {}.into()
        }
    }
}
