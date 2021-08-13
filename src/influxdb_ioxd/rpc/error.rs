use generated_types::google::{
    FieldViolation, InternalError, NotFound, PreconditionViolation, QuotaFailure,
};
use observability_deps::tracing::error;

/// map common [`server::Error`] errors  to the appropriate tonic Status
pub fn default_server_error_handler(error: server::Error) -> tonic::Status {
    use server::Error;

    match error {
        Error::IdNotSet => PreconditionViolation {
            category: "Writer ID".to_string(),
            subject: "influxdata.com/iox".to_string(),
            description: "Writer ID must be set".to_string(),
        }
        .into(),
        Error::DatabaseNotInitialized { db_name } => tonic::Status::unavailable(
            format!("Database ({}) is not yet initialized", db_name)
        ),
        Error::ServerNotInitialized{ server_id } => tonic::Status::unavailable(
            format!("Server ID is set ({}) but server is not yet initialized (e.g. DBs and remotes are not loaded). Server is not yet ready to read/write data.", server_id)
        ),
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
        Error::HardLimitReached {} => QuotaFailure {
            subject: "influxdata.com/iox/buffer".to_string(),
            description: "hard buffer limit reached".to_string(),
        }
        .into(),
        source @ Error::WritingOnlyAllowedThroughWriteBuffer { .. } |
            source @ Error::LineConversion { .. } => tonic::Status::failed_precondition(source.to_string()),
        Error::NoRemoteConfigured { node_group } => NotFound {
            resource_type: "remote".to_string(),
            resource_name: format!("{:?}", node_group),
            ..Default::default()
        }
        .into(),
        Error::RemoteError { source } => tonic::Status::unavailable(source.to_string()),
        Error::WipePreservedCatalog { source } => default_database_error_handler(source),
        error => {
            error!(?error, "Unexpected error");
            InternalError {}.into()
        }
    }
}

/// map common [`catalog::Error`](server::db::catalog::Error) errors to the appropriate tonic Status
pub fn default_catalog_error_handler(error: server::db::catalog::Error) -> tonic::Status {
    use server::db::catalog::Error;
    match error {
        Error::TableNotFound { table } => NotFound {
            resource_type: "table".to_string(),
            resource_name: table,
            ..Default::default()
        }
        .into(),
        Error::PartitionNotFound { partition, table } => NotFound {
            resource_type: "partition".to_string(),
            resource_name: format!("{}:{}", table, partition),
            ..Default::default()
        }
        .into(),
        Error::ChunkNotFound {
            chunk_id,
            partition,
            table,
        } => NotFound {
            resource_type: "chunk".to_string(),
            resource_name: format!("{}:{}:{}", table, partition, chunk_id),
            ..Default::default()
        }
        .into(),
    }
}

/// map common [`database::Error`](server::database::Error) errors  to the appropriate tonic Status
pub fn default_database_error_handler(error: server::database::Error) -> tonic::Status {
    use server::database::Error;
    match error {
        Error::TransitionInProgress { .. } => {
            tonic::Status::unavailable("another operation already in progress")
        }
        Error::InvalidState { .. } => tonic::Status::failed_precondition(error.to_string()),
        Error::WipePreservedCatalog { source, .. } => {
            error!(%source, "Unexpected error while wiping catalog");
            InternalError {}.into()
        }
        Error::SkipReplay { source, .. } => {
            error!(%source, "Unexpected error skipping replay");
            InternalError {}.into()
        }
    }
}

/// map common [`db::Error`](server::db::Error) errors  to the appropriate tonic Status
pub fn default_db_error_handler(error: server::db::Error) -> tonic::Status {
    use server::db::Error;
    match error {
        Error::DatabaseNotWriteable {} => PreconditionViolation {
            category: "database".to_string(),
            subject: "influxdata.com/iox".to_string(),
            description: "Cannot write to database: no mutable buffer configured".to_string(),
        }
        .into(),
        Error::LifecycleError { source } => PreconditionViolation {
            category: "chunk".to_string(),
            subject: "influxdata.com/iox".to_string(),
            description: format!(
                "Cannot perform operation due to wrong chunk lifecycle: {}",
                source
            ),
        }
        .into(),
        Error::CatalogError { source } => default_catalog_error_handler(source),
        error => {
            error!(?error, "Unexpected error");
            InternalError {}.into()
        }
    }
}
