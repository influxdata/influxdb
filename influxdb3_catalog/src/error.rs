use std::sync::Arc;

use anyhow::anyhow;
use humantime::Duration;
use schema::InfluxColumnType;

use crate::{
    catalog::NUM_TAG_COLUMNS_LIMIT, channel::SubscriptionError,
    object_store::ObjectStoreCatalogError,
};

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("object store error: {0:?}")]
    ObjectStore(#[from] ObjectStoreCatalogError),

    #[error("attempted to create a resource that already exists")]
    AlreadyExists,

    #[error("the requested resource was not found")]
    NotFound,

    #[error("attempted to delete resource that was already deleted")]
    AlreadyDeleted,

    #[error("invalid configuration provided: {message}")]
    InvalidConfiguration { message: Box<str> },

    #[error("only tag and string columns are supported in the distinct cache")]
    InvalidDistinctCacheColumnType,

    #[error("only uint64, int64, bool, tag, and string columns are supported in the last cache")]
    InvalidLastCacheKeyColumnType,

    #[error("plugin trigger is already enabled")]
    TriggerAlreadyEnabled,

    #[error("plugin trigger is already disabled")]
    TriggerAlreadyDisabled,

    #[error("invalid column type for column '{column_name}', expected {expected}, got {got}")]
    InvalidColumnType {
        column_name: Arc<str>,
        expected: InfluxColumnType,
        got: InfluxColumnType,
    },

    #[error("invalid node registration")]
    InvalidNodeRegistration,

    #[error("Update to schema would exceed number of columns per table limit of {0} columns")]
    TooManyColumns(usize),

    #[error(
        "Update to schema would exceed number of tag columns per table limit of {} columns",
        NUM_TAG_COLUMNS_LIMIT
    )]
    TooManyTagColumns,

    #[error("Update to schema would exceed number of tables limit of {0} tables")]
    TooManyTables(usize),

    #[error("Adding a new database would exceed limit of {0} databases")]
    TooManyDbs(usize),

    #[error("Table {} not in DB schema for {}", table_name, db_name)]
    TableNotFound {
        db_name: Arc<str>,
        table_name: Arc<str>,
    },

    #[error(
        "Field type mismatch on table {} column {}. Existing column is {} but attempted to add {}",
        table_name,
        column_name,
        existing,
        attempted
    )]
    FieldTypeMismatch {
        table_name: String,
        column_name: String,
        existing: InfluxColumnType,
        attempted: InfluxColumnType,
    },

    #[error(
        "Series key mismatch on table {}. Existing table has {}",
        table_name,
        existing
    )]
    SeriesKeyMismatch {
        table_name: String,
        existing: String,
    },

    #[error("catalog subscription error: {0}")]
    Subscription(#[from] SubscriptionError),

    #[error(transparent)]
    Other(#[from] anyhow::Error),

    #[error(
        "Cannot overwrite Processing Engine Trigger {} in Database {}",
        trigger_name,
        database_name
    )]
    ProcessingEngineTriggerExists {
        database_name: String,
        trigger_name: String,
    },

    #[error(
        "Cannot delete running plugin {}. Disable it first or use --force.",
        trigger_name
    )]
    ProcessingEngineTriggerRunning { trigger_name: String },

    #[error(
        "Cannot delete plugin {} in database {} because it is used by trigger {}",
        plugin_name,
        database_name,
        trigger_name
    )]
    ProcessingEnginePluginInUse {
        database_name: String,
        plugin_name: String,
        trigger_name: String,
    },

    #[error(
        "Processing Engine Plugin {} not in DB schema for {}",
        plugin_name,
        database_name
    )]
    ProcessingEnginePluginNotFound {
        plugin_name: String,
        database_name: String,
    },

    #[error("Processing Engine Unimplemented: {}", feature_description)]
    ProcessingEngineUnimplemented { feature_description: String },

    #[error(
        "Processing Engine Trigger {} not in DB {}",
        trigger_name,
        database_name
    )]
    ProcessingEngineTriggerNotFound {
        database_name: String,
        trigger_name: String,
    },

    #[error("failed to parse trigger from {}", trigger_spec)]
    ProcessingEngineTriggerSpecParseError { trigger_spec: String },

    #[error("last cache size must be greater than 0")]
    InvalidLastCacheSize,

    #[error("failed to parse trigger from {trigger_spec}{}", .context.as_ref().map(|context| format!(": {context}")).unwrap_or_default())]
    TriggerSpecificationParseError {
        trigger_spec: String,
        context: Option<String>,
    },

    #[error("invalid error behavior {0}")]
    InvalidErrorBehavior(String),

    #[error("token name already exists, {0}")]
    TokenNameAlreadyExists(String),

    #[error("missing admin token, cannot update")]
    MissingAdminTokenToUpdate,

    #[error("cannot delete internal db")]
    CannotDeleteInternalDatabase,

    #[error("tried to stop a node ({node_id}) that is already stopped")]
    NodeAlreadyStopped { node_id: Arc<str> },

    #[error("cannot delete operator token")]
    CannotDeleteOperatorToken,

    #[error(
        "cannot change the configured generation duration for level {level}; \
        attempted to set to {attempted:#} but its already set to {existing:#}"
    )]
    CannotChangeGenerationDuration {
        level: u8,
        existing: Duration,
        attempted: Duration,
    },
}

impl CatalogError {
    pub fn invalid_configuration(message: impl AsRef<str>) -> Self {
        Self::InvalidConfiguration {
            message: Box::from(message.as_ref()),
        }
    }

    pub fn unexpected(message: impl Into<String>) -> Self {
        Self::Other(anyhow!(message.into()))
    }
}
