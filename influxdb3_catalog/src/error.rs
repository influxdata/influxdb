use std::{fmt::Display, sync::Arc};

use anyhow::anyhow;
use enterprise::EnterpriseCatalogError;
use humantime::Duration;
use influxdb3_id::QueryGroupId;
use schema::InfluxColumnType;

pub(crate) mod enterprise;

const MAX_TABLE_NAME_ERROR_LEN: usize = 256;

/// New type to keep long table names from making catalog error messages too large.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncatedTableName(Arc<str>);

impl TruncatedTableName {
    pub fn new(s: impl Into<Arc<str>>) -> Self {
        let s: Arc<str> = s.into();
        Self(s[..s.floor_char_boundary(MAX_TABLE_NAME_ERROR_LEN)].into())
    }

    pub fn inner(&self) -> &str {
        self.0.as_ref()
    }
}

impl Display for TruncatedTableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner())
    }
}

use crate::{
    channel::SubscriptionError, format::FeatureLevel, format::FormatError,
    log::versions::v4::StorageMode, object_store::ObjectStoreCatalogError,
};

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    Enterprise(#[from] EnterpriseCatalogError),

    #[error("object store error: {0:?}")]
    ObjectStore(#[from] ObjectStoreCatalogError),

    #[error("catalog format error: {0}")]
    Format(#[from] FormatError),

    #[error("attempted to create a resource that already exists")]
    AlreadyExists,

    #[error("the requested resource was not found: {0}")]
    NotFound(String),

    #[error("attempted to modify resource that was already deleted: {0}")]
    AlreadyDeleted(String),

    /// Request is idempotent: no catalog state would change.
    #[error("no catalog changes to apply: {details}")]
    NoCatalogChange { details: String },

    /// Request is invalid.
    #[error("catalog internal error: {details}")]
    Internal { details: String },

    #[error(
        "persisted catalog checkpoint sequence {checkpoint_sequence} is ahead of live catalog sequence {live_sequence}"
    )]
    BackupCheckpointAhead {
        checkpoint_sequence: u64,
        live_sequence: u64,
    },

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

    #[error("'{0}' is a reserved column")]
    ReservedColumn(Arc<str>),

    #[error("invalid node registration")]
    InvalidNodeRegistration,

    #[error("invalid node name ({0})")]
    InvalidNodeName(String),

    #[error("invalid node spec: {0}")]
    InvalidNodeSpec(#[source] anyhow::Error),

    #[error(
        "Schema update for table '{table_name}' would exceed the column limit: \
        proposed schema update would have {attempted} columns, but the limit is {limit}"
    )]
    TooManyColumns {
        table_name: TruncatedTableName,
        attempted: usize,
        limit: usize,
    },

    #[error(
        "Schema update for table '{table_name}' would exceed the tag column limit: \
        proposed schema would have {attempted} tag columns, but the limit is {limit}"
    )]
    TooManyTagColumns {
        table_name: TruncatedTableName,
        attempted: usize,
        limit: usize,
    },

    #[error(
        "Update to schema would exceed number of tables limit: attempted to create table but already have {current} table(s) (limit: {limit})"
    )]
    TooManyTables { current: usize, limit: usize },

    #[error("Adding a new database would exceed limit of {0} databases")]
    TooManyDbs(usize),

    #[error(
        "Update to schema would exceed the field limit ({limit}) for field family '{field_family}'"
    )]
    TooManyFields { field_family: String, limit: usize },

    #[error("Update to schema would exceed the field family limit of {0}")]
    TooManyFieldFamilies(usize),

    #[error(
        "table '{table_name}' in storage mode {storage_mode:?} cannot allocate more legacy column IDs"
    )]
    LegacyColumnIdsExhausted {
        table_name: Arc<str>,
        storage_mode: StorageMode,
    },

    #[error("Database not found {}", db_name)]
    DatabaseNotFound { db_name: Arc<str> },

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

    #[error("cannot parse token permission, {0}")]
    CannotParsePermissionForToken(String),

    #[error("token name already exists, {0}")]
    TokenNameAlreadyExists(String),

    #[error("missing admin token, cannot update")]
    MissingAdminTokenToUpdate,

    #[error("cannot delete internal db")]
    CannotDeleteInternalDatabase,

    #[error("cannot modify internal db")]
    CannotModifyInternalDatabase,

    #[error("tried to stop a node ({node_id}) that is already stopped")]
    NodeAlreadyStopped { node_id: Arc<str> },

    #[error(
        "node '{node_id}' is not fully stopped (current state: {current_state}); run \"stop node\" first"
    )]
    NodeNotFullyStopped {
        node_id: Arc<str>,
        current_state: &'static str,
    },

    #[error("node '{node_id}' has compact mode and cannot be removed")]
    NodeModeNotRemovable { node_id: Arc<str> },

    #[error("invalid stop ack for node '{node_id}' (current state: {current_state})")]
    InvalidStopAck {
        node_id: Arc<str>,
        current_state: &'static str,
    },

    #[error("invalid unregister for node '{node_id}' (current state: {current_state})")]
    InvalidUnregister {
        node_id: Arc<str>,
        current_state: &'static str,
    },

    #[error("idempotent no-op")]
    IdempotentNoOp,

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

    #[error("cannot add column {name} because it already exists with type {existing}")]
    DuplicateColumn {
        name: Arc<str>,
        existing: InfluxColumnType,
    },

    #[error(
        "record id {record_id} exceeds the cluster's committed feature level (core={}, enterprise={}); \
        the cluster must finish upgrading before this operation is available",
        committed.core,
        committed.enterprise,
    )]
    RecordExceedsCommittedFeatureLevel {
        record_id: u16,
        committed: FeatureLevel,
    },

    #[error(
        "this node's feature level (core={}, enterprise={}) is below the cluster's committed level (core={}, enterprise={}); upgrade required",
        local.core,
        local.enterprise,
        committed.core,
        committed.enterprise,
    )]
    NodeBelowCommittedFeatureLevel {
        committed: FeatureLevel,
        local: FeatureLevel,
    },

    #[error("missing object store for restore operation")]
    MissingObjectStoreForRestore,

    #[error(
        "cannot remove node '{node_id}' because it is a member of query group '{query_group_name}' (id {query_group_id})"
    )]
    NodeInQueryGroup {
        node_id: Arc<str>,
        query_group_name: Arc<str>,
        query_group_id: QueryGroupId,
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

    pub(crate) fn too_many_columns(
        table_name: impl Into<Arc<str>>,
        attempted: usize,
        limit: usize,
    ) -> Self {
        Self::TooManyColumns {
            table_name: TruncatedTableName::new(table_name),
            attempted,
            limit,
        }
    }

    pub(crate) fn too_many_tag_columns(
        table_name: impl Into<Arc<str>>,
        attempted: usize,
        limit: usize,
    ) -> Self {
        Self::TooManyTagColumns {
            table_name: TruncatedTableName::new(table_name),
            attempted,
            limit,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CatalogError, MAX_TABLE_NAME_ERROR_LEN};

    #[test]
    fn column_limit_error_truncates_table_name() {
        let table_name = format!("{}suffix", "a".repeat(MAX_TABLE_NAME_ERROR_LEN));
        let err = CatalogError::too_many_columns(table_name, 11, 10).to_string();

        assert!(err.contains(&"a".repeat(MAX_TABLE_NAME_ERROR_LEN)));
        assert!(!err.contains("suffix"));
    }
}
