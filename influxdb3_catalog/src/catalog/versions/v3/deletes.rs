use std::sync::Arc;

use iox_time::TimeProvider;
use serde::{Deserialize, Serialize};

use crate::catalog::{DeletionStatus, versions::v3::schema::database::DatabaseSchema};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DeletionScope {
    /// Remove database data and all catalog resources
    #[default]
    DataAndCatalog,
    /// Remove database data and table-level catalog resources, but keep database-level catalog resources
    DataOnlyRemoveTables,
    /// Remove database data, but keep database-level and table-level resources
    DataOnlyKeepResources,
}

impl DeletionScope {
    /// Returns `None` if the scope is `DataAndCatalog`, otherwise `Some(scope)`.
    pub(crate) fn as_option(self) -> Option<Self> {
        match self {
            Self::DataAndCatalog => None,
            scope => Some(scope),
        }
    }

    /// Returns the provided scope or the default `DataAndCatalog` if `None`.
    pub fn from_option(scope: Option<Self>) -> Self {
        scope.unwrap_or(Self::DataAndCatalog)
    }
}

impl std::fmt::Display for DeletionScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeletionScope::DataAndCatalog => write!(f, "data_and_catalog"),
            DeletionScope::DataOnlyRemoveTables => write!(f, "data_only_remove_tables"),
            DeletionScope::DataOnlyKeepResources => write!(f, "data_only_keep_resources"),
        }
    }
}

pub(crate) fn database_or_deletion_status(
    db_schema: Option<Arc<DatabaseSchema>>,
    time_provider: &Arc<dyn TimeProvider>,
) -> Result<Arc<DatabaseSchema>, DeletionStatus> {
    match db_schema {
        Some(db_schema) if db_schema.deleted => Err(db_schema
            .hard_delete_time
            .and_then(|time| {
                time_provider
                    .now()
                    .checked_duration_since(time)
                    .map(DeletionStatus::Hard)
            })
            .unwrap_or(DeletionStatus::Soft)),
        Some(db_schema) => Ok(db_schema),
        None => Err(DeletionStatus::NotFound),
    }
}
