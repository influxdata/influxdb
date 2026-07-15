//! Implementation of the Catalog that sits entirely in memory.

/// Export version 3 of the catalog API.
pub use ::schema::{InfluxColumnType, InfluxFieldType};

pub use versions::v3::backup::{
    CatalogBackupView, CatalogCheckpointForBackup, CatalogLogFileForBackup, CatalogRestoreSource,
};
#[cfg(any(test, feature = "test_helpers"))]
pub use versions::v3::catalog::CatalogBuilder;
pub use versions::v3::catalog::{
    ApiNodeSpec, Catalog, CatalogArgs, Committed, CreateDatabaseOptions, CreateTableOptions,
    HardDeletionTime,
};
pub use versions::v3::deletes::DeletionScope;
pub use versions::v3::events::{
    CatalogEvent, CatalogUpdate, CatalogUpdateMessage, CatalogUpdateReceiver,
};
pub use versions::v3::legacy;
pub use versions::v3::schema;
pub use versions::v3::schema::cache::{
    CacheSource, DistinctCacheDefinition, LastCacheDefinition, LastCacheSize, LastCacheTtl,
    LastCacheValueColumnsDef, MaxAge, MaxCardinality, RefreshInterval,
};
pub use versions::v3::schema::column::{
    ColumnDefinition, ColumnSet, FieldColumn, FieldDataType, FieldFamilyDefinition,
    FieldFamilyMode, FieldFamilyName, TagColumn, TimestampColumn,
};
pub use versions::v3::schema::database::DatabaseSchema;
pub use versions::v3::schema::node::{NodeDefinition, NodeMode, NodeModes, NodeSpec, NodeState};
pub use versions::v3::schema::query_group::{
    QueryGroupDefinition, QueryGroupInsertPosition, QueryGroupUpdate,
};
pub use versions::v3::schema::retention::RetentionPeriod;
pub use versions::v3::schema::storage::{GenerationConfig, StorageMode};
pub use versions::v3::schema::table::TableDefinition;
pub use versions::v3::schema::trigger::{
    ErrorBehavior, PluginType, TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition,
    ValidPluginFilename,
};
pub use versions::v3::schema::user::{LoginIdentities, LoginIdentityUsernamePassword, UserInfo};
pub use versions::v3::transaction::{
    CatalogTransaction, DatabaseCatalogTransaction, Prompt, TableTransaction,
};
pub use versions::v3::usage::{
    CatalogLimiter, CatalogLimits, CurrentCatalogUsage, MaximumColumnCountLimiter,
};

use ahash::RandomState as AHashBuilder;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use influxdb3_authz::TokenInfo;
use influxdb3_id::TokenId;
use iox_time::Time;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha512;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};

mod key;
pub(crate) mod migrations;
pub mod versions;

#[cfg(test)]
mod tests;

use crate::repository::Repository;
use crate::resource::CatalogResource;
use crate::{CatalogError, Result};

const SOFT_DELETION_TIME_FORMAT: &str = "%Y%m%dT%H%M%S";

pub const INTERNAL_DB_NAME: &str = "_internal";
pub const INTERNAL_DB_RETENTION_PERIOD: Duration = Duration::from_secs(60 * 60 * 24 * 7); // Default to 7 days

pub const TIME_COLUMN_NAME: &str = "time";
pub const CHUNK_ORDER_COLUMN_NAME: &str = "__chunk_order";

/// List of reserved column names that cannot be used as user-defined columns
pub const RESERVED_COLUMN_NAMES: &[&str] = &[TIME_COLUMN_NAME, CHUNK_ORDER_COLUMN_NAME];

const DEFAULT_OPERATOR_TOKEN_NAME: &str = "_admin";

// Type alias for BiHashMap with AHash for better performance and DoS resistance
pub(crate) type BiHashMap<L, R> = bimap::BiHashMap<L, R, AHashBuilder, AHashBuilder>;

/// Represents the deletion status of a database or table in the catalog
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DeletionStatus {
    /// The resource has been soft deleted but not yet hard deleted
    Soft,
    /// The resource has been hard deleted with the duration since deletion
    Hard(Duration),
    /// The resource was not found in the catalog
    NotFound,
}

/// The sequence number of a batch of WAL operations.
#[derive(
    Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct CatalogSequenceNumber(u64);

impl CatalogSequenceNumber {
    pub const fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl From<u64> for CatalogSequenceNumber {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

static CATALOG_WRITE_PERMIT: Mutex<CatalogSequenceNumber> =
    Mutex::const_new(CatalogSequenceNumber::new(0));

/// Convenience type alias for the write permit on the catalog
///
/// This is a mutex that, when a lock is acquired, holds the next catalog sequence number at the
/// time that the permit was acquired.
pub type CatalogWritePermit = MutexGuard<'static, CatalogSequenceNumber>;

/// Trait for schema objects that can be marked as deleted.
pub trait DeletedSchema: Sized {
    /// Check if the schema is marked as deleted.
    fn is_deleted(&self) -> bool;
}

/// A trait for types that can filter themselves based on deletion status.
///
/// This trait provides a convenient way to filter out deleted items by converting
/// them to `None` if they are marked as deleted. It is typically implemented on
/// types that also implement [`DeletedSchema`].
///
/// # Examples
///
/// ```ignore
/// // Get a database schema and filter out if deleted
/// let Some(db) = catalog.db_schema("my_db").not_deleted() else { continue };
/// ```
pub trait IfNotDeleted {
    /// The type that is returned when the item is not deleted.
    type T;

    /// Returns `Some(self)` if the item is not deleted, otherwise returns `None`.
    ///
    /// This method provides a convenient way to filter out deleted items
    /// from the catalog without explicit conditional checks.
    fn if_not_deleted(self) -> Option<Self::T>;
}

impl<T: DeletedSchema> DeletedSchema for Option<T> {
    fn is_deleted(&self) -> bool {
        self.as_ref().is_some_and(DeletedSchema::is_deleted)
    }
}

impl<T: DeletedSchema> IfNotDeleted for Option<T> {
    type T = T;

    fn if_not_deleted(self) -> Option<Self::T> {
        self.and_then(|d| (!d.is_deleted()).then_some(d))
    }
}

impl<T: DeletedSchema> DeletedSchema for Arc<T> {
    fn is_deleted(&self) -> bool {
        self.as_ref().is_deleted()
    }
}

impl<T: DeletedSchema> IfNotDeleted for Arc<T> {
    type T = Self;

    fn if_not_deleted(self) -> Option<Self::T> {
        (!self.is_deleted()).then_some(self)
    }
}

fn make_new_name_using_deleted_time(name: &str, deletion_time: Time) -> Arc<str> {
    Arc::from(format!(
        "{}-{}",
        name,
        deletion_time.date_time().format(SOFT_DELETION_TIME_FORMAT)
    ))
}

/// Stores tokens in the catalog. Wraps a [`Repository`] while providing additional functionality
/// needed for looking up tokens at runtime.
#[derive(Debug, Clone, Default)]
pub(crate) struct TokenRepository {
    /// The collection of tokens
    repo: Repository<TokenId, TokenInfo>,
    /// Bi-directional map for quick lookup of tokens by their hash
    hash_lookup_map: BiHashMap<TokenId, Vec<u8>>,
}

impl TokenRepository {
    pub(crate) fn new(
        repo: Repository<TokenId, TokenInfo>,
        hash_lookup_map: BiHashMap<TokenId, Vec<u8>>,
    ) -> Self {
        Self {
            repo,
            hash_lookup_map,
        }
    }

    pub(crate) fn repo(&self) -> &Repository<TokenId, TokenInfo> {
        &self.repo
    }

    pub(crate) fn get_and_increment_next_id(&mut self) -> TokenId {
        self.repo.get_and_increment_next_id()
    }

    pub(crate) fn hash_to_id(&self, hash: Vec<u8>) -> Option<TokenId> {
        self.hash_lookup_map
            .get_by_right(&hash)
            .map(|id| id.to_owned())
    }

    pub(crate) fn hash_to_info(&self, hash: Vec<u8>) -> Option<Arc<TokenInfo>> {
        let id = self
            .hash_lookup_map
            .get_by_right(&hash)
            .map(|id| id.to_owned())?;
        self.repo.get_by_id(&id)
    }

    pub(crate) fn add_token(&mut self, token_id: TokenId, token_info: TokenInfo) -> Result<()> {
        let token_info_hash = token_info.hash.clone();
        self.repo.insert(token_id, token_info)?;
        // insert to hash_lookup_map second in case the repo insert fails
        self.hash_lookup_map.insert(token_id, token_info_hash);
        Ok(())
    }

    pub(crate) fn update_admin_token_hash(
        &mut self,
        token_id: TokenId,
        hash: Vec<u8>,
        updated_at: i64,
    ) -> Result<()> {
        let mut token_info = self
            .repo
            .get_by_id(&token_id)
            .ok_or_else(|| CatalogError::MissingAdminTokenToUpdate)?;
        let updatable = Arc::make_mut(&mut token_info);

        updatable.hash = hash.clone();
        updatable.updated_at = Some(updated_at);
        updatable.updated_by = Some(token_id);
        self.repo.update(token_id, token_info)?;
        self.hash_lookup_map.insert(token_id, hash);
        Ok(())
    }

    pub(crate) fn delete_token(&mut self, token_name: String) -> Result<TokenId> {
        let token_id = self
            .repo
            .name_to_id(&token_name)
            .ok_or_else(|| CatalogError::NotFound(token_name))?;
        self.repo.remove(&token_id);
        self.hash_lookup_map.remove_by_left(&token_id);
        Ok(token_id)
    }

    pub(crate) fn update_token(&mut self, token_id: TokenId, token_info: TokenInfo) -> Result<()> {
        self.repo.update(token_id, Arc::new(token_info))?;
        Ok(())
    }
}

impl CatalogResource for TokenInfo {
    type Identifier = TokenId;

    const CATEGORY: &'static str = "tokens";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

/// Compute the SHA512 hash of a token string
/// This is the canonical way to hash tokens across the codebase
pub fn compute_token_hash(token: &str) -> Vec<u8> {
    Sha512::digest(token).to_vec()
}

pub fn create_token_and_hash() -> (String, Vec<u8>) {
    let token = {
        let mut token = String::from("apiv3_");
        let mut key = [0u8; 64];
        OsRng.fill_bytes(&mut key);
        token.push_str(&B64.encode(key));
        token
    };
    (token.clone(), compute_token_hash(&token))
}
