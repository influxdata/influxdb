//! Implementation of the Catalog that sits entirely in memory.

/// Export version 1 of the catalog API
pub use versions::v2::*;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use bimap::BiHashMap;
use influxdb3_authz::TokenInfo;
use influxdb3_id::{CatalogId, SerdeVecMap, TokenId};
use iox_time::Time;
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use sha2::Sha512;
use std::cmp::Ordering;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, MutexGuard};

mod key;
pub(crate) mod migrations;
pub mod versions;

use crate::resource::CatalogResource;
use crate::{CatalogError, Result};

const SOFT_DELETION_TIME_FORMAT: &str = "%Y%m%dT%H%M%S";

pub const INTERNAL_DB_NAME: &str = "_internal";

pub const TIME_COLUMN_NAME: &str = "time";
pub const CHUNK_ORDER_COLUMN_NAME: &str = "__chunk_order";

/// List of reserved column names that cannot be used as user-defined columns
pub const RESERVED_COLUMN_NAMES: &[&str] = &[TIME_COLUMN_NAME, CHUNK_ORDER_COLUMN_NAME];

const DEFAULT_OPERATOR_TOKEN_NAME: &str = "_admin";

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

/// General purpose type for storing a collection of things in the catalog
///
/// Each item in the repository has a unique identifier and name. The repository tracks the next
/// identifier that will be used for a new resource added to the repository, with the assumption
/// that identifiers are monotonically increasing unsigned integers.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Repository<I: CatalogId, R: CatalogResource> {
    /// Store for items in the repository
    pub(crate) repo: SerdeVecMap<I, Arc<R>>,
    /// Bi-directional map of identifiers to names in the repository
    pub(crate) id_name_map: BiHashMap<I, Arc<str>>,
    /// The next identifier that will be used when a new resource is added to the repository
    pub(crate) next_id: I,
}

impl<I: CatalogId, R: CatalogResource> Repository<I, R> {
    pub fn new() -> Self {
        Self {
            repo: SerdeVecMap::new(),
            id_name_map: BiHashMap::new(),
            next_id: I::default(),
        }
    }

    pub(crate) fn get_and_increment_next_id(&mut self) -> I {
        let next_id = self.next_id;
        self.next_id = self.next_id.next();
        next_id
    }

    pub(crate) fn next_id(&self) -> I {
        self.next_id
    }

    pub(crate) fn set_next_id(&mut self, id: I) {
        self.next_id = id;
    }

    pub fn name_to_id(&self, name: &str) -> Option<I> {
        self.id_name_map.get_by_right(name).copied()
    }

    pub fn id_to_name(&self, id: &I) -> Option<Arc<str>> {
        self.id_name_map.get_by_left(id).cloned()
    }

    pub fn get_by_name(&self, name: &str) -> Option<Arc<R>> {
        self.id_name_map
            .get_by_right(name)
            .and_then(|id| self.repo.get(id))
            .cloned()
    }

    pub fn get_mut_by_name(&mut self, name: &str) -> Option<&mut Arc<R>> {
        self.id_name_map
            .get_by_right(name)
            .and_then(|id| self.repo.get_mut(id))
    }

    pub fn get_by_id(&self, id: &I) -> Option<Arc<R>> {
        self.repo.get(id).cloned()
    }

    pub fn get_mut_by_id(&mut self, id: &I) -> Option<&mut Arc<R>> {
        self.repo.get_mut(id)
    }

    pub fn contains_id(&self, id: &I) -> bool {
        self.repo.contains_key(id)
    }

    pub fn contains_name(&self, name: &str) -> bool {
        self.id_name_map.contains_right(name)
    }

    pub fn len(&self) -> usize {
        self.repo.len()
    }

    pub fn is_empty(&self) -> bool {
        self.repo.is_empty()
    }

    /// Check if a resource exists in the repository by `id`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn id_exists(&self, id: &I) -> bool {
        let id_in_map = self.id_name_map.contains_left(id);
        let id_in_repo = self.repo.contains_key(id);
        assert_eq!(
            id_in_map, id_in_repo,
            "id map and repository are in an inconsistent state, \
            in map: {id_in_map}, in repo: {id_in_repo}"
        );
        id_in_repo
    }

    /// Check if a resource exists in the repository by `id` and `name`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn id_and_name_exists(&self, id: &I, name: &str) -> bool {
        let name_in_map = self.id_name_map.contains_right(name);
        self.id_exists(id) && name_in_map
    }

    /// Insert a new resource to the repository
    pub(crate) fn insert(&mut self, id: I, resource: impl Into<Arc<R>>) -> Result<()> {
        let resource = resource.into();
        if self.id_and_name_exists(&id, resource.name().as_ref()) {
            return Err(CatalogError::AlreadyExists);
        }
        self.id_name_map.insert(id, resource.name());
        self.repo.insert(id, resource);
        self.next_id = match self.next_id.cmp(&id) {
            // If id is has reached MAX, we can't increment it.
            //
            // Invariants of this data type prevent duplicate IDs, and consumers are expected
            // to handle I::MAX gracefully to avoid runtime panics.
            Ordering::Less | Ordering::Equal if id < I::MAX => id.next(),
            _ => self.next_id,
        };
        Ok(())
    }

    /// Update an existing resource in the repository
    pub(crate) fn update(&mut self, id: I, resource: impl Into<Arc<R>>) -> Result<()> {
        let resource = resource.into();
        if !self.id_exists(&id) {
            return Err(CatalogError::NotFound);
        }
        self.id_name_map.insert(id, resource.name());
        self.repo.insert(id, resource);
        Ok(())
    }

    pub(crate) fn remove(&mut self, id: &I) {
        self.id_name_map.remove_by_left(id);
        self.repo.shift_remove(id);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&I, &Arc<R>)> {
        self.repo.iter()
    }

    pub fn id_iter(&self) -> impl Iterator<Item = &I> {
        self.repo.keys()
    }

    pub fn resource_iter(&self) -> impl Iterator<Item = &Arc<R>> {
        self.repo.values()
    }
}

impl<I: CatalogId, R: CatalogResource> Default for Repository<I, R> {
    fn default() -> Self {
        Self::new()
    }
}

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

    pub(crate) fn hash_to_info(&self, hash: Vec<u8>) -> Option<Arc<TokenInfo>> {
        let id = self
            .hash_lookup_map
            .get_by_right(&hash)
            .map(|id| id.to_owned())?;
        self.repo.get_by_id(&id)
    }

    pub(crate) fn add_token(&mut self, token_id: TokenId, token_info: TokenInfo) -> Result<()> {
        self.hash_lookup_map
            .insert(token_id, token_info.hash.clone());
        self.repo.insert(token_id, token_info)?;
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

    pub(crate) fn delete_token(&mut self, token_name: String) -> Result<()> {
        let token_id = self
            .repo
            .name_to_id(&token_name)
            .ok_or_else(|| CatalogError::NotFound)?;
        self.repo.remove(&token_id);
        self.hash_lookup_map.remove_by_left(&token_id);
        Ok(())
    }
}

impl CatalogResource for TokenInfo {
    type Identifier = TokenId;

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
