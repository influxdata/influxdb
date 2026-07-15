//! General purpose collection type for the catalog.
//!
//! A [`Repository`] stores catalog resources keyed by a unique identifier and
//! name, and tracks the next identifier to use for new resources.

use std::cmp::Ordering;
use std::sync::Arc;

use ahash::RandomState as AHashBuilder;
use influxdb3_id::{CatalogId, SerdeVecMap};

use crate::CatalogError;
use crate::catalog::BiHashMap;
use crate::resource::CatalogResource;

/// Error returned by [`Repository`] operations.
///
/// Carries the resource label (from [`CatalogResource::CATEGORY`]) and the
/// offending `id` so the error message is self-identifying without requiring
/// callers to add context.
#[derive(thiserror::Error)]
pub enum RepositoryError<I: CatalogId> {
    #[error("{resource} with id {id} already exists")]
    AlreadyExists { resource: &'static str, id: I },

    #[error("{resource} with name '{name}' already exists")]
    AlreadyExistsByName {
        resource: &'static str,
        name: String,
    },

    #[error("{resource} with id {id} not found")]
    NotFound { resource: &'static str, id: I },

    #[error("{resource} with name '{name}' not found")]
    NotFoundByName {
        resource: &'static str,
        name: String,
    },
}

// Manual `Debug` impl so the enum does not impose a `Debug` bound on `I` ---
// every concrete [`CatalogId`] in this crate derives `Debug`, but the trait
// itself does not require it.
impl<I: CatalogId> std::fmt::Debug for RepositoryError<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists { resource, id } => f
                .debug_struct("AlreadyExists")
                .field("resource", resource)
                .field("id", &format_args!("{id}"))
                .finish(),
            Self::AlreadyExistsByName { resource, name } => f
                .debug_struct("AlreadyExistsByName")
                .field("resource", resource)
                .field("name", name)
                .finish(),
            Self::NotFound { resource, id } => f
                .debug_struct("NotFound")
                .field("resource", resource)
                .field("id", &format_args!("{id}"))
                .finish(),
            Self::NotFoundByName { resource, name } => f
                .debug_struct("NotFoundByName")
                .field("resource", resource)
                .field("name", name)
                .finish(),
        }
    }
}

impl<I: CatalogId> From<RepositoryError<I>> for CatalogError {
    fn from(e: RepositoryError<I>) -> Self {
        match e {
            RepositoryError::AlreadyExists { .. } | RepositoryError::AlreadyExistsByName { .. } => {
                Self::AlreadyExists
            }
            RepositoryError::NotFound { resource, id } => {
                Self::NotFound(format!("{resource}: {id}"))
            }
            RepositoryError::NotFoundByName { resource, name } => {
                Self::NotFound(format!("{resource}: {name}"))
            }
        }
    }
}

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

impl<I: CatalogId, R: CatalogResource> Default for Repository<I, R> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: CatalogId, R: CatalogResource> Repository<I, R> {
    pub fn new() -> Self {
        Self {
            repo: SerdeVecMap::new(),
            id_name_map: bimap::BiHashMap::with_hashers(
                AHashBuilder::default(),
                AHashBuilder::default(),
            ),
            next_id: I::default(),
        }
    }

    /// Remove all resources from the repository.
    ///
    /// This does not reset `next_id` — IDs are never reused.
    pub(crate) fn clear(&mut self) {
        self.id_name_map.clear();
        self.repo.clear();
    }

    /// Get a resource by id, or return `NotFound` if it does not exist.
    pub fn require_by_id(&self, id: &I) -> Result<Arc<R>, RepositoryError<I>> {
        self.get_by_id(id).ok_or(RepositoryError::NotFound {
            resource: R::CATEGORY,
            id: *id,
        })
    }

    /// Mutate an existing resource by `id`, keeping the id↔name map consistent.
    ///
    /// Clones the resource out, hands `f` a `&mut R` (via `Arc::make_mut`), and
    /// writes the result back via [`update`](Self::update) only if `f` succeeds
    /// — so an error from `f` leaves the repository untouched. Returns `f`'s
    /// value. Renaming the resource onto a name another resource already holds
    /// fails with `AlreadyExistsByName` and writes nothing.
    pub fn modify_by_id<T, E>(
        &mut self,
        id: &I,
        f: impl FnOnce(&mut R) -> Result<T, E>,
    ) -> Result<T, E>
    where
        E: From<RepositoryError<I>>,
    {
        let mut resource = self.require_by_id(id)?;
        let output = f(Arc::make_mut(&mut resource))?;
        self.update(*id, resource)?;
        Ok(output)
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

    pub fn get_ref_by_id(&self, id: &I) -> Option<&Arc<R>> {
        self.repo.get(id)
    }

    pub fn get_by_id(&self, id: &I) -> Option<Arc<R>> {
        self.repo.get(id).cloned()
    }

    /// Mutable handle to the stored `Arc` that bypasses the id↔name map.
    ///
    /// Crate-internal only: renaming through this handle desyncs the bimap.
    /// Prefer [`modify_by_id`](Self::modify_by_id); reach for this only when a
    /// caller needs in-place, unique-owner mutation that the clone-out in
    /// `modify_by_id` would defeat.
    pub(crate) fn get_mut_by_id(&mut self, id: &I) -> Option<&mut Arc<R>> {
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
    pub(crate) fn insert(
        &mut self,
        id: I,
        resource: impl Into<Arc<R>>,
    ) -> Result<(), RepositoryError<I>> {
        let resource = resource.into();
        if self.id_and_name_exists(&id, resource.name().as_ref()) {
            return Err(RepositoryError::AlreadyExists {
                resource: R::CATEGORY,
                id,
            });
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

    /// Update an existing resource in the repository.
    ///
    /// Returns `NotFound` if `id` is absent, or `AlreadyExistsByName` if the
    /// resource was renamed onto a name another resource already holds. On
    /// error the repository is left unchanged.
    pub(crate) fn update(
        &mut self,
        id: I,
        resource: impl Into<Arc<R>>,
    ) -> Result<(), RepositoryError<I>> {
        let resource = resource.into();
        if !self.id_exists(&id) {
            return Err(RepositoryError::NotFound {
                resource: R::CATEGORY,
                id,
            });
        }
        // Guard the id↔name bijection: if the (possibly renamed) resource's name
        // is already held by a different id, `id_name_map.insert` would evict
        // that id's name entry and leave it in `repo` with no name — a desync
        // that later trips `id_exists`. Reject the rename instead.
        let name = resource.name();
        if self
            .id_name_map
            .get_by_right(name.as_ref())
            .is_some_and(|owner| *owner != id)
        {
            return Err(RepositoryError::AlreadyExistsByName {
                resource: R::CATEGORY,
                name: name.to_string(),
            });
        }
        self.id_name_map.insert(id, name);
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

#[cfg(test)]
mod tests;
