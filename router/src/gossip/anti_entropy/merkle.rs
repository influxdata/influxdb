//! Maintain a [Merkle Search Tree] covering the content of a
//! [`NamespaceCache`].
//!
//! [Merkle Search Tree]: https://inria.hal.science/hal-02303490

use std::sync::Arc;

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};
use merkle_search_tree::MerkleSearchTree;
use parking_lot::Mutex;

use crate::namespace_cache::{ChangeStats, NamespaceCache};

/// A [`NamespaceCache`] decorator that maintains a content hash / consistency
/// proof.
///
/// This [`MerkleTree`] tracks the content of the underlying [`NamespaceCache`]
/// delegate, maintaining a compact, serialisable representation in a
/// [MerkleSearchTree] that can be used to perform efficient differential
/// convergence (anti-entropy) of peers to provide eventual consistency.
///
/// # Merge Correctness
///
/// The inner [`NamespaceCache`] implementation MUST commutatively &
/// deterministically merge two [`NamespaceSchema`] to converge (monotonically)
/// towards the same result (gossip payloads are CmRDTs).
///
/// # Portability
///
/// This implementation relies on the rust [`Hash`] implementation, which is
/// specifically defined as being allowed to differ across platforms (for
/// example, with differing endianness) and across different Rust complier
/// versions.
///
/// If two nodes are producing differing hashes for the same underlying content,
/// they will appear to never converge.
#[derive(Debug)]
pub struct MerkleTree<T> {
    inner: T,

    mst: Mutex<MerkleSearchTree<NamespaceName<'static>, NamespaceSchema>>,
}

impl<T> MerkleTree<T> {
    /// Initialise a new [`MerkleTree`] that generates a content hash covering
    /// `inner`.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            mst: Mutex::new(MerkleSearchTree::default()),
        }
    }

    /// Return a 128-bit hash describing the content of the inner `T`.
    ///
    /// This hash only covers a subset of schema fields (see
    /// [`NamespaceContentHash`]).
    pub fn content_hash(&self) -> merkle_search_tree::digest::RootHash {
        self.mst.lock().root_hash().clone()
    }
}

#[async_trait]
impl<T> NamespaceCache for MerkleTree<T>
where
    T: NamespaceCache,
{
    type ReadError = T::ReadError;

    async fn get_schema(
        &self,
        namespace: &NamespaceName<'static>,
    ) -> Result<Arc<NamespaceSchema>, Self::ReadError> {
        self.inner.get_schema(namespace).await
    }

    fn put_schema(
        &self,
        namespace: NamespaceName<'static>,
        schema: NamespaceSchema,
    ) -> (Arc<NamespaceSchema>, ChangeStats) {
        // Pass the namespace into the inner storage, and evaluate the merged
        // return value (the new content of the cache).
        let (schema, diff) = self.inner.put_schema(namespace.clone(), schema);

        // Intercept the the resulting cache entry state and merge it into the
        // merkle tree.
        self.mst.lock().upsert(namespace, &schema);

        // And pass through the return value to the caller.
        (schema, diff)
    }
}

/// A [`NamespaceSchema`] decorator that produces a content hash covering fields
/// that SHOULD be converged across gossip peers.
#[derive(Debug)]
struct NamespaceContentHash<'a>(&'a NamespaceSchema);

impl<'a> std::hash::Hash for NamespaceContentHash<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Technically the ID does not need to be covered by the content hash
        // (the namespace name -> namespace ID is immutable and asserted
        // elsewhere) but it's not harmful to include it, and would drive
        // detection of a broken mapping invariant.
        self.0.id.hash(state);

        // The set of tables, and their schemas MUST form part of the content
        // hash as they are part of the content that must be converged.
        self.0.tables.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::hash_map::DefaultHasher, hash::Hasher};

    use super::*;

    use super::super::tests::arbitrary_namespace_schema;

    use data_types::NamespaceId;
    use proptest::prelude::*;

    proptest! {
        /// Assert the [`NamespaceContentHash`] decorator results in hashes that
        /// are equal iff the tables and namespace ID match.
        ///
        /// All other fields may vary without affecting the hash.
        #[test]
        fn prop_content_hash_coverage(
            mut a in arbitrary_namespace_schema(0..1),
            b in arbitrary_namespace_schema(0..1)
        ) {
            assert_eq!(a.id, b.id); // Test invariant

            let wrap_a = NamespaceContentHash(&a);
            let wrap_b = NamespaceContentHash(&b);

            // Invariant: if the schemas are equal, the content hashes match
            if a == b {
                assert_eq!(hash(&wrap_a), hash(&wrap_b));
            }

            // True if the content hashes of a and b are equal.
            let is_hash_eq = hash(wrap_a) == hash(wrap_b);

            // Invariant: if the tables and ID match, the content hashes match
            assert_eq!(
                ((a.tables == b.tables) && (a.id == b.id)),
                is_hash_eq
            );

            // Invariant: the hashes chaange if the ID is modified
            let new_id = NamespaceId::new(a.id.get().wrapping_add(1));
            let hash_old_a = hash(&a);
            a.id = new_id;
            assert_ne!(hash_old_a, hash(a));
        }
    }

    fn hash(v: impl std::hash::Hash) -> u64 {
        let mut hasher = DefaultHasher::default();
        v.hash(&mut hasher);
        hasher.finish()
    }
}
