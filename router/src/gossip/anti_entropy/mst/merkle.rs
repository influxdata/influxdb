//! Maintain a [Merkle Search Tree] covering the content of a
//! [`NamespaceCache`].
//!
//! [Merkle Search Tree]: https://inria.hal.science/hal-02303490

use std::sync::Arc;

use async_trait::async_trait;
use data_types::{NamespaceName, NamespaceSchema};

use crate::namespace_cache::{ChangeStats, NamespaceCache};

use super::handle::AntiEntropyHandle;

/// A [`NamespaceCache`] decorator that maintains a content hash / consistency
/// proof.
///
/// This [`MerkleTree`] tracks the content of the underlying [`NamespaceCache`]
/// delegate, maintaining a compact, serialisable representation in a
/// [`MerkleSearchTree`] that can be used to perform efficient differential
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
///
/// [`MerkleSearchTree`]: merkle_search_tree::MerkleSearchTree
#[derive(Debug)]
pub struct MerkleTree<T> {
    inner: T,

    handle: AntiEntropyHandle,
}

impl<T> MerkleTree<T>
where
    T: Send + Sync,
{
    /// Initialise a new [`MerkleTree`] that generates a content hash covering
    /// `inner`.
    pub fn new(inner: T, handle: AntiEntropyHandle) -> Self {
        Self { inner, handle }
    }

    /// Return a 128-bit hash describing the content of the inner `T`.
    ///
    /// This hash only covers a subset of schema fields (see
    /// [`NamespaceContentHash`]).
    pub async fn content_hash(&self) -> merkle_search_tree::digest::RootHash {
        self.handle.content_hash().await
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

        // Attempt to asynchronously update the MST.
        self.handle.observe_update(namespace.clone());

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

    use data_types::{
        partition_template::test_table_partition_override, ColumnId, ColumnsByName, NamespaceId,
        TableId, TableSchema,
    };
    use proptest::prelude::*;

    proptest! {
        /// Assert the [`NamespaceContentHash`] decorator results in hashes that
        /// are equal iff the tables and namespace ID match.
        ///
        /// All other fields may vary without affecting the hash.
        #[test]
        fn prop_content_hash_coverage(
            mut a in arbitrary_namespace_schema(Just(1)),
            b in arbitrary_namespace_schema(Just(1))
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

        /// A fixture test that asserts the content hash of a given
        /// [`NamespaceSchema`] does not change.
        ///
        /// This uses randomised inputs for fields that do not form part of the
        /// content hash, proving they're not used, and fixes fields that do
        /// form the hash to assert a static value given static content hash
        /// inputs.
        #[test]
        fn prop_content_hash_fixture(
            mut ns in arbitrary_namespace_schema(Just(42))
        ) {
            ns.tables = [(
                "bananas".to_string(),
                TableSchema {
                    id: TableId::new(24),
                    columns: ColumnsByName::new([data_types::Column {
                        name: "platanos".to_string(),
                        column_type: data_types::ColumnType::String,
                        id: ColumnId::new(2442),
                        table_id: TableId::new(24),
                    }]),
                    partition_template: test_table_partition_override(vec![
                        data_types::partition_template::TemplatePart::TagValue("bananatastic"),
                    ]),
                },
            )]
            .into_iter()
            .collect();

            let wrap_ns = NamespaceContentHash(&ns);

            // If this assert fails, the content hash for a given representation
            // has changed, and this will cause peers to consider each other
            // completely inconsistent regardless of actual content.
            //
            // You need to be careful about doing this!
            assert_eq!(hash(wrap_ns), 13889074233458619864);
        }
    }

    fn hash(v: impl std::hash::Hash) -> u64 {
        let mut hasher = DefaultHasher::default();
        v.hash(&mut hasher);
        hasher.finish()
    }
}
