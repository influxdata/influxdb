//! Snapshot definition for root
use bytes::Bytes;
use generated_types::influxdata::iox::catalog_cache::v1 as proto;
use snafu::{ResultExt, Snafu};

use crate::{Namespace, NamespaceId, Timestamp};

use super::{
    hash::{HashBuckets, HashBucketsEncoder},
    list::{GetId, MessageList, SortedById},
};

/// Error for [`RootSnapshot`]
#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum Error {
    #[snafu(display("Error decoding namespace names: {source}"))]
    NamespaceNamesDecode {
        source: crate::snapshot::hash::Error,
    },

    #[snafu(display("Error encoding namespaces: {source}"))]
    NamespaceEncode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error decoding namespaces: {source}"))]
    NamespaceDecode {
        source: crate::snapshot::list::Error,
    },
}

/// Result for [`RootSnapshot`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl GetId for proto::RootNamespace {
    fn id(&self) -> i64 {
        self.id
    }
}

/// A snapshot of root.
///
/// # Soft Deletion
/// This snapshot also contains soft-deleted namespaces.
#[derive(Debug, Clone)]
pub struct RootSnapshot {
    /// List of encoded namespaces sorted by ID.
    namespaces: MessageList<proto::RootNamespace>,
    /// Hash table of namespaces keyed by name. Does not include soft-deleted entries.
    namespace_names: HashBuckets,
    generation: u64,
}

impl RootSnapshot {
    /// Create a new [`RootSnapshot`] from the provided state
    pub fn encode(
        namespaces: impl IntoIterator<Item = Namespace>,
        generation: u64,
    ) -> Result<Self> {
        let namespaces: SortedById<_> = namespaces
            .into_iter()
            .map(|ns| proto::RootNamespace {
                id: ns.id.get(),
                name: ns.name.into(),
                deleted_at: ns.deleted_at.as_ref().map(Timestamp::get),
            })
            .collect();

        let mut namespace_names = HashBucketsEncoder::new(namespaces.len());
        for (index, ns) in namespaces.iter().enumerate() {
            // exclude soft-deleted entries from name table
            if ns.deleted_at.is_none() {
                namespace_names.push(&ns.name, index as u32);
            }
        }

        Ok(Self {
            namespaces: MessageList::encode(namespaces).context(NamespaceEncodeSnafu)?,
            namespace_names: namespace_names.finish(),
            generation,
        })
    }

    /// Create a new [`RootSnapshot`] from a `proto` and generation
    pub fn decode(proto: proto::Root, generation: u64) -> Result<Self> {
        Ok(Self {
            namespaces: MessageList::from(proto.namespaces.unwrap_or_default()),
            namespace_names: proto
                .namespace_names
                .unwrap_or_default()
                .try_into()
                .context(NamespaceNamesDecodeSnafu)?,
            generation,
        })
    }

    /// Returns an iterator of the [`RootSnapshotNamespace`]s in this root snapshot
    pub fn namespaces(&self) -> impl Iterator<Item = Result<RootSnapshotNamespace>> + '_ {
        (0..self.namespaces.len()).map(|idx| {
            let t = self.namespaces.get(idx).context(NamespaceDecodeSnafu)?;
            Ok(t.into())
        })
    }

    /// Look up a [`RootSnapshotNamespace`] by `NamespaceId` using binary search of the list of
    /// namespaces. _Does_ include soft-deleted entries.
    ///
    /// Hard-deleted namespaces may still appear in the namespace cache, but should NOT appear in
    /// the root cache, so this method must be used to check actual presence or absence before
    /// looking up additional namespace information in the namespace cache.
    ///
    /// # Performance
    ///
    /// This method decodes each record the binary search needs to check, so may not be appropriate
    /// for performance-sensitive use cases.
    pub fn lookup_namespace_by_id(&self, id: NamespaceId) -> Result<Option<RootSnapshotNamespace>> {
        // This requires that the namespaces are sorted by ID, which `encode` does.
        Ok(self
            .namespaces
            .get_by_id(id.get())
            .context(NamespaceDecodeSnafu)?
            .map(|ns| ns.into()))
    }

    /// Lookup a [`RootSnapshotNamespace`] by name. Does not include deleted entries.
    pub fn lookup_namespace_by_name(&self, name: &str) -> Result<Option<RootSnapshotNamespace>> {
        for idx in self.namespace_names.lookup(name.as_bytes()) {
            let ns = self.namespaces.get(idx).context(NamespaceDecodeSnafu)?;
            if ns.name == name.as_bytes() {
                return Ok(Some(ns.into()));
            }
        }
        Ok(None)
    }

    /// Returns the generation of this snapshot
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Namespace information stored within [`RootSnapshot`]
#[derive(Debug)]
pub struct RootSnapshotNamespace {
    id: NamespaceId,
    name: Bytes,
    deleted_at: Option<Timestamp>,
}

impl RootSnapshotNamespace {
    /// Returns the [`NamespaceId`] for this namespace
    pub fn id(&self) -> NamespaceId {
        self.id
    }

    /// Returns the name for this namespace
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Returns the timestamp when the namespace was marked for deletion
    pub fn deleted_at(&self) -> Option<Timestamp> {
        self.deleted_at
    }
}

impl From<proto::RootNamespace> for RootSnapshotNamespace {
    fn from(value: proto::RootNamespace) -> Self {
        Self {
            id: NamespaceId::new(value.id),
            name: value.name,
            deleted_at: value.deleted_at.map(Timestamp::new),
        }
    }
}

impl From<RootSnapshot> for proto::Root {
    fn from(value: RootSnapshot) -> Self {
        Self {
            namespaces: Some(value.namespaces.into()),
            namespace_names: Some(value.namespace_names.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_namespace_by_id() {
        let ns_1 = Namespace {
            id: NamespaceId::new(1),
            name: "Namespace1".into(),
            deleted_at: None,
            max_columns_per_table: Default::default(),
            max_tables: Default::default(),
            partition_template: Default::default(),
            retention_period_ns: Default::default(),
            router_version: Default::default(),
            created_at: Default::default(),
        };
        // Deliberately don't include an `ns_2` with `NamespaceId` 2 to test that indices aren't
        // being conflated with `NamespaceId`s.
        // `ns_3` happens to be soft-deleted; it is still accessible by ID.
        let ns_3 = Namespace {
            id: NamespaceId::new(3),
            name: "Namespace3".into(),
            deleted_at: Some(Timestamp::new(1)),
            ..ns_1.clone()
        };

        // IDs happen to be in order
        let root = RootSnapshot::encode(vec![ns_1.clone(), ns_3.clone()], 1).unwrap();
        assert_eq!(
            ns_1.id,
            root.lookup_namespace_by_id(ns_1.id).unwrap().unwrap().id()
        );
        assert_eq!(
            ns_3.id,
            root.lookup_namespace_by_id(ns_3.id).unwrap().unwrap().id()
        );
        assert!(
            root.lookup_namespace_by_id(NamespaceId::new(2))
                .unwrap()
                .is_none()
        );
        assert!(
            root.lookup_namespace_by_id(NamespaceId::new(4))
                .unwrap()
                .is_none()
        );

        // IDs happen to be out of order
        let root = RootSnapshot::encode(vec![ns_3.clone(), ns_1.clone()], 2).unwrap();
        assert_eq!(
            ns_1.id,
            root.lookup_namespace_by_id(ns_1.id).unwrap().unwrap().id()
        );
        assert_eq!(
            ns_3.id,
            root.lookup_namespace_by_id(ns_3.id).unwrap().unwrap().id()
        );
        assert!(
            root.lookup_namespace_by_id(NamespaceId::new(2))
                .unwrap()
                .is_none()
        );
        assert!(
            root.lookup_namespace_by_id(NamespaceId::new(4))
                .unwrap()
                .is_none()
        );
    }
}
