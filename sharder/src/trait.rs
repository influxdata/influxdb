use data_types::NamespaceName;
use std::{fmt::Debug, sync::Arc};

/// A [`Sharder`] implementation is responsible for mapping an opaque payload
/// for a given table name & namespace to an output type.
///
/// [`Sharder`] instances can be generic over any payload type (in which case,
/// the implementation operates exclusively on the table name and/or namespace)
/// or they can be implemented for, and inspect, a specific payload type while
/// sharding.
///
/// NOTE: It is a system invariant that deletes are routed to (all of) the same
/// shards as a write for the same table.
pub trait Sharder<P>: Debug + Send + Sync {
    /// The type returned by a sharder.
    ///
    /// This could be a shard ID, a shard index, an array of multiple shards,
    /// etc.
    type Item: Debug + Send + Sync;

    /// Map the specified `payload` to a shard.
    fn shard(&self, table: &str, namespace: &NamespaceName<'_>, payload: &P) -> Self::Item;
}

impl<T, P> Sharder<P> for Arc<T>
where
    T: Sharder<P>,
{
    type Item = T::Item;

    fn shard(&self, table: &str, namespace: &NamespaceName<'_>, payload: &P) -> Self::Item {
        (**self).shard(table, namespace, payload)
    }
}

#[cfg(test)]
mod tests {
    use mutable_batch::MutableBatch;

    use crate::JumpHash;

    use super::*;

    #[test]
    fn test_arc_wrapped_sharder() {
        let hasher: Arc<dyn Sharder<MutableBatch, Item = Arc<u32>>> =
            Arc::new(JumpHash::new((0..10_u32).map(Arc::new)));

        let _ = hasher.shard(
            "table",
            &NamespaceName::try_from("namespace").unwrap(),
            &MutableBatch::default(),
        );
    }
}
