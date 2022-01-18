use std::fmt::Debug;

use data_types::DatabaseName;

/// A [`Sharder`] implementation is responsible for mapping an opaque payload
/// for a given table name & namespace to an output type.
///
/// [`Sharder`] instances can be generic over any payload type (in which case,
/// the implementation operates exclusively on the table name and/or namespace)
/// or they can be implemented for, and inspect, a specific payload type while
/// sharding.
///
/// NOTE: It is a system invariant that deletes are routed to (all of) the same
/// sequencers as a write for the same table.
pub trait Sharder<P>: Debug + Send + Sync {
    /// The type returned by a sharder.
    ///
    /// This could be a shard ID, a sequencer, an array of multiple sequencers,
    /// etc.
    type Item: Debug + Send + Sync;

    /// Map the specified `payload` to a shard.
    fn shard(&self, table: &str, namespace: &DatabaseName<'_>, payload: &P) -> &Self::Item;
}
