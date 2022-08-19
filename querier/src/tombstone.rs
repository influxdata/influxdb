use data_types::{DeletePredicate, SequenceNumber, ShardId, Tombstone, TombstoneId};
use predicate::delete_predicate::parse_delete_predicate;
use std::sync::Arc;

/// Tombstone as it is handled by the querier.
#[derive(Debug, Clone)]
pub struct QuerierTombstone {
    /// Delete predicate associated with this tombstone.
    delete_predicate: Arc<DeletePredicate>,

    /// Shard that this tombstone affects.
    shard_id: ShardId,

    /// The sequence number assigned to the tombstone from the shard.
    sequence_number: SequenceNumber,

    /// Tombstone ID.
    tombstone_id: TombstoneId,
}

impl QuerierTombstone {
    /// Delete predicate associated with this tombstone.
    pub fn delete_predicate(&self) -> &Arc<DeletePredicate> {
        &self.delete_predicate
    }

    /// Shard that this tombstone affects.
    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// The sequence number assigned to the tombstone from the shard.
    pub fn sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }

    /// Tombstone ID.
    pub fn tombstone_id(&self) -> TombstoneId {
        self.tombstone_id
    }
}

impl From<&Tombstone> for QuerierTombstone {
    fn from(tombstone: &Tombstone) -> Self {
        let delete_predicate = Arc::new(
            parse_delete_predicate(
                &tombstone.min_time.get().to_string(),
                &tombstone.max_time.get().to_string(),
                &tombstone.serialized_predicate,
            )
            .expect("broken delete predicate"),
        );

        Self {
            delete_predicate,
            shard_id: tombstone.shard_id,
            sequence_number: tombstone.sequence_number,
            tombstone_id: tombstone.id,
        }
    }
}

impl From<Arc<Tombstone>> for QuerierTombstone {
    fn from(tombstone: Arc<Tombstone>) -> Self {
        tombstone.as_ref().into()
    }
}
