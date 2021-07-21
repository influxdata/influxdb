//! Tooling to capture the current playback state of a database.
//!
//! Checkpoint data declared in this module is usually written during persistence. The data is then used when the server
//! starts up and orchestrates replays.
//!
//! # Example
//! These examples show how the tooling in this module can be integrated into a larger application. They are -- as the
//! title of this section suggest -- examples and the real application might look different and also to deal with more
//! complicated edge cases and error handling.
//!
//! ## Persistence
//! First let us see how checkpoints are persisted:
//!
//! ```
//! use std::sync::{Arc, RwLock};
//! use persistence_windows::checkpoint::{PersistCheckpointBuilder, PartitionCheckpoint};
//!
//! # // mocking for the example below
//! # use chrono::Utc;
//! #
//! # struct Partition {
//! #     id: u32,
//! # }
//! #
//! # impl Partition {
//! #     fn id(&self) -> u32 {
//! #         self.id
//! #     }
//! #
//! #     fn prepare_persist(&mut self) -> FlushHandle {
//! #         FlushHandle {}
//! #     }
//! #
//! #     fn get_checkpoint(&self) -> PartitionCheckpoint {
//! #         PartitionCheckpoint::new(
//! #             Arc::from("table"),
//! #             Arc::from("part"),
//! #             Default::default(),
//! #             Utc::now(),
//! #         )
//! #     }
//! # }
//! #
//! # struct FlushHandle {}
//! #
//! # impl FlushHandle {
//! #     fn checkpoint(&self) -> PartitionCheckpoint {
//! #          PartitionCheckpoint::new(
//! #             Arc::from("table"),
//! #             Arc::from("part"),
//! #             Default::default(),
//! #             Utc::now(),
//! #         )
//! #     }
//! # }
//! #
//! #
//! # struct Db {
//! #     partitions: Vec<Arc<RwLock<Partition>>>,
//! # }
//! #
//! # impl Db {
//! #     fn get_persistable_partition(&self) -> Arc<RwLock<Partition>> {
//! #         Arc::clone(&self.partitions[0])
//! #     }
//! #
//! #     fn partitions(&self) -> Vec<Arc<RwLock<Partition>>> {
//! #         self.partitions.clone()
//! #     }
//! # }
//! #
//! # let db = Db{
//! #     partitions: vec![
//! #         Arc::new(RwLock::new(Partition{id: 1})),
//! #         Arc::new(RwLock::new(Partition{id: 2})),
//! #     ],
//! # };
//! #
//! // get a partition that we wanna persist
//! let to_persist: Arc<RwLock<Partition>> = db.get_persistable_partition();
//!
//! // get partition write lock
//! let mut write_guard = to_persist.write().unwrap();
//!
//! // prepare the persistence transaction
//! let handle = write_guard.prepare_persist();
//!
//! // remember to-be-persisted partition ID
//! let id = write_guard.id();
//!
//! // drop write guard - flush handle ensures the persistence windows
//! // are protected from modification
//! std::mem::drop(write_guard);
//!
//! // Perform other operations, e.g. split
//!
//! // Compute checkpoint
//! let mut builder = PersistCheckpointBuilder::new(handle.checkpoint());
//! for partition in db.partitions() {
//!     // get read guard
//!     let read_guard = partition.read().unwrap();
//!
//!     // check if this is the partition that we are about to persist
//!     if read_guard.id() != id {
//!         // this is another partition
//!         // fold in checkpoint
//!         builder.register_other_partition(&read_guard.get_checkpoint());
//!     }
//! }
//!
//! // checkpoints can now be persisted alongside the parquet files
//! let (partition_checkpoint, database_checkpoint) = builder.build();
//! ```
//!
//! ## Replay
//! Here is an example on how to organize replay:
//!
//! ```
//! use persistence_windows::checkpoint::ReplayPlanner;
//!
//! # // mocking for the example below
//! # use std::sync::Arc;
//! # use chrono::Utc;
//! # use persistence_windows::checkpoint::{DatabaseCheckpoint, PartitionCheckpoint, PersistCheckpointBuilder};
//! #
//! # struct File {}
//! #
//! # impl File {
//! #     fn extract_partition_checkpoint(&self) -> PartitionCheckpoint {
//! #         PartitionCheckpoint::new(
//! #             Arc::from("table"),
//! #             Arc::from("part"),
//! #             Default::default(),
//! #             Utc::now(),
//! #         )
//! #     }
//! #
//! #     fn extract_database_checkpoint(&self) -> DatabaseCheckpoint {
//! #         let builder = PersistCheckpointBuilder::new(self.extract_partition_checkpoint());
//! #         builder.build().1
//! #     }
//! # }
//! #
//! # struct Catalog {
//! #     files: Vec<File>,
//! # }
//! #
//! # impl Catalog {
//! #     fn files(&self) -> &[File] {
//! #         &self.files
//! #     }
//! # }
//! #
//! # let catalog = Catalog {
//! #     files: vec![
//! #         File {},
//! #         File {},
//! #     ],
//! # };
//! #
//! # struct Sequencer {
//! #     id: u32,
//! # }
//! #
//! # impl Sequencer {
//! #     fn id(&self) -> u32 {
//! #         self.id
//! #     }
//! # }
//! #
//! # let sequencers = vec![
//! #     Sequencer {id: 1},
//! #     Sequencer {id: 2},
//! # ];
//! #
//! // create planner object that receives all relevant checkpoints
//! let mut planner = ReplayPlanner::new();
//!
//! // scan preserved catalog
//! // Note: While technically we only need to scan the last parquet file per partition,
//! //       it is totally valid to scan the whole catalog.
//! for file in catalog.files() {
//!     planner.register_partition_checkpoint(&file.extract_partition_checkpoint());
//!     planner.register_database_checkpoint(&file.extract_database_checkpoint());
//! }
//!
//! // create replay plan
//! let plan = planner.build().unwrap();
//!
//! // replay all sequencers
//! for sequencer in sequencers {
//!     // check if replay is required
//!     if let Some(min_max) = plan.replay_range(sequencer.id()) {
//!         // do actual replay...
//!     }
//! }
//!
//! // database is now ready for normal playback
//! ```
use std::{
    collections::{
        btree_map::Entry::{Occupied, Vacant},
        BTreeMap,
    },
    sync::Arc,
};

use chrono::{DateTime, Utc};
use snafu::Snafu;

use crate::min_max_sequence::MinMaxSequence;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Did not find a DatabaseCheckpoint for sequencer {}", sequencer_id))]
    NoDatabaseCheckpointFound { sequencer_id: u32 },

    #[snafu(display("Minimum sequence number in partition checkpoint ({}) is lower than database-wide number ({}) for partition {}:{}", partition_checkpoint_sequence_number, database_checkpoint_sequence_number, table_name, partition_key))]
    PartitionCheckpointMinimumBeforeDatabase {
        partition_checkpoint_sequence_number: u64,
        database_checkpoint_sequence_number: u64,
        table_name: Arc<str>,
        partition_key: Arc<str>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Immutable record of the playback state for a single partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionCheckpoint {
    /// Table of the partition.
    table_name: Arc<str>,

    /// Partition key.
    partition_key: Arc<str>,

    /// Maps `sequencer_id` to the minimum and maximum sequence numbers seen.
    sequencer_numbers: BTreeMap<u32, MinMaxSequence>,

    /// Minimum unpersisted timestamp.
    min_unpersisted_timestamp: DateTime<Utc>,
}

impl PartitionCheckpoint {
    /// Create new checkpoint.
    pub fn new(
        table_name: Arc<str>,
        partition_key: Arc<str>,
        sequencer_numbers: BTreeMap<u32, MinMaxSequence>,
        min_unpersisted_timestamp: DateTime<Utc>,
    ) -> Self {
        Self {
            table_name,
            partition_key,
            sequencer_numbers,
            min_unpersisted_timestamp,
        }
    }

    /// Table of the partition.
    pub fn table_name(&self) -> &Arc<str> {
        &self.table_name
    }

    /// Partition key.
    pub fn partition_key(&self) -> &Arc<str> {
        &self.partition_key
    }

    /// Maps `sequencer_id` to the minimum and maximum sequence numbers seen.
    ///
    /// Will return `None` if the sequencer was not yet seen in which case there is not need to replay data from this
    /// sequencer for this partition.
    pub fn sequencer_numbers(&self, sequencer_id: u32) -> Option<MinMaxSequence> {
        self.sequencer_numbers.get(&sequencer_id).copied()
    }

    /// Sorted list of sequencer IDs that are included in this checkpoint.
    pub fn sequencer_ids(&self) -> Vec<u32> {
        self.sequencer_numbers.keys().copied().collect()
    }

    /// Iterate over sequencer numbers.
    pub fn sequencer_numbers_iter(&self) -> impl Iterator<Item = (u32, MinMaxSequence)> + '_ {
        self.sequencer_numbers
            .iter()
            .map(|(sequencer_id, min_max)| (*sequencer_id, *min_max))
    }

    /// Minimum unpersisted timestamp.
    pub fn min_unpersisted_timestamp(&self) -> DateTime<Utc> {
        self.min_unpersisted_timestamp
    }
}

/// Immutable record of the playback state for the whole database.
///
/// This effectively contains the minimum sequence numbers over the whole database that are the starting point for replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseCheckpoint {
    /// Maps `sequencer_id` to the minimum and maximum sequence numbers seen.
    sequencer_numbers: BTreeMap<u32, MinMaxSequence>,
}

impl DatabaseCheckpoint {
    /// Create new database checkpoint.
    ///
    /// **This should only rarely be be used directly. Consider using [`PersistCheckpointBuilder`] to collect
    /// database-wide checkpoints!**
    pub fn new(sequencer_numbers: BTreeMap<u32, MinMaxSequence>) -> Self {
        Self { sequencer_numbers }
    }

    /// Get sequence number range that should be used during replay of the given sequencer.
    ///
    /// This will return `None` for unknown sequencer. This might have multiple reasons, e.g. in case of Apache Kafka it
    /// might be that a partition has not delivered any data yet (for a quite young database) or that the partitioning
    /// was wrongly reconfigured (to more partitions in which case the ordering would be wrong). The latter case MUST be
    /// detected by another layer, e.g. using persisted database rules. The reaction to `None` in this layer should be
    /// "no replay required for this sequencer, just continue with normal playback".
    pub fn sequencer_number(&self, sequencer_id: u32) -> Option<MinMaxSequence> {
        self.sequencer_numbers.get(&sequencer_id).copied()
    }

    /// Sorted list of sequencer IDs that are included in this checkpoint.
    pub fn sequencer_ids(&self) -> Vec<u32> {
        self.sequencer_numbers.keys().copied().collect()
    }

    /// Iterate over sequencer numbers
    pub fn sequencer_numbers_iter(&self) -> impl Iterator<Item = (u32, MinMaxSequence)> + '_ {
        self.sequencer_numbers
            .iter()
            .map(|(sequencer_id, min_max)| (*sequencer_id, *min_max))
    }
}

/// Builder that helps with recording checkpoints from persistence windows during the persistence phase.
#[derive(Debug)]
pub struct PersistCheckpointBuilder {
    /// Checkpoint for the to-be-persisted partition.
    partition_checkpoint: PartitionCheckpoint,

    /// Database-wide checkpoint.
    ///
    /// Note: This database checkpoint is currently being built and is mutable (in contrast to the immutable result that
    ///       will will in the end present to the API user).
    database_checkpoint: DatabaseCheckpoint,
}

impl PersistCheckpointBuilder {
    /// Create new builder from the persistence window of the to-be-persisted partition.
    pub fn new(partition_checkpoint: PartitionCheckpoint) -> Self {
        // database-wide checkpoint also includes the to-be-persisted partition
        let database_checkpoint = DatabaseCheckpoint {
            sequencer_numbers: partition_checkpoint.sequencer_numbers.clone(),
        };

        Self {
            partition_checkpoint,
            database_checkpoint,
        }
    }

    /// Registers other partition and keeps track of the overall min sequence numbers.
    pub fn register_other_partition(&mut self, partition_checkpoint: &PartitionCheckpoint) {
        for (sequencer_id, min_max) in &partition_checkpoint.sequencer_numbers {
            match self
                .database_checkpoint
                .sequencer_numbers
                .entry(*sequencer_id)
            {
                Vacant(v) => {
                    v.insert(*min_max);
                }
                Occupied(mut o) => {
                    let existing_min_max = o.get_mut();
                    *existing_min_max = MinMaxSequence::new(
                        existing_min_max.min().min(min_max.min()),
                        existing_min_max.max().max(min_max.max()),
                    );
                }
            }
        }
    }

    /// Build immutable checkpoints.
    pub fn build(self) -> (PartitionCheckpoint, DatabaseCheckpoint) {
        let Self {
            partition_checkpoint,
            database_checkpoint,
        } = self;
        (partition_checkpoint, database_checkpoint)
    }
}

/// Plan your sequencer replays after server restarts.
///
/// To plan your replay successfull, you CAN register all [`PartitionCheckpoint`]s and [`DatabaseCheckpoint`]s that are
/// persisted in the catalog. However it is sufficient to only register the last [`PartitionCheckpoint`] for every
/// partition and the last [`DatabaseCheckpoint`] for the whole database.
#[derive(Debug)]
pub struct ReplayPlanner {
    /// Range (inclusive minimum, inclusive maximum) of sequence number to be replayed for each sequencer.
    replay_ranges: BTreeMap<u32, (Option<u64>, u64)>,

    /// Last known partition checkpoint, mapped via table name and partition key.
    last_partition_checkpoints: BTreeMap<(Arc<str>, Arc<str>), PartitionCheckpoint>,
}

impl ReplayPlanner {
    /// Create new empty replay planner.
    pub fn new() -> Self {
        Self {
            replay_ranges: Default::default(),
            last_partition_checkpoints: Default::default(),
        }
    }

    /// Register a partition checkpoint that was found in the catalog.
    pub fn register_partition_checkpoint(&mut self, partition_checkpoint: &PartitionCheckpoint) {
        for (sequencer_id, min_max) in &partition_checkpoint.sequencer_numbers {
            match self.replay_ranges.entry(*sequencer_id) {
                Vacant(v) => {
                    // unknown sequencer => just store max value for now
                    v.insert((None, min_max.max()));
                }
                Occupied(mut o) => {
                    // known sequencer => fold in:
                    // - max value (we take the max of all maximums!)
                    //
                    // We are NOT folding in the min value here since other partitions might not be that far yet.
                    let existing_min_max = o.get_mut();
                    existing_min_max.1 = existing_min_max.1.max(min_max.max());
                }
            }
        }

        match self.last_partition_checkpoints.entry((
            Arc::clone(partition_checkpoint.table_name()),
            Arc::clone(partition_checkpoint.partition_key()),
        )) {
            Vacant(v) => {
                // new partition => insert
                v.insert(partition_checkpoint.clone());
            }
            Occupied(mut o) => {
                // known partition => check which one is the newer checkpoint
                if o.get().min_unpersisted_timestamp
                    < partition_checkpoint.min_unpersisted_timestamp
                {
                    o.insert(partition_checkpoint.clone());
                }
            }
        }
    }

    /// Register a database checkpoint that was found in the catalog.
    pub fn register_database_checkpoint(&mut self, database_checkpoint: &DatabaseCheckpoint) {
        for (sequencer_id, min_max) in &database_checkpoint.sequencer_numbers {
            match self.replay_ranges.entry(*sequencer_id) {
                Vacant(v) => {
                    // unknown sequencer => store min value (we keep the latest value for that one) and max value (in
                    // case when we don't find another partition checkpoint for this sequencer, so we have a fallback)
                    v.insert((Some(min_max.min()), min_max.max()));
                }
                Occupied(mut o) => {
                    // known sequencer => fold in:
                    // - min value (we take the max of all mins!)
                    // - max value (we take the max of all max!)
                    let existing_min_max = o.get_mut();
                    existing_min_max.0 = Some(
                        existing_min_max
                            .0
                            .map_or(min_max.min(), |min2| min2.max(min_max.min())),
                    );
                    existing_min_max.1 = existing_min_max.1.max(min_max.max());
                }
            }
        }
    }

    /// Build plan that is then used for replay.
    pub fn build(self) -> Result<ReplayPlan> {
        let mut replay_ranges = BTreeMap::new();
        for (sequencer_id, min_max) in self.replay_ranges {
            match min_max {
                (Some(min), max) => {
                    replay_ranges.insert(sequencer_id, MinMaxSequence::new(min, max));
                }
                (None, _max) => {
                    // We've got data for this sequencer via a PartitionCheckpoint but did not see corresponding data in
                    // any of the DatabaseCheckpoints. This is clearly a data error, because for the PartitionCheckpoint
                    // in question we should have persisted (and read back) a DatabaseCheckpoint in the very same
                    // Parquet file that contains a value for that sequencer.
                    return Err(Error::NoDatabaseCheckpointFound { sequencer_id });
                }
            }
        }

        // sanity-check partition checkpoints
        for ((table_name, partition_key), partition_checkpoint) in &self.last_partition_checkpoints
        {
            for (sequencer_id, min_max) in &partition_checkpoint.sequencer_numbers {
                let database_wide_min_max = replay_ranges.get(sequencer_id).expect("every partition checkpoint should have resulted in a replay range or an error by now");

                if min_max.min() < database_wide_min_max.min() {
                    return Err(Error::PartitionCheckpointMinimumBeforeDatabase {
                        partition_checkpoint_sequence_number: min_max.min(),
                        database_checkpoint_sequence_number: database_wide_min_max.min(),
                        table_name: Arc::clone(table_name),
                        partition_key: Arc::clone(partition_key),
                    });
                }
            }
        }

        Ok(ReplayPlan {
            replay_ranges,
            last_partition_checkpoints: self.last_partition_checkpoints,
        })
    }
}

impl Default for ReplayPlanner {
    fn default() -> Self {
        Self::new()
    }
}

/// Plan that contains all necessary information to orchastrate a replay.
#[derive(Debug)]
pub struct ReplayPlan {
    /// Replay range (inclusive minimum sequence number, inclusive maximum sequence number) for every sequencer.
    ///
    /// For sequencers not included in this map, no replay is required.
    replay_ranges: BTreeMap<u32, MinMaxSequence>,

    /// Last known partition checkpoint, mapped via table name and partition key.
    last_partition_checkpoints: BTreeMap<(Arc<str>, Arc<str>), PartitionCheckpoint>,
}

impl ReplayPlan {
    /// Get replay range for a sequencer.
    ///
    /// When this returns `None`, no replay is required for this sequencer and we can just start to playback normally.
    pub fn replay_range(&self, sequencer_id: u32) -> Option<MinMaxSequence> {
        self.replay_ranges.get(&sequencer_id).copied()
    }

    /// Get last known partition checkpoint.
    ///
    /// If no partition checkpoint was ever written, `None` will be returned. In that case this partition can be skipped
    /// during replay.
    pub fn last_partition_checkpoint(
        &self,
        table_name: &str,
        partition_key: &str,
    ) -> Option<&PartitionCheckpoint> {
        self.last_partition_checkpoints
            .get(&(Arc::from(table_name), Arc::from(partition_key)))
    }

    /// Sorted list of sequencer IDs that have to be replayed.
    pub fn sequencer_ids(&self) -> Vec<u32> {
        self.replay_ranges.keys().copied().collect()
    }

    /// Sorted list of partitions (by table name and partition key) that have to be replayed.
    pub fn partitions(&self) -> Vec<(Arc<str>, Arc<str>)> {
        self.last_partition_checkpoints.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create [`PartitionCheckpoint`].
    macro_rules! part_ckpt {
        ($table_name:expr, $partition_key:expr, {$($sequencer_number:expr => ($min:expr, $max:expr)),*}) => {
            {
                let mut sequencer_numbers = BTreeMap::new();
                $(
                    sequencer_numbers.insert($sequencer_number, MinMaxSequence::new($min, $max));
                )*

                let min_unpersisted_timestamp = DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(0, 0), Utc);

                PartitionCheckpoint::new(Arc::from($table_name), Arc::from($partition_key), sequencer_numbers, min_unpersisted_timestamp)
            }
        };
    }

    /// Create [`DatabaseCheckpoint`].
    macro_rules! db_ckpt {
        ({$($sequencer_number:expr => ($min:expr, $max:expr)),*}) => {
            {
                let mut sequencer_numbers = BTreeMap::new();
                $(
                    sequencer_numbers.insert($sequencer_number, MinMaxSequence::new($min, $max));
                )*
                DatabaseCheckpoint{sequencer_numbers}
            }
        };
    }

    #[test]
    fn test_partition_checkpoint() {
        let pckpt = part_ckpt!("table_1", "partition_1", {1 => (10, 20), 2 => (5, 15)});

        assert_eq!(pckpt.table_name().as_ref(), "table_1");
        assert_eq!(pckpt.partition_key().as_ref(), "partition_1");
        assert_eq!(
            pckpt.sequencer_numbers(1).unwrap(),
            MinMaxSequence::new(10, 20)
        );
        assert_eq!(
            pckpt.sequencer_numbers(2).unwrap(),
            MinMaxSequence::new(5, 15)
        );
        assert!(pckpt.sequencer_numbers(3).is_none());
        assert_eq!(pckpt.sequencer_ids(), vec![1, 2]);

        assert_eq!(
            pckpt,
            part_ckpt!("table_1", "partition_1", {1 => (10, 20), 2 => (5, 15)})
        );
    }

    #[test]
    fn test_database_checkpoint() {
        let dckpt = db_ckpt!({1 => (10, 20), 2 => (5, 15)});

        assert_eq!(
            dckpt.sequencer_number(1).unwrap(),
            MinMaxSequence::new(10, 20)
        );
        assert_eq!(
            dckpt.sequencer_number(2).unwrap(),
            MinMaxSequence::new(5, 15)
        );
        assert!(dckpt.sequencer_number(3).is_none());
        assert_eq!(dckpt.sequencer_ids(), vec![1, 2]);

        assert_eq!(dckpt, db_ckpt!({1 => (10, 20), 2 => (5, 15)}));
    }

    #[test]
    fn test_persist_checkpoint_builder_no_other() {
        let pckpt_orig = part_ckpt!("table_1", "partition_1", {1 => (10, 20), 2 => (5, 15)});
        let builder = PersistCheckpointBuilder::new(pckpt_orig.clone());

        let (pckpt, dckpt) = builder.build();

        assert_eq!(pckpt, pckpt_orig);
        assert_eq!(dckpt, db_ckpt!({1 => (10, 20), 2 => (5, 15)}));
    }

    #[test]
    fn test_persist_checkpoint_builder_others() {
        let pckpt_orig =
            part_ckpt!("table_1", "partition_1", {1 => (10, 20), 2 => (5, 15), 3 => (15, 26)});
        let mut builder = PersistCheckpointBuilder::new(pckpt_orig.clone());

        builder.register_other_partition(
            &part_ckpt!("table_1", "partition_2", {2 => (2, 16), 3 => (20, 25), 4 => (13, 14)}),
        );

        let (pckpt, dckpt) = builder.build();

        assert_eq!(pckpt, pckpt_orig);
        assert_eq!(
            dckpt,
            db_ckpt!({1 => (10, 20), 2 => (2, 16), 3 => (15, 26), 4 => (13, 14)})
        );
    }

    #[test]
    fn test_replay_planner_empty() {
        let planner = ReplayPlanner::new();
        let plan = planner.build().unwrap();

        assert!(plan.sequencer_ids().is_empty());
        assert!(plan.replay_range(1).is_none());

        assert!(plan.partitions().is_empty());
        assert!(plan
            .last_partition_checkpoint("table_1", "partition_1")
            .is_none());
    }

    #[test]
    fn test_replay_planner_normal() {
        let mut planner = ReplayPlanner::new();

        planner.register_partition_checkpoint(
            &part_ckpt!("table_1", "partition_1", {1 => (15, 19), 2 => (21, 28)}),
        );
        planner.register_database_checkpoint(&db_ckpt!({1 => (10, 19), 2 => (20, 27)}));

        planner.register_partition_checkpoint(
            &part_ckpt!("table_1", "partition_2", {2 => (22, 27), 3 => (35, 39)}),
        );
        planner
            .register_database_checkpoint(&db_ckpt!({1 => (11, 20), 3 => (30, 40), 4 => (40, 50)}));

        let plan = planner.build().unwrap();

        assert_eq!(plan.sequencer_ids(), vec![1, 2, 3, 4]);
        assert_eq!(plan.replay_range(1).unwrap(), MinMaxSequence::new(11, 20));
        assert_eq!(plan.replay_range(2).unwrap(), MinMaxSequence::new(20, 28));
        assert_eq!(plan.replay_range(3).unwrap(), MinMaxSequence::new(30, 40));
        assert_eq!(plan.replay_range(4).unwrap(), MinMaxSequence::new(40, 50));
        assert!(plan.replay_range(5).is_none());

        assert_eq!(
            plan.partitions(),
            vec![
                (Arc::from("table_1"), Arc::from("partition_1")),
                (Arc::from("table_1"), Arc::from("partition_2"))
            ]
        );
        assert_eq!(
            plan.last_partition_checkpoint("table_1", "partition_1")
                .unwrap(),
            &part_ckpt!("table_1", "partition_1", {1 => (15, 19), 2 => (21, 28)})
        );
        assert_eq!(
            plan.last_partition_checkpoint("table_1", "partition_2")
                .unwrap(),
            &part_ckpt!("table_1", "partition_2", {2 => (22, 27), 3 => (35, 39)})
        );
        assert!(plan
            .last_partition_checkpoint("table_1", "partition_3")
            .is_none());
    }

    #[test]
    fn test_replay_planner_fail_missing_database_checkpoint() {
        let mut planner = ReplayPlanner::new();

        planner.register_partition_checkpoint(
            &part_ckpt!("table_1", "partition_1", {1 => (11, 12), 2 => (21, 22)}),
        );
        planner.register_database_checkpoint(&db_ckpt!({1 => (10, 20), 3 => (30, 40)}));

        let err = planner.build().unwrap_err();
        assert!(matches!(err, Error::NoDatabaseCheckpointFound { .. }));
    }

    #[test]
    fn test_replay_planner_fail_minima_out_of_sync() {
        let mut planner = ReplayPlanner::new();

        planner
            .register_partition_checkpoint(&part_ckpt!("table_1", "partition_1", {1 => (10, 12)}));
        planner.register_database_checkpoint(&db_ckpt!({1 => (11, 20)}));

        let err = planner.build().unwrap_err();
        assert!(matches!(
            err,
            Error::PartitionCheckpointMinimumBeforeDatabase { .. }
        ));
    }
}
