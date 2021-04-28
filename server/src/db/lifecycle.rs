use std::convert::TryInto;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use observability_deps::tracing::{info, warn};
use parking_lot::{RwLock, RwLockUpgradableReadGuard};

use data_types::{database_rules::LifecycleRules, error::ErrorLogger, job::Job};

use tracker::TaskTracker;

use super::{
    catalog::chunk::{Chunk, ChunkState},
    Db,
};
use data_types::database_rules::SortOrder;

/// Handles the lifecycle of chunks within a Db
pub struct LifecycleManager {
    db: Arc<Db>,
    db_name: String,
    move_task: Option<TaskTracker<Job>>,
    write_task: Option<TaskTracker<Job>>,
}

impl LifecycleManager {
    pub fn new(db: Arc<Db>) -> Self {
        let db_name = db.rules.read().name.clone().into();

        Self {
            db,
            db_name,
            move_task: None,
            write_task: None,
        }
    }

    /// Polls the lifecycle manager to find and spawn work that needs
    /// to be done as determined by the database's lifecycle policy
    ///
    /// Should be called periodically and should spawn any long-running
    /// work onto the tokio threadpool and return
    pub fn check_for_work(&mut self) {
        ChunkMover::check_for_work(self, Utc::now())
    }
}

/// A trait that encapsulates the core chunk lifecycle logic
///
/// This is to enable independent testing of the policy logic
trait ChunkMover {
    /// Returns the size of a chunk - overridden for testing
    fn chunk_size(chunk: &Chunk) -> usize {
        chunk.size()
    }

    /// Return the name of the database
    fn db_name(&self) -> &str;

    /// Returns the lifecycle policy
    fn rules(&self) -> LifecycleRules;

    /// Returns a list of chunks sorted in the order
    /// they should prioritised
    fn chunks(&self, order: &SortOrder) -> Vec<Arc<RwLock<Chunk>>>;

    /// Returns a boolean indicating if a move is in progress
    fn is_move_active(&self) -> bool;

    /// Returns a boolean indicating if a write is in progress
    fn is_write_active(&self) -> bool;

    /// Starts an operation to move a chunk to the read buffer
    fn move_to_read_buffer(&mut self, partition_key: String, table_name: String, chunk_id: u32);

    /// Starts an operation to write a chunk to the object store
    fn write_to_object_store(&mut self, partition_key: String, table_name: String, chunk_id: u32);

    /// Drops a chunk from the database
    fn drop_chunk(&mut self, partition_key: String, table_name: String, chunk_id: u32);

    /// The core policy logic
    fn check_for_work(&mut self, now: DateTime<Utc>) {
        let rules = self.rules();
        let chunks = self.chunks(&rules.sort_order);

        let mut buffer_size = 0;

        // Only want to start a new move/write task if there isn't one already in-flight
        //
        // Note: This does not take into account manually triggered tasks
        let mut move_active = self.is_move_active();
        let mut write_active = self.is_write_active();

        // Iterate through the chunks to determine
        // - total memory consumption
        // - any chunks to move

        // TODO: Track size globally to avoid iterating through all chunks (#1100)
        for chunk in &chunks {
            let chunk_guard = chunk.upgradable_read();

            buffer_size += Self::chunk_size(&*chunk_guard);

            let would_move = can_move(&rules, &*chunk_guard, now);
            let would_write = !write_active && rules.persist;

            match chunk_guard.state() {
                ChunkState::Open(_) if !move_active && would_move => {
                    let mut chunk_guard = RwLockUpgradableReadGuard::upgrade(chunk_guard);
                    chunk_guard.set_closing().expect("cannot close open chunk");

                    let partition_key = chunk_guard.key().to_string();
                    let table_name = chunk_guard.table_name().to_string();
                    let chunk_id = chunk_guard.id();

                    std::mem::drop(chunk_guard);

                    move_active = true;
                    self.move_to_read_buffer(partition_key, table_name, chunk_id);
                }
                ChunkState::Closing(_) if !move_active => {
                    let partition_key = chunk_guard.key().to_string();
                    let table_name = chunk_guard.table_name().to_string();
                    let chunk_id = chunk_guard.id();

                    std::mem::drop(chunk_guard);

                    move_active = true;
                    self.move_to_read_buffer(partition_key, table_name, chunk_id);
                }
                ChunkState::Moved(_) if would_write => {
                    let partition_key = chunk_guard.key().to_string();
                    let table_name = chunk_guard.table_name().to_string();
                    let chunk_id = chunk_guard.id();

                    std::mem::drop(chunk_guard);

                    write_active = true;
                    self.write_to_object_store(partition_key, table_name, chunk_id);
                }
                _ => {}
            }

            // TODO: Find and recover cancelled move jobs (#1099)
        }

        if let Some(soft_limit) = rules.buffer_size_soft {
            let mut chunks = chunks.iter();

            while buffer_size > soft_limit.get() {
                match chunks.next() {
                    Some(chunk) => {
                        let chunk_guard = chunk.read();
                        if (rules.drop_non_persisted
                            && matches!(chunk_guard.state(), ChunkState::Moved(_)))
                            || matches!(chunk_guard.state(), ChunkState::WrittenToObjectStore(_, _))
                        {
                            let partition_key = chunk_guard.key().to_string();
                            let table_name = chunk_guard.table_name().to_string();
                            let chunk_id = chunk_guard.id();
                            buffer_size =
                                buffer_size.saturating_sub(Self::chunk_size(&*chunk_guard));

                            std::mem::drop(chunk_guard);

                            self.drop_chunk(partition_key, table_name, chunk_id)
                        }
                    }
                    None => {
                        warn!(db_name=self.db_name(), soft_limit, buffer_size,
                              "soft limited exceeded, but no chunks found that can be evicted. Check lifecycle rules");
                        break;
                    }
                }
            }
        }
    }
}

impl ChunkMover for LifecycleManager {
    fn rules(&self) -> LifecycleRules {
        self.db.rules.read().lifecycle_rules.clone()
    }

    fn chunks(&self, sort_order: &SortOrder) -> Vec<Arc<RwLock<Chunk>>> {
        self.db.catalog.chunks_sorted_by(sort_order)
    }

    fn is_move_active(&self) -> bool {
        self.move_task
            .as_ref()
            .map(|x| !x.is_complete())
            .unwrap_or(false)
    }

    fn is_write_active(&self) -> bool {
        self.write_task
            .as_ref()
            .map(|x| !x.is_complete())
            .unwrap_or(false)
    }

    fn move_to_read_buffer(&mut self, partition_key: String, table_name: String, chunk_id: u32) {
        info!(%partition_key, %chunk_id, "moving chunk to read buffer");
        self.move_task = Some(self.db.load_chunk_to_read_buffer_in_background(
            partition_key,
            table_name,
            chunk_id,
        ))
    }

    fn write_to_object_store(&mut self, partition_key: String, table_name: String, chunk_id: u32) {
        info!(%partition_key, %chunk_id, "write chunk to object store");
        self.write_task = Some(self.db.write_chunk_to_object_store_in_background(
            partition_key,
            table_name,
            chunk_id,
        ))
    }

    fn drop_chunk(&mut self, partition_key: String, table_name: String, chunk_id: u32) {
        info!(%partition_key, %chunk_id, "dropping chunk");
        let _ = self
            .db
            .drop_chunk(&partition_key, &table_name, chunk_id)
            .log_if_error("dropping chunk to free up memory");
    }

    fn db_name(&self) -> &str {
        &self.db_name
    }
}

/// Returns the number of seconds between two times
///
/// Computes a - b
fn elapsed_seconds(a: DateTime<Utc>, b: DateTime<Utc>) -> u32 {
    let seconds = (a - b).num_seconds();
    if seconds < 0 {
        0 // This can occur as DateTime is not monotonic
    } else {
        seconds.try_into().unwrap_or(u32::max_value())
    }
}

/// Returns if the chunk is sufficiently cold and old to move
///
/// Note: Does not check the chunk is the correct state
fn can_move(rules: &LifecycleRules, chunk: &Chunk, now: DateTime<Utc>) -> bool {
    match (rules.mutable_linger_seconds, chunk.time_of_last_write()) {
        (Some(linger), Some(last_write)) if elapsed_seconds(now, last_write) >= linger.get() => {
            match (
                rules.mutable_minimum_age_seconds,
                chunk.time_of_first_write(),
            ) {
                (Some(min_age), Some(first_write)) => {
                    // Chunk can be moved if it is old enough
                    elapsed_seconds(now, first_write) >= min_age.get()
                }
                // If no minimum age set - permit chunk movement
                (None, _) => true,
                (_, None) => unreachable!("chunk with last write and no first write"),
            }
        }

        // Disable movement if no mutable_linger set,
        // or the chunk is empty, or the linger hasn't expired
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::server_id::ServerId;
    use internal_types::entry::{test_helpers::lp_to_entry, ClockValue};
    use std::{
        convert::TryFrom,
        num::{NonZeroU32, NonZeroUsize},
    };
    use tracker::MemRegistry;

    fn from_secs(secs: i64) -> DateTime<Utc> {
        DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(secs, 0), Utc)
    }

    fn new_chunk(
        id: u32,
        time_of_first_write: Option<i64>,
        time_of_last_write: Option<i64>,
    ) -> Chunk {
        let entry = lp_to_entry("table1 bar=10 10");
        let write = entry.partition_writes().unwrap().remove(0);
        let batch = write.table_batches().remove(0);
        let mut chunk = Chunk::new_open(
            batch,
            "",
            id,
            ClockValue::try_from(5).unwrap(),
            ServerId::try_from(1).unwrap(),
            &MemRegistry::new(),
        )
        .unwrap();
        chunk.set_timestamps(
            time_of_first_write.map(from_secs),
            time_of_last_write.map(from_secs),
        );
        chunk
    }

    /// Transitions a new ("open") chunk into the "moving" state.
    fn transition_to_moving(mut chunk: Chunk) -> Chunk {
        chunk.set_closing().unwrap();
        chunk.set_moving().unwrap();
        chunk
    }

    /// Transitions a new ("open") chunk into the "moved" state.
    fn transition_to_moved(mut chunk: Chunk, rb: &Arc<read_buffer::Chunk>) -> Chunk {
        chunk = transition_to_moving(chunk);
        chunk.set_moved(Arc::clone(&rb)).unwrap();
        chunk
    }

    /// Transitions a new ("open") chunk into the "writing to object store"
    /// state.
    fn transition_to_writing_to_object_store(
        mut chunk: Chunk,
        rb: &Arc<read_buffer::Chunk>,
    ) -> Chunk {
        chunk = transition_to_moved(chunk, rb);
        chunk.set_writing_to_object_store().unwrap();
        chunk
    }

    /// Transitions a new ("open") chunk into the "written to object store"
    /// state.
    fn transition_to_written_to_object_store(
        mut chunk: Chunk,
        rb: &Arc<read_buffer::Chunk>,
    ) -> Chunk {
        chunk = transition_to_writing_to_object_store(chunk, rb);
        let parquet_chunk = new_parquet_chunk(&chunk);
        chunk
            .set_written_to_object_store(Arc::new(parquet_chunk))
            .unwrap();
        chunk
    }

    fn new_parquet_chunk(chunk: &Chunk) -> parquet_file::chunk::Chunk {
        parquet_file::chunk::Chunk::new(
            chunk.key().to_string(),
            chunk.id(),
            &tracker::MemRegistry::new(),
        )
    }

    #[derive(Debug, Eq, PartialEq)]
    enum MoverEvents {
        Move(u32),
        Write(u32),
        Drop(u32),
    }

    /// A dummy mover that is used to test the policy
    /// logic within ChunkMover::poll
    struct DummyMover {
        rules: LifecycleRules,
        move_active: bool,
        write_active: bool,
        chunks: Vec<Arc<RwLock<Chunk>>>,
        events: Vec<MoverEvents>,
    }

    impl DummyMover {
        fn new(rules: LifecycleRules, chunks: Vec<Chunk>) -> Self {
            Self {
                rules,
                chunks: chunks
                    .into_iter()
                    .map(|x| Arc::new(RwLock::new(x)))
                    .collect(),
                move_active: false,
                write_active: false,
                events: vec![],
            }
        }
    }

    impl ChunkMover for DummyMover {
        fn chunk_size(_: &Chunk) -> usize {
            // All chunks are 20 bytes
            20
        }

        fn rules(&self) -> LifecycleRules {
            self.rules.clone()
        }

        fn chunks(&self, _: &SortOrder) -> Vec<Arc<RwLock<Chunk>>> {
            self.chunks.clone()
        }

        fn is_move_active(&self) -> bool {
            self.move_active
        }

        fn is_write_active(&self) -> bool {
            self.write_active
        }

        fn move_to_read_buffer(
            &mut self,
            _partition_key: String,
            _table_name: String,
            chunk_id: u32,
        ) {
            let chunk = self
                .chunks
                .iter()
                .find(|x| x.read().id() == chunk_id)
                .unwrap();
            chunk.write().set_moving().unwrap();
            self.events.push(MoverEvents::Move(chunk_id))
        }

        fn write_to_object_store(
            &mut self,
            _partition_key: String,
            _table_name: String,
            chunk_id: u32,
        ) {
            let chunk = self
                .chunks
                .iter()
                .find(|x| x.read().id() == chunk_id)
                .unwrap();
            chunk.write().set_writing_to_object_store().unwrap();
            self.events.push(MoverEvents::Write(chunk_id))
        }

        fn drop_chunk(&mut self, _partition_key: String, _table_name: String, chunk_id: u32) {
            self.chunks = self
                .chunks
                .drain(..)
                .filter(|x| x.read().id() != chunk_id)
                .collect();
            self.events.push(MoverEvents::Drop(chunk_id))
        }

        fn db_name(&self) -> &str {
            "my_awesome_db"
        }
    }

    #[test]
    fn test_elapsed_seconds() {
        assert_eq!(elapsed_seconds(from_secs(10), from_secs(5)), 5);
        assert_eq!(elapsed_seconds(from_secs(10), from_secs(10)), 0);
        assert_eq!(elapsed_seconds(from_secs(10), from_secs(15)), 0);
    }

    #[test]
    fn test_can_move() {
        // Cannot move by default
        let rules = LifecycleRules::default();
        let chunk = new_chunk(0, Some(0), Some(0));
        assert!(!can_move(&rules, &chunk, from_secs(20)));

        // If only mutable_linger set can move a chunk once passed
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };
        let chunk = new_chunk(0, Some(0), Some(0));
        assert!(!can_move(&rules, &chunk, from_secs(9)));
        assert!(can_move(&rules, &chunk, from_secs(11)));

        // If mutable_minimum_age_seconds set must also take this into account
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            mutable_minimum_age_seconds: Some(NonZeroU32::new(60).unwrap()),
            ..Default::default()
        };
        let chunk = new_chunk(0, Some(0), Some(0));
        assert!(!can_move(&rules, &chunk, from_secs(9)));
        assert!(!can_move(&rules, &chunk, from_secs(11)));
        assert!(can_move(&rules, &chunk, from_secs(61)));

        let chunk = new_chunk(0, Some(0), Some(70));
        assert!(!can_move(&rules, &chunk, from_secs(71)));
        assert!(can_move(&rules, &chunk, from_secs(81)));
    }

    #[test]
    fn test_default_rules() {
        // The default rules shouldn't do anything
        let rules = LifecycleRules::default();
        let chunks = vec![
            new_chunk(0, Some(1), Some(1)),
            new_chunk(1, Some(20), Some(1)),
            new_chunk(2, Some(30), Some(1)),
        ];

        let mut mover = DummyMover::new(rules, chunks);
        mover.check_for_work(from_secs(40));
        assert_eq!(mover.events, vec![]);
    }

    #[test]
    fn test_mutable_linger() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };
        let chunks = vec![
            new_chunk(0, Some(0), Some(8)),
            new_chunk(1, Some(0), Some(5)),
            new_chunk(2, Some(0), Some(0)),
        ];

        let mut mover = DummyMover::new(rules, chunks);
        mover.check_for_work(from_secs(9));

        assert_eq!(mover.events, vec![]);

        mover.check_for_work(from_secs(11));
        assert_eq!(mover.events, vec![MoverEvents::Move(2)]);

        mover.check_for_work(from_secs(12));
        assert_eq!(mover.events, vec![MoverEvents::Move(2)]);

        // Should move in the order of chunks in the case of multiple candidates
        mover.check_for_work(from_secs(20));
        assert_eq!(
            mover.events,
            vec![MoverEvents::Move(2), MoverEvents::Move(0)]
        );

        mover.check_for_work(from_secs(20));

        assert_eq!(
            mover.events,
            vec![
                MoverEvents::Move(2),
                MoverEvents::Move(0),
                MoverEvents::Move(1)
            ]
        );
    }

    #[test]
    fn test_in_progress() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };
        let chunks = vec![new_chunk(0, Some(0), Some(0))];

        let mut mover = DummyMover::new(rules, chunks);
        mover.move_active = true;

        mover.check_for_work(from_secs(80));

        assert_eq!(mover.events, vec![]);

        mover.move_active = false;

        mover.check_for_work(from_secs(80));

        assert_eq!(mover.events, vec![MoverEvents::Move(0)]);
    }

    #[test]
    fn test_minimum_age() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            mutable_minimum_age_seconds: Some(NonZeroU32::new(60).unwrap()),
            ..Default::default()
        };
        let chunks = vec![
            new_chunk(0, Some(40), Some(40)),
            new_chunk(1, Some(0), Some(0)),
        ];

        let mut mover = DummyMover::new(rules, chunks);

        // Expect to move chunk_id=1 first, despite it coming second in
        // the order, because chunk_id=0 will only become old enough at t=100

        mover.check_for_work(from_secs(80));
        assert_eq!(mover.events, vec![MoverEvents::Move(1)]);

        mover.check_for_work(from_secs(90));
        assert_eq!(mover.events, vec![MoverEvents::Move(1)]);

        mover.check_for_work(from_secs(110));

        assert_eq!(
            mover.events,
            vec![MoverEvents::Move(1), MoverEvents::Move(0)]
        );
    }

    #[test]
    fn test_buffer_size_soft_drop_non_persisted() {
        // test that chunk mover only drops moved and written chunks

        // IMPORTANT: the lifecycle rules have the default `persist` flag (false) so NOT
        // "write" events will be triggered
        let rules = LifecycleRules {
            buffer_size_soft: Some(NonZeroUsize::new(5).unwrap()),
            drop_non_persisted: true,
            ..Default::default()
        };

        let rb = Arc::new(read_buffer::Chunk::new_with_memory_tracker(
            22,
            &tracker::MemRegistry::new(),
        ));

        let chunks = vec![new_chunk(0, Some(0), Some(0))];

        let mut mover = DummyMover::new(rules.clone(), chunks);

        mover.check_for_work(from_secs(10));
        assert_eq!(mover.events, vec![]);

        let chunks = vec![
            // two "open" chunks => they must not be dropped (yet)
            new_chunk(0, Some(0), Some(0)),
            new_chunk(1, Some(0), Some(0)),
            // "moved" chunk => can be dropped because `drop_non_persistent=true`
            transition_to_moved(new_chunk(2, Some(0), Some(0)), &rb),
            // "writing" chunk => cannot be drop while write is in-progess
            transition_to_writing_to_object_store(new_chunk(3, Some(0), Some(0)), &rb),
            // "written" chunk => can be dropped
            transition_to_written_to_object_store(new_chunk(4, Some(0), Some(0)), &rb),
        ];

        let mut mover = DummyMover::new(rules, chunks);

        mover.check_for_work(from_secs(10));
        assert_eq!(
            mover.events,
            vec![MoverEvents::Drop(2), MoverEvents::Drop(4)]
        );
    }

    #[test]
    fn test_buffer_size_soft_dont_drop_non_persisted() {
        // test that chunk mover only drops written chunks

        // IMPORTANT: the lifecycle rules have the default `persist` flag (false) so NOT
        // "write" events will be triggered
        let rules = LifecycleRules {
            buffer_size_soft: Some(NonZeroUsize::new(5).unwrap()),
            ..Default::default()
        };

        let rb = Arc::new(read_buffer::Chunk::new_with_memory_tracker(
            22,
            &tracker::MemRegistry::new(),
        ));

        let chunks = vec![new_chunk(0, Some(0), Some(0))];

        let mut mover = DummyMover::new(rules.clone(), chunks);

        mover.check_for_work(from_secs(10));
        assert_eq!(mover.events, vec![]);

        let chunks = vec![
            // two "open" chunks => they must not be dropped (yet)
            new_chunk(0, Some(0), Some(0)),
            new_chunk(1, Some(0), Some(0)),
            // "moved" chunk => cannot be dropped because `drop_non_persistent=false`
            transition_to_moved(new_chunk(2, Some(0), Some(0)), &rb),
            // "writing" chunk => cannot be drop while write is in-progess
            transition_to_writing_to_object_store(new_chunk(3, Some(0), Some(0)), &rb),
            // "written" chunk => can be dropped
            transition_to_written_to_object_store(new_chunk(4, Some(0), Some(0)), &rb),
        ];

        let mut mover = DummyMover::new(rules, chunks);

        mover.check_for_work(from_secs(10));
        assert_eq!(mover.events, vec![MoverEvents::Drop(4)]);
    }

    #[test]
    fn test_buffer_size_soft_no_op() {
        // check that we don't drop anything if nothing is to drop
        let rules = LifecycleRules {
            buffer_size_soft: Some(NonZeroUsize::new(40).unwrap()),
            ..Default::default()
        };

        let chunks = vec![new_chunk(0, Some(0), Some(0))];

        let mut mover = DummyMover::new(rules, chunks);

        mover.check_for_work(from_secs(10));
        assert_eq!(mover.events, vec![]);
    }

    #[test]
    fn test_persist() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            persist: true,
            ..Default::default()
        };

        let rb = Arc::new(read_buffer::Chunk::new_with_memory_tracker(
            22,
            &tracker::MemRegistry::new(),
        ));

        let chunks = vec![
            // still moving => cannot write
            transition_to_moving(new_chunk(0, Some(0), Some(0))),
            // moved => write to object store
            transition_to_moved(new_chunk(1, Some(0), Some(0)), &rb),
            // moved, but there will be already a write in progress (previous chunk) => don't write
            transition_to_moved(new_chunk(2, Some(0), Some(0)), &rb),
        ];

        let mut mover = DummyMover::new(rules, chunks);

        mover.check_for_work(from_secs(0));

        assert_eq!(mover.events, vec![MoverEvents::Write(1)]);
    }

    #[test]
    fn test_moves_closing() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            mutable_minimum_age_seconds: Some(NonZeroU32::new(60).unwrap()),
            ..Default::default()
        };
        let chunks = vec![new_chunk(0, Some(40), Some(40))];

        let mut mover = DummyMover::new(rules, chunks);

        // Initially can't move
        mover.check_for_work(from_secs(80));
        assert_eq!(mover.events, vec![]);

        mover.chunks[0].write().set_closing().unwrap();

        // As soon as closing can move
        mover.check_for_work(from_secs(80));
        assert_eq!(mover.events, vec![MoverEvents::Move(0)]);
    }
}
