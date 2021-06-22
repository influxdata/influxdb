use std::convert::TryInto;
use std::fmt::Debug;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use hashbrown::HashSet;

use data_types::database_rules::LifecycleRules;
use observability_deps::tracing::{debug, warn};
use tracker::TaskTracker;

use crate::{LifecycleChunk, LifecycleDb, LifecyclePartition, LockableChunk, LockablePartition};
use data_types::chunk_metadata::ChunkStorage;

pub const DEFAULT_LIFECYCLE_BACKOFF: Duration = Duration::from_secs(1);
/// Number of seconds to wait before retying a failed lifecycle action
pub const LIFECYCLE_ACTION_BACKOFF: Duration = Duration::from_secs(10);

/// A `LifecyclePolicy` is created with a `LifecycleDb`
///
/// `LifecyclePolicy::check_for_work` can then be used to drive progress
/// of the `LifecycleChunk` contained within this `LifecycleDb`
pub struct LifecyclePolicy<'a, M>
where
    &'a M: LifecycleDb,
{
    db: &'a M,
    // TODO: Remove these and use values from chunks within partition
    move_tracker: Option<TaskTracker<<<&'a M as LifecycleDb>::Chunk as LockableChunk>::Job>>,
    write_tracker: Option<TaskTracker<<<&'a M as LifecycleDb>::Chunk as LockableChunk>::Job>>,
}

impl<'a, M> LifecyclePolicy<'a, M>
where
    &'a M: LifecycleDb,
{
    pub fn new(db: &'a M) -> Self {
        Self {
            db,
            move_tracker: None,
            write_tracker: None,
        }
    }

    /// Check if database exceeds memory limits and free memory if necessary
    ///
    /// The policy will first try to unload persisted chunks in order of creation
    /// time, starting with the oldest.
    ///
    /// If permitted by the lifecycle policy, it will then drop unpersisted chunks,
    /// also in order of creation time, starting with the oldest.
    ///
    /// TODO: use LRU instead of creation time
    ///
    fn maybe_free_memory(&mut self, soft_limit: usize, drop_non_persisted: bool) {
        let buffer_size = self.db.buffer_size();
        if buffer_size < soft_limit {
            debug!(buffer_size, %soft_limit, "memory use under soft limit");
            return;
        }

        let partitions = self.db.partitions();

        // Collect a list of candidates to free memory
        let mut candidates = Vec::new();
        for partition in &partitions {
            let guard = partition.read();
            for chunk in LockablePartition::chunks(&guard) {
                let chunk = chunk.read();
                if chunk.lifecycle_action().is_some() {
                    continue;
                }

                let action = match chunk.storage() {
                    ChunkStorage::ReadBuffer | ChunkStorage::ClosedMutableBuffer
                        if drop_non_persisted =>
                    {
                        FreeAction::Drop
                    }
                    ChunkStorage::ReadBufferAndObjectStore => FreeAction::Unload,
                    _ => continue,
                };

                candidates.push(FreeCandidate {
                    partition,
                    action,
                    chunk_id: chunk.chunk_id(),
                    first_write: chunk.time_of_first_write(),
                })
            }
        }

        sort_free_candidates(&mut candidates);
        let mut candidates = candidates.into_iter();

        // Loop through trying to free memory
        //
        // There is an intentional lock gap here, to avoid holding read locks on all
        // the droppable chunks within the database. The downside is we have to
        // re-check pre-conditions in case they no longer hold
        //
        loop {
            let buffer_size = self.db.buffer_size();
            if buffer_size < soft_limit {
                debug!(buffer_size, %soft_limit, "memory use under soft limit");
                break;
            }
            debug!(buffer_size, %soft_limit, "memory use over soft limit");

            match candidates.next() {
                Some(candidate) => {
                    let partition = candidate.partition.read();
                    match LockablePartition::chunk(&partition, candidate.chunk_id) {
                        Some(chunk) => {
                            let chunk = chunk.read();
                            if chunk.lifecycle_action().is_some() {
                                debug!(
                                    chunk_id = candidate.chunk_id,
                                    partition = partition.partition_key(),
                                    "cannot mutate chunk with in-progress lifecycle action"
                                );
                                continue;
                            }

                            match candidate.action {
                                FreeAction::Drop => match chunk.storage() {
                                    ChunkStorage::ReadBuffer
                                    | ChunkStorage::ClosedMutableBuffer => {
                                        LockablePartition::drop_chunk(
                                            partition.upgrade(),
                                            candidate.chunk_id,
                                        )
                                        .expect("failed to drop")
                                    }
                                    storage => debug!(
                                        chunk_id = candidate.chunk_id,
                                        partition = partition.partition_key(),
                                        ?storage,
                                        "unexpected storage for drop"
                                    ),
                                },
                                FreeAction::Unload => match chunk.storage() {
                                    ChunkStorage::ReadBufferAndObjectStore => {
                                        LockableChunk::unload_read_buffer(chunk.upgrade())
                                            .expect("failed to unload")
                                    }
                                    storage => debug!(
                                        chunk_id = candidate.chunk_id,
                                        partition = partition.partition_key(),
                                        ?storage,
                                        "unexpected storage for unload"
                                    ),
                                },
                            }
                        }
                        None => debug!(
                            chunk_id = candidate.chunk_id,
                            partition = partition.partition_key(),
                            "cannot drop chunk that no longer exists on partition"
                        ),
                    }
                }
                None => {
                    warn!(soft_limit, buffer_size,
                          "soft limited exceeded, but no chunks found that can be evicted. Check lifecycle rules");
                    break;
                }
            }
        }
    }

    /// The core policy logic
    ///
    /// Returns a future that resolves when this method should be called next
    pub fn check_for_work(
        &mut self,
        now: DateTime<Utc>,
        now_instant: Instant,
    ) -> BoxFuture<'static, ()> {
        let rules = self.db.rules();

        let mut open_partitions: HashSet<String> = HashSet::new();

        // Only want to start a new move/write task if there isn't one already in-flight
        //
        // Fetch the trackers for tasks created by previous loop iterations. If the trackers exist
        // and haven't completed, we don't want to create a new task of that type as there
        // is still one in-flight.
        //
        // This is a very weak heuristic for preventing background work from starving query
        // workload - i.e. only use one thread for each type of background task.
        //
        // We may want to revisit this in future
        self.move_tracker = self.move_tracker.take().filter(|x| !x.is_complete());
        self.write_tracker = self.write_tracker.take().filter(|x| !x.is_complete());

        // Loop 1: Determine which chunks need to be driven though the
        // persistence lifecycle to ultimately be persisted to object storage
        for chunk in self.db.chunks(&rules.sort_order) {
            let chunk_guard = chunk.read();

            let would_move = can_move(&rules, &*chunk_guard, now);
            let would_write = self.write_tracker.is_none() && rules.persist;

            if let Some(lifecycle_action) = chunk_guard.lifecycle_action() {
                if lifecycle_action.is_complete()
                    && now_instant.duration_since(lifecycle_action.start_instant())
                        >= LIFECYCLE_ACTION_BACKOFF
                {
                    chunk_guard.upgrade().clear_lifecycle_action();
                }
                continue;
            }

            match chunk_guard.storage() {
                ChunkStorage::OpenMutableBuffer => {
                    open_partitions.insert(chunk_guard.partition_key().to_string());
                    if self.move_tracker.is_none() && would_move {
                        self.move_tracker = Some(
                            LockableChunk::move_to_read_buffer(chunk_guard.upgrade())
                                .expect("task preparation failed"),
                        );
                    }
                }
                ChunkStorage::ClosedMutableBuffer if self.move_tracker.is_none() => {
                    self.move_tracker = Some(
                        LockableChunk::move_to_read_buffer(chunk_guard.upgrade())
                            .expect("task preparation failed"),
                    );
                }
                ChunkStorage::ReadBuffer if would_write => {
                    self.write_tracker = Some(
                        LockableChunk::write_to_object_store(chunk_guard.upgrade())
                            .expect("task preparation failed"),
                    );
                }
                _ => {
                    // Chunk is already persisted, no additional work needed to persist it
                }
            }
        }

        if let Some(soft_limit) = rules.buffer_size_soft {
            self.maybe_free_memory(soft_limit.get(), rules.drop_non_persisted)
        }

        let move_tracker = self.move_tracker.clone();
        let write_tracker = self.write_tracker.clone();

        Box::pin(async move {
            let backoff = rules
                .worker_backoff_millis
                .map(|x| Duration::from_millis(x.get()))
                .unwrap_or(DEFAULT_LIFECYCLE_BACKOFF);

            // `check_for_work` should be called again if either of the tasks completes
            // or the backoff expires.
            //
            // This formulation ensures that the loop will run again in backoff
            // number of milliseconds regardless of if any tasks have finished
            //
            // Consider the situation where we have an in-progress write but no
            // in-progress move. We will look again for new tasks as soon
            // as either the backoff expires or the write task finishes
            //
            // Similarly if there are no in-progress tasks, we will look again
            // after the backoff interval
            //
            // Finally if all tasks are running we still want to be invoked
            // after the backoff interval, even if no tasks have completed by then,
            // as we may need to drop chunks
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = wait_optional_tracker(move_tracker) => {}
                _ = wait_optional_tracker(write_tracker) => {}
            };
        })
    }
}

impl<'a, M> Debug for LifecyclePolicy<'a, M>
where
    &'a M: LifecycleDb,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LifecyclePolicy{{..}}")
    }
}

/// Completes when the provided tracker completes or never if None provided
async fn wait_optional_tracker<Job: Send + Sync + 'static>(tracker: Option<TaskTracker<Job>>) {
    match tracker {
        None => futures::future::pending().await,
        Some(move_tracker) => move_tracker.join().await,
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
fn can_move<C: LifecycleChunk>(rules: &LifecycleRules, chunk: &C, now: DateTime<Utc>) -> bool {
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

/// An action to free up memory
#[derive(Debug, Copy, Clone, PartialOrd, Ord, PartialEq, Eq)]
enum FreeAction {
    // Variants are in-order of preference
    Unload,
    Drop,
}

/// Describes a candidate to free up memory
struct FreeCandidate<'a, P> {
    partition: &'a P,
    chunk_id: u32,
    action: FreeAction,
    first_write: Option<DateTime<Utc>>,
}

fn sort_free_candidates<P>(candidates: &mut Vec<FreeCandidate<'_, P>>) {
    candidates.sort_unstable_by(|a, b| match a.action.cmp(&b.action) {
        // Order candidates with the same FreeAction by first write time, with nulls last
        std::cmp::Ordering::Equal => match (a.first_write.as_ref(), b.first_write.as_ref()) {
            (Some(a), Some(b)) => a.cmp(b),
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, None) => std::cmp::Ordering::Equal,
        },
        o => o,
    })
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};

    use data_types::chunk_metadata::ChunkStorage;
    use data_types::database_rules::SortOrder;
    use tracker::{RwLock, TaskId, TaskRegistration, TaskRegistry};

    use super::*;
    use crate::{
        ChunkLifecycleAction, LifecycleReadGuard, LifecycleWriteGuard, LockableChunk,
        LockablePartition,
    };
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::sync::Arc;

    #[derive(Debug, Eq, PartialEq)]
    enum MoverEvents {
        Move(u32),
        Write(u32),
        Drop(u32),
        Unload(u32),
    }

    struct TestPartition {
        chunks: BTreeMap<u32, Arc<RwLock<TestChunk>>>,
    }

    struct TestChunk {
        id: u32,
        time_of_first_write: Option<DateTime<Utc>>,
        time_of_last_write: Option<DateTime<Utc>>,
        lifecycle_action: Option<TaskTracker<ChunkLifecycleAction>>,
        storage: ChunkStorage,
    }

    impl TestChunk {
        fn new(
            id: u32,
            time_of_first_write: Option<i64>,
            time_of_last_write: Option<i64>,
            storage: ChunkStorage,
        ) -> Self {
            Self {
                id,
                time_of_first_write: time_of_first_write.map(from_secs),
                time_of_last_write: time_of_last_write.map(from_secs),
                lifecycle_action: None,
                storage,
            }
        }

        fn with_action(self, action: ChunkLifecycleAction) -> Self {
            Self {
                id: self.id,
                time_of_first_write: self.time_of_first_write,
                time_of_last_write: self.time_of_last_write,
                lifecycle_action: Some(TaskTracker::complete(action)),
                storage: self.storage,
            }
        }
    }

    #[derive(Clone)]
    struct TestLockablePartition<'a> {
        db: &'a TestDb,
        partition: Arc<RwLock<TestPartition>>,
    }

    #[derive(Clone)]
    struct TestLockableChunk<'a> {
        db: &'a TestDb,
        chunk: Arc<RwLock<TestChunk>>,
    }

    impl<'a> LockablePartition for TestLockablePartition<'a> {
        type Partition = TestPartition;
        type Chunk = TestLockableChunk<'a>;
        type Error = Infallible;

        fn read(&self) -> LifecycleReadGuard<'_, Self::Partition, Self> {
            LifecycleReadGuard::new(self.clone(), &self.partition)
        }

        fn write(&self) -> LifecycleWriteGuard<'_, Self::Partition, Self> {
            LifecycleWriteGuard::new(self.clone(), &self.partition)
        }

        fn chunk(
            s: &LifecycleReadGuard<'_, Self::Partition, Self>,
            chunk_id: u32,
        ) -> Option<Self::Chunk> {
            let db = s.data().db;
            s.chunks.get(&chunk_id).map(|x| TestLockableChunk {
                db,
                chunk: Arc::clone(x),
            })
        }

        fn chunks(s: &LifecycleReadGuard<'_, Self::Partition, Self>) -> Vec<Self::Chunk> {
            let db = s.data().db;
            s.chunks
                .values()
                .map(|chunk| TestLockableChunk {
                    db,
                    chunk: Arc::clone(chunk),
                })
                .collect()
        }

        fn drop_chunk(
            mut s: LifecycleWriteGuard<'_, Self::Partition, Self>,
            chunk_id: u32,
        ) -> Result<(), Self::Error> {
            s.chunks.remove(&chunk_id);
            s.data().db.events.write().push(MoverEvents::Drop(chunk_id));
            Ok(())
        }
    }

    impl<'a> LockableChunk for TestLockableChunk<'a> {
        type Chunk = TestChunk;
        type Job = ();
        type Error = Infallible;

        fn read(&self) -> LifecycleReadGuard<'_, Self::Chunk, Self> {
            LifecycleReadGuard::new(self.clone(), &self.chunk)
        }

        fn write(&self) -> LifecycleWriteGuard<'_, Self::Chunk, Self> {
            LifecycleWriteGuard::new(self.clone(), &self.chunk)
        }

        fn move_to_read_buffer(
            mut s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
        ) -> Result<TaskTracker<()>, Self::Error> {
            s.storage = ChunkStorage::ReadBuffer;
            s.data()
                .db
                .events
                .write()
                .push(MoverEvents::Move(s.chunk_id()));
            Ok(TaskTracker::complete(()))
        }

        fn write_to_object_store(
            mut s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
        ) -> Result<TaskTracker<()>, Self::Error> {
            s.storage = ChunkStorage::ReadBufferAndObjectStore;
            s.data()
                .db
                .events
                .write()
                .push(MoverEvents::Write(s.chunk_id()));
            Ok(TaskTracker::complete(()))
        }

        fn unload_read_buffer(
            mut s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
        ) -> Result<(), Self::Error> {
            s.storage = ChunkStorage::ObjectStoreOnly;
            s.data()
                .db
                .events
                .write()
                .push(MoverEvents::Unload(s.chunk_id()));
            Ok(())
        }
    }

    impl LifecyclePartition for TestPartition {
        fn partition_key(&self) -> &str {
            "test"
        }
    }

    impl LifecycleChunk for TestChunk {
        fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>> {
            self.lifecycle_action.as_ref()
        }

        fn clear_lifecycle_action(&mut self) {
            self.lifecycle_action = None
        }

        fn time_of_first_write(&self) -> Option<DateTime<Utc>> {
            self.time_of_first_write
        }

        fn time_of_last_write(&self) -> Option<DateTime<Utc>> {
            self.time_of_last_write
        }

        fn table_name(&self) -> String {
            Default::default()
        }

        fn partition_key(&self) -> String {
            Default::default()
        }

        fn chunk_id(&self) -> u32 {
            self.id
        }

        fn storage(&self) -> ChunkStorage {
            self.storage
        }
    }

    /// A dummy db that is used to test the policy logic
    struct TestDb {
        rules: LifecycleRules,
        partitions: RwLock<Vec<Arc<RwLock<TestPartition>>>>,
        events: RwLock<Vec<MoverEvents>>,
    }

    impl TestDb {
        fn new(rules: LifecycleRules, chunks: Vec<TestChunk>) -> Self {
            Self {
                rules,
                partitions: RwLock::new(vec![Arc::new(RwLock::new(TestPartition {
                    chunks: chunks
                        .into_iter()
                        .map(|x| (x.id, Arc::new(RwLock::new(x))))
                        .collect(),
                }))]),
                events: RwLock::new(vec![]),
            }
        }
    }

    impl<'a> LifecycleDb for &'a TestDb {
        type Chunk = TestLockableChunk<'a>;
        type Partition = TestLockablePartition<'a>;

        fn buffer_size(self) -> usize {
            // All chunks are 20 bytes
            self.partitions
                .read()
                .iter()
                .map(|x| x.read().chunks.len() * 20)
                .sum()
        }

        fn rules(self) -> LifecycleRules {
            self.rules.clone()
        }

        fn partitions(self) -> Vec<Self::Partition> {
            self.partitions
                .read()
                .iter()
                .map(|x| TestLockablePartition {
                    db: self,
                    partition: Arc::clone(x),
                })
                .collect()
        }

        fn chunks(self, _: &SortOrder) -> Vec<Self::Chunk> {
            let mut chunks = Vec::new();

            let partitions = self.partitions.read();
            for partition in partitions.iter() {
                let partition = partition.read();
                for chunk in partition.chunks.values() {
                    chunks.push(TestLockableChunk {
                        db: self,
                        chunk: Arc::clone(chunk),
                    })
                }
            }
            chunks
        }
    }

    fn from_secs(secs: i64) -> DateTime<Utc> {
        DateTime::from_utc(chrono::NaiveDateTime::from_timestamp(secs, 0), Utc)
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
        let chunk = TestChunk::new(0, Some(0), Some(0), ChunkStorage::OpenMutableBuffer);
        assert!(!can_move(&rules, &chunk, from_secs(20)));

        // If only mutable_linger set can move a chunk once passed
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };
        let chunk = TestChunk::new(0, Some(0), Some(0), ChunkStorage::OpenMutableBuffer);
        assert!(!can_move(&rules, &chunk, from_secs(9)));
        assert!(can_move(&rules, &chunk, from_secs(11)));

        // If mutable_minimum_age_seconds set must also take this into account
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            mutable_minimum_age_seconds: Some(NonZeroU32::new(60).unwrap()),
            ..Default::default()
        };
        let chunk = TestChunk::new(0, Some(0), Some(0), ChunkStorage::OpenMutableBuffer);
        assert!(!can_move(&rules, &chunk, from_secs(9)));
        assert!(!can_move(&rules, &chunk, from_secs(11)));
        assert!(can_move(&rules, &chunk, from_secs(61)));

        let chunk = TestChunk::new(0, Some(0), Some(70), ChunkStorage::OpenMutableBuffer);
        assert!(!can_move(&rules, &chunk, from_secs(71)));
        assert!(can_move(&rules, &chunk, from_secs(81)));
    }

    #[test]
    fn test_sort_free_candidates() {
        let mut candidates = vec![
            FreeCandidate {
                partition: &(),
                chunk_id: 0,
                action: FreeAction::Drop,
                first_write: None,
            },
            FreeCandidate {
                partition: &(),
                chunk_id: 2,
                action: FreeAction::Unload,
                first_write: None,
            },
            FreeCandidate {
                partition: &(),
                chunk_id: 1,
                action: FreeAction::Unload,
                first_write: Some(from_secs(40)),
            },
            FreeCandidate {
                partition: &(),
                chunk_id: 3,
                action: FreeAction::Unload,
                first_write: Some(from_secs(20)),
            },
            FreeCandidate {
                partition: &(),
                chunk_id: 4,
                action: FreeAction::Unload,
                first_write: Some(from_secs(10)),
            },
            FreeCandidate {
                partition: &(),
                chunk_id: 5,
                action: FreeAction::Drop,
                first_write: Some(from_secs(10)),
            },
            FreeCandidate {
                partition: &(),
                chunk_id: 6,
                action: FreeAction::Drop,
                first_write: Some(from_secs(5)),
            },
        ];

        sort_free_candidates(&mut candidates);

        let ids: Vec<_> = candidates.into_iter().map(|x| x.chunk_id).collect();

        // Should first unload, then drop
        //
        // Should order the same actions by write time, with nulls last
        assert_eq!(ids, vec![4, 3, 1, 2, 6, 5, 0])
    }

    #[test]
    fn test_default_rules() {
        // The default rules shouldn't do anything
        let rules = LifecycleRules::default();
        let chunks = vec![
            TestChunk::new(0, Some(1), Some(1), ChunkStorage::OpenMutableBuffer),
            TestChunk::new(1, Some(20), Some(1), ChunkStorage::OpenMutableBuffer),
            TestChunk::new(2, Some(30), Some(1), ChunkStorage::OpenMutableBuffer),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);
        lifecycle.check_for_work(from_secs(40), Instant::now());
        assert_eq!(*db.events.read(), vec![]);
    }

    #[test]
    fn test_mutable_linger() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };
        let chunks = vec![
            TestChunk::new(0, Some(0), Some(8), ChunkStorage::OpenMutableBuffer),
            TestChunk::new(1, Some(0), Some(5), ChunkStorage::OpenMutableBuffer),
            TestChunk::new(2, Some(0), Some(0), ChunkStorage::OpenMutableBuffer),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(9), Instant::now());

        assert_eq!(*db.events.read(), vec![]);

        lifecycle.check_for_work(from_secs(11), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Move(2)]);

        lifecycle.check_for_work(from_secs(12), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Move(2)]);

        // Should move in the order of chunks in the case of multiple candidates
        lifecycle.check_for_work(from_secs(20), Instant::now());
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Move(2), MoverEvents::Move(0)]
        );

        lifecycle.check_for_work(from_secs(20), Instant::now());

        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Move(2),
                MoverEvents::Move(0),
                MoverEvents::Move(1)
            ]
        );
    }

    #[test]
    fn test_in_progress() {
        let mut registry = TaskRegistry::new();
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };
        let chunks = vec![TestChunk::new(
            0,
            Some(0),
            Some(0),
            ChunkStorage::OpenMutableBuffer,
        )];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        let (tracker, registration) = registry.register(());
        lifecycle.move_tracker = Some(tracker);
        lifecycle.check_for_work(from_secs(80), Instant::now());

        assert_eq!(*db.events.read(), vec![]);

        std::mem::drop(registration);

        lifecycle.check_for_work(from_secs(80), Instant::now());

        assert_eq!(*db.events.read(), vec![MoverEvents::Move(0)]);
    }

    #[tokio::test]
    async fn test_backoff() {
        let mut registry = TaskRegistry::new();
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(100).unwrap()),
            ..Default::default()
        };
        let db = TestDb::new(rules, vec![]);
        let mut lifecycle = LifecyclePolicy::new(&db);

        let (tracker, registration) = registry.register(());

        // Manually set the move_tracker on the DummyMover as if a previous invocation
        // of check_for_work had started a background move task
        lifecycle.move_tracker = Some(tracker);

        let future = lifecycle.check_for_work(from_secs(0), Instant::now());
        tokio::time::timeout(Duration::from_millis(1), future)
            .await
            .expect_err("expected timeout");

        let future = lifecycle.check_for_work(from_secs(0), Instant::now());
        std::mem::drop(registration);
        tokio::time::timeout(Duration::from_millis(1), future)
            .await
            .expect("expect early return due to task completion");
    }

    #[test]
    fn test_minimum_age() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            mutable_minimum_age_seconds: Some(NonZeroU32::new(60).unwrap()),
            ..Default::default()
        };
        let chunks = vec![
            TestChunk::new(0, Some(40), Some(40), ChunkStorage::OpenMutableBuffer),
            TestChunk::new(1, Some(0), Some(0), ChunkStorage::OpenMutableBuffer),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        // Expect to move chunk_id=1 first, despite it coming second in
        // the order, because chunk_id=0 will only become old enough at t=100

        lifecycle.check_for_work(from_secs(80), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Move(1)]);

        lifecycle.check_for_work(from_secs(90), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Move(1)]);

        lifecycle.check_for_work(from_secs(110), Instant::now());
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Move(1), MoverEvents::Move(0)]
        );
    }

    #[tokio::test]
    async fn test_buffer_size_soft_drop_non_persisted() {
        // test that chunk mover can drop non persisted chunks
        // if limit has been exceeded

        // IMPORTANT: the lifecycle rules have the default `persist` flag (false) so NOT
        // "write" events will be triggered
        let rules = LifecycleRules {
            buffer_size_soft: Some(NonZeroUsize::new(5).unwrap()),
            drop_non_persisted: true,
            ..Default::default()
        };

        let chunks = vec![TestChunk::new(
            0,
            Some(0),
            Some(0),
            ChunkStorage::OpenMutableBuffer,
        )];

        let db = TestDb::new(rules.clone(), chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(10), Instant::now());
        assert_eq!(*db.events.read(), vec![]);

        let chunks = vec![
            // two "open" chunks => they must not be dropped (yet)
            TestChunk::new(0, Some(0), Some(0), ChunkStorage::OpenMutableBuffer),
            TestChunk::new(1, Some(0), Some(0), ChunkStorage::OpenMutableBuffer),
            // "moved" chunk => can be dropped because `drop_non_persistent=true`
            TestChunk::new(2, Some(0), Some(0), ChunkStorage::ReadBuffer),
            // "writing" chunk => cannot be unloaded while write is in-progress
            TestChunk::new(3, Some(0), Some(0), ChunkStorage::ReadBuffer)
                .with_action(ChunkLifecycleAction::Persisting),
            // "written" chunk => can be unloaded
            TestChunk::new(4, Some(0), Some(0), ChunkStorage::ReadBufferAndObjectStore),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(10), Instant::now());
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Unload(4), MoverEvents::Drop(2)]
        );
    }

    #[tokio::test]
    async fn test_buffer_size_soft_dont_drop_non_persisted() {
        // test that chunk mover unloads written chunks and can't drop
        // unpersisted chunks when the persist flag is false

        // IMPORTANT: the lifecycle rules have the default `persist` flag (false) so NOT
        // "write" events will be triggered
        let rules = LifecycleRules {
            buffer_size_soft: Some(NonZeroUsize::new(5).unwrap()),
            ..Default::default()
        };
        assert!(!rules.drop_non_persisted);

        let chunks = vec![TestChunk::new(
            0,
            Some(0),
            Some(0),
            ChunkStorage::OpenMutableBuffer,
        )];

        let db = TestDb::new(rules.clone(), chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(10), Instant::now());
        assert_eq!(*db.events.read(), vec![]);

        let chunks = vec![
            // two "open" chunks => they must not be dropped (yet)
            TestChunk::new(0, Some(0), Some(0), ChunkStorage::OpenMutableBuffer),
            TestChunk::new(1, Some(0), Some(0), ChunkStorage::OpenMutableBuffer),
            // "moved" chunk => cannot be dropped because `drop_non_persistent=false`
            TestChunk::new(2, Some(0), Some(0), ChunkStorage::ReadBuffer),
            // "writing" chunk => cannot be drop while write is in-progess
            TestChunk::new(3, Some(0), Some(0), ChunkStorage::ReadBuffer)
                .with_action(ChunkLifecycleAction::Persisting),
            // "written" chunk => can be unloaded
            TestChunk::new(4, Some(0), Some(0), ChunkStorage::ReadBufferAndObjectStore),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(10), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Unload(4)]);
    }

    #[test]
    fn test_buffer_size_soft_no_op() {
        // check that we don't drop anything if nothing is to drop
        let rules = LifecycleRules {
            buffer_size_soft: Some(NonZeroUsize::new(40).unwrap()),
            ..Default::default()
        };

        let chunks = vec![TestChunk::new(
            0,
            Some(0),
            Some(0),
            ChunkStorage::OpenMutableBuffer,
        )];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(10), Instant::now());
        assert_eq!(*db.events.read(), vec![]);
    }

    #[test]
    fn test_persist() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            persist: true,
            ..Default::default()
        };

        let chunks = vec![
            // still moving => cannot write
            TestChunk::new(0, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                .with_action(ChunkLifecycleAction::Moving),
            // moved => write to object store
            TestChunk::new(1, Some(0), Some(0), ChunkStorage::ReadBuffer),
            // moved, but there will be already a write in progress (previous chunk) => don't write
            TestChunk::new(2, Some(0), Some(0), ChunkStorage::ReadBufferAndObjectStore),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(0), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Write(1)]);
    }

    #[test]
    fn test_moves_closed() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            mutable_minimum_age_seconds: Some(NonZeroU32::new(60).unwrap()),
            ..Default::default()
        };
        let chunks = vec![TestChunk::new(
            0,
            Some(40),
            Some(40),
            ChunkStorage::OpenMutableBuffer,
        )];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        // Initially can't move
        lifecycle.check_for_work(from_secs(80), Instant::now());
        assert_eq!(*db.events.read(), vec![]);

        db.partitions.read()[0].read().chunks[&0].write().storage =
            ChunkStorage::ClosedMutableBuffer;

        // As soon as closed can move
        lifecycle.check_for_work(from_secs(80), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Move(0)]);
    }

    #[test]
    fn test_recovers_lifecycle_action() {
        let rules = LifecycleRules::default();
        let chunks = vec![TestChunk::new(
            0,
            None,
            None,
            ChunkStorage::ClosedMutableBuffer,
        )];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);
        let chunk = Arc::clone(&db.partitions.read()[0].read().chunks[&0]);

        let r0 = TaskRegistration::default();
        let tracker = TaskTracker::new(TaskId(0), &r0, ChunkLifecycleAction::Moving);
        chunk.write().lifecycle_action = Some(tracker.clone());

        // Shouldn't do anything
        lifecycle.check_for_work(from_secs(0), tracker.start_instant());
        assert!(chunk.read().lifecycle_action().is_some());

        // Shouldn't do anything as job hasn't finished
        lifecycle.check_for_work(
            from_secs(0),
            tracker.start_instant() + LIFECYCLE_ACTION_BACKOFF,
        );
        assert!(chunk.read().lifecycle_action().is_some());

        // "Finish" job
        std::mem::drop(r0);

        // Shouldn't do anything as insufficient time passed
        lifecycle.check_for_work(from_secs(0), tracker.start_instant());
        assert!(chunk.read().lifecycle_action().is_some());

        // Should clear job
        lifecycle.check_for_work(
            from_secs(0),
            tracker.start_instant() + LIFECYCLE_ACTION_BACKOFF,
        );
        assert!(chunk.read().lifecycle_action().is_none());
    }
}
