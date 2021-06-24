use std::convert::TryInto;
use std::fmt::Debug;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;

use data_types::chunk_metadata::ChunkStorage;
use data_types::database_rules::LifecycleRules;
use observability_deps::tracing::{debug, info, warn};
use tracker::TaskTracker;

use crate::{
    ChunkLifecycleAction, LifecycleChunk, LifecycleDb, LifecyclePartition, LockableChunk,
    LockablePartition,
};

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
    /// The `LifecycleDb` this policy is automating
    db: &'a M,

    /// Background tasks spawned by this `LifecyclePolicy`
    trackers: Vec<TaskTracker<ChunkLifecycleAction>>,
}

impl<'a, M> LifecyclePolicy<'a, M>
where
    &'a M: LifecycleDb,
{
    pub fn new(db: &'a M) -> Self {
        Self {
            db,
            trackers: vec![],
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
    fn maybe_free_memory<P: LockablePartition>(
        &mut self,
        partitions: &[P],
        soft_limit: usize,
        drop_non_persisted: bool,
    ) {
        let buffer_size = self.db.buffer_size();
        if buffer_size < soft_limit {
            debug!(buffer_size, %soft_limit, "memory use under soft limit");
            return;
        }

        // Collect a list of candidates to free memory
        let mut candidates = Vec::new();
        for partition in partitions {
            let guard = partition.read();
            for (_, chunk) in LockablePartition::chunks(&guard) {
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
                    chunk_id: chunk.addr().chunk_id,
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

    /// Find chunks to compact together
    ///
    /// Finds unpersisted chunks with no in-progress lifecycle actions
    /// and compacts them into a single RUB chunk
    ///
    /// Will include the open chunk if it is cold for writes as determined
    /// by the mutable linger threshold
    fn maybe_compact_chunks<P: LockablePartition>(
        &mut self,
        partitions: &[P],
        rules: &LifecycleRules,
        now: DateTime<Utc>,
    ) {
        // TODO: Skip partitions with no PersistenceWindows (i.e. fully persisted)
        // TODO: Encapsulate locking into a CatalogTransaction type
        for partition in partitions {
            let partition = partition.read();

            let mut chunks = LockablePartition::chunks(&partition);
            // Sort by chunk ID to ensure a stable lock order
            chunks.sort_by_key(|x| x.0);

            let mut has_mub_snapshot = false;
            let mut to_compact = Vec::new();
            for (_, chunk) in &chunks {
                let chunk = chunk.read();
                if chunk.lifecycle_action().is_some() {
                    // This likely means it is being persisted
                    continue;
                }

                match chunk.storage() {
                    ChunkStorage::OpenMutableBuffer => {
                        if can_move(rules, &*chunk, now) {
                            has_mub_snapshot = true;
                            to_compact.push(chunk);
                        }
                    }
                    ChunkStorage::ClosedMutableBuffer => {
                        has_mub_snapshot = true;
                        to_compact.push(chunk);
                    }
                    ChunkStorage::ReadBuffer => {
                        to_compact.push(chunk);
                    }
                    _ => {}
                }
            }

            if to_compact.len() >= 2 || has_mub_snapshot {
                // Upgrade partition first
                let partition = partition.upgrade();
                let chunks = to_compact
                    .into_iter()
                    .map(|chunk| chunk.upgrade())
                    .collect();

                let tracker = LockablePartition::compact_chunks(partition, chunks)
                    .expect("failed to compact chunks")
                    .with_metadata(ChunkLifecycleAction::Compacting);

                self.trackers.push(tracker);
            }
        }
    }

    /// Check persistence
    ///
    /// Looks for read buffer chunks to persist
    ///
    /// A chunk will be persisted if it has more than `persist_row_threshold` rows
    /// or it was last written to more than `late_arrive_window_seconds` ago
    ///
    fn maybe_persist_chunks<P: LockablePartition>(
        &mut self,
        partitions: &[P],
        rules: &LifecycleRules,
        now: DateTime<Utc>,
    ) {
        // TODO: Skip partitions with no PersistenceWindows (i.e. fully persisted)
        // TODO: Use PersistenceWindows, split chunks, etc...

        let row_threshold = rules.persist_row_threshold.get();
        let late_arrive_window_seconds = rules.late_arrive_window_seconds.get();

        for partition in partitions {
            let partition = partition.read();
            let chunks = LockablePartition::chunks(&partition);

            // The candidate RUB chunk to persist
            let mut persist_candidate = None;

            // If there are other unpersisted chunks in the partition
            let mut has_other_unpersisted = false;

            for (_, chunk) in &chunks {
                let chunk = chunk.read();
                match chunk.storage() {
                    ChunkStorage::ReadBuffer => {}
                    ChunkStorage::OpenMutableBuffer | ChunkStorage::ClosedMutableBuffer => {
                        has_other_unpersisted = true;
                        continue;
                    }
                    ChunkStorage::ReadBufferAndObjectStore | ChunkStorage::ObjectStoreOnly => {
                        continue
                    }
                }

                if chunk.lifecycle_action().is_some() {
                    continue;
                }

                if persist_candidate.is_some() {
                    debug!(
                        partition = partition.partition_key(),
                        "found multiple read buffer chunks"
                    );

                    has_other_unpersisted = true;
                    continue;
                }

                persist_candidate = Some(chunk);
            }

            if let Some(chunk) = persist_candidate {
                let mut should_persist = chunk.row_count() >= row_threshold;
                if !should_persist && !has_other_unpersisted {
                    if let Some(last_write) = chunk.time_of_last_write() {
                        should_persist =
                            elapsed_seconds(now, last_write) > late_arrive_window_seconds;
                    }
                }

                if should_persist {
                    let tracker = LockableChunk::write_to_object_store(chunk.upgrade())
                        .expect("task preparation failed")
                        .with_metadata(ChunkLifecycleAction::Persisting);

                    self.trackers.push(tracker);
                }
            }
        }
    }

    /// Find failed lifecycle actions to cleanup
    ///
    /// Iterates through the chunks in the database, looking for chunks marked with lifecycle
    /// actions that are no longer running.
    ///
    /// As success should clear the action, the fact the action is still present but the job
    /// is no longer running, implies the job failed
    ///
    /// Clear any such jobs if they exited more than `LIFECYCLE_ACTION_BACKOFF` seconds ago
    ///
    fn maybe_cleanup_failed<P: LockablePartition>(&mut self, partitions: &[P], now: Instant) {
        for partition in partitions {
            let partition = partition.read();
            for (_, chunk) in LockablePartition::chunks(&partition) {
                let chunk = chunk.read();
                if let Some(lifecycle_action) = chunk.lifecycle_action() {
                    if lifecycle_action.is_complete()
                        && now.duration_since(lifecycle_action.start_instant())
                            >= LIFECYCLE_ACTION_BACKOFF
                    {
                        info!(chunk=%chunk.addr(), action=?lifecycle_action.metadata(), "clearing failed lifecycle action");
                        chunk.upgrade().clear_lifecycle_action();
                    }
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
    ) -> BoxFuture<'_, ()> {
        // TODO: Add loop iteration count and duration metrics

        let rules = self.db.rules();
        let partitions = self.db.partitions();

        // Try to persist first as in future this may involve
        // operations akin to compacting
        //
        // Any time-consuming work should be spawned as tokio tasks and not
        // run directly within this loop
        self.maybe_cleanup_failed(&partitions, now_instant);

        if rules.persist {
            self.maybe_persist_chunks(&partitions, &rules, now);
        }

        self.maybe_compact_chunks(&partitions, &rules, now);

        if let Some(soft_limit) = rules.buffer_size_soft {
            self.maybe_free_memory(&partitions, soft_limit.get(), rules.drop_non_persisted)
        }

        // Clear out completed tasks
        self.trackers.retain(|x| !x.is_complete());

        let tracker_fut = match self.trackers.is_empty() {
            false => futures::future::Either::Left(futures::future::select_all(
                self.trackers.iter().map(|x| Box::pin(x.join())),
            )),
            true => futures::future::Either::Right(futures::future::pending()),
        };

        Box::pin(async move {
            let backoff = rules.worker_backoff_millis.get();

            // `check_for_work` should be called again if any of the tasks completes
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
            // as we may need to drop chunks to free up memory
            tokio::select! {
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(backoff)) => {}
                _ = tracker_fut => {}
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

/// Returns the number of seconds between two times
///
/// Computes a - b
#[inline]
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
    use std::cmp::max;
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::num::{NonZeroU32, NonZeroUsize};
    use std::sync::Arc;

    use data_types::chunk_metadata::{ChunkAddr, ChunkStorage};
    use data_types::database_rules::SortOrder;
    use tracker::{RwLock, TaskId, TaskRegistration, TaskRegistry};

    use crate::{
        ChunkLifecycleAction, LifecycleReadGuard, LifecycleWriteGuard, LockableChunk,
        LockablePartition,
    };

    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    enum MoverEvents {
        Move(u32),
        Persist(u32),
        Drop(u32),
        Unload(u32),
        Compact(Vec<u32>),
    }

    struct TestPartition {
        chunks: BTreeMap<u32, Arc<RwLock<TestChunk>>>,
        next_id: u32,
    }

    struct TestChunk {
        addr: ChunkAddr,
        row_count: usize,
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
            let addr = ChunkAddr {
                db_name: Arc::from(""),
                table_name: Arc::from(""),
                partition_key: Arc::from(""),
                chunk_id: id,
            };

            Self {
                addr,
                row_count: 10,
                time_of_first_write: time_of_first_write.map(from_secs),
                time_of_last_write: time_of_last_write.map(from_secs),
                lifecycle_action: None,
                storage,
            }
        }

        fn with_row_count(self, row_count: usize) -> Self {
            Self {
                addr: self.addr,
                row_count,
                time_of_first_write: self.time_of_first_write,
                time_of_last_write: self.time_of_last_write,
                lifecycle_action: self.lifecycle_action,
                storage: self.storage,
            }
        }

        fn with_action(self, action: ChunkLifecycleAction) -> Self {
            Self {
                addr: self.addr,
                row_count: self.row_count,
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
        type DropError = Infallible;
        type CompactError = Infallible;

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

        fn chunks(s: &LifecycleReadGuard<'_, Self::Partition, Self>) -> Vec<(u32, Self::Chunk)> {
            let db = s.data().db;
            s.chunks
                .iter()
                .map(|(id, chunk)| {
                    (
                        *id,
                        TestLockableChunk {
                            db,
                            chunk: Arc::clone(chunk),
                        },
                    )
                })
                .collect()
        }

        fn compact_chunks(
            mut partition: LifecycleWriteGuard<'_, TestPartition, Self>,
            chunks: Vec<LifecycleWriteGuard<'_, TestChunk, Self::Chunk>>,
        ) -> Result<TaskTracker<()>, Self::CompactError> {
            let id = partition.next_id;
            partition.next_id += 1;

            let mut new_chunk = TestChunk::new(id, None, None, ChunkStorage::ReadBuffer);
            new_chunk.row_count = 0;

            for chunk in &chunks {
                partition.chunks.remove(&chunk.addr.chunk_id);
                new_chunk.row_count += chunk.row_count;
            }

            partition
                .chunks
                .insert(id, Arc::new(RwLock::new(new_chunk)));

            let event = MoverEvents::Compact(chunks.iter().map(|x| x.addr.chunk_id).collect());
            partition.data().db.events.write().push(event);

            Ok(TaskTracker::complete(()))
        }

        fn drop_chunk(
            mut s: LifecycleWriteGuard<'_, Self::Partition, Self>,
            chunk_id: u32,
        ) -> Result<(), Self::DropError> {
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
                .push(MoverEvents::Move(s.addr.chunk_id));
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
                .push(MoverEvents::Persist(s.addr.chunk_id));
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
                .push(MoverEvents::Unload(s.addr.chunk_id));
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

        fn addr(&self) -> &ChunkAddr {
            &self.addr
        }

        fn storage(&self) -> ChunkStorage {
            self.storage
        }

        fn row_count(&self) -> usize {
            self.row_count
        }
    }

    impl TestPartition {
        fn new(chunks: Vec<TestChunk>) -> Self {
            let mut max_id = 0;
            let chunks = chunks
                .into_iter()
                .map(|x| {
                    max_id = max(max_id, x.addr.chunk_id);
                    (x.addr.chunk_id, Arc::new(RwLock::new(x)))
                })
                .collect();

            Self {
                chunks,
                next_id: max_id + 1,
            }
        }
    }

    /// A dummy db that is used to test the policy logic
    struct TestDb {
        rules: LifecycleRules,
        partitions: RwLock<Vec<Arc<RwLock<TestPartition>>>>,
        // TODO: Move onto TestPartition
        events: RwLock<Vec<MoverEvents>>,
    }

    impl TestDb {
        fn new(rules: LifecycleRules, chunks: Vec<TestChunk>) -> Self {
            Self::from_partitions(rules, std::iter::once(TestPartition::new(chunks)))
        }

        fn from_partitions(
            rules: LifecycleRules,
            partitions: impl IntoIterator<Item = TestPartition>,
        ) -> Self {
            Self {
                rules,
                partitions: RwLock::new(
                    partitions
                        .into_iter()
                        .map(|x| Arc::new(RwLock::new(x)))
                        .collect(),
                ),
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
        let partition = Arc::clone(&db.partitions.read()[0]);

        lifecycle.check_for_work(from_secs(9), Instant::now());

        assert_eq!(*db.events.read(), vec![]);

        lifecycle.check_for_work(from_secs(11), Instant::now());
        let chunks = partition.read().chunks.keys().cloned().collect::<Vec<_>>();
        // expect chunk 2 to have been compacted into a new chunk 3
        assert_eq!(*db.events.read(), vec![MoverEvents::Compact(vec![2])]);
        assert_eq!(chunks, vec![0, 1, 3]);

        lifecycle.check_for_work(from_secs(12), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Compact(vec![2])]);

        // Should compact everything possible
        lifecycle.check_for_work(from_secs(20), Instant::now());
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Compact(vec![2]),
                MoverEvents::Compact(vec![0, 1, 3])
            ]
        );

        assert_eq!(partition.read().chunks.len(), 1);
        assert_eq!(partition.read().chunks[&4].read().row_count, 30);
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

        let (tracker, registration) = registry.register(ChunkLifecycleAction::Moving);

        // Manually add the tracker to the policy as if a previous invocation
        // of check_for_work had started a background move task
        lifecycle.trackers.push(tracker);

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
        let partition = Arc::clone(&db.partitions.read()[0]);

        // Expect to move chunk_id=1 first, despite it coming second in
        // the order, because chunk_id=0 will only become old enough at t=100

        lifecycle.check_for_work(from_secs(80), Instant::now());
        let chunks = partition.read().chunks.keys().cloned().collect::<Vec<_>>();
        // Expect chunk 1 to have been compacted into a new chunk 2
        assert_eq!(*db.events.read(), vec![MoverEvents::Compact(vec![1])]);
        assert_eq!(chunks, vec![0, 2]);

        lifecycle.check_for_work(from_secs(90), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Compact(vec![1])]);

        lifecycle.check_for_work(from_secs(110), Instant::now());
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Compact(vec![1]),
                MoverEvents::Compact(vec![0, 2])
            ]
        );

        assert_eq!(partition.read().chunks.len(), 1);
        assert_eq!(partition.read().chunks[&3].read().row_count, 20);
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
    fn test_compact() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };

        let now = from_secs(20);

        let partitions = vec![
            TestPartition::new(vec![
                // still receiving writes => cannot compact
                TestChunk::new(0, Some(0), Some(20), ChunkStorage::OpenMutableBuffer),
            ]),
            TestPartition::new(vec![
                // still receiving writes => cannot compact
                TestChunk::new(1, Some(0), Some(20), ChunkStorage::OpenMutableBuffer),
                // closed => can compact
                TestChunk::new(2, Some(0), Some(20), ChunkStorage::ClosedMutableBuffer),
            ]),
            TestPartition::new(vec![
                // open but cold => can compact
                TestChunk::new(3, Some(0), Some(5), ChunkStorage::OpenMutableBuffer),
                // closed => can compact
                TestChunk::new(4, Some(0), Some(20), ChunkStorage::ClosedMutableBuffer),
                // closed => can compact
                TestChunk::new(5, Some(0), Some(20), ChunkStorage::ReadBuffer),
                // persisted => cannot compact
                TestChunk::new(6, Some(0), Some(20), ChunkStorage::ReadBufferAndObjectStore),
                // persisted => cannot compact
                TestChunk::new(7, Some(0), Some(20), ChunkStorage::ObjectStoreOnly),
            ]),
            TestPartition::new(vec![
                // closed => can compact
                TestChunk::new(8, Some(0), Some(20), ChunkStorage::ReadBuffer),
                // closed => can compact
                TestChunk::new(9, Some(0), Some(20), ChunkStorage::ReadBuffer),
                // persisted => cannot compact
                TestChunk::new(
                    10,
                    Some(0),
                    Some(20),
                    ChunkStorage::ReadBufferAndObjectStore,
                ),
                // persisted => cannot compact
                TestChunk::new(11, Some(0), Some(20), ChunkStorage::ObjectStoreOnly),
            ]),
            TestPartition::new(vec![
                // open but cold => can compact
                TestChunk::new(12, Some(0), Some(5), ChunkStorage::OpenMutableBuffer),
            ]),
            TestPartition::new(vec![
                // already compacted => should not compact
                TestChunk::new(13, Some(0), Some(5), ChunkStorage::ReadBuffer),
            ]),
        ];

        let db = TestDb::from_partitions(rules, partitions);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(now, Instant::now());
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Compact(vec![2]),
                MoverEvents::Compact(vec![3, 4, 5]),
                MoverEvents::Compact(vec![8, 9]),
                MoverEvents::Compact(vec![12])
            ]
        );
    }

    #[test]
    fn test_persist() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            persist: true,
            persist_row_threshold: NonZeroUsize::new(1_000).unwrap(),
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            ..Default::default()
        };

        let partitions = vec![
            TestPartition::new(vec![
                // not compacted => cannot write
                TestChunk::new(0, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer),
                // not enough rows
                TestChunk::new(1, Some(0), Some(0), ChunkStorage::ReadBuffer),
                // already moved
                TestChunk::new(2, Some(0), Some(0), ChunkStorage::ReadBufferAndObjectStore),
            ]),
            TestPartition::new(vec![
                TestChunk::new(3, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_action(ChunkLifecycleAction::Compacting),
                // reached row count => write
                TestChunk::new(4, Some(0), Some(0), ChunkStorage::ReadBuffer).with_row_count(1_000),
            ]),
            TestPartition::new(vec![
                TestChunk::new(5, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_action(ChunkLifecycleAction::Compacting),
                // still compacting => cannot write
                TestChunk::new(6, Some(0), Some(0), ChunkStorage::ReadBuffer)
                    .with_row_count(1_000)
                    .with_action(ChunkLifecycleAction::Compacting),
            ]),
            TestPartition::new(vec![
                // chunk cold => write
                TestChunk::new(7, Some(0), Some(0), ChunkStorage::ReadBuffer).with_row_count(20),
            ]),
            TestPartition::new(vec![
                // reached row count => write
                TestChunk::new(8, Some(0), Some(0), ChunkStorage::ReadBuffer).with_row_count(1_000),
                // could persist, but already persisting above
                TestChunk::new(9, Some(0), Some(0), ChunkStorage::ReadBuffer).with_row_count(1_000),
            ]),
            TestPartition::new(vec![
                // chunk not cold => cannot write
                TestChunk::new(10, Some(0), Some(11), ChunkStorage::ReadBuffer).with_row_count(20),
            ]),
        ];

        let db = TestDb::from_partitions(rules, partitions);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(20), Instant::now());
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Persist(4),
                MoverEvents::Persist(7),
                MoverEvents::Persist(8),
                MoverEvents::Compact(vec![0, 1])
            ]
        );
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
        assert_eq!(*db.events.read(), vec![MoverEvents::Compact(vec![0])]);
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
