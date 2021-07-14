use std::convert::TryInto;
use std::fmt::Debug;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use data_types::DatabaseName;
use futures::future::BoxFuture;

use data_types::chunk_metadata::{ChunkLifecycleAction, ChunkStorage};
use data_types::database_rules::{LifecycleRules, DEFAULT_MUB_ROW_THRESHOLD};
use observability_deps::tracing::{debug, info, trace, warn};
use tracker::TaskTracker;

use crate::{
    LifecycleChunk, LifecycleDb, LifecyclePartition, LockableChunk, LockablePartition,
    PersistHandle,
};

/// Number of seconds to wait before retying a failed lifecycle action
pub const LIFECYCLE_ACTION_BACKOFF: Duration = Duration::from_secs(10);

/// A `LifecyclePolicy` is created with a `LifecycleDb`
///
/// `LifecyclePolicy::check_for_work` can then be used to drive progress
/// of the `LifecycleChunk` contained within this `LifecycleDb`
pub struct LifecyclePolicy<M>
where
    M: LifecycleDb,
{
    /// The `LifecycleDb` this policy is automating
    db: M,

    /// Background tasks spawned by this `LifecyclePolicy`
    trackers: Vec<TaskTracker<ChunkLifecycleAction>>,
}

impl<M> LifecyclePolicy<M>
where
    M: LifecycleDb,
{
    pub fn new(db: M) -> Self {
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
        db_name: &DatabaseName<'static>,
        partitions: &[P],
        soft_limit: usize,
        drop_non_persisted: bool,
    ) {
        let buffer_size = self.db.buffer_size();
        if buffer_size < soft_limit {
            debug!(%db_name, buffer_size, %soft_limit, "memory use under soft limit");
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
                info!(%db_name, buffer_size, %soft_limit, "memory use under soft limit");
                break;
            }
            info!(%db_name, buffer_size, %soft_limit, "memory use over soft limit");

            match candidates.next() {
                Some(candidate) => {
                    let partition = candidate.partition.read();
                    match LockablePartition::chunk(&partition, candidate.chunk_id) {
                        Some(chunk) => {
                            let chunk = chunk.read();
                            if chunk.lifecycle_action().is_some() {
                                info!(
                                    %db_name,
                                    chunk_id = candidate.chunk_id,
                                    %partition,
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
                                    storage => warn!(
                                        %db_name,
                                        chunk_id = candidate.chunk_id,
                                        %partition,
                                        ?storage,
                                        "unexpected storage for drop"
                                    ),
                                },
                                FreeAction::Unload => match chunk.storage() {
                                    ChunkStorage::ReadBufferAndObjectStore => {
                                        LockableChunk::unload_read_buffer(chunk.upgrade())
                                            .expect("failed to unload")
                                    }
                                    storage => warn!(
                                        %db_name,
                                        chunk_id = candidate.chunk_id,
                                        %partition,
                                        ?storage,
                                        "unexpected storage for unload"
                                    ),
                                },
                            }
                        }
                        None => info!(
                            %db_name,
                            chunk_id = candidate.chunk_id,
                            %partition,
                            "cannot drop chunk that no longer exists on partition"
                        ),
                    }
                }
                None => {
                    warn!(%db_name, soft_limit, buffer_size,
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
        partition: &P,
        rules: &LifecycleRules,
        now: DateTime<Utc>,
    ) {
        let mut rows_left = rules.persist_row_threshold.get();

        // TODO: Encapsulate locking into a CatalogTransaction type
        let partition = partition.read();
        if partition.is_persisted() {
            debug!(db_name = %self.db.name(), %partition, "nothing to be compacted for partition");
            return;
        }

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

            let to_compact_len_before = to_compact.len();
            let storage = chunk.storage();
            match storage {
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
                    let row_count = chunk.row_count();
                    if row_count >= rows_left {
                        continue;
                    }
                    rows_left = rows_left.saturating_sub(row_count);
                    to_compact.push(chunk);
                }
                _ => {}
            }
            let has_added_to_compact = to_compact.len() > to_compact_len_before;
            debug!(db_name = %self.db.name(),
                   %partition,
                   ?has_added_to_compact,
                   chunk_storage = ?storage,
                   ?has_mub_snapshot,
                   "maybe compacting chunks");
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

    /// Check persistence
    ///
    /// Looks for chunks to combine together in the "persist"
    /// operation. The "persist" operation combines the data from a
    /// list chunks and creates two new chunks: one persisted, with
    /// all data that eligible for persistence, and the second with
    /// all data that is not yet eligible for persistence (it was
    /// written to recently)
    ///
    /// A chunk will be chosen for the persist operation if either:
    ///
    /// 1. it has more than `persist_row_threshold` rows
    /// 2. it was last written to more than `late_arrive_window_seconds` ago
    ///
    /// Returns true if persistence is being blocked by compaction,
    /// signaling compaction should be stalled to allow persistence to
    /// make progress
    ///
    /// Returns a boolean to indicate if it should stall compaction to allow
    /// persistence to make progress
    ///
    /// The rationale for stalling compaction until a persist can start:
    ///
    /// 1. It is a simple way to ensure a persist can start. Once the
    /// persist has started (which might also effectively compact
    /// several chunks as well) then compactions can start again
    ///
    /// 2. It is not likely to change the number of compactions
    /// significantly. Since the policy goal at time of writing is to
    /// end up with ~ 2 unpersisted chunks at any time, any chunk that
    /// is persistable is also likely to be one of the ones being
    /// compacted.
    fn maybe_persist_chunks<P: LockablePartition>(
        &mut self,
        db_name: &DatabaseName<'static>,
        partition: &P,
        rules: &LifecycleRules,
        now: Instant,
    ) -> bool {
        // TODO: Encapsulate locking into a CatalogTransaction type
        let partition = partition.read();

        if partition.is_persisted() {
            debug!(%db_name, %partition, "nothing to persist for partition");
            return false;
        }

        let persistable_age_seconds = partition
            .minimum_unpersisted_age()
            .and_then(|minimum_unpersisted_age| {
                // If writes happened between when the policy loop
                // started and this check is done, the duration may be
                // negative. Skip persistence in this case to avoid
                // panic in `duration_since`
                Some(
                    now.checked_duration_since(minimum_unpersisted_age)?
                        .as_secs(),
                )
            })
            .unwrap_or_default() as u32;

        let persistable_row_count = partition.persistable_row_count(now);
        debug!(%db_name, %partition,
               partition_persist_row_count=persistable_row_count,
               rules_persist_row_count=%rules.persist_row_threshold.get(),
               partition_persistable_age_seconds=persistable_age_seconds,
               rules_persist_age_threshold_seconds=%rules.persist_age_threshold_seconds.get(),
               "considering for persistence");

        if persistable_row_count >= rules.persist_row_threshold.get() {
            info!(%db_name, %partition, persistable_row_count, "persisting partition as exceeds row threshold");
        } else if persistable_age_seconds >= rules.persist_age_threshold_seconds.get() {
            info!(%db_name, %partition, persistable_age_seconds, "persisting partition as exceeds age threshold");
        } else {
            debug!(%db_name, %partition, persistable_row_count, "partition not eligible for persist");
            return false;
        }

        let mut chunks = LockablePartition::chunks(&partition);

        // Upgrade partition to be able to rotate persistence windows
        let mut partition = partition.upgrade();

        let persist_handle = match LockablePartition::prepare_persist(&mut partition, now) {
            Some(x) => x,
            None => {
                debug!(%db_name, %partition, "no persistable windows or previous outstanding persist");
                return false;
            }
        };

        // Sort by chunk ID to ensure a stable lock order
        chunks.sort_by_key(|x| x.0);
        let mut to_persist = Vec::new();
        for (_, chunk) in &chunks {
            let chunk = chunk.read();
            trace!(%db_name, %partition, chunk=%chunk.addr(), "considering chunk for persistence");

            // Check if chunk is eligible for persistence
            match chunk.storage() {
                ChunkStorage::OpenMutableBuffer
                | ChunkStorage::ClosedMutableBuffer
                | ChunkStorage::ReadBuffer => {}
                ChunkStorage::ReadBufferAndObjectStore | ChunkStorage::ObjectStoreOnly => {
                    debug!(%db_name, %partition, chunk=%chunk.addr(), storage=?chunk.storage(),
                           "chunk not eligible due to storage");
                    continue;
                }
            }

            // Chunk's data is entirely after the time we are flushing
            // up to, and thus there is reason to include it in the
            // plan
            if chunk.min_timestamp() > persist_handle.timestamp() {
                // Can safely ignore chunk
                debug!(%db_name, %partition, chunk=%chunk.addr(),
                       "chunk does not contain data eligible for persistence");
                continue;
            }

            // If the chunk has an outstanding lifecycle action
            if let Some(action) = chunk.lifecycle_action() {
                // see if we should stall subsequent pull it is
                // preventing us from persisting
                let stall = action.metadata() == &ChunkLifecycleAction::Compacting;
                info!(%db_name, ?action, chunk=%chunk.addr(), "Chunk to persist has outstanding action");
                return stall;
            }

            to_persist.push(chunk);
        }

        if to_persist.is_empty() {
            info!(%db_name, %partition, "expected to persist but found no eligible chunks");
            return false;
        }

        let chunks = to_persist
            .into_iter()
            .map(|chunk| chunk.upgrade())
            .collect();

        let tracker = LockablePartition::persist_chunks(partition, chunks, persist_handle)
            .expect("failed to persist chunks")
            .with_metadata(ChunkLifecycleAction::Persisting);

        self.trackers.push(tracker);
        false
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
    fn maybe_cleanup_failed<P: LockablePartition>(
        &mut self,
        db_name: &DatabaseName<'static>,
        partition: &P,
        now: Instant,
    ) {
        let partition = partition.read();
        for (_, chunk) in LockablePartition::chunks(&partition) {
            let chunk = chunk.read();
            if let Some(lifecycle_action) = chunk.lifecycle_action() {
                if lifecycle_action.is_complete()
                    && now
                        .checked_duration_since(lifecycle_action.start_instant())
                        .map(|x| x >= LIFECYCLE_ACTION_BACKOFF)
                        .unwrap_or(false)
                {
                    info!(%db_name, chunk=%chunk.addr(), action=?lifecycle_action.metadata(), "clearing failed lifecycle action");
                    chunk.upgrade().clear_lifecycle_action();
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
        // Any time-consuming work should be spawned as tokio tasks and not
        // run directly within this loop

        // TODO: Add loop iteration count and duration metrics

        let db_name = self.db.name();
        let rules = self.db.rules();
        let partitions = self.db.partitions();

        for partition in &partitions {
            self.maybe_cleanup_failed(&db_name, partition, now_instant);

            // Persistence cannot split chunks if they are currently being compacted
            //
            // To avoid compaction "starving" persistence we employ a
            // heavy-handed approach of temporarily pausing compaction
            // if the criteria for persistence have been satisfied,
            // but persistence cannot proceed because of in-progress
            // compactions
            let stall_compaction = if rules.persist {
                self.maybe_persist_chunks(&db_name, partition, &rules, now_instant)
            } else {
                false
            };

            if !stall_compaction {
                self.maybe_compact_chunks(partition, &rules, now);
            } else {
                debug!(%db_name, %partition, "stalling compaction to allow persist");
            }
        }

        if let Some(soft_limit) = rules.buffer_size_soft {
            self.maybe_free_memory(
                &db_name,
                &partitions,
                soft_limit.get(),
                rules.drop_non_persisted,
            )
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

impl<M> Debug for LifecyclePolicy<M>
where
    M: LifecycleDb + Copy,
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
    if chunk.row_count() >= DEFAULT_MUB_ROW_THRESHOLD {
        return true;
    }

    match chunk.time_of_last_write() {
        Some(last_write)
            if elapsed_seconds(now, last_write) >= rules.late_arrive_window_seconds.get() =>
        {
            true
        }

        // Disable movement the chunk is empty, or the linger hasn't expired
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
    use tracker::{RwLock, TaskId, TaskRegistration, TaskRegistry};

    use crate::{
        ChunkLifecycleAction, LifecycleReadGuard, LifecycleWriteGuard, LockableChunk,
        LockablePartition, PersistHandle,
    };

    use super::*;

    #[derive(Debug, Eq, PartialEq)]
    enum MoverEvents {
        Drop(u32),
        Unload(u32),
        Compact(Vec<u32>),
        Persist(Vec<u32>),
    }

    #[derive(Debug)]
    struct TestPartition {
        chunks: BTreeMap<u32, Arc<RwLock<TestChunk>>>,
        persistable_row_count: usize,
        minimum_unpersisted_age: Option<Instant>,
        max_persistable_timestamp: Option<DateTime<Utc>>,
        next_id: u32,
    }

    impl TestPartition {
        fn with_persistence(
            self,
            persistable_row_count: usize,
            minimum_unpersisted_age: Instant,
            max_persistable_timestamp: DateTime<Utc>,
        ) -> Self {
            Self {
                chunks: self.chunks,
                persistable_row_count,
                minimum_unpersisted_age: Some(minimum_unpersisted_age),
                max_persistable_timestamp: Some(max_persistable_timestamp),
                next_id: self.next_id,
            }
        }
    }

    #[derive(Debug)]
    struct TestChunk {
        addr: ChunkAddr,
        row_count: usize,
        min_timestamp: Option<DateTime<Utc>>,
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
                min_timestamp: None,
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
                min_timestamp: self.min_timestamp,
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
                min_timestamp: self.min_timestamp,
                time_of_first_write: self.time_of_first_write,
                time_of_last_write: self.time_of_last_write,
                lifecycle_action: Some(TaskTracker::complete(action)),
                storage: self.storage,
            }
        }

        fn with_min_timestamp(self, min_timestamp: DateTime<Utc>) -> Self {
            Self {
                addr: self.addr,
                row_count: self.row_count,
                min_timestamp: Some(min_timestamp),
                time_of_first_write: self.time_of_first_write,
                time_of_last_write: self.time_of_last_write,
                lifecycle_action: self.lifecycle_action,
                storage: self.storage,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct TestLockablePartition<'a> {
        db: &'a TestDb,
        partition: Arc<RwLock<TestPartition>>,
    }

    impl<'a> std::fmt::Display for TestLockablePartition<'a> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self)
        }
    }

    #[derive(Clone)]
    struct TestLockableChunk<'a> {
        db: &'a TestDb,
        chunk: Arc<RwLock<TestChunk>>,
    }

    #[derive(Debug)]
    struct TestPersistHandle {
        timestamp: DateTime<Utc>,
    }

    impl PersistHandle for TestPersistHandle {
        fn timestamp(&self) -> DateTime<Utc> {
            self.timestamp
        }
    }

    impl<'a> LockablePartition for TestLockablePartition<'a> {
        type Partition = TestPartition;
        type Chunk = TestLockableChunk<'a>;
        type PersistHandle = TestPersistHandle;
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
        ) -> Result<TaskTracker<()>, Self::Error> {
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

        fn prepare_persist(
            partition: &mut LifecycleWriteGuard<'_, Self::Partition, Self>,
            _now: Instant,
        ) -> Option<Self::PersistHandle> {
            Some(TestPersistHandle {
                timestamp: partition.max_persistable_timestamp.unwrap(),
            })
        }

        fn persist_chunks(
            mut partition: LifecycleWriteGuard<'_, TestPartition, Self>,
            chunks: Vec<LifecycleWriteGuard<'_, TestChunk, Self::Chunk>>,
            handle: Self::PersistHandle,
        ) -> Result<TaskTracker<()>, Self::Error> {
            for chunk in &chunks {
                partition.chunks.remove(&chunk.addr.chunk_id);
            }

            let id = partition.next_id;
            partition.next_id += 1;

            // The remainder left behind after the split
            let new_chunk = TestChunk::new(id, None, None, ChunkStorage::ReadBuffer)
                .with_min_timestamp(handle.timestamp + chrono::Duration::nanoseconds(1));

            partition
                .chunks
                .insert(id, Arc::new(RwLock::new(new_chunk)));

            let event = MoverEvents::Persist(chunks.iter().map(|x| x.addr.chunk_id).collect());
            partition.data().db.events.write().push(event);
            Ok(TaskTracker::complete(()))
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
            _s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
        ) -> Result<TaskTracker<Self::Job>, Self::Error> {
            // Isn't used by the lifecycle policy
            // TODO: Remove this
            unreachable!()
        }

        fn write_to_object_store(
            _s: LifecycleWriteGuard<'_, Self::Chunk, Self>,
        ) -> Result<TaskTracker<Self::Job>, Self::Error> {
            // Isn't used by the lifecycle policy
            // TODO: Remove this
            unreachable!()
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

        fn is_persisted(&self) -> bool {
            false
        }

        fn persistable_row_count(&self, _now: Instant) -> usize {
            self.persistable_row_count
        }

        fn minimum_unpersisted_age(&self) -> Option<Instant> {
            self.minimum_unpersisted_age
        }
    }

    impl LifecycleChunk for TestChunk {
        fn lifecycle_action(&self) -> Option<&TaskTracker<ChunkLifecycleAction>> {
            self.lifecycle_action.as_ref()
        }

        fn clear_lifecycle_action(&mut self) {
            self.lifecycle_action = None
        }

        fn min_timestamp(&self) -> DateTime<Utc> {
            self.min_timestamp.unwrap()
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
                persistable_row_count: 0,
                minimum_unpersisted_age: None,
                max_persistable_timestamp: None,
                next_id: max_id + 1,
            }
        }
    }

    /// A dummy db that is used to test the policy logic
    #[derive(Debug)]
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

        fn buffer_size(&self) -> usize {
            // All chunks are 20 bytes
            self.partitions
                .read()
                .iter()
                .map(|x| x.read().chunks.len() * 20)
                .sum()
        }

        fn rules(&self) -> LifecycleRules {
            self.rules.clone()
        }

        fn partitions(&self) -> Vec<Self::Partition> {
            self.partitions
                .read()
                .iter()
                .map(|x| TestLockablePartition {
                    db: self,
                    partition: Arc::clone(x),
                })
                .collect()
        }

        fn name(&self) -> DatabaseName<'static> {
            DatabaseName::new("test_db").unwrap()
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
        // If only late_arrival set can move a chunk once passed
        let rules = LifecycleRules {
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            ..Default::default()
        };
        let chunk = TestChunk::new(0, Some(0), Some(0), ChunkStorage::OpenMutableBuffer);
        assert!(!can_move(&rules, &chunk, from_secs(9)));
        assert!(can_move(&rules, &chunk, from_secs(11)));

        // can move even if the chunk is small
        let chunk = TestChunk::new(0, Some(0), Some(0), ChunkStorage::OpenMutableBuffer)
            .with_row_count(DEFAULT_MUB_ROW_THRESHOLD - 1);
        assert!(can_move(&rules, &chunk, from_secs(11)));

        // If over the default row count threshold, we should be able to move
        let chunk = TestChunk::new(0, None, None, ChunkStorage::OpenMutableBuffer)
            .with_row_count(DEFAULT_MUB_ROW_THRESHOLD);
        assert!(can_move(&rules, &chunk, from_secs(0)));

        // If below the default row count threshold, it shouldn't move
        let chunk = TestChunk::new(0, None, None, ChunkStorage::OpenMutableBuffer)
            .with_row_count(DEFAULT_MUB_ROW_THRESHOLD - 1);
        assert!(!can_move(&rules, &chunk, from_secs(0)));
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
    fn test_late_arrival() {
        let rules = LifecycleRules {
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
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
            late_arrive_window_seconds: NonZeroU32::new(100).unwrap(),
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

    #[tokio::test]
    async fn test_buffer_size_soft_drop_non_persisted() {
        // test that chunk mover can drop non persisted chunks
        // if limit has been exceeded

        // IMPORTANT: the lifecycle rules have the default `persist` flag (false) so NO
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
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            persist_row_threshold: NonZeroUsize::new(1_000).unwrap(),
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
            TestPartition::new(vec![
                // closed => can compact
                TestChunk::new(14, Some(0), Some(20), ChunkStorage::ReadBuffer).with_row_count(400),
                // too many individual rows => ignore
                TestChunk::new(15, Some(0), Some(20), ChunkStorage::ReadBuffer)
                    .with_row_count(1_000),
                // closed => can compact
                TestChunk::new(16, Some(0), Some(20), ChunkStorage::ReadBuffer).with_row_count(400),
                // too many total rows => next compaction job
                TestChunk::new(17, Some(0), Some(20), ChunkStorage::ReadBuffer).with_row_count(400),
                // too many total rows => next compaction job
                TestChunk::new(18, Some(0), Some(20), ChunkStorage::ReadBuffer).with_row_count(400),
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
                MoverEvents::Compact(vec![12]),
                MoverEvents::Compact(vec![14, 16]),
            ],
        );

        db.events.write().clear();
        lifecycle.check_for_work(now, Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Compact(vec![17, 18])]);
    }

    #[test]
    fn test_persist() {
        let rules = LifecycleRules {
            persist: true,
            persist_row_threshold: NonZeroUsize::new(1_000).unwrap(),
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            persist_age_threshold_seconds: NonZeroU32::new(10).unwrap(),
            ..Default::default()
        };
        let now = Instant::now();

        let partitions = vec![
            // Insufficient rows and not old enough => don't persist but can compact
            TestPartition::new(vec![
                TestChunk::new(0, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(1, Some(0), Some(0), ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(10, now, from_secs(20)),
            // Sufficient rows => persist
            TestPartition::new(vec![
                TestChunk::new(2, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(3, Some(0), Some(0), ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(1_000, now, from_secs(20)),
            // Writes too old => persist
            TestPartition::new(vec![
                // Should split open chunks
                TestChunk::new(4, Some(0), Some(20), ChunkStorage::OpenMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(5, Some(0), Some(0), ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
                TestChunk::new(6, Some(0), Some(0), ChunkStorage::ObjectStoreOnly)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(10, now - Duration::from_secs(10), from_secs(20)),
            // Sufficient rows but conflicting compaction => prevent compaction
            TestPartition::new(vec![
                TestChunk::new(7, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10))
                    .with_action(ChunkLifecycleAction::Compacting),
                // This chunk would be a compaction candidate, but we want to persist it
                TestChunk::new(8, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(9, Some(0), Some(0), ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(1_000, now, from_secs(20)),
            // Sufficient rows and non-conflicting compaction => persist
            TestPartition::new(vec![
                TestChunk::new(10, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(21))
                    .with_action(ChunkLifecycleAction::Compacting),
                TestChunk::new(11, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(12, Some(0), Some(0), ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(1_000, now, from_secs(20)),
            // Sufficient rows, non-conflicting compaction and compact-able chunk => persist + compact
            TestPartition::new(vec![
                TestChunk::new(13, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(21))
                    .with_action(ChunkLifecycleAction::Compacting),
                TestChunk::new(14, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(21)),
                TestChunk::new(15, Some(0), Some(0), ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(16, Some(0), Some(0), ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(1_000, now, from_secs(20)),
        ];

        let db = TestDb::from_partitions(rules, partitions);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(0), now);
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Compact(vec![0, 1]),
                MoverEvents::Persist(vec![2, 3]),
                MoverEvents::Persist(vec![4, 5]),
                MoverEvents::Persist(vec![11, 12]),
                MoverEvents::Persist(vec![15, 16]),
                // 17 is the resulting chunk from the persist split above
                // This is "quirk" of TestPartition operations being instantaneous
                MoverEvents::Compact(vec![14, 17])
            ]
        );
    }

    #[test]
    fn test_moves_open() {
        let rules = LifecycleRules {
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
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

        lifecycle.check_for_work(from_secs(80), Instant::now());
        assert_eq!(*db.events.read(), vec![MoverEvents::Compact(vec![0])]);
    }

    #[test]
    fn test_moves_closed() {
        let rules = LifecycleRules {
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            ..Default::default()
        };
        let chunks = vec![TestChunk::new(
            0,
            Some(40),
            Some(40),
            ChunkStorage::ClosedMutableBuffer,
        )];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

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
