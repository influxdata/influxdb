use crate::{
    LifecycleChunk, LifecycleDb, LifecyclePartition, LifecycleWriteGuard, LockableChunk,
    LockablePartition, PersistHandle,
};
use chrono::{DateTime, Utc};
use data_types::{
    chunk_metadata::{ChunkId, ChunkLifecycleAction, ChunkStorage},
    database_rules::LifecycleRules,
    DatabaseName,
};
use futures::future::BoxFuture;
use internal_types::access::AccessMetrics;
use observability_deps::tracing::{debug, info, trace, warn};
use std::{convert::TryInto, fmt::Debug};
use tracker::TaskTracker;

/// Number of seconds to wait before retrying a failed lifecycle action
pub const LIFECYCLE_ACTION_BACKOFF_SECONDS: i64 = 10;

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

    /// The current number of active compactions.
    active_compactions: usize,

    /// Background tasks spawned by this `LifecyclePolicy`
    trackers: Vec<TaskTracker<ChunkLifecycleAction>>,

    /// Do not allow persistence even when the database rules would allow that.
    ///
    /// This can be helpful during some phases of the database startup process.
    suppress_persistence: bool,
}

impl<M> LifecyclePolicy<M>
where
    M: LifecycleDb,
{
    /// Create new policy.
    ///
    /// Persistence is allowed if the database rules allow it.
    pub fn new(db: M) -> Self {
        Self {
            db,
            trackers: vec![],
            active_compactions: 0,
            suppress_persistence: false,
        }
    }

    /// Create new policy that suppresses persistence even when the database rules allow it.
    pub fn new_suppress_persistence(db: M) -> Self {
        Self {
            db,
            trackers: vec![],
            active_compactions: 0,
            suppress_persistence: true,
        }
    }

    /// Stop suppressing persistence and allow it if the database rules allow it.
    pub fn unsuppress_persistence(&mut self) {
        self.suppress_persistence = false;
    }

    /// Check if database exceeds memory limits and free memory if necessary.
    ///
    /// The behavior depends on the `persist` flag from the lifecycle rules:
    ///
    /// - If persist is `true` it will only unload persisted chunks in order of creation time, starting with the oldest.
    /// - If persist is `false` it will consider all chunks, also in order of creation time, starting with the oldest.
    ///
    fn maybe_free_memory<P: LockablePartition>(
        &mut self,
        db_name: &DatabaseName<'static>,
        partitions: &[P],
        soft_limit: usize,
        persist: bool,
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
            for chunk in LockablePartition::chunks(&guard) {
                let chunk = chunk.read();
                if chunk.lifecycle_action().is_some() {
                    continue;
                }

                let action = match chunk.storage() {
                    ChunkStorage::ReadBuffer | ChunkStorage::ClosedMutableBuffer if !persist => {
                        FreeAction::Drop
                    }
                    ChunkStorage::ReadBufferAndObjectStore => FreeAction::Unload,
                    _ => continue,
                };

                candidates.push(FreeCandidate {
                    partition,
                    action,
                    chunk_id: chunk.addr().chunk_id,
                    access_metrics: chunk.access_metrics(),
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
                                    chunk_id=%candidate.chunk_id.get(),
                                    %partition,
                                    "cannot mutate chunk with in-progress lifecycle action"
                                );
                                continue;
                            }

                            match candidate.action {
                                FreeAction::Drop => match chunk.storage() {
                                    ChunkStorage::ReadBuffer
                                    | ChunkStorage::ClosedMutableBuffer => {
                                        let tracker = LockablePartition::drop_chunk(
                                            partition.upgrade(),
                                            chunk.upgrade(),
                                        )
                                        .expect("failed to drop")
                                        .with_metadata(ChunkLifecycleAction::Dropping);
                                        self.trackers.push(tracker);
                                    }
                                    storage => warn!(
                                        %db_name,
                                        chunk_id=%candidate.chunk_id.get(),
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
                                        chunk_id=%candidate.chunk_id.get(),
                                        %partition,
                                        ?storage,
                                        "unexpected storage for unload"
                                    ),
                                },
                            }
                        }
                        None => info!(
                            %db_name,
                            chunk_id=%candidate.chunk_id.get(),
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

        let chunks = LockablePartition::chunks(&partition);

        let mut has_mub_snapshot = false;
        let mut to_compact = Vec::new();
        for chunk in &chunks {
            let chunk = chunk.read();
            if matches!(
                chunk.storage(),
                ChunkStorage::ReadBufferAndObjectStore { .. }
                    | ChunkStorage::ObjectStoreOnly { .. }
            ) {
                continue;
            }
            if chunk.lifecycle_action().is_some() {
                if to_compact.is_empty() {
                    // just skip this chunk
                    continue;
                } else {
                    // must stop here because we must not "jump" the chunks sorted by `order`.
                    break;
                }
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
                _ => unreachable!("this chunk should be have filtered out already"),
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
            // caller's responsibility to determine if we can maybe compact.
            assert!(self.active_compactions < rules.max_active_compactions.get() as usize);

            // Upgrade partition first
            let partition = partition.upgrade();
            let chunks = to_compact
                .into_iter()
                .map(|chunk| chunk.upgrade())
                .collect();

            let tracker = LockablePartition::compact_chunks(partition, chunks)
                .expect("failed to compact chunks")
                .with_metadata(ChunkLifecycleAction::Compacting);

            self.active_compactions += 1;
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
        now: DateTime<Utc>,
    ) -> bool {
        // TODO: Encapsulate locking into a CatalogTransaction type
        let partition = partition.read();

        if partition.is_persisted() {
            debug!(%db_name, %partition, "nothing to persist for partition");
            return false;
        }

        let persistable_age_seconds: u32 = partition
            .minimum_unpersisted_age()
            .and_then(|minimum_unpersisted_age| {
                (now - minimum_unpersisted_age)
                    .num_seconds()
                    .try_into()
                    .ok()
            })
            .unwrap_or_default();

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

        let chunks = LockablePartition::chunks(&partition);

        // Upgrade partition to be able to rotate persistence windows
        let mut partition = partition.upgrade();

        let persist_handle = match LockablePartition::prepare_persist(&mut partition, now) {
            Some(x) => x,
            None => {
                debug!(%db_name, %partition, "no persistable windows or previous outstanding persist");
                return false;
            }
        };

        let chunks = match select_persistable_chunks(&chunks, db_name, persist_handle.timestamp()) {
            Ok(chunks) => chunks,
            Err(stall) => {
                return stall;
            }
        };

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
        now: DateTime<Utc>,
    ) {
        let partition = partition.read();
        for chunk in LockablePartition::chunks(&partition) {
            let chunk = chunk.read();
            if let Some(lifecycle_action) = chunk.lifecycle_action() {
                if lifecycle_action.is_complete()
                    && now
                        .signed_duration_since(lifecycle_action.start_time())
                        .num_seconds()
                        >= LIFECYCLE_ACTION_BACKOFF_SECONDS
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
    pub fn check_for_work(&mut self, now: DateTime<Utc>) -> BoxFuture<'_, ()> {
        // Any time-consuming work should be spawned as tokio tasks and not
        // run directly within this loop

        // TODO: Add loop iteration count and duration metrics

        let db_name = self.db.name();
        let rules = self.db.rules();
        let partitions = self.db.partitions();

        for partition in &partitions {
            self.maybe_cleanup_failed(&db_name, partition, now);

            // Persistence cannot split chunks if they are currently being compacted
            //
            // To avoid compaction "starving" persistence we employ a
            // heavy-handed approach of temporarily pausing compaction
            // if the criteria for persistence have been satisfied,
            // but persistence cannot proceed because of in-progress
            // compactions
            let stall_compaction_persisting = if rules.persist && !self.suppress_persistence {
                let persisting = self.maybe_persist_chunks(&db_name, partition, &rules, now);
                if persisting {
                    debug!(%db_name, %partition, reason="persisting", "stalling compaction");
                }
                persisting
            } else {
                false
            };

            // Until we have a more sophisticated compaction policy that can
            // allocate resources appropriately, we limit the number of
            // compactions that may run concurrently. Compactions are
            // completely disabled if max_compactions is Some(0), whilst if
            // it is None then the compaction limiter is disabled (unlimited
            // concurrent compactions).
            let stall_compaction_no_slots = {
                let max_compactions = self.db.rules().max_active_compactions.get();
                let slots_full = self.active_compactions >= max_compactions as usize;
                if slots_full {
                    debug!(%db_name, %partition, ?max_compactions, reason="slots_full", "stalling compaction");
                }
                slots_full
            };

            // conditions where no compactions will be scheduled.
            if stall_compaction_persisting || stall_compaction_no_slots {
                continue;
            }

            // possibly do a compaction
            self.maybe_compact_chunks(partition, &rules, now);
        }

        if let Some(soft_limit) = rules.buffer_size_soft {
            self.maybe_free_memory(&db_name, &partitions, soft_limit.get(), rules.persist)
        }

        // Clear out completed tasks
        let mut completed_compactions = 0;
        self.trackers.retain(|x| {
            let completed = x.is_complete();
            if completed && matches!(x.metadata(), ChunkLifecycleAction::Compacting) {
                // free up slot for another compaction
                completed_compactions += 1;
            }

            !completed
        });

        // update active compactions
        if completed_compactions > 0 {
            debug!(?completed_compactions, active_compactions=?self.active_compactions,
                max_compactions=?self.db.rules().max_active_compactions, "releasing compaction slots")
        }

        assert!(completed_compactions <= self.active_compactions);
        self.active_compactions -= completed_compactions;

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
    M: LifecycleDb,
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
    if chunk.row_count() >= rules.mub_row_threshold.get() {
        return true;
    }

    elapsed_seconds(now, chunk.time_of_last_write()) >= rules.late_arrive_window_seconds.get()
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
    chunk_id: ChunkId,
    action: FreeAction,
    access_metrics: AccessMetrics,
}

fn sort_free_candidates<P>(candidates: &mut Vec<FreeCandidate<'_, P>>) {
    candidates.sort_unstable_by(|a, b| match a.action.cmp(&b.action) {
        // Order candidates with the same FreeAction by last access time
        std::cmp::Ordering::Equal => a
            .access_metrics
            .last_access
            .cmp(&b.access_metrics.last_access),
        o => o,
    })
}

/// Select persistable chunks.
///
/// # Error Handling
/// This can fail if chunks that should be persisted have an active lifecycle action. In that case an `Err(bool)` is
/// returned.
///
/// If the error boolean is `true`, compaction is currently blocking persistence and you should stall compaction (aka
/// to prevent new compaction jobs from starting) to be able to proceed with persistence.
///
/// If the error boolean is `false`, there are other active lifecycle actions preventing persistence (e.g. a persistence
/// job that is already running).
pub fn select_persistable_chunks<'a, P, D>(
    chunks: &'a [D],
    db_name: &DatabaseName<'static>,
    flush_ts: DateTime<Utc>,
) -> Result<Vec<LifecycleWriteGuard<'a, P, D>>, bool>
where
    D: LockableChunk<Chunk = P>,
    P: LifecycleChunk,
{
    let mut to_persist = Vec::with_capacity(chunks.len());
    let mut to_persist_gap = Vec::with_capacity(chunks.len());

    for chunk in chunks {
        let chunk = chunk.read();
        trace!(%db_name, chunk=%chunk.addr(), "considering chunk for persistence");

        // Check if chunk is eligible for persistence
        match chunk.storage() {
            ChunkStorage::OpenMutableBuffer
            | ChunkStorage::ClosedMutableBuffer
            | ChunkStorage::ReadBuffer => {}
            ChunkStorage::ReadBufferAndObjectStore | ChunkStorage::ObjectStoreOnly => {
                debug!(%db_name, chunk=%chunk.addr(), storage=?chunk.storage(),
                        "chunk not eligible due to storage");
                continue;
            }
        }

        // Chunk's data is entirely after the time we are flushing
        // up to, and thus there is reason to include it in the
        // plan
        if chunk.min_timestamp() > flush_ts {
            // Ignore chunk for now, but we might need it later to close chunk order gaps
            debug!(%db_name, chunk=%chunk.addr(),
                    "chunk does not contain data eligible for persistence");
            if chunk.lifecycle_action().is_none() {
                to_persist_gap.push(chunk);
            }
            continue;
        }

        // If the chunk has an outstanding lifecycle action
        if let Some(action) = chunk.lifecycle_action() {
            // see if we should stall subsequent pull it is
            // preventing us from persisting
            let stall = action.metadata() == &ChunkLifecycleAction::Compacting;
            info!(%db_name, ?action, chunk=%chunk.addr(), "Chunk to persist has outstanding action");

            // NOTE: This early exit also ensures that we are not "jumping" over chunks sorted by `order`.
            return Err(stall);
        }

        // persist this chunk and the gap
        to_persist.append(&mut to_persist_gap);
        to_persist.push(chunk);
    }

    // At this point `to_persist_gap` might be non-empty. This is fine since these are only chunks at the end of the
    // order-based list, so it's not really a gap.

    let chunks = to_persist
        .into_iter()
        .map(|chunk| chunk.upgrade())
        .collect();
    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ChunkLifecycleAction, LifecycleReadGuard, LifecycleWriteGuard, LockableChunk,
        LockablePartition, PersistHandle,
    };
    use chrono::TimeZone;
    use data_types::chunk_metadata::{ChunkAddr, ChunkId, ChunkOrder, ChunkStorage};
    use data_types::database_rules::MaxActiveCompactions::MaxActiveCompactions;
    use std::time::Duration;
    use std::{
        cmp::max,
        collections::BTreeMap,
        convert::Infallible,
        num::{NonZeroU32, NonZeroUsize},
        sync::Arc,
    };
    use tracker::{RwLock, TaskId, TaskRegistration, TaskRegistry};

    #[derive(Debug, Eq, PartialEq)]
    enum MoverEvents {
        Drop(ChunkId),
        Unload(ChunkId),
        Compact(Vec<ChunkId>),
        Persist(Vec<ChunkId>),
    }

    #[derive(Debug)]
    struct TestPartition {
        chunks: BTreeMap<ChunkId, (ChunkOrder, Arc<RwLock<TestChunk>>)>,
        persistable_row_count: usize,
        minimum_unpersisted_age: Option<DateTime<Utc>>,
        max_persistable_timestamp: Option<DateTime<Utc>>,
        next_id: u128,
    }

    impl TestPartition {
        fn with_persistence(
            self,
            persistable_row_count: usize,
            minimum_unpersisted_age: DateTime<Utc>,
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
        access_metrics: AccessMetrics,
        time_of_last_write: DateTime<Utc>,
        lifecycle_action: Option<TaskTracker<ChunkLifecycleAction>>,
        storage: ChunkStorage,
        order: ChunkOrder,
    }

    impl TestChunk {
        fn new(id: ChunkId, time_of_last_write: i64, storage: ChunkStorage) -> Self {
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
                access_metrics: AccessMetrics {
                    count: 0,
                    last_access: Utc::now(),
                },
                time_of_last_write: from_secs(time_of_last_write),
                lifecycle_action: None,
                storage,
                order: ChunkOrder::MIN,
            }
        }

        fn with_row_count(mut self, row_count: usize) -> Self {
            self.row_count = row_count;
            self
        }

        fn with_action(mut self, action: ChunkLifecycleAction) -> Self {
            self.lifecycle_action = Some(TaskTracker::complete(action));
            self
        }

        fn with_min_timestamp(mut self, min_timestamp: DateTime<Utc>) -> Self {
            self.min_timestamp = Some(min_timestamp);
            self
        }

        fn with_access_metrics(mut self, metrics: AccessMetrics) -> Self {
            self.access_metrics = metrics;
            self
        }

        fn with_order(mut self, order: ChunkOrder) -> Self {
            self.order = order;
            self
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
        id: ChunkId,
        order: ChunkOrder,
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
            chunk_id: ChunkId,
        ) -> Option<Self::Chunk> {
            let db = s.data().db;
            s.chunks
                .get(&chunk_id)
                .map(|(order, chunk)| TestLockableChunk {
                    db,
                    chunk: Arc::clone(chunk),
                    id: chunk_id,
                    order: *order,
                })
        }

        fn chunks(s: &LifecycleReadGuard<'_, Self::Partition, Self>) -> Vec<Self::Chunk> {
            let db = s.data().db;
            let mut chunks: Vec<Self::Chunk> = s
                .chunks
                .iter()
                .map(|(id, (order, chunk))| TestLockableChunk {
                    db,
                    chunk: Arc::clone(chunk),
                    id: *id,
                    order: *order,
                })
                .collect();
            chunks.sort_by_key(|chunk| (chunk.order(), chunk.id()));
            chunks
        }

        fn compact_chunks(
            mut partition: LifecycleWriteGuard<'_, TestPartition, Self>,
            chunks: Vec<LifecycleWriteGuard<'_, TestChunk, Self::Chunk>>,
        ) -> Result<TaskTracker<()>, Self::Error> {
            let id = ChunkId::new_test(partition.next_id);
            partition.next_id += 1;

            let mut new_chunk = TestChunk::new(id, 0, ChunkStorage::ReadBuffer);
            new_chunk.row_count = 0;

            let mut order = ChunkOrder::MAX;
            for chunk in &chunks {
                partition.chunks.remove(&chunk.addr.chunk_id);
                new_chunk.row_count += chunk.row_count;
                new_chunk.min_timestamp = match (new_chunk.min_timestamp, chunk.min_timestamp) {
                    (Some(ts1), Some(ts2)) => Some(ts1.min(ts2)),
                    (Some(ts), None) => Some(ts),
                    (None, Some(ts)) => Some(ts),
                    (None, None) => None,
                };
                order = order.min(chunk.order);
            }

            partition
                .chunks
                .insert(id, (order, Arc::new(RwLock::new(new_chunk))));

            let event = MoverEvents::Compact(chunks.iter().map(|x| x.addr.chunk_id).collect());
            partition.data().db.events.write().push(event);

            Ok(TaskTracker::complete(()))
        }

        fn prepare_persist(
            partition: &mut LifecycleWriteGuard<'_, Self::Partition, Self>,
            _now: DateTime<Utc>,
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
            let mut order = ChunkOrder::MAX;
            for chunk in &chunks {
                partition.chunks.remove(&chunk.addr.chunk_id);
                order = order.min(chunk.order);
            }

            let id = ChunkId::new_test(partition.next_id);
            partition.next_id += 1;

            // The remainder left behind after the split
            let new_chunk = TestChunk::new(id, 0, ChunkStorage::ReadBuffer)
                .with_min_timestamp(handle.timestamp + chrono::Duration::nanoseconds(1));

            partition
                .chunks
                .insert(id, (order, Arc::new(RwLock::new(new_chunk))));

            let event = MoverEvents::Persist(chunks.iter().map(|x| x.addr.chunk_id).collect());
            partition.data().db.events.write().push(event);
            Ok(TaskTracker::complete(()))
        }

        fn drop_chunk(
            mut partition: LifecycleWriteGuard<'_, Self::Partition, Self>,
            chunk: LifecycleWriteGuard<'_, TestChunk, Self::Chunk>,
        ) -> Result<TaskTracker<()>, Self::Error> {
            let chunk_id = chunk.addr().chunk_id;
            partition.chunks.remove(&chunk_id);
            partition
                .data()
                .db
                .events
                .write()
                .push(MoverEvents::Drop(chunk_id));
            Ok(TaskTracker::complete(()))
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

        fn id(&self) -> ChunkId {
            self.id
        }

        fn order(&self) -> ChunkOrder {
            self.order
        }
    }

    impl LifecyclePartition for TestPartition {
        fn partition_key(&self) -> &str {
            "test"
        }

        fn is_persisted(&self) -> bool {
            false
        }

        fn persistable_row_count(&self, _now: DateTime<Utc>) -> usize {
            self.persistable_row_count
        }

        fn minimum_unpersisted_age(&self) -> Option<DateTime<Utc>> {
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

        fn access_metrics(&self) -> AccessMetrics {
            self.access_metrics.clone()
        }

        fn time_of_last_write(&self) -> DateTime<Utc> {
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
                    max_id = max(max_id, x.addr.chunk_id.get().as_u128());
                    (x.addr.chunk_id, (x.order, Arc::new(RwLock::new(x))))
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
        Utc.timestamp(secs, 0)
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
            mub_row_threshold: NonZeroUsize::new(74).unwrap(),
            ..Default::default()
        };
        let chunk = TestChunk::new(ChunkId::new_test(0), 0, ChunkStorage::OpenMutableBuffer);
        assert!(!can_move(&rules, &chunk, from_secs(9)));
        assert!(can_move(&rules, &chunk, from_secs(11)));

        // can move even if the chunk is small
        let chunk = TestChunk::new(ChunkId::new_test(0), 0, ChunkStorage::OpenMutableBuffer)
            .with_row_count(73);
        assert!(can_move(&rules, &chunk, from_secs(11)));

        // If over the row count threshold, we should be able to move
        let chunk = TestChunk::new(ChunkId::new_test(0), 0, ChunkStorage::OpenMutableBuffer)
            .with_row_count(74);
        assert!(can_move(&rules, &chunk, from_secs(0)));

        // If below the default row count threshold, it shouldn't move
        let chunk = TestChunk::new(ChunkId::new_test(0), 0, ChunkStorage::OpenMutableBuffer)
            .with_row_count(73);
        assert!(!can_move(&rules, &chunk, from_secs(0)));
    }

    #[test]
    fn test_sort_free_candidates() {
        let now = Utc::now();
        let access_metrics = |secs: i64| AccessMetrics {
            count: 1,
            last_access: now + chrono::Duration::seconds(secs),
        };

        let mut candidates = vec![
            FreeCandidate {
                partition: &(),
                chunk_id: ChunkId::new_test(1),
                action: FreeAction::Unload,
                access_metrics: access_metrics(40),
            },
            FreeCandidate {
                partition: &(),
                chunk_id: ChunkId::new_test(3),
                action: FreeAction::Unload,
                access_metrics: access_metrics(20),
            },
            FreeCandidate {
                partition: &(),
                chunk_id: ChunkId::new_test(4),
                action: FreeAction::Unload,
                access_metrics: access_metrics(10),
            },
            FreeCandidate {
                partition: &(),
                chunk_id: ChunkId::new_test(5),
                action: FreeAction::Drop,
                access_metrics: access_metrics(10),
            },
            FreeCandidate {
                partition: &(),
                chunk_id: ChunkId::new_test(6),
                action: FreeAction::Drop,
                access_metrics: access_metrics(5),
            },
        ];

        sort_free_candidates(&mut candidates);

        let ids: Vec<_> = candidates.into_iter().map(|x| x.chunk_id).collect();

        // Should first unload, then drop
        //
        // Should order the same actions by access time, with nulls last
        assert_eq!(
            ids,
            vec![
                ChunkId::new_test(4),
                ChunkId::new_test(3),
                ChunkId::new_test(1),
                ChunkId::new_test(6),
                ChunkId::new_test(5)
            ]
        )
    }

    #[test]
    fn test_default_rules() {
        // The default rules shouldn't do anything
        let rules = LifecycleRules::default();
        let chunks = vec![
            TestChunk::new(ChunkId::new_test(0), 1, ChunkStorage::OpenMutableBuffer),
            TestChunk::new(ChunkId::new_test(1), 1, ChunkStorage::OpenMutableBuffer),
            TestChunk::new(ChunkId::new_test(2), 1, ChunkStorage::OpenMutableBuffer),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);
        lifecycle.check_for_work(from_secs(40));
        assert_eq!(*db.events.read(), vec![]);
    }

    #[test]
    fn test_late_arrival() {
        let rules = LifecycleRules {
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            ..Default::default()
        };
        let chunks = vec![
            TestChunk::new(ChunkId::new_test(0), 8, ChunkStorage::OpenMutableBuffer),
            TestChunk::new(ChunkId::new_test(1), 5, ChunkStorage::OpenMutableBuffer),
            TestChunk::new(ChunkId::new_test(2), 0, ChunkStorage::OpenMutableBuffer),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);
        let partition = Arc::clone(&db.partitions.read()[0]);

        lifecycle.check_for_work(from_secs(9));

        assert_eq!(*db.events.read(), vec![]);

        lifecycle.check_for_work(from_secs(11));
        let chunks = partition.read().chunks.keys().cloned().collect::<Vec<_>>();
        // expect chunk 2 to have been compacted into a new chunk 3
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Compact(vec![ChunkId::new_test(2)])]
        );
        assert_eq!(
            chunks,
            vec![
                ChunkId::new_test(0),
                ChunkId::new_test(1),
                ChunkId::new_test(3)
            ]
        );

        lifecycle.check_for_work(from_secs(12));
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Compact(vec![ChunkId::new_test(2)])]
        );

        // Should compact everything possible
        lifecycle.check_for_work(from_secs(20));
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Compact(vec![ChunkId::new_test(2)]),
                MoverEvents::Compact(vec![
                    ChunkId::new_test(0),
                    ChunkId::new_test(1),
                    ChunkId::new_test(3)
                ])
            ]
        );

        assert_eq!(partition.read().chunks.len(), 1);
        assert_eq!(
            partition.read().chunks[&ChunkId::new_test(4)]
                .1
                .read()
                .row_count,
            30
        );
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

        let (tracker, registration) = registry.register(ChunkLifecycleAction::Compacting);

        // Manually add the tracker to the policy as if a previous invocation
        // of check_for_work had started a background move task
        lifecycle.trackers.push(tracker);

        let future = lifecycle.check_for_work(from_secs(0));
        tokio::time::timeout(Duration::from_millis(1), future)
            .await
            .expect_err("expected timeout");

        let future = lifecycle.check_for_work(from_secs(0));
        std::mem::drop(registration);
        tokio::time::timeout(Duration::from_millis(1), future)
            .await
            .expect("expect early return due to task completion");
    }

    #[test]
    fn test_buffer_size_soft_drop_non_persisted() {
        // test that chunk mover can drop non persisted chunks
        // if limit has been exceeded

        // IMPORTANT: the lifecycle rules have the default `persist` flag (false) so NO
        // "write" events will be triggered
        let rules = LifecycleRules {
            buffer_size_soft: Some(NonZeroUsize::new(5).unwrap()),
            persist: false,
            ..Default::default()
        };

        let chunks = vec![TestChunk::new(
            ChunkId::new_test(0),
            0,
            ChunkStorage::OpenMutableBuffer,
        )];

        let db = TestDb::new(rules.clone(), chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(10));
        assert_eq!(*db.events.read(), vec![]);

        let now = Utc::now();

        let chunks = vec![
            // two "open" chunks => they must not be dropped (yet)
            TestChunk::new(ChunkId::new_test(0), 0, ChunkStorage::OpenMutableBuffer),
            TestChunk::new(ChunkId::new_test(1), 0, ChunkStorage::OpenMutableBuffer),
            // "moved" chunk => can be dropped because `drop_non_persistent=true`
            TestChunk::new(ChunkId::new_test(2), 0, ChunkStorage::ReadBuffer),
            // "writing" chunk => cannot be unloaded while write is in-progress
            TestChunk::new(ChunkId::new_test(3), 0, ChunkStorage::ReadBuffer)
                .with_action(ChunkLifecycleAction::Persisting),
            // "written" chunk => can be unloaded
            TestChunk::new(
                ChunkId::new_test(4),
                0,
                ChunkStorage::ReadBufferAndObjectStore,
            )
            .with_access_metrics(AccessMetrics {
                count: 1,
                last_access: now,
            }),
            // "written" chunk => can be unloaded
            TestChunk::new(
                ChunkId::new_test(5),
                0,
                ChunkStorage::ReadBufferAndObjectStore,
            )
            .with_access_metrics(AccessMetrics {
                count: 12,
                last_access: now - chrono::Duration::seconds(1),
            }),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        // Should unload chunk 5 first as access time is smaller
        lifecycle.check_for_work(from_secs(10));
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Unload(ChunkId::new_test(5)),
                MoverEvents::Unload(ChunkId::new_test(4)),
                MoverEvents::Drop(ChunkId::new_test(2))
            ]
        );
    }

    #[test]
    fn test_buffer_size_soft_dont_drop_non_persisted() {
        // test that chunk mover unloads written chunks and can't drop
        // unpersisted chunks when the persist flag is true
        let rules = LifecycleRules {
            buffer_size_soft: Some(NonZeroUsize::new(5).unwrap()),
            persist: true,
            ..Default::default()
        };

        let chunks = vec![TestChunk::new(
            ChunkId::new_test(0),
            0,
            ChunkStorage::OpenMutableBuffer,
        )];

        let db = TestDb::new(rules.clone(), chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(10));
        assert_eq!(*db.events.read(), vec![]);

        let chunks = vec![
            // two "open" chunks => they must not be dropped (yet)
            TestChunk::new(ChunkId::new_test(0), 0, ChunkStorage::OpenMutableBuffer),
            TestChunk::new(ChunkId::new_test(1), 0, ChunkStorage::OpenMutableBuffer),
            // "moved" chunk => cannot be dropped because `drop_non_persistent=false`
            TestChunk::new(ChunkId::new_test(2), 0, ChunkStorage::ReadBuffer),
            // "writing" chunk => cannot be drop while write is in-progess
            TestChunk::new(ChunkId::new_test(3), 0, ChunkStorage::ReadBuffer)
                .with_action(ChunkLifecycleAction::Persisting),
            // "written" chunk => can be unloaded
            TestChunk::new(
                ChunkId::new_test(4),
                0,
                ChunkStorage::ReadBufferAndObjectStore,
            ),
        ];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(10));
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Unload(ChunkId::new_test(4))]
        );
    }

    #[test]
    fn test_buffer_size_soft_no_op() {
        // check that we don't drop anything if nothing is to drop
        let rules = LifecycleRules {
            buffer_size_soft: Some(NonZeroUsize::new(40).unwrap()),
            ..Default::default()
        };

        let chunks = vec![TestChunk::new(
            ChunkId::new_test(0),
            0,
            ChunkStorage::OpenMutableBuffer,
        )];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(10));
        assert_eq!(*db.events.read(), vec![]);
    }

    #[test]
    fn test_compact() {
        let rules = LifecycleRules {
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            persist_row_threshold: NonZeroUsize::new(1_000).unwrap(),
            max_active_compactions: MaxActiveCompactions(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };

        let now = from_secs(20);

        let partitions = vec![
            TestPartition::new(vec![
                // still receiving writes => cannot compact
                TestChunk::new(ChunkId::new_test(0), 20, ChunkStorage::OpenMutableBuffer),
            ]),
            TestPartition::new(vec![
                // still receiving writes => cannot compact
                TestChunk::new(ChunkId::new_test(1), 20, ChunkStorage::OpenMutableBuffer),
                // closed => can compact
                TestChunk::new(ChunkId::new_test(2), 20, ChunkStorage::ClosedMutableBuffer),
            ]),
            TestPartition::new(vec![
                // open but cold => can compact
                TestChunk::new(ChunkId::new_test(3), 5, ChunkStorage::OpenMutableBuffer),
                // closed => can compact
                TestChunk::new(ChunkId::new_test(4), 20, ChunkStorage::ClosedMutableBuffer),
                // closed => can compact
                TestChunk::new(ChunkId::new_test(5), 20, ChunkStorage::ReadBuffer),
                // persisted => cannot compact
                TestChunk::new(
                    ChunkId::new_test(6),
                    20,
                    ChunkStorage::ReadBufferAndObjectStore,
                ),
                // persisted => cannot compact
                TestChunk::new(ChunkId::new_test(7), 20, ChunkStorage::ObjectStoreOnly),
            ]),
            TestPartition::new(vec![
                // closed => can compact
                TestChunk::new(ChunkId::new_test(8), 20, ChunkStorage::ReadBuffer),
                // closed => can compact
                TestChunk::new(ChunkId::new_test(9), 20, ChunkStorage::ReadBuffer),
                // persisted => cannot compact
                TestChunk::new(
                    ChunkId::new_test(10),
                    20,
                    ChunkStorage::ReadBufferAndObjectStore,
                ),
                // persisted => cannot compact
                TestChunk::new(ChunkId::new_test(11), 20, ChunkStorage::ObjectStoreOnly),
            ]),
            TestPartition::new(vec![
                // open but cold => can compact
                TestChunk::new(ChunkId::new_test(12), 5, ChunkStorage::OpenMutableBuffer),
            ]),
            TestPartition::new(vec![
                // already compacted => should not compact
                TestChunk::new(ChunkId::new_test(13), 5, ChunkStorage::ReadBuffer),
            ]),
            TestPartition::new(vec![
                // closed => can compact
                TestChunk::new(ChunkId::new_test(14), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(400),
                // too many individual rows => ignore
                TestChunk::new(ChunkId::new_test(15), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(1_000),
                // closed => can compact
                TestChunk::new(ChunkId::new_test(16), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(400),
                // too many total rows => next compaction job
                TestChunk::new(ChunkId::new_test(17), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(400),
                // too many total rows => next compaction job
                TestChunk::new(ChunkId::new_test(18), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(400),
            ]),
            TestPartition::new(vec![
                // chunks in this partition listed in reverse `order` to make sure that the compaction actually sorts
                // them
                //
                // blocked by action below
                TestChunk::new(ChunkId::new_test(19), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(400)
                    .with_order(ChunkOrder::new(5).unwrap()),
                // has an action
                TestChunk::new(ChunkId::new_test(20), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(400)
                    .with_order(ChunkOrder::new(4).unwrap())
                    .with_action(ChunkLifecycleAction::Compacting),
                // closed => can compact
                TestChunk::new(ChunkId::new_test(21), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(400)
                    .with_order(ChunkOrder::new(3).unwrap()),
                TestChunk::new(ChunkId::new_test(22), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(400)
                    .with_order(ChunkOrder::new(2).unwrap()),
                // has an action, but doesn't block because it's first
                TestChunk::new(ChunkId::new_test(23), 20, ChunkStorage::ReadBuffer)
                    .with_row_count(400)
                    .with_order(ChunkOrder::new(1).unwrap())
                    .with_action(ChunkLifecycleAction::Compacting),
            ]),
        ];

        let db = TestDb::from_partitions(rules, partitions);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(now);
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Compact(vec![ChunkId::new_test(2)]),
                MoverEvents::Compact(vec![
                    ChunkId::new_test(3),
                    ChunkId::new_test(4),
                    ChunkId::new_test(5)
                ]),
                MoverEvents::Compact(vec![ChunkId::new_test(8), ChunkId::new_test(9)]),
                MoverEvents::Compact(vec![ChunkId::new_test(12)]),
                MoverEvents::Compact(vec![ChunkId::new_test(14), ChunkId::new_test(16)]),
                MoverEvents::Compact(vec![ChunkId::new_test(22), ChunkId::new_test(21)]),
            ],
        );

        db.events.write().clear();
        lifecycle.check_for_work(now);
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Compact(vec![
                ChunkId::new_test(17),
                ChunkId::new_test(18)
            ])]
        );
    }

    #[test]
    fn test_compaction_limiter() {
        let rules = LifecycleRules {
            max_active_compactions: MaxActiveCompactions(2.try_into().unwrap()),
            ..Default::default()
        };

        let now = from_secs(50);
        let partitions = vec![
            TestPartition::new(vec![
                // closed => can compact
                TestChunk::new(ChunkId::new_test(0), 20, ChunkStorage::ClosedMutableBuffer),
                // closed => can compact
                TestChunk::new(ChunkId::new_test(10), 30, ChunkStorage::ClosedMutableBuffer),
                // closed => can compact
                TestChunk::new(ChunkId::new_test(12), 40, ChunkStorage::ClosedMutableBuffer),
            ]),
            TestPartition::new(vec![
                // closed => can compact
                TestChunk::new(ChunkId::new_test(1), 20, ChunkStorage::ClosedMutableBuffer),
            ]),
            TestPartition::new(vec![
                // closed => can compact
                TestChunk::new(
                    ChunkId::new_test(200),
                    10,
                    ChunkStorage::ClosedMutableBuffer,
                ),
            ]),
        ];

        let db = TestDb::from_partitions(rules, partitions);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(now);
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Compact(vec![
                    ChunkId::new_test(0),
                    ChunkId::new_test(10),
                    ChunkId::new_test(12)
                ]),
                MoverEvents::Compact(vec![ChunkId::new_test(1)]),
            ],
        );

        db.events.write().clear();

        // Compaction slots freed up, other partition can now compact.
        lifecycle.check_for_work(now);
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Compact(vec![ChunkId::new_test(200)]),],
        );
    }

    #[test]
    fn test_persist() {
        let rules = LifecycleRules {
            persist: true,
            persist_row_threshold: NonZeroUsize::new(1_000).unwrap(),
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            persist_age_threshold_seconds: NonZeroU32::new(10).unwrap(),
            max_active_compactions: MaxActiveCompactions(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };
        let now = from_secs(0);

        let partitions = vec![
            // Insufficient rows and not old enough => don't persist but can compact
            TestPartition::new(vec![
                TestChunk::new(ChunkId::new_test(0), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(ChunkId::new_test(1), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(10, now, from_secs(20)),
            // Sufficient rows => persist
            TestPartition::new(vec![
                TestChunk::new(ChunkId::new_test(2), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(ChunkId::new_test(3), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(1_000, now, from_secs(20)),
            // Writes too old => persist
            TestPartition::new(vec![
                // Should split open chunks
                TestChunk::new(ChunkId::new_test(4), 20, ChunkStorage::OpenMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(ChunkId::new_test(5), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
                TestChunk::new(ChunkId::new_test(6), 0, ChunkStorage::ObjectStoreOnly)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(10, now - chrono::Duration::seconds(10), from_secs(20)),
            // Sufficient rows but conflicting compaction => prevent compaction
            TestPartition::new(vec![
                TestChunk::new(ChunkId::new_test(7), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10))
                    .with_action(ChunkLifecycleAction::Compacting),
                // This chunk would be a compaction candidate, but we want to persist it
                TestChunk::new(ChunkId::new_test(8), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(ChunkId::new_test(9), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(1_000, now, from_secs(20)),
            // Sufficient rows and non-conflicting compaction => persist
            TestPartition::new(vec![
                TestChunk::new(ChunkId::new_test(10), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(21))
                    .with_action(ChunkLifecycleAction::Compacting),
                TestChunk::new(ChunkId::new_test(11), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(ChunkId::new_test(12), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(1_000, now, from_secs(20)),
            // Sufficient rows, non-conflicting compaction and compact-able chunk => persist + compact
            TestPartition::new(vec![
                TestChunk::new(ChunkId::new_test(13), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(21))
                    .with_action(ChunkLifecycleAction::Compacting),
                TestChunk::new(ChunkId::new_test(14), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(21))
                    .with_order(ChunkOrder::new(10).unwrap()),
                TestChunk::new(ChunkId::new_test(15), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(ChunkId::new_test(16), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(1_000, now, from_secs(20)),
            // Checks that we include chunks in a closed "order"-based interval.
            // Note that the chunks here are ordered in reverse to check if the lifecycle policy really uses the chunk
            // order during iteration.
            TestPartition::new(vec![
                TestChunk::new(ChunkId::new_test(24), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(25))
                    .with_order(ChunkOrder::new(5).unwrap()),
                TestChunk::new(ChunkId::new_test(25), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5))
                    .with_order(ChunkOrder::new(4).unwrap()),
                TestChunk::new(ChunkId::new_test(26), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(25))
                    .with_order(ChunkOrder::new(3).unwrap()),
                TestChunk::new(ChunkId::new_test(27), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5))
                    .with_order(ChunkOrder::new(2).unwrap()),
                TestChunk::new(ChunkId::new_test(28), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(25))
                    .with_order(ChunkOrder::new(1).unwrap()),
            ])
            .with_persistence(1_000, now, from_secs(20)),
        ];

        let db = TestDb::from_partitions(rules, partitions);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(0));
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Compact(vec![ChunkId::new_test(0), ChunkId::new_test(1)]),
                MoverEvents::Persist(vec![ChunkId::new_test(2), ChunkId::new_test(3)]),
                MoverEvents::Persist(vec![ChunkId::new_test(4), ChunkId::new_test(5)]),
                MoverEvents::Persist(vec![ChunkId::new_test(11), ChunkId::new_test(12)]),
                MoverEvents::Persist(vec![ChunkId::new_test(15), ChunkId::new_test(16)]),
                // 17 is the resulting chunk from the persist split above
                // This is "quirk" of TestPartition operations being instantaneous
                MoverEvents::Compact(vec![ChunkId::new_test(17), ChunkId::new_test(14)]),
                MoverEvents::Persist(vec![
                    ChunkId::new_test(28),
                    ChunkId::new_test(27),
                    ChunkId::new_test(26),
                    ChunkId::new_test(25)
                ]),
                // 29 is the resulting chunk from the persist split above
                // This is "quirk" of TestPartition operations being instantaneous
                MoverEvents::Compact(vec![ChunkId::new_test(29), ChunkId::new_test(24)]),
            ]
        );
    }

    #[test]
    fn test_persist_empty() {
        let rules = LifecycleRules {
            persist: true,
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            persist_age_threshold_seconds: NonZeroU32::new(20).unwrap(),
            ..Default::default()
        };
        let now = Utc::now();

        // This could occur if the in-memory contents of a partition are deleted, and
        // compaction causes the chunks to be removed. In such a scenario the persistence
        // windows will still think there are rows to be persisted
        let partitions = vec![TestPartition::new(vec![]).with_persistence(
            10,
            now - chrono::Duration::seconds(20),
            from_secs(20),
        )];

        let db = TestDb::from_partitions(rules, partitions);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(now);
        assert_eq!(*db.events.read(), vec![MoverEvents::Persist(vec![]),]);
    }

    #[test]
    fn test_suppress_persistence() {
        let rules = LifecycleRules {
            persist: true,
            persist_row_threshold: NonZeroUsize::new(1_000).unwrap(),
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            persist_age_threshold_seconds: NonZeroU32::new(10).unwrap(),
            max_active_compactions: MaxActiveCompactions(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };
        let now = Utc::now();

        let partitions = vec![
            // Sufficient rows => could persist but should be suppressed
            TestPartition::new(vec![
                TestChunk::new(ChunkId::new_test(2), 0, ChunkStorage::ClosedMutableBuffer)
                    .with_min_timestamp(from_secs(10)),
                TestChunk::new(ChunkId::new_test(3), 0, ChunkStorage::ReadBuffer)
                    .with_min_timestamp(from_secs(5)),
            ])
            .with_persistence(1_000, now, from_secs(20)),
        ];

        let db = TestDb::from_partitions(rules, partitions);
        let mut lifecycle = LifecyclePolicy::new_suppress_persistence(&db);

        lifecycle.check_for_work(now);
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Compact(vec![
                ChunkId::new_test(2),
                ChunkId::new_test(3)
            ]),]
        );

        lifecycle.check_for_work(now);
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Compact(vec![
                ChunkId::new_test(2),
                ChunkId::new_test(3)
            ]),]
        );

        lifecycle.unsuppress_persistence();

        lifecycle.check_for_work(now);
        assert_eq!(
            *db.events.read(),
            vec![
                MoverEvents::Compact(vec![ChunkId::new_test(2), ChunkId::new_test(3)]),
                MoverEvents::Persist(vec![ChunkId::new_test(4)])
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
            ChunkId::new_test(0),
            40,
            ChunkStorage::OpenMutableBuffer,
        )];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(80));
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Compact(vec![ChunkId::new_test(0)])]
        );
    }

    #[test]
    fn test_moves_closed() {
        let rules = LifecycleRules {
            late_arrive_window_seconds: NonZeroU32::new(10).unwrap(),
            ..Default::default()
        };
        let chunks = vec![TestChunk::new(
            ChunkId::new_test(0),
            40,
            ChunkStorage::ClosedMutableBuffer,
        )];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);

        lifecycle.check_for_work(from_secs(80));
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Compact(vec![ChunkId::new_test(0)])]
        );
    }

    #[test]
    fn test_recovers_lifecycle_action() {
        let rules = LifecycleRules::default();
        let chunks = vec![TestChunk::new(
            ChunkId::new_test(0),
            0,
            ChunkStorage::ClosedMutableBuffer,
        )];

        let db = TestDb::new(rules, chunks);
        let mut lifecycle = LifecyclePolicy::new(&db);
        let chunk = Arc::clone(&db.partitions.read()[0].read().chunks[&ChunkId::new_test(0)].1);

        let r0 = TaskRegistration::default();
        let tracker = TaskTracker::new(TaskId(0), &r0, ChunkLifecycleAction::Compacting);
        chunk.write().lifecycle_action = Some(tracker.clone());

        // Shouldn't do anything
        lifecycle.check_for_work(tracker.start_time());
        assert!(chunk.read().lifecycle_action().is_some());

        // Shouldn't do anything as job hasn't finished
        lifecycle.check_for_work(
            tracker.start_time() + chrono::Duration::seconds(LIFECYCLE_ACTION_BACKOFF_SECONDS),
        );
        assert!(chunk.read().lifecycle_action().is_some());

        // "Finish" job
        std::mem::drop(r0);

        // Shouldn't do anything as insufficient time passed
        lifecycle.check_for_work(tracker.start_time());
        assert!(chunk.read().lifecycle_action().is_some());

        // Should clear job
        lifecycle.check_for_work(
            tracker.start_time() + chrono::Duration::seconds(LIFECYCLE_ACTION_BACKOFF_SECONDS),
        );
        assert!(chunk.read().lifecycle_action().is_none());
    }
}
