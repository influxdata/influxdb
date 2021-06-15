use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use hashbrown::HashSet;

use data_types::database_rules::LifecycleRules;
use observability_deps::tracing::{debug, warn};
use tracker::TaskTracker;

use crate::{LifecycleChunk, LifecycleDb};
use data_types::chunk_metadata::ChunkStorage;

pub const DEFAULT_LIFECYCLE_BACKOFF: Duration = Duration::from_secs(1);
/// Number of seconds to wait before retying a failed lifecycle action
pub const LIFECYCLE_ACTION_BACKOFF: Duration = Duration::from_secs(10);

/// A `LifecyclePolicy` is created with a `LifecycleDb`
///
/// `LifecyclePolicy::check_for_work` can then be used to drive progress
/// of the `LifecycleChunk` contained within this `LifecycleDb`
pub struct LifecyclePolicy<M: LifecycleDb> {
    db: Arc<M>,
    // TODO: Remove these and use values from chunks within partition
    move_tracker: Option<TaskTracker<()>>,
    write_tracker: Option<TaskTracker<()>>,
}

impl<M: LifecycleDb + Sync + Send> LifecyclePolicy<M> {
    pub fn new(db: Arc<M>) -> Self {
        Self {
            db,
            move_tracker: None,
            write_tracker: None,
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

        // NB the rules for "which to drop/unload first" are defined
        // by rules.sort_order
        let chunks = self.db.chunks(&rules.sort_order);

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
        // persistence lifecycle to ulitimately be persisted to object storage
        for chunk in &chunks {
            let chunk_guard = chunk.read();

            let would_move = can_move(&rules, &*chunk_guard, now);
            let would_write = self.write_tracker.is_none() && rules.persist;

            if let Some(lifecycle_action) = chunk_guard.lifecycle_action() {
                if lifecycle_action.is_complete()
                    && now_instant.duration_since(lifecycle_action.start_instant())
                        >= LIFECYCLE_ACTION_BACKOFF
                {
                    std::mem::drop(chunk_guard);
                    chunk.write().clear_lifecycle_action();
                }
                continue;
            }

            match chunk_guard.storage() {
                ChunkStorage::OpenMutableBuffer => {
                    open_partitions.insert(chunk_guard.partition_key().to_string());
                    if self.move_tracker.is_none() && would_move {
                        let partition_key = chunk_guard.partition_key();
                        let table_name = chunk_guard.table_name();
                        let chunk_id = chunk_guard.chunk_id();

                        std::mem::drop(chunk_guard);

                        self.move_tracker = Some(self.db.move_to_read_buffer(
                            table_name,
                            partition_key,
                            chunk_id,
                        ));
                    }
                }
                ChunkStorage::ClosedMutableBuffer if self.move_tracker.is_none() => {
                    let partition_key = chunk_guard.partition_key();
                    let table_name = chunk_guard.table_name();
                    let chunk_id = chunk_guard.chunk_id();

                    std::mem::drop(chunk_guard);

                    self.move_tracker = Some(self.db.move_to_read_buffer(
                        table_name,
                        partition_key,
                        chunk_id,
                    ));
                }
                ChunkStorage::ReadBuffer if would_write => {
                    let partition_key = chunk_guard.partition_key();
                    let table_name = chunk_guard.table_name();
                    let chunk_id = chunk_guard.chunk_id();

                    std::mem::drop(chunk_guard);

                    self.write_tracker = Some(self.db.write_to_object_store(
                        table_name,
                        partition_key,
                        chunk_id,
                    ));
                }
                _ => {
                    // Chunk is already persisted, no additional work needed to persist it
                }
            }
        }

        // Loop 2: Determine which chunks to clear from memory when
        // the overall database buffer size is exceeded
        if let Some(soft_limit) = rules.buffer_size_soft {
            let mut chunks = chunks.iter();

            loop {
                let buffer_size = self.db.buffer_size();
                if buffer_size < soft_limit.get() {
                    debug!(buffer_size, %soft_limit, "memory use under soft limit");
                    break;
                }
                debug!(buffer_size, %soft_limit, "memory use over soft limit");

                // Dropping chunks that are currently in use by
                // queries frees no memory until the query completes
                match chunks.next() {
                    Some(chunk) => {
                        let chunk_guard = chunk.read();
                        if chunk_guard.lifecycle_action().is_some() {
                            debug!(table_name=%chunk_guard.table_name(), partition_key=%chunk_guard.partition_key(),
                                   chunk_id=%chunk_guard.chunk_id(), lifecycle_action=?chunk_guard.lifecycle_action(),
                                   "can not drop chunk with in-progress lifecycle action");
                            continue;
                        }

                        enum Action {
                            Drop,
                            Unload,
                        }

                        // Figure out if we can drop or unload this chunk
                        let action = match chunk_guard.storage() {
                            ChunkStorage::ReadBuffer | ChunkStorage::ClosedMutableBuffer
                                if rules.drop_non_persisted =>
                            {
                                // Chunk isn't yet persisted but we have
                                // hit the limit so we need to drop it
                                Action::Drop
                            }
                            ChunkStorage::ReadBufferAndObjectStore => Action::Unload,
                            _ => continue,
                        };

                        let partition_key = chunk_guard.partition_key();
                        let table_name = chunk_guard.table_name();
                        let chunk_id = chunk_guard.chunk_id();
                        std::mem::drop(chunk_guard);

                        match action {
                            Action::Drop => {
                                if open_partitions.contains(&partition_key) {
                                    warn!(%partition_key, chunk_id, soft_limit, buffer_size,
                                          "dropping chunk prior to persistence. Consider increasing the soft buffer limit");
                                }

                                self.db.drop_chunk(table_name, partition_key, chunk_id)
                            }
                            Action::Unload => {
                                self.db
                                    .unload_read_buffer(table_name, partition_key, chunk_id)
                            }
                        }
                    }
                    None => {
                        warn!(soft_limit, buffer_size,
                              "soft limited exceeded, but no chunks found that can be evicted. Check lifecycle rules");
                        break;
                    }
                };
            }
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

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};

    use data_types::chunk_metadata::ChunkStorage;
    use data_types::database_rules::SortOrder;
    use tracker::{RwLock, TaskId, TaskRegistration, TaskRegistry};

    use super::*;
    use crate::ChunkLifecycleAction;

    #[derive(Debug, Eq, PartialEq)]
    enum MoverEvents {
        Move(u32),
        Write(u32),
        Drop(u32),
        Unload(u32),
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
        chunks: RwLock<Vec<Arc<RwLock<TestChunk>>>>,
        events: RwLock<Vec<MoverEvents>>,
    }

    impl TestDb {
        fn new(rules: LifecycleRules, chunks: Vec<TestChunk>) -> Self {
            Self {
                rules,
                chunks: RwLock::new(
                    chunks
                        .into_iter()
                        .map(|x| Arc::new(RwLock::new(x)))
                        .collect(),
                ),
                events: RwLock::new(vec![]),
            }
        }
    }

    impl LifecycleDb for TestDb {
        type Chunk = TestChunk;

        fn buffer_size(&self) -> usize {
            // All chunks are 20 bytes
            self.chunks.read().len() * 20
        }

        fn rules(&self) -> LifecycleRules {
            self.rules.clone()
        }

        fn chunks(&self, _: &SortOrder) -> Vec<Arc<RwLock<TestChunk>>> {
            self.chunks.read().clone()
        }

        fn move_to_read_buffer(
            &self,
            _table_name: String,
            _partition_key: String,
            chunk_id: u32,
        ) -> TaskTracker<()> {
            let chunks = self.chunks.read();
            let chunk = chunks
                .iter()
                .find(|x| x.read().chunk_id() == chunk_id)
                .unwrap();
            chunk.write().storage = ChunkStorage::ReadBuffer;
            self.events.write().push(MoverEvents::Move(chunk_id));
            TaskTracker::complete(())
        }

        fn write_to_object_store(
            &self,
            _table_name: String,
            _partition_key: String,
            chunk_id: u32,
        ) -> TaskTracker<()> {
            let chunks = self.chunks.read();
            let chunk = chunks
                .iter()
                .find(|x| x.read().chunk_id() == chunk_id)
                .unwrap();
            chunk.write().storage = ChunkStorage::ReadBufferAndObjectStore;
            self.events.write().push(MoverEvents::Write(chunk_id));
            TaskTracker::complete(())
        }

        fn drop_chunk(&self, _table_name: String, _partition_key: String, chunk_id: u32) {
            let mut chunks = self.chunks.write();
            chunks.retain(|x| x.read().chunk_id() != chunk_id);
            self.events.write().push(MoverEvents::Drop(chunk_id))
        }

        fn unload_read_buffer(&self, _table_name: String, _partition_key: String, chunk_id: u32) {
            let chunks = self.chunks.read();
            let chunk = chunks
                .iter()
                .find(|x| x.read().chunk_id() == chunk_id)
                .unwrap();

            chunk.write().storage = ChunkStorage::ObjectStoreOnly;
            self.events.write().push(MoverEvents::Unload(chunk_id))
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
    fn test_default_rules() {
        // The default rules shouldn't do anything
        let rules = LifecycleRules::default();
        let chunks = vec![
            TestChunk::new(0, Some(1), Some(1), ChunkStorage::OpenMutableBuffer),
            TestChunk::new(1, Some(20), Some(1), ChunkStorage::OpenMutableBuffer),
            TestChunk::new(2, Some(30), Some(1), ChunkStorage::OpenMutableBuffer),
        ];

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));
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

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

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

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

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
        let db = Arc::new(TestDb::new(rules, vec![]));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

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

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

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

        let db = Arc::new(TestDb::new(rules.clone(), chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

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

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

        lifecycle.check_for_work(from_secs(10), Instant::now());
        assert_eq!(
            *db.events.read(),
            vec![MoverEvents::Drop(2), MoverEvents::Unload(4)]
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

        let db = Arc::new(TestDb::new(rules.clone(), chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

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

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

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

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

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

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

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

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));

        // Initially can't move
        lifecycle.check_for_work(from_secs(80), Instant::now());
        assert_eq!(*db.events.read(), vec![]);

        db.chunks.read()[0].write().storage = ChunkStorage::ClosedMutableBuffer;

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

        let db = Arc::new(TestDb::new(rules, chunks));
        let mut lifecycle = LifecyclePolicy::new(Arc::clone(&db));
        let chunk = &db.chunks.read()[0];

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
