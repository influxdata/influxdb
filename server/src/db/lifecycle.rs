use std::convert::TryInto;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use observability_deps::tracing::{info, warn};

use data_types::{database_rules::LifecycleRules, error::ErrorLogger, job::Job};

use tracker::{RwLock, TaskTracker};

use super::{
    catalog::chunk::{Chunk, ChunkStage, ChunkStageFrozenRepr},
    Db,
};
use data_types::database_rules::SortOrder;
use futures::future::BoxFuture;
use std::collections::HashSet;
use std::time::{Duration, Instant};

pub const DEFAULT_LIFECYCLE_BACKOFF: Duration = Duration::from_secs(1);
/// Number of seconds to wait before retying a failed lifecycle action
pub const LIFECYCLE_ACTION_BACKOFF: Duration = Duration::from_secs(10);

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
    ///
    /// Returns a future that resolves when this method should be called next
    pub fn check_for_work(&mut self) -> BoxFuture<'static, ()> {
        ChunkMover::check_for_work(self, Utc::now(), Instant::now())
    }
}

/// A trait that encapsulates the core chunk lifecycle logic
///
/// This is to enable independent testing of the policy logic
trait ChunkMover {
    type Job: Send + Sync + 'static;

    /// Return the in-memory size of the database
    fn buffer_size(&self) -> usize;

    /// Return the name of the database
    fn db_name(&self) -> &str;

    /// Returns the lifecycle policy
    fn rules(&self) -> LifecycleRules;

    /// Returns a list of chunks sorted in the order
    /// they should prioritised
    fn chunks(&self, order: &SortOrder) -> Vec<Arc<RwLock<Chunk>>>;

    /// Returns a tracker for the running move task if any
    fn move_tracker(&self) -> Option<&TaskTracker<Self::Job>>;

    /// Returns a tracker for the running write task if any
    fn write_tracker(&self) -> Option<&TaskTracker<Self::Job>>;

    /// Starts an operation to move a chunk to the read buffer
    fn move_to_read_buffer(
        &mut self,
        partition_key: String,
        table_name: String,
        chunk_id: u32,
    ) -> TaskTracker<Self::Job>;

    /// Starts an operation to write a chunk to the object store
    fn write_to_object_store(
        &mut self,
        partition_key: String,
        table_name: String,
        chunk_id: u32,
    ) -> TaskTracker<Self::Job>;

    /// Drops a chunk from the database
    fn drop_chunk(&mut self, partition_key: String, table_name: String, chunk_id: u32);

    /// The core policy logic
    ///
    /// Returns a future that resolves when this method should be called next
    fn check_for_work(
        &mut self,
        now: DateTime<Utc>,
        now_instant: Instant,
    ) -> BoxFuture<'static, ()> {
        let rules = self.rules();
        let chunks = self.chunks(&rules.sort_order);

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
        let mut move_tracker = self.move_tracker().filter(|x| !x.is_complete()).cloned();
        let mut write_tracker = self.write_tracker().filter(|x| !x.is_complete()).cloned();

        // Iterate through the chunks to determine
        // - total memory consumption
        // - any chunks to move

        for chunk in &chunks {
            let chunk_guard = chunk.read();

            let would_move = can_move(&rules, &*chunk_guard, now);
            let would_write = write_tracker.is_none() && rules.persist;

            if let Some(lifecycle_action) = chunk_guard.lifecycle_action() {
                if lifecycle_action.is_complete()
                    && now_instant.duration_since(lifecycle_action.start_instant())
                        >= LIFECYCLE_ACTION_BACKOFF
                {
                    std::mem::drop(chunk_guard);
                    chunk
                        .write()
                        .clear_lifecycle_action()
                        .expect("failed to clear lifecycle action");
                }
                continue;
            }

            match chunk_guard.stage() {
                ChunkStage::Open { .. } => {
                    open_partitions.insert(chunk_guard.key().to_string());
                    if move_tracker.is_none() && would_move {
                        let partition_key = chunk_guard.key().to_string();
                        let table_name = chunk_guard.table_name().to_string();
                        let chunk_id = chunk_guard.id();

                        std::mem::drop(chunk_guard);

                        move_tracker =
                            Some(self.move_to_read_buffer(partition_key, table_name, chunk_id));
                    }
                }
                ChunkStage::Frozen { representation, .. } => match &representation {
                    ChunkStageFrozenRepr::MutableBufferSnapshot(_) if move_tracker.is_none() => {
                        let partition_key = chunk_guard.key().to_string();
                        let table_name = chunk_guard.table_name().to_string();
                        let chunk_id = chunk_guard.id();

                        std::mem::drop(chunk_guard);

                        move_tracker =
                            Some(self.move_to_read_buffer(partition_key, table_name, chunk_id));
                    }
                    ChunkStageFrozenRepr::ReadBuffer { .. } if would_write => {
                        let partition_key = chunk_guard.key().to_string();
                        let table_name = chunk_guard.table_name().to_string();
                        let chunk_id = chunk_guard.id();

                        std::mem::drop(chunk_guard);

                        write_tracker =
                            Some(self.write_to_object_store(partition_key, table_name, chunk_id));
                    }
                    _ => {}
                },
                // TODO: unload read buffer (https://github.com/influxdata/influxdb_iox/issues/1400)
                _ => {}
            }
        }

        if let Some(soft_limit) = rules.buffer_size_soft {
            let mut chunks = chunks.iter();

            loop {
                let buffer_size = self.buffer_size();
                if buffer_size < soft_limit.get() {
                    break;
                }

                // Dropping chunks that are kept in memory by queries frees no memory
                // TODO: LRU drop policy
                match chunks.next() {
                    Some(chunk) => {
                        let chunk_guard = chunk.read();
                        if chunk_guard.lifecycle_action().is_some() {
                            // Cannot drop chunk with in-progress lifecycle action
                            continue;
                        }

                        if (rules.drop_non_persisted
                            && matches!(
                                chunk_guard.stage(),
                                ChunkStage::Frozen {
                                    representation: ChunkStageFrozenRepr::ReadBuffer(_),
                                    ..
                                }
                            ))
                            || matches!(chunk_guard.stage(), ChunkStage::Persisted { .. })
                        {
                            let partition_key = chunk_guard.key().to_string();
                            let table_name = chunk_guard.table_name().to_string();
                            let chunk_id = chunk_guard.id();

                            std::mem::drop(chunk_guard);

                            if open_partitions.contains(&partition_key) {
                                warn!(db_name=self.db_name(), %partition_key, chunk_id, soft_limit, buffer_size,
                                      "dropping chunk from partition containing open chunk. Consider increasing the soft buffer limit");
                            }

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

impl ChunkMover for LifecycleManager {
    type Job = Job;

    fn buffer_size(&self) -> usize {
        self.db.preserved_catalog.state().metrics().memory().total()
    }

    fn db_name(&self) -> &str {
        &self.db_name
    }

    fn rules(&self) -> LifecycleRules {
        self.db.rules.read().lifecycle_rules.clone()
    }

    fn chunks(&self, sort_order: &SortOrder) -> Vec<Arc<RwLock<Chunk>>> {
        self.db
            .preserved_catalog
            .state()
            .chunks_sorted_by(sort_order)
    }

    fn move_tracker(&self) -> Option<&TaskTracker<Job>> {
        self.move_task.as_ref()
    }

    fn write_tracker(&self) -> Option<&TaskTracker<Job>> {
        self.write_task.as_ref()
    }

    fn move_to_read_buffer(
        &mut self,
        partition_key: String,
        table_name: String,
        chunk_id: u32,
    ) -> TaskTracker<Self::Job> {
        info!(%partition_key, %chunk_id, "moving chunk to read buffer");
        let tracker =
            self.db
                .load_chunk_to_read_buffer_in_background(partition_key, table_name, chunk_id);
        self.move_task = Some(tracker.clone());
        tracker
    }

    fn write_to_object_store(
        &mut self,
        partition_key: String,
        table_name: String,
        chunk_id: u32,
    ) -> TaskTracker<Self::Job> {
        info!(%partition_key, %chunk_id, "write chunk to object store");
        let tracker =
            self.db
                .write_chunk_to_object_store_in_background(partition_key, table_name, chunk_id);
        self.write_task = Some(tracker.clone());
        tracker
    }

    fn drop_chunk(&mut self, partition_key: String, table_name: String, chunk_id: u32) {
        info!(%partition_key, %chunk_id, "dropping chunk");
        let _ = self
            .db
            .drop_chunk(&partition_key, &table_name, chunk_id)
            .log_if_error("dropping chunk to free up memory");
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
    use crate::db::catalog::chunk::ChunkMetrics;
    use data_types::partition_metadata::TableSummary;
    use entry::test_helpers::lp_to_entry;
    use object_store::{memory::InMemory, parsed_path, ObjectStore};
    use std::num::{NonZeroU32, NonZeroUsize};
    use tracker::{TaskRegistration, TaskRegistry};

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
        let mut mb_chunk = mutable_buffer::chunk::Chunk::new(
            "table1",
            mutable_buffer::chunk::ChunkMetrics::new_unregistered(),
        );
        mb_chunk.write_table_batch(1, 5, batch).unwrap();

        let mut chunk =
            Chunk::new_open(id, "", mb_chunk, ChunkMetrics::new_unregistered()).unwrap();
        chunk.set_timestamps(
            time_of_first_write.map(from_secs),
            time_of_last_write.map(from_secs),
        );
        chunk
    }

    /// Transitions a new ("open") chunk into the "moving" state.
    fn transition_to_moving(mut chunk: Chunk) -> Chunk {
        chunk.freeze().unwrap();
        chunk.set_moving(&Default::default()).unwrap();
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
        chunk
            .set_writing_to_object_store(&Default::default())
            .unwrap();
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
        let in_memory = InMemory::new();

        let schema = internal_types::schema::builder::SchemaBuilder::new()
            .tag("foo")
            .build()
            .unwrap();

        let object_store = Arc::new(ObjectStore::new_in_memory(in_memory));
        let path = object_store.path_from_dirs_and_filename(parsed_path!("foo"));

        parquet_file::chunk::Chunk::new(
            chunk.key(),
            TableSummary::new("my_awesome_table"),
            path,
            object_store,
            schema,
            parquet_file::chunk::ChunkMetrics::new_unregistered(),
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
        move_tracker: Option<TaskTracker<()>>,
        write_tracker: Option<TaskTracker<()>>,
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
                move_tracker: None,
                write_tracker: None,
                events: vec![],
            }
        }
    }

    impl ChunkMover for DummyMover {
        type Job = ();

        fn buffer_size(&self) -> usize {
            // All chunks are 20 bytes
            self.chunks.len() * 20
        }

        fn db_name(&self) -> &str {
            "my_awesome_db"
        }

        fn rules(&self) -> LifecycleRules {
            self.rules.clone()
        }

        fn chunks(&self, _: &SortOrder) -> Vec<Arc<RwLock<Chunk>>> {
            self.chunks.clone()
        }

        fn move_tracker(&self) -> Option<&TaskTracker<Self::Job>> {
            self.move_tracker.as_ref()
        }

        fn write_tracker(&self) -> Option<&TaskTracker<Self::Job>> {
            self.write_tracker.as_ref()
        }

        fn move_to_read_buffer(
            &mut self,
            _partition_key: String,
            _table_name: String,
            chunk_id: u32,
        ) -> TaskTracker<Self::Job> {
            let chunk = self
                .chunks
                .iter()
                .find(|x| x.read().id() == chunk_id)
                .unwrap();
            chunk.write().set_moving(&Default::default()).unwrap();
            self.events.push(MoverEvents::Move(chunk_id));
            let tracker = TaskTracker::complete(());
            self.move_tracker = Some(tracker.clone());
            tracker
        }

        fn write_to_object_store(
            &mut self,
            _partition_key: String,
            _table_name: String,
            chunk_id: u32,
        ) -> TaskTracker<Self::Job> {
            let chunk = self
                .chunks
                .iter()
                .find(|x| x.read().id() == chunk_id)
                .unwrap();
            chunk
                .write()
                .set_writing_to_object_store(&Default::default())
                .unwrap();
            self.events.push(MoverEvents::Write(chunk_id));
            let tracker = TaskTracker::complete(());
            self.write_tracker = Some(tracker.clone());
            tracker
        }

        fn drop_chunk(&mut self, _partition_key: String, _table_name: String, chunk_id: u32) {
            self.chunks = self
                .chunks
                .drain(..)
                .filter(|x| x.read().id() != chunk_id)
                .collect();
            self.events.push(MoverEvents::Drop(chunk_id))
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
        mover.check_for_work(from_secs(40), Instant::now());
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
        mover.check_for_work(from_secs(9), Instant::now());

        assert_eq!(mover.events, vec![]);

        mover.check_for_work(from_secs(11), Instant::now());
        assert_eq!(mover.events, vec![MoverEvents::Move(2)]);

        mover.check_for_work(from_secs(12), Instant::now());
        assert_eq!(mover.events, vec![MoverEvents::Move(2)]);

        // Should move in the order of chunks in the case of multiple candidates
        mover.check_for_work(from_secs(20), Instant::now());
        assert_eq!(
            mover.events,
            vec![MoverEvents::Move(2), MoverEvents::Move(0)]
        );

        mover.check_for_work(from_secs(20), Instant::now());

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
        let mut registry = TaskRegistry::new();
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            ..Default::default()
        };
        let chunks = vec![new_chunk(0, Some(0), Some(0))];

        let mut mover = DummyMover::new(rules, chunks);

        let (tracker, registration) = registry.register(());
        mover.move_tracker = Some(tracker);

        mover.check_for_work(from_secs(80), Instant::now());

        assert_eq!(mover.events, vec![]);

        std::mem::drop(registration);

        mover.check_for_work(from_secs(80), Instant::now());

        assert_eq!(mover.events, vec![MoverEvents::Move(0)]);
    }

    #[tokio::test]
    async fn test_backoff() {
        let mut registry = TaskRegistry::new();
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(100).unwrap()),
            ..Default::default()
        };
        let mut mover = DummyMover::new(rules, vec![]);

        let (tracker, registration) = registry.register(());

        // Manually set the move_tracker on the DummyMover as if a previous invocation
        // of check_for_work had started a background move task
        mover.move_tracker = Some(tracker);

        let future = mover.check_for_work(from_secs(0), Instant::now());
        tokio::time::timeout(Duration::from_millis(1), future)
            .await
            .expect_err("expected timeout");

        let future = mover.check_for_work(from_secs(0), Instant::now());
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
            new_chunk(0, Some(40), Some(40)),
            new_chunk(1, Some(0), Some(0)),
        ];

        let mut mover = DummyMover::new(rules, chunks);

        // Expect to move chunk_id=1 first, despite it coming second in
        // the order, because chunk_id=0 will only become old enough at t=100

        mover.check_for_work(from_secs(80), Instant::now());
        assert_eq!(mover.events, vec![MoverEvents::Move(1)]);

        mover.check_for_work(from_secs(90), Instant::now());
        assert_eq!(mover.events, vec![MoverEvents::Move(1)]);

        mover.check_for_work(from_secs(110), Instant::now());

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

        let rb = Arc::new(read_buffer::Chunk::new(
            read_buffer::ChunkMetrics::new_unregistered(),
        ));

        let chunks = vec![new_chunk(0, Some(0), Some(0))];

        let mut mover = DummyMover::new(rules.clone(), chunks);

        mover.check_for_work(from_secs(10), Instant::now());
        assert_eq!(mover.events, vec![]);

        let chunks = vec![
            // two "open" chunks => they must not be dropped (yet)
            new_chunk(0, Some(0), Some(0)),
            new_chunk(1, Some(0), Some(0)),
            // "moved" chunk => can be dropped because `drop_non_persistent=true`
            transition_to_moved(new_chunk(2, Some(0), Some(0)), &rb),
            // "writing" chunk => cannot be drop while write is in-progress
            transition_to_writing_to_object_store(new_chunk(3, Some(0), Some(0)), &rb),
            // "written" chunk => can be dropped
            transition_to_written_to_object_store(new_chunk(4, Some(0), Some(0)), &rb),
        ];

        let mut mover = DummyMover::new(rules, chunks);

        mover.check_for_work(from_secs(10), Instant::now());
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

        let rb = Arc::new(read_buffer::Chunk::new(
            read_buffer::ChunkMetrics::new_unregistered(),
        ));

        let chunks = vec![new_chunk(0, Some(0), Some(0))];

        let mut mover = DummyMover::new(rules.clone(), chunks);

        mover.check_for_work(from_secs(10), Instant::now());
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

        mover.check_for_work(from_secs(10), Instant::now());
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

        mover.check_for_work(from_secs(10), Instant::now());
        assert_eq!(mover.events, vec![]);
    }

    #[test]
    fn test_persist() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            persist: true,
            ..Default::default()
        };

        let rb = Arc::new(read_buffer::Chunk::new(
            read_buffer::ChunkMetrics::new_unregistered(),
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

        mover.check_for_work(from_secs(0), Instant::now());

        assert_eq!(mover.events, vec![MoverEvents::Write(1)]);
    }

    #[test]
    fn test_moves_closed() {
        let rules = LifecycleRules {
            mutable_linger_seconds: Some(NonZeroU32::new(10).unwrap()),
            mutable_minimum_age_seconds: Some(NonZeroU32::new(60).unwrap()),
            ..Default::default()
        };
        let chunks = vec![new_chunk(0, Some(40), Some(40))];

        let mut mover = DummyMover::new(rules, chunks);

        // Initially can't move
        mover.check_for_work(from_secs(80), Instant::now());
        assert_eq!(mover.events, vec![]);

        mover.chunks[0].write().freeze().unwrap();

        // As soon as closed can move
        mover.check_for_work(from_secs(80), Instant::now());
        assert_eq!(mover.events, vec![MoverEvents::Move(0)]);
    }

    #[test]
    fn test_recovers_lifecycle_action() {
        let rules = LifecycleRules::default();
        let chunks = vec![new_chunk(0, None, None)];
        let mut mover = DummyMover::new(rules, chunks);

        let chunk = Arc::clone(&mover.chunks[0]);
        chunk.write().freeze().unwrap();

        let r0 = TaskRegistration::default();
        let tracker = {
            let mut chunk = chunk.write();
            chunk.set_moving(&r0).unwrap();
            chunk.lifecycle_action().unwrap().clone()
        };

        // Shouldn't do anything
        mover.check_for_work(from_secs(0), tracker.start_instant());
        assert!(chunk.read().lifecycle_action().is_some());

        // Shouldn't do anything as job hasn't finished
        mover.check_for_work(
            from_secs(0),
            tracker.start_instant() + LIFECYCLE_ACTION_BACKOFF,
        );
        assert!(chunk.read().lifecycle_action().is_some());

        // "Finish" job
        std::mem::drop(r0);

        // Shouldn't do anything as insufficient time passed
        mover.check_for_work(from_secs(0), tracker.start_instant());
        assert!(chunk.read().lifecycle_action().is_some());

        // Should clear job
        mover.check_for_work(
            from_secs(0),
            tracker.start_instant() + LIFECYCLE_ACTION_BACKOFF,
        );
        assert!(chunk.read().lifecycle_action().is_none());
    }
}
