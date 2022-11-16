//! Manages the persistence and eviction lifecycle of data in the buffer across all shards.
//! Note that the byte counts logged by the lifecycle manager and when exactly persistence gets
//! triggered aren't required to be absolutely accurate. The byte count is just an estimate
//! anyway, this just needs to keep things moving along to keep memory use roughly under
//! some absolute number and individual Parquet files that get persisted below some number. It
//! is expected that they may be above or below the absolute thresholds.

pub mod mock_handle;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use data_types::{NamespaceId, PartitionId, SequenceNumber, ShardId, TableId};
use iox_time::{Time, TimeProvider};
use metric::{Metric, U64Counter};
use observability_deps::tracing::{error, info, trace, warn};
use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;
use tracker::TrackedFutureExt;

use crate::{
    data::Persister,
    job::{Job, JobRegistry},
    poison::{PoisonCabinet, PoisonPill},
};

/// API suitable for ingester tasks to query and update the server lifecycle state.
pub trait LifecycleHandle: Send + Sync + 'static {
    /// Logs bytes written into a partition so that it can be tracked for the manager to
    /// trigger persistence. Returns true if the ingester should pause consuming from the
    /// write buffer so that persistence can catch up and free up memory.
    #[allow(clippy::too_many_arguments)]
    fn log_write(
        &self,
        partition_id: PartitionId,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        sequence_number: SequenceNumber,
        bytes_written: usize,
        rows_written: usize,
    ) -> bool;

    /// Returns true if the `total_bytes` tracked by the manager is less than the pause amount.
    /// As persistence runs, the `total_bytes` go down.
    fn can_resume_ingest(&self) -> bool;
}

/// A handle for shard consumers to interact with the global
/// [`LifecycleManager`] instance.
///
/// This handle presents an API suitable for ingester tasks to query and update
/// the [`LifecycleManager`] state.
#[derive(Debug, Clone)]
pub(crate) struct LifecycleHandleImpl {
    time_provider: Arc<dyn TimeProvider>,

    config: Arc<LifecycleConfig>,

    /// The state shared with the [`LifecycleManager`].
    state: Arc<Mutex<LifecycleState>>,
}

impl LifecycleHandle for LifecycleHandleImpl {
    fn log_write(
        &self,
        partition_id: PartitionId,
        shard_id: ShardId,
        namespace_id: NamespaceId,
        table_id: TableId,
        sequence_number: SequenceNumber,
        bytes_written: usize,
        rows_written: usize,
    ) -> bool {
        let mut s = self.state.lock();
        let now = self.time_provider.now();

        let mut stats =
            s.partition_stats
                .entry(partition_id)
                .or_insert_with(|| PartitionLifecycleStats {
                    shard_id,
                    partition_id,
                    namespace_id,
                    table_id,
                    first_write: now,
                    last_write: now,
                    bytes_written: 0,
                    rows_written: 0,
                    first_sequence_number: sequence_number,
                });

        assert_eq!(stats.shard_id, shard_id);
        assert_eq!(stats.namespace_id, namespace_id);
        assert_eq!(stats.table_id, table_id);

        stats.bytes_written += bytes_written;
        stats.last_write = now;
        stats.rows_written += rows_written;

        trace!(
            shard_id=%stats.shard_id,
            partition_id=%stats.partition_id,
            namespace_id=%stats.namespace_id,
            table_id=%stats.table_id,
            first_write=%stats.first_write,
            last_write=%stats.last_write,
            bytes_written=%stats.bytes_written,
            first_sequence_number=?stats.first_sequence_number,
            "logged write"
        );

        s.total_bytes += bytes_written;

        // Pause if the server has exceeded the configured memory limit.
        s.total_bytes >= self.config.pause_ingest_size
    }

    fn can_resume_ingest(&self) -> bool {
        let s = self.state.lock();
        s.total_bytes < self.config.pause_ingest_size
    }
}

/// The lifecycle manager keeps track of the size and age of partitions across
/// all shards. It triggers persistence based on keeping total memory usage
/// around a set amount while ensuring that partitions don't get too old or
/// large before being persisted.
///
/// The [`LifecycleManager`] instance is responsible for evaluating the state of
/// the partitions and periodically persisting and evicting partitions to
/// maintain the memory usage and acceptable data loss windows within the
/// defined configuration limits. It is expected to act as a singleton within an
/// ingester process.
///
/// The current system state is tracked by ingester tasks pushing updates to the
/// [`LifecycleManager`] through their respective [`LifecycleHandle`] instances.
///
/// A [`LifecycleManager`] MUST be driven by an external actor periodically
/// calling [`LifecycleManager::maybe_persist()`].
#[derive(Debug)]
pub(crate) struct LifecycleManager {
    config: Arc<LifecycleConfig>,
    time_provider: Arc<dyn TimeProvider>,
    job_registry: Arc<JobRegistry>,

    /// State shared with the [`LifecycleHandle`].
    ///
    /// [`LifecycleHandle`] instances update statistics directly by calling
    /// [`LifecycleHandle::log_write()`].
    state: Arc<Mutex<LifecycleState>>,

    /// Counter for memory pressure triggering a persist.
    persist_memory_counter: U64Counter,
    /// Counter for the size of a partition triggering a persist.
    persist_size_counter: U64Counter,
    /// Counter for the age of a partition triggering a persist.
    persist_age_counter: U64Counter,
    /// Counter for a partition going cold for writes triggering a persist.
    persist_cold_counter: U64Counter,
    /// Counter tracking the number of times a partition has been evicted for
    /// containing too many rows.
    persist_rows_counter: U64Counter,
}

/// The configuration options for the lifecycle on the ingester.
#[derive(Debug, Clone, Copy)]
pub struct LifecycleConfig {
    /// The ingester will pause pulling data from the write buffer if it hits this amount of memory
    /// used, waiting until persistence evicts partitions from memory.
    pause_ingest_size: usize,
    /// When the ingester hits this threshold, the lifecycle manager will persist the largest
    /// partitions currently buffered until it falls below this threshold. An ingester running
    /// in a steady state should operate around this amount of memory usage.
    persist_memory_threshold: usize,
    /// If the total bytes written to an individual partition crosses
    /// this threshold, it will be persisted.
    ///
    /// NOTE: This number is related, but *NOT* the same as the size
    /// of the memory used to keep the partition buffered.
    ///
    /// The purpose of this setting to to ensure the ingester doesn't
    /// create Parquet files that are too large.
    partition_size_threshold: usize,
    /// If an individual partitiion has had data buffered for longer than this period of time, the
    /// manager will persist it. This setting is to ensure we have an upper bound on how far back
    /// we will need to read in the write buffer on restart or recovery.
    partition_age_threshold: Duration,
    /// If an individual partition hasn't received a write for longer than this period of time, the
    /// manager will persist it. This is to ensure that cold partitions get cleared out to make
    /// room for partitions that are actively receiving writes.
    partition_cold_threshold: Duration,

    /// The maximum number of rows allowed within a single partition before it
    /// MUST be persisted.
    ///
    /// Reaching this limit pauses ingest while the partition is flushed to
    /// object storage.
    partition_row_max: usize,
}

impl LifecycleConfig {
    /// Initialize a new LifecycleConfig. panics if the passed `pause_ingest_size` is less than the
    /// `persist_memory_threshold`.
    pub const fn new(
        pause_ingest_size: usize,
        persist_memory_threshold: usize,
        partition_size_threshold: usize,
        partition_age_threshold: Duration,
        partition_cold_threshold: Duration,
        partition_row_max: usize,
    ) -> Self {
        // this must be true to ensure that persistence will get triggered, freeing up memory
        assert!(pause_ingest_size > persist_memory_threshold);

        Self {
            pause_ingest_size,
            persist_memory_threshold,
            partition_size_threshold,
            partition_age_threshold,
            partition_cold_threshold,
            partition_row_max,
        }
    }
}

#[derive(Default, Debug)]
struct LifecycleState {
    total_bytes: usize,
    partition_stats: BTreeMap<PartitionId, PartitionLifecycleStats>,
}

impl LifecycleState {
    fn remove(&mut self, partition_id: &PartitionId) -> Option<PartitionLifecycleStats> {
        self.partition_stats.remove(partition_id)
    }
}

/// A snapshot of the stats for the lifecycle manager
#[derive(Debug)]
struct LifecycleStats {
    /// total number of bytes the lifecycle manager is aware of across all shards and
    /// partitions. Based on the mutable batch sizes received into all partitions.
    pub total_bytes: usize,
    /// the stats for every partition the lifecycle manager is tracking.
    pub partition_stats: Vec<PartitionLifecycleStats>,
}

/// The stats for a partition
#[derive(Debug, Clone)]
struct PartitionLifecycleStats {
    /// The shard this partition is under
    shard_id: ShardId,
    /// The namespace identifier
    namespace_id: NamespaceId,
    /// The table identifier
    table_id: TableId,
    /// The partition identifier
    partition_id: PartitionId,
    /// Time that the partition received its first write. This is reset anytime
    /// the partition is persisted.
    first_write: Time,
    /// Time that the partition received its last write. This is reset anytime
    /// the partition is persisted.
    last_write: Time,
    /// The number of bytes in the partition as estimated by the mutable batch sizes.
    bytes_written: usize,
    /// The number of rows in the partition as estimated by the mutable batch
    /// sizes + snapshots.
    rows_written: usize,
    /// The sequence number the partition received on its first write. This is reset anytime
    /// the partition is persisted.
    first_sequence_number: SequenceNumber,
}

impl LifecycleManager {
    /// Initialize a new lifecycle manager that will persist when `maybe_persist` is called
    /// if anything is over the size or age threshold.
    pub(crate) fn new(
        config: LifecycleConfig,
        metric_registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let persist_counter: Metric<U64Counter> = metric_registry.register_metric(
            "ingester_lifecycle_persist_count",
            "counter for different triggers that cause partition persistence",
        );

        let persist_memory_counter = persist_counter.recorder(&[("trigger", "memory")]);
        let persist_size_counter = persist_counter.recorder(&[("trigger", "size")]);
        let persist_age_counter = persist_counter.recorder(&[("trigger", "age")]);
        let persist_cold_counter = persist_counter.recorder(&[("trigger", "cold")]);
        let persist_rows_counter = persist_counter.recorder(&[("trigger", "rows")]);

        let job_registry = Arc::new(JobRegistry::new(
            metric_registry,
            Arc::clone(&time_provider),
        ));
        Self {
            config: Arc::new(config),
            time_provider,
            job_registry,
            state: Default::default(),
            persist_memory_counter,
            persist_size_counter,
            persist_age_counter,
            persist_cold_counter,
            persist_rows_counter,
        }
    }

    /// Acquire a shareable [`LifecycleHandle`] for this manager instance.
    pub(super) fn handle(&self) -> LifecycleHandleImpl {
        LifecycleHandleImpl {
            time_provider: Arc::clone(&self.time_provider),
            config: Arc::clone(&self.config),
            state: Arc::clone(&self.state),
        }
    }

    /// This will persist any partitions that are over their size or age thresholds and
    /// persist as many partitions as necessary (largest first) to get below the memory threshold.
    /// The persist operations are spawned in new tasks and run at the same time, but the
    /// function waits for all to return before completing.
    pub async fn maybe_persist<P: Persister>(&mut self, persister: &Arc<P>) {
        let LifecycleStats {
            mut total_bytes,
            partition_stats,
        } = self.stats();

        // get anything over the threshold size or age to persist
        let now = self.time_provider.now();

        let (mut to_persist, mut rest): (
            Vec<PartitionLifecycleStats>,
            Vec<PartitionLifecycleStats>,
        ) = partition_stats.into_iter().partition(|s| {
            //
            // Log the partitions that are marked for persistence using
            // consistent fields across all trigger types.
            //

            // Check if this partition's first write occurred long enough ago
            // that the data is considered "old" and can be flushed.
            let aged_out = match now.checked_duration_since(s.first_write) {
                Some(age) if age > self.config.partition_age_threshold => {
                    info!(
                        shard_id=%s.shard_id,
                        partition_id=%s.partition_id,
                        first_write=%s.first_write,
                        last_write=%s.last_write,
                        bytes_written=s.bytes_written,
                        rows_written=s.rows_written,
                        first_sequence_number=?s.first_sequence_number,
                        age=?age,
                        "partition is over age threshold, persisting"
                    );
                    self.persist_age_counter.inc(1);
                    true
                }
                None => {
                    warn!(
                        shard_id=%s.shard_id,
                        partition_id=%s.partition_id,
                        "unable to calculate partition age"
                    );
                    false
                }
                _ => false,
            };

            // Check if this partition's most recent write was long enough ago
            // that the partition is considered "cold" and is unlikely to see
            // new writes imminently.
            let is_cold = match now.checked_duration_since(s.last_write) {
                Some(age) if age > self.config.partition_cold_threshold => {
                    info!(
                        shard_id=%s.shard_id,
                        partition_id=%s.partition_id,
                        first_write=%s.first_write,
                        last_write=%s.last_write,
                        bytes_written=s.bytes_written,
                        rows_written=s.rows_written,
                        first_sequence_number=?s.first_sequence_number,
                        no_writes_for=?age,
                        "partition is cold, persisting"
                    );
                    self.persist_cold_counter.inc(1);
                    true
                }
                None => {
                    warn!(
                        shard_id=%s.shard_id,
                        partition_id=%s.partition_id,
                        "unable to calculate partition cold duration"
                    );
                    false
                }
                _ => false,
            };

            // If this partition contains more rows than it is permitted, flush
            // it.
            let exceeded_max_rows = s.rows_written >= self.config.partition_row_max;
            if exceeded_max_rows {
                info!(
                    shard_id=%s.shard_id,
                    partition_id=%s.partition_id,
                    first_write=%s.first_write,
                    last_write=%s.last_write,
                    bytes_written=s.bytes_written,
                    rows_written=s.rows_written,
                    first_sequence_number=?s.first_sequence_number,
                    "partition is over max row, persisting"
                );
                self.persist_rows_counter.inc(1);
            }

            // If the partition's in-memory buffer is larger than the configured
            // maximum byte size, flush it.
            let sized_out = s.bytes_written > self.config.partition_size_threshold;
            if sized_out {
                info!(
                    shard_id=%s.shard_id,
                    partition_id=%s.partition_id,
                    first_write=%s.first_write,
                    last_write=%s.last_write,
                    bytes_written=s.bytes_written,
                    rows_written=s.rows_written,
                    first_sequence_number=?s.first_sequence_number,
                    "partition exceeded byte size threshold, persisting"
                );
                self.persist_size_counter.inc(1);
            }

            aged_out || sized_out || is_cold || exceeded_max_rows
        });

        // keep track of what we'll be evicting to see what else to drop
        for s in &to_persist {
            total_bytes -= s.bytes_written;
        }

        // if we're still over the memory threshold, persist as many of the largest partitions
        // until we're under. It's ok if this is stale, it'll just get handled on the next pass
        // through.
        if total_bytes > self.config.persist_memory_threshold {
            info!(
                total_bytes,
                persist_memory_threshold = self.config.persist_memory_threshold,
                "process-wide memory threshold exceeded, force-persisting partitions",
            );
            rest.sort_by(|a, b| b.bytes_written.cmp(&a.bytes_written));

            let mut remaining = vec![];

            let mut memory_persist_counter = 0;
            for s in rest {
                if total_bytes >= self.config.persist_memory_threshold {
                    total_bytes -= s.bytes_written;
                    to_persist.push(s);
                    memory_persist_counter += 1;
                } else {
                    remaining.push(s);
                }
            }

            self.persist_memory_counter.inc(memory_persist_counter);

            rest = remaining;
        }

        // for the shards that are getting data persisted, keep track of what
        // the highest seqeunce number was for each.
        let mut shard_maxes = BTreeMap::new();
        for s in &to_persist {
            shard_maxes
                .entry(s.shard_id)
                .and_modify(|sn| {
                    if *sn < s.first_sequence_number {
                        *sn = s.first_sequence_number;
                    }
                })
                .or_insert(s.first_sequence_number);
        }

        let persist_tasks: Vec<_> = to_persist
            .into_iter()
            .map(|s| {
                // BUG: TOCTOU: memory usage released may be incorrect.
                //
                // Here the amount of memory to be reduced is acquired, but this
                // code does not prevent continued writes adding more data to
                // the partition in another thread.
                //
                // This may lead to more actual data being persisted than the
                // call below returns to the server pool - this would slowly
                // starve the ingester of memory it thinks it has.
                //
                // See https://github.com/influxdata/influxdb_iox/issues/5777

                // Mark this partition as being persisted, and remember the
                // memory allocation it had accumulated.
                let partition_memory_usage = self
                    .remove(s.partition_id)
                    .map(|s| s.bytes_written)
                    .unwrap_or_default();
                let persister = Arc::clone(persister);

                let (_tracker, registration) = self.job_registry.register(Job::Persist {
                    partition_id: s.partition_id,
                });

                let state = Arc::clone(&self.state);
                tokio::task::spawn(async move {
                    persister
                        .persist(s.shard_id, s.namespace_id, s.table_id, s.partition_id)
                        .await;
                    // Now the data has been uploaded and the memory it was
                    // using has been freed, released the memory capacity back
                    // the ingester.
                    state.lock().total_bytes -= partition_memory_usage;
                })
                .track(registration)
            })
            .collect();

        if !persist_tasks.is_empty() {
            let persists = futures::future::join_all(persist_tasks.into_iter());
            let results = persists.await;
            for res in results {
                res.expect("not aborted").expect("task finished");
            }

            // for the shards that had data persisted, update their min_unpersisted_sequence_number
            // to either the minimum remaining in everything that didn't get persisted, or the
            // highest number that was persisted. Marking the highest number as the state is ok
            // because it just needs to represent the farthest we'd have to seek back in the write
            // buffer. Any data replayed during recovery that has already been persisted will just
            // be ignored.
            //
            // The calculation of the min unpersisted sequence number is:
            //
            // - If there is any unpersisted data (i.e. `rest` contains entries for that shard)
            //   then we take the minimum sequence number from there. Note that there might be a
            //   gap between the data that we have just persisted and the unpersisted section.
            // - If there is NO unpersisted data, we take the max sequence number of the part that
            //   we have just persisted. Note that can cannot use "max + 1" because the lifecycle
            //   handler receives writes on a partition level and therefore might run while data
            //   for a single sequence number is added. So the max sequence number that we have
            //   just persisted might have more data.
            for (shard_id, sequence_number) in shard_maxes {
                let min = rest
                    .iter()
                    .filter(|s| s.shard_id == shard_id)
                    .map(|s| s.first_sequence_number)
                    .min()
                    .unwrap_or(sequence_number);
                trace!(
                    min_unpersisted_sequence_number=?min,
                    shard_id=%shard_id,
                    sequence_number=?sequence_number,
                    "updated min_unpersisted_sequence_number for persisted shard"
                );
                persister
                    .update_min_unpersisted_sequence_number(shard_id, min)
                    .await;
            }
        }
    }

    /// Returns a point in time snapshot of the lifecycle state.
    fn stats(&self) -> LifecycleStats {
        let s = self.state.lock();
        let partition_stats: Vec<_> = s.partition_stats.values().cloned().collect();

        LifecycleStats {
            total_bytes: s.total_bytes,
            partition_stats,
        }
    }

    /// Removes the partition from the state
    fn remove(&self, partition_id: PartitionId) -> Option<PartitionLifecycleStats> {
        let mut s = self.state.lock();
        s.remove(&partition_id)
    }
}

const CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Runs the lifecycle manager to trigger persistence every second.
pub(crate) async fn run_lifecycle_manager<P: Persister>(
    mut manager: LifecycleManager,
    persister: Arc<P>,
    shutdown: CancellationToken,
    poison_cabinet: Arc<PoisonCabinet>,
) {
    loop {
        if poison_cabinet.contains(&PoisonPill::LifecyclePanic) {
            panic!("Lifecycle manager poisoned, panic");
        }
        if poison_cabinet.contains(&PoisonPill::LifecycleExit) {
            error!("Lifecycle manager poisoned, exit early");
            return;
        }

        if shutdown.is_cancelled() {
            info!("Lifecycle manager shutdown");
            return;
        }

        manager.maybe_persist(&persister).await;

        manager.job_registry.reclaim();

        tokio::select!(
            _ = tokio::time::sleep(CHECK_INTERVAL) => {},
            _ = shutdown.cancelled() => {},
        );
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use async_trait::async_trait;
    use iox_time::MockProvider;
    use metric::{Attributes, Registry};
    use tokio::sync::Barrier;

    use super::*;

    #[derive(Default)]
    struct TestPersister {
        persist_called: Mutex<BTreeSet<PartitionId>>,
        update_min_calls: Mutex<Vec<(ShardId, SequenceNumber)>>,
    }

    #[async_trait]
    impl Persister for TestPersister {
        async fn persist(
            &self,
            _shard_id: ShardId,
            _namespace_id: NamespaceId,
            _table_id: TableId,
            partition_id: PartitionId,
        ) {
            let mut p = self.persist_called.lock();
            p.insert(partition_id);
        }

        async fn update_min_unpersisted_sequence_number(
            &self,
            shard_id: ShardId,
            sequence_number: SequenceNumber,
        ) {
            let mut u = self.update_min_calls.lock();
            u.push((shard_id, sequence_number));
        }
    }

    impl TestPersister {
        fn persist_called_for(&self, partition_id: PartitionId) -> bool {
            let p = self.persist_called.lock();
            p.contains(&partition_id)
        }

        fn update_min_calls(&self) -> Vec<(ShardId, SequenceNumber)> {
            let u = self.update_min_calls.lock();
            u.clone()
        }
    }

    #[derive(Debug, Clone)]
    /// Synchronizes waiting on some test event
    struct EventBarrier {
        before: Arc<Barrier>,
        after: Arc<Barrier>,
    }

    impl EventBarrier {
        fn new() -> Self {
            Self {
                before: Arc::new(Barrier::new(2)),
                after: Arc::new(Barrier::new(2)),
            }
        }
    }

    /// This persister will pause after persist is called
    struct PausablePersister {
        inner: TestPersister,
        events: Mutex<BTreeMap<PartitionId, EventBarrier>>,
    }

    impl PausablePersister {
        fn new() -> Self {
            Self {
                inner: TestPersister::default(),
                events: Mutex::new(BTreeMap::new()),
            }
        }
    }

    #[async_trait]
    impl Persister for PausablePersister {
        async fn persist(
            &self,
            shard_id: ShardId,
            namespace_id: NamespaceId,
            table_id: TableId,
            partition_id: PartitionId,
        ) {
            self.inner
                .persist(shard_id, namespace_id, table_id, partition_id)
                .await;
            if let Some(event) = self.event(partition_id) {
                event.before.wait().await;
                event.after.wait().await;
            }
        }

        async fn update_min_unpersisted_sequence_number(
            &self,
            shard_id: ShardId,
            sequence_number: SequenceNumber,
        ) {
            self.inner
                .update_min_unpersisted_sequence_number(shard_id, sequence_number)
                .await
        }
    }

    impl PausablePersister {
        /// Wait until the persist operation has started
        async fn wait_for_persist(&self, partition_id: PartitionId) {
            let event = self
                .event(partition_id)
                .expect("partition not configured to wait");
            event.before.wait().await;
        }

        /// Allow the persist operation to complete
        async fn complete_persist(&self, partition_id: PartitionId) {
            let event = self
                .take_event(partition_id)
                .expect("partition not configured to wait");
            event.after.wait().await;
        }

        /// reset so a persist operation can begin again
        fn pause_next(&self, partition_id: PartitionId) {
            self.events.lock().insert(partition_id, EventBarrier::new());
        }

        /// get the event barrier configured for the specified partition
        fn event(&self, partition_id: PartitionId) -> Option<EventBarrier> {
            self.events.lock().get(&partition_id).cloned()
        }

        /// get the event barrier configured for the specified partition removing it
        fn take_event(&self, partition_id: PartitionId) -> Option<EventBarrier> {
            self.events.lock().remove(&partition_id)
        }
    }

    #[test]
    fn logs_write() {
        let config = LifecycleConfig {
            pause_ingest_size: 20,
            persist_memory_threshold: 10,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_nanos(0),
            partition_cold_threshold: Duration::from_secs(500),
            partition_row_max: 100,
        };
        let TestLifecycleManger {
            m, time_provider, ..
        } = TestLifecycleManger::new(config);
        let shard_id = ShardId::new(1);
        let h = m.handle();

        // log first two writes at different times
        assert!(!h.log_write(
            PartitionId::new(1),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            1,
            1
        ));
        time_provider.inc(Duration::from_nanos(10));
        assert!(!h.log_write(
            PartitionId::new(1),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            1,
            1
        ));

        // log another write for different partition using a different handle
        assert!(!m.handle().log_write(
            PartitionId::new(2),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(3),
            3,
            3
        ));

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 5);

        let p1 = stats.partition_stats.get(0).unwrap();
        assert_eq!(p1.bytes_written, 2);
        assert_eq!(p1.rows_written, 2);
        assert_eq!(p1.partition_id, PartitionId::new(1));
        assert_eq!(p1.first_write, Time::from_timestamp_nanos(0));

        let p2 = stats.partition_stats.get(1).unwrap();
        assert_eq!(p2.bytes_written, 3);
        assert_eq!(p2.rows_written, 3);
        assert_eq!(p2.partition_id, PartitionId::new(2));
        assert_eq!(p2.first_write, Time::from_timestamp_nanos(10));
    }

    #[tokio::test]
    async fn pausing_and_resuming_ingest() {
        let config = LifecycleConfig {
            pause_ingest_size: 10,
            persist_memory_threshold: 10,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_nanos(0),
            partition_cold_threshold: Duration::from_secs(500),
            partition_row_max: 100,
        };
        let partition_id = PartitionId::new(1);
        let TestLifecycleManger { mut m, .. } = TestLifecycleManger::new(config);
        let shard_id = ShardId::new(1);
        let h = m.handle();

        // write more than the limit (10)
        assert!(h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            15,
            1
        ));
        assert!(!h.can_resume_ingest());

        // all subsequent writes should also indicate a pause
        assert!(h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            1,
            1
        ));
        assert!(!h.can_resume_ingest());

        // persist the partition
        let persister = Arc::new(TestPersister::default());
        m.maybe_persist(&persister).await;

        // ingest can resume
        assert!(h.can_resume_ingest());
        assert!(!h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(3),
            3,
            1
        ));
    }

    #[tokio::test]
    async fn pausing_and_resuming_ingest_row_count_allowed() {
        let config = LifecycleConfig {
            pause_ingest_size: 20,
            persist_memory_threshold: 10,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_nanos(0),
            partition_cold_threshold: Duration::from_secs(500),
            partition_row_max: 10,
        };
        let partition_id = PartitionId::new(1);
        let TestLifecycleManger { mut m, .. } = TestLifecycleManger::new(config);
        let shard_id = ShardId::new(1);
        let h = m.handle();

        // No stats exist before writing.
        {
            let s = m.state.lock();
            assert!(s.partition_stats.get(&partition_id).is_none());
        }

        // write more than the limit (10) and don't get stopped, because the
        // per-partition limit does not pause the server from ingesting.
        assert!(!h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            1,
            50
        ));
        assert!(h.can_resume_ingest());

        // Rows were counted
        {
            let s = m.state.lock();
            let stats = s
                .partition_stats
                .get(&partition_id)
                .expect("should have partition stats");
            assert_eq!(stats.rows_written, 50);
        }

        // all subsequent writes should also be allowed
        assert!(!h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            1,
            1
        ));
        assert!(h.can_resume_ingest());

        // persist the partition
        let persister = Arc::new(TestPersister::default());
        m.maybe_persist(&persister).await;

        // Stats were reset
        {
            let s = m.state.lock();
            assert!(s.partition_stats.get(&partition_id).is_none());
        }

        // ingest can continue
        assert!(h.can_resume_ingest());
        assert!(!h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(3),
            1,
            1
        ));
    }

    #[tokio::test]
    async fn pausing_ingest_waits_until_persist_completes() {
        let config = LifecycleConfig {
            pause_ingest_size: 20,
            persist_memory_threshold: 10,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_nanos(0),
            partition_cold_threshold: Duration::from_secs(500),
            partition_row_max: 100,
        };
        let partition_id = PartitionId::new(1);
        let TestLifecycleManger { mut m, .. } = TestLifecycleManger::new(config);
        let shard_id = ShardId::new(1);
        let h = m.handle();

        // write more than the limit (20)
        h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            25,
            1,
        );

        // can not resume ingest as we are overall the pause ingest limit
        assert!(!h.can_resume_ingest());

        // persist the partition, pausing once it starts
        let persister = Arc::new(PausablePersister::new());
        persister.pause_next(partition_id);

        let captured_persister = Arc::clone(&persister);
        let persist = tokio::task::spawn(async move {
            m.maybe_persist(&captured_persister).await;
            m
        });

        // wait for persist to have started
        persister.wait_for_persist(partition_id).await;

        // until persist completes, ingest can not proceed (as the
        // ingester is still over the limit).
        assert!(!h.can_resume_ingest());

        // allow persist to complete
        persister.complete_persist(partition_id).await;
        persist.await.expect("task panic'd");

        // ingest can resume
        assert!(h.can_resume_ingest());
        assert!(!h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            3,
            1
        ));
    }

    #[tokio::test]
    async fn persists_based_on_age() {
        let config = LifecycleConfig {
            pause_ingest_size: 30,
            persist_memory_threshold: 20,
            partition_size_threshold: 10,
            partition_age_threshold: Duration::from_nanos(5),
            partition_cold_threshold: Duration::from_secs(500),
            partition_row_max: 100,
        };
        let TestLifecycleManger {
            mut m,
            time_provider,
            metric_registry,
        } = TestLifecycleManger::new(config);
        let partition_id = PartitionId::new(1);
        let persister = Arc::new(TestPersister::default());
        let shard_id = ShardId::new(1);
        let h = m.handle();

        h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            10,
            1,
        );

        m.maybe_persist(&persister).await;
        let stats = m.stats();
        assert_eq!(stats.total_bytes, 10);
        assert_eq!(stats.partition_stats[0].partition_id, partition_id);

        // age out the partition
        time_provider.inc(Duration::from_nanos(6));

        // validate that from before, persist wasn't called for the partition
        assert!(!persister.persist_called_for(partition_id));

        // write in data for a new partition so we can be sure it isn't persisted, but the older
        // one is
        h.log_write(
            PartitionId::new(2),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            6,
            1,
        );

        m.maybe_persist(&persister).await;

        assert!(persister.persist_called_for(partition_id));
        assert!(!persister.persist_called_for(PartitionId::new(2)));
        assert_eq!(
            persister.update_min_calls(),
            vec![(shard_id, SequenceNumber::new(2))]
        );

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 6);
        assert_eq!(stats.partition_stats.len(), 1);
        assert_eq!(stats.partition_stats[0].partition_id, PartitionId::new(2));

        let age_counter = get_counter(&metric_registry, "age");
        assert_eq!(age_counter, 1);
    }

    #[tokio::test]
    async fn persists_based_on_age_with_multiple_unpersisted() {
        let config = LifecycleConfig {
            pause_ingest_size: 30,
            persist_memory_threshold: 20,
            partition_size_threshold: 10,
            partition_age_threshold: Duration::from_nanos(5),
            partition_cold_threshold: Duration::from_secs(500),
            partition_row_max: 100,
        };
        let TestLifecycleManger {
            mut m,
            time_provider,
            metric_registry,
        } = TestLifecycleManger::new(config);
        let partition_id = PartitionId::new(1);
        let persister = Arc::new(TestPersister::default());
        let shard_id = ShardId::new(1);
        let h = m.handle();

        h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            10,
            1,
        );

        m.maybe_persist(&persister).await;
        let stats = m.stats();
        assert_eq!(stats.total_bytes, 10);
        assert_eq!(stats.partition_stats[0].partition_id, partition_id);

        // age out the partition
        time_provider.inc(Duration::from_nanos(6));

        // validate that from before, persist wasn't called for the partition
        assert!(!persister.persist_called_for(partition_id));

        // write in data for a new partition so we can be sure it isn't persisted, but the older
        // one is
        h.log_write(
            PartitionId::new(2),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            6,
            1,
        );
        h.log_write(
            PartitionId::new(3),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(3),
            7,
            1,
        );

        m.maybe_persist(&persister).await;

        assert!(persister.persist_called_for(partition_id));
        assert!(!persister.persist_called_for(PartitionId::new(2)));
        assert_eq!(
            persister.update_min_calls(),
            vec![(shard_id, SequenceNumber::new(2))]
        );

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 13);
        assert_eq!(stats.partition_stats.len(), 2);
        assert_eq!(stats.partition_stats[0].partition_id, PartitionId::new(2));
        assert_eq!(stats.partition_stats[1].partition_id, PartitionId::new(3));

        let age_counter = get_counter(&metric_registry, "age");
        assert_eq!(age_counter, 1);
    }

    #[tokio::test]
    async fn persists_based_on_partition_size() {
        let config = LifecycleConfig {
            pause_ingest_size: 30,
            persist_memory_threshold: 20,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_millis(100),
            partition_cold_threshold: Duration::from_secs(500),
            partition_row_max: 100,
        };
        let TestLifecycleManger {
            mut m,
            metric_registry,
            ..
        } = TestLifecycleManger::new(config);
        let shard_id = ShardId::new(1);
        let h = m.handle();

        let partition_id = PartitionId::new(1);
        let persister = Arc::new(TestPersister::default());
        h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            4,
            1,
        );

        m.maybe_persist(&persister).await;

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 4);
        assert_eq!(stats.partition_stats[0].partition_id, partition_id);
        assert!(!persister.persist_called_for(partition_id));

        // introduce a new partition under the limit to verify it doesn't get taken with the other
        h.log_write(
            PartitionId::new(2),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            3,
            1,
        );
        h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(3),
            5,
            1,
        );

        m.maybe_persist(&persister).await;

        assert!(persister.persist_called_for(partition_id));
        assert!(!persister.persist_called_for(PartitionId::new(2)));
        assert_eq!(
            persister.update_min_calls(),
            vec![(shard_id, SequenceNumber::new(2))]
        );

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 3);
        assert_eq!(stats.partition_stats.len(), 1);
        assert_eq!(stats.partition_stats[0].partition_id, PartitionId::new(2));

        let size_counter = get_counter(&metric_registry, "size");
        assert_eq!(size_counter, 1);
    }

    #[tokio::test]
    async fn persists_based_on_memory_size() {
        let config = LifecycleConfig {
            pause_ingest_size: 60,
            persist_memory_threshold: 20,
            partition_size_threshold: 20,
            partition_age_threshold: Duration::from_millis(1000),
            partition_cold_threshold: Duration::from_secs(500),
            partition_row_max: 100,
        };
        let shard_id = ShardId::new(1);
        let TestLifecycleManger {
            mut m,
            metric_registry,
            ..
        } = TestLifecycleManger::new(config);
        let h = m.handle();
        let partition_id = PartitionId::new(1);
        let persister = Arc::new(TestPersister::default());
        h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            8,
            1,
        );
        h.log_write(
            PartitionId::new(2),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            13,
            1,
        );

        m.maybe_persist(&persister).await;

        // the bigger of the two partitions should have been persisted, leaving the smaller behind
        let stats = m.stats();
        assert_eq!(stats.total_bytes, 8);
        assert_eq!(stats.partition_stats[0].partition_id, partition_id);
        assert!(!persister.persist_called_for(partition_id));
        assert!(persister.persist_called_for(PartitionId::new(2)));
        assert_eq!(
            persister.update_min_calls(),
            vec![(shard_id, SequenceNumber::new(1))]
        );

        // add that partition back in over size
        h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(3),
            20,
            1,
        );
        h.log_write(
            PartitionId::new(2),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(4),
            21,
            1,
        );

        // both partitions should now need to be persisted to bring us below the mem threshold of
        // 20.
        m.maybe_persist(&persister).await;

        assert!(persister.persist_called_for(partition_id));
        assert!(persister.persist_called_for(PartitionId::new(2)));
        // because the persister is now empty, it has to make the last call with the last sequence
        // number it knows about. Even though this has been persisted, this fine since any already
        // persisted data will just be ignored on startup.
        assert_eq!(
            persister.update_min_calls(),
            vec![
                (shard_id, SequenceNumber::new(1)),
                (shard_id, SequenceNumber::new(4))
            ]
        );

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.partition_stats.len(), 0);

        let mem_counter = get_counter(&metric_registry, "memory");
        assert_eq!(mem_counter, 1);
    }

    #[tokio::test]
    async fn persist_based_on_partition_and_memory_size() {
        let config = LifecycleConfig {
            pause_ingest_size: 60,
            persist_memory_threshold: 6,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_millis(1000),
            partition_cold_threshold: Duration::from_secs(500),
            partition_row_max: 100,
        };
        let shard_id = ShardId::new(1);
        let TestLifecycleManger {
            mut m,
            time_provider,
            metric_registry,
        } = TestLifecycleManger::new(config);
        let h = m.handle();
        let persister = Arc::new(TestPersister::default());
        h.log_write(
            PartitionId::new(1),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            4,
            1,
        );
        time_provider.inc(Duration::from_nanos(1));
        h.log_write(
            PartitionId::new(2),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            6,
            1,
        );
        time_provider.inc(Duration::from_nanos(1));
        h.log_write(
            PartitionId::new(3),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(3),
            3,
            1,
        );

        m.maybe_persist(&persister).await;

        // the bigger of the two partitions should have been persisted, leaving the smaller behind
        let stats = m.stats();
        assert_eq!(stats.total_bytes, 3);
        assert_eq!(stats.partition_stats[0].partition_id, PartitionId::new(3));
        assert!(!persister.persist_called_for(PartitionId::new(3)));
        assert!(persister.persist_called_for(PartitionId::new(2)));
        assert!(persister.persist_called_for(PartitionId::new(1)));
        assert_eq!(
            persister.update_min_calls(),
            vec![(shard_id, SequenceNumber::new(3))]
        );

        let memory_counter = get_counter(&metric_registry, "memory");
        assert_eq!(memory_counter, 1);
    }

    #[tokio::test]
    async fn persists_based_on_cold() {
        let config = LifecycleConfig {
            pause_ingest_size: 500,
            persist_memory_threshold: 500,
            partition_size_threshold: 500,
            partition_age_threshold: Duration::from_secs(1000),
            partition_cold_threshold: Duration::from_secs(5),
            partition_row_max: 100,
        };
        let TestLifecycleManger {
            mut m,
            time_provider,
            metric_registry,
        } = TestLifecycleManger::new(config);
        let h = m.handle();
        let partition_id = PartitionId::new(1);
        let persister = Arc::new(TestPersister::default());
        let shard_id = ShardId::new(1);

        h.log_write(
            partition_id,
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(1),
            10,
            1,
        );

        m.maybe_persist(&persister).await;
        let stats = m.stats();
        assert_eq!(stats.total_bytes, 10);
        assert_eq!(stats.partition_stats[0].partition_id, partition_id);

        // make the partition go cold
        time_provider.inc(Duration::from_secs(6));

        // validate that from before, persist wasn't called for the partition
        assert!(!persister.persist_called_for(partition_id));

        // write in data for a new partition so we can be sure it isn't persisted, but the older one is
        h.log_write(
            PartitionId::new(2),
            shard_id,
            NamespaceId::new(91),
            TableId::new(92),
            SequenceNumber::new(2),
            6,
            1,
        );

        m.maybe_persist(&persister).await;

        assert!(persister.persist_called_for(partition_id));
        assert!(!persister.persist_called_for(PartitionId::new(2)));
        assert_eq!(
            persister.update_min_calls(),
            vec![(shard_id, SequenceNumber::new(2))]
        );

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 6);
        assert_eq!(stats.partition_stats.len(), 1);
        assert_eq!(stats.partition_stats[0].partition_id, PartitionId::new(2));

        let cold_counter = get_counter(&metric_registry, "cold");
        assert_eq!(cold_counter, 1);
    }

    struct TestLifecycleManger {
        m: LifecycleManager,
        time_provider: Arc<MockProvider>,
        metric_registry: Arc<Registry>,
    }

    impl TestLifecycleManger {
        fn new(config: LifecycleConfig) -> Self {
            let metric_registry = Arc::new(metric::Registry::new());
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
            let m = LifecycleManager::new(
                config,
                Arc::clone(&metric_registry),
                Arc::<MockProvider>::clone(&time_provider),
            );
            Self {
                m,
                time_provider,
                metric_registry,
            }
        }
    }

    fn get_counter(registry: &Registry, trigger: &'static str) -> u64 {
        let m: Metric<U64Counter> = registry
            .get_instrument("ingester_lifecycle_persist_count")
            .unwrap();
        let v = m
            .get_observer(&Attributes::from(&[("trigger", trigger)]))
            .unwrap()
            .fetch();
        v
    }
}
