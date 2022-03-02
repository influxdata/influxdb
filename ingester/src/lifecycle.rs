//! Manages the persistence and eviction lifecycle of data in the buffer across all sequencers.
//! Note that the byte counts logged by the lifecycle manager and when exactly persistence gets
//! triggered aren't required to be absolutely accurate. The byte count is just an estimate
//! anyway, this just needs to keep things moving along to keep memory use roughly under
//! some absolute number and individual Parquet files that get persisted below some number. It
//! is expected that they may be above or below the absolute thresholds.

use crate::{
    data::Persister,
    job::{Job, JobRegistry},
    poison::{PoisonCabinet, PoisonPill},
};
use data_types2::{PartitionId, SequenceNumber, SequencerId};
use observability_deps::tracing::{error, info};
use parking_lot::Mutex;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use time::{Time, TimeProvider};
use tokio_util::sync::CancellationToken;
use tracker::TrackedFutureExt;

/// The lifecycle manager keeps track of the size and age of partitions across all sequencers.
/// It triggers persistence based on keeping total memory usage around a set amount while
/// ensuring that partitions don't get too old or large before being persisted.
pub struct LifecycleManager {
    config: LifecycleConfig,
    time_provider: Arc<dyn TimeProvider>,
    state: Mutex<LifecycleState>,
    persist_running: tokio::sync::Mutex<()>,
    job_registry: Arc<JobRegistry>,
}

/// The configuration options for the lifecycle on the ingester.
#[derive(Debug, Clone, Copy)]
pub struct LifecycleConfig {
    /// The ingester will pause pulling data from Kafka if it hits this amount of memory used, waiting
    /// until persistence evicts partitions from memory.
    pause_ingest_size: usize,
    /// When the ingester hits this threshold, the lifecycle manager will persist the largest
    /// partitions currently buffered until it falls below this threshold. An ingester running
    /// in a steady state should operate around this amount of memory usage.
    persist_memory_threshold: usize,
    /// If an individual partition crosses this threshold, it will be persisted. The purpose of this
    /// setting to to ensure the ingester doesn't create Parquet files that are too large.
    partition_size_threshold: usize,
    /// If an individual partitiion has had data buffered for longer than this period of time, the
    /// manager will persist it. This setting is to ensure we have an upper bound on how far back
    /// we will need to read in Kafka on restart or recovery.
    partition_age_threshold: Duration,
}

impl LifecycleConfig {
    /// Initialize a new LifecycleConfig. panics if the passed `pause_ingest_size` is less than the
    /// `persist_memory_threshold`.
    pub fn new(
        pause_ingest_size: usize,
        persist_memory_threshold: usize,
        partition_size_threshold: usize,
        partition_age_threshold: Duration,
    ) -> Self {
        // this must be true to ensure that persistence will get triggered, freeing up memory
        assert!(pause_ingest_size > persist_memory_threshold);

        Self {
            pause_ingest_size,
            persist_memory_threshold,
            partition_size_threshold,
            partition_age_threshold,
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
        self.partition_stats.remove(partition_id).map(|stats| {
            self.total_bytes -= stats.bytes_written;
            stats
        })
    }
}

/// A snapshot of the stats for the lifecycle manager
#[derive(Debug)]
pub struct LifecycleStats {
    /// total number of bytes the lifecycle manager is aware of across all sequencers and
    /// partitions. Based on the mutable batch sizes received into all partitions.
    pub total_bytes: usize,
    /// the stats for every partition the lifecycle manager is tracking.
    pub partition_stats: Vec<PartitionLifecycleStats>,
}

/// The stats for a partition
#[derive(Debug, Clone, Copy)]
pub struct PartitionLifecycleStats {
    /// The sequencer this partition is under
    sequencer_id: SequencerId,
    /// The partition identifier
    partition_id: PartitionId,
    /// Time that the partition received its first write. This is reset anytime
    /// the partition is persisted.
    first_write: Time,
    /// The number of bytes in the partition as estimated by the mutable batch sizes.
    bytes_written: usize,
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
        let job_registry = Arc::new(JobRegistry::new(
            metric_registry,
            Arc::clone(&time_provider),
        ));
        Self {
            config,
            time_provider,
            state: Default::default(),
            persist_running: Default::default(),
            job_registry,
        }
    }

    /// Logs bytes written into a partition so that it can be tracked for the manager to
    /// trigger persistence. Returns true if the ingester should pause consuming from the
    /// write buffer so that persistence can catch up and free up memory.
    pub fn log_write(
        &self,
        partition_id: PartitionId,
        sequencer_id: SequencerId,
        sequence_number: SequenceNumber,
        bytes_written: usize,
    ) -> bool {
        let mut s = self.state.lock();
        s.partition_stats
            .entry(partition_id)
            .or_insert_with(|| PartitionLifecycleStats {
                sequencer_id,
                partition_id,
                first_write: self.time_provider.now(),
                bytes_written: 0,
                first_sequence_number: sequence_number,
            })
            .bytes_written += bytes_written;
        s.total_bytes += bytes_written;

        s.total_bytes > self.config.pause_ingest_size
    }

    /// Returns true if the `total_bytes` tracked by the manager is less than the pause amount.
    /// As persistence runs, the `total_bytes` go down.
    pub fn can_resume_ingest(&self) -> bool {
        let s = self.state.lock();
        s.total_bytes < self.config.pause_ingest_size
    }

    /// This will persist any partitions that are over their size or age thresholds and
    /// persist as many partitions as necessary (largest first) to get below the memory threshold.
    /// The persist operations are spawned in new tasks and run at the same time, but the
    /// function waits for all to return before completing.
    pub async fn maybe_persist<P: Persister>(&self, persister: &Arc<P>) {
        // ensure that this is only running one at a time
        self.persist_running.lock().await;

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
            let aged_out = now
                .checked_duration_since(s.first_write)
                .map(|age| age > self.config.partition_age_threshold)
                .unwrap_or(false);
            let sized_out = s.bytes_written > self.config.partition_size_threshold;

            aged_out || sized_out
        });

        // keep track of what we'll be evicting to see what else to drop
        for s in &to_persist {
            total_bytes -= s.bytes_written;
        }

        // if we're still over the memory threshold, persist as many of the largest partitions
        // until we're under. It's ok if this is stale, it'll just get handled on the next pass
        // through.
        if total_bytes > self.config.persist_memory_threshold {
            rest.sort_by(|a, b| b.bytes_written.cmp(&a.bytes_written));

            let mut remaining = vec![];

            for s in rest {
                if total_bytes >= self.config.persist_memory_threshold {
                    total_bytes -= s.bytes_written;
                    to_persist.push(s);
                } else {
                    remaining.push(s);
                }
            }

            rest = remaining;
        }

        // for the sequencers that are getting data persisted, keep track of what
        // the highest seqeunce number was for each.
        let mut sequencer_maxes = BTreeMap::new();
        for s in &to_persist {
            sequencer_maxes
                .entry(s.sequencer_id)
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
                self.remove(s.partition_id);
                let persister = Arc::clone(persister);

                let (_tracker, registration) = self.job_registry.register(Job::Persist {
                    partition_id: s.partition_id,
                });
                tokio::task::spawn(async move {
                    persister.persist(s.partition_id).await;
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

            // for the sequencers that had data persisted, update their min_unpersisted_sequence_number to
            // either the minimum remaining in everything that didn't get persisted, or the highest
            // number that was persisted. Marking the highest number as the state is ok because it
            // just needs to represent the farthest we'd have to seek back in the write buffer. Any
            // data replayed during recovery that has already been persisted will just be ignored.
            for (sequencer_id, sequence_number) in sequencer_maxes {
                let min = rest
                    .iter()
                    .filter(|s| s.sequencer_id == sequencer_id)
                    .max_by_key(|s| s.first_sequence_number)
                    .map(|s| s.first_sequence_number)
                    .unwrap_or(sequence_number);
                persister
                    .update_min_unpersisted_sequence_number(sequencer_id, min)
                    .await;
            }
        }
    }

    /// Returns a point in time snapshot of the lifecycle state.
    pub fn stats(&self) -> LifecycleStats {
        let s = self.state.lock();
        let partition_stats: Vec<_> = s.partition_stats.values().cloned().collect();

        LifecycleStats {
            total_bytes: s.total_bytes,
            partition_stats,
        }
    }

    /// Removes the partition from the state
    pub fn remove(&self, partition_id: PartitionId) -> Option<PartitionLifecycleStats> {
        let mut s = self.state.lock();
        s.remove(&partition_id)
    }
}

const CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Runs the lifecycle manager to trigger persistence every second.
pub(crate) async fn run_lifecycle_manager<P: Persister>(
    manager: Arc<LifecycleManager>,
    persister: Arc<P>,
    shutdown: CancellationToken,
    poison_cabinet: Arc<PoisonCabinet>,
) {
    loop {
        if poison_cabinet.contains(&PoisonPill::LifecyclePanic) {
            panic!("Lifecycle manager poisened, panic");
        }
        if poison_cabinet.contains(&PoisonPill::LifecycleExit) {
            error!("Lifecycle manager poisened, exit early");
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
    use super::*;
    use async_trait::async_trait;
    use std::collections::BTreeSet;
    use time::MockProvider;

    #[derive(Default)]
    struct TestPersister {
        persist_called: Mutex<BTreeSet<PartitionId>>,
        update_min_calls: Mutex<Vec<(SequencerId, SequenceNumber)>>,
    }

    #[async_trait]
    impl Persister for TestPersister {
        async fn persist(&self, partition_id: PartitionId) {
            let mut p = self.persist_called.lock();
            p.insert(partition_id);
        }

        async fn update_min_unpersisted_sequence_number(
            &self,
            sequencer_id: SequencerId,
            sequence_number: SequenceNumber,
        ) {
            let mut u = self.update_min_calls.lock();
            u.push((sequencer_id, sequence_number));
        }
    }

    impl TestPersister {
        fn persist_called_for(&self, partition_id: PartitionId) -> bool {
            let p = self.persist_called.lock();
            p.contains(&partition_id)
        }

        fn update_min_calls(&self) -> Vec<(SequencerId, SequenceNumber)> {
            let u = self.update_min_calls.lock();
            u.clone()
        }
    }

    #[test]
    fn logs_write() {
        let config = LifecycleConfig {
            pause_ingest_size: 20,
            persist_memory_threshold: 10,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_nanos(0),
        };
        let TestLifecycleManger { m, time_provider } = TestLifecycleManger::new(config);
        let sequencer_id = SequencerId::new(1);

        // log first two writes at different times
        assert!(!m.log_write(PartitionId::new(1), sequencer_id, SequenceNumber::new(1), 1));
        time_provider.inc(Duration::from_nanos(10));
        assert!(!m.log_write(PartitionId::new(1), sequencer_id, SequenceNumber::new(2), 1));

        // log another write for different partition
        assert!(!m.log_write(PartitionId::new(2), sequencer_id, SequenceNumber::new(3), 3));

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 5);

        let p1 = stats.partition_stats.get(0).unwrap();
        assert_eq!(p1.bytes_written, 2);
        assert_eq!(p1.partition_id, PartitionId::new(1));
        assert_eq!(p1.first_write, Time::from_timestamp_nanos(0));

        let p2 = stats.partition_stats.get(1).unwrap();
        assert_eq!(p2.bytes_written, 3);
        assert_eq!(p2.partition_id, PartitionId::new(2));
        assert_eq!(p2.first_write, Time::from_timestamp_nanos(10));
    }

    #[test]
    fn pausing_and_resuming_ingest() {
        let config = LifecycleConfig {
            pause_ingest_size: 20,
            persist_memory_threshold: 10,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_nanos(0),
        };
        let TestLifecycleManger { m, .. } = TestLifecycleManger::new(config);
        let sequencer_id = SequencerId::new(1);

        assert!(!m.log_write(
            PartitionId::new(1),
            sequencer_id,
            SequenceNumber::new(1),
            15
        ));

        // now it should indicate a pause
        assert!(m.log_write(
            PartitionId::new(1),
            sequencer_id,
            SequenceNumber::new(2),
            10
        ));
        assert!(!m.can_resume_ingest());

        m.remove(PartitionId::new(1));
        assert!(m.can_resume_ingest());
        assert!(!m.log_write(PartitionId::new(1), sequencer_id, SequenceNumber::new(3), 3));
    }

    #[tokio::test]
    async fn persists_based_on_age() {
        let config = LifecycleConfig {
            pause_ingest_size: 30,
            persist_memory_threshold: 20,
            partition_size_threshold: 10,
            partition_age_threshold: Duration::from_nanos(5),
        };
        let TestLifecycleManger { m, time_provider } = TestLifecycleManger::new(config);
        let partition_id = PartitionId::new(1);
        let persister = Arc::new(TestPersister::default());
        let sequencer_id = SequencerId::new(1);

        m.log_write(partition_id, sequencer_id, SequenceNumber::new(1), 10);

        m.maybe_persist(&persister).await;
        let stats = m.stats();
        assert_eq!(stats.total_bytes, 10);
        assert_eq!(stats.partition_stats[0].partition_id, partition_id);

        // age out the partition
        time_provider.inc(Duration::from_nanos(6));

        // validate that from before, persist wasn't called for the partition
        assert!(!persister.persist_called_for(partition_id));

        // write in data for a new partition so we can be sure it isn't persisted, but the older one is
        m.log_write(PartitionId::new(2), sequencer_id, SequenceNumber::new(2), 6);

        m.maybe_persist(&persister).await;

        assert!(persister.persist_called_for(partition_id));
        assert!(!persister.persist_called_for(PartitionId::new(2)));
        assert_eq!(
            persister.update_min_calls(),
            vec![(sequencer_id, SequenceNumber::new(2))]
        );

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 6);
        assert_eq!(stats.partition_stats.len(), 1);
        assert_eq!(stats.partition_stats[0].partition_id, PartitionId::new(2));
    }

    #[tokio::test]
    async fn persists_based_on_partition_size() {
        let config = LifecycleConfig {
            pause_ingest_size: 30,
            persist_memory_threshold: 20,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_millis(100),
        };
        let TestLifecycleManger { m, .. } = TestLifecycleManger::new(config);
        let sequencer_id = SequencerId::new(1);

        let partition_id = PartitionId::new(1);
        let persister = Arc::new(TestPersister::default());
        m.log_write(partition_id, sequencer_id, SequenceNumber::new(1), 4);

        m.maybe_persist(&persister).await;

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 4);
        assert_eq!(stats.partition_stats[0].partition_id, partition_id);
        assert!(!persister.persist_called_for(partition_id));

        // introduce a new partition under the limit to verify it doesn't get taken with the other
        m.log_write(PartitionId::new(2), sequencer_id, SequenceNumber::new(2), 3);
        m.log_write(partition_id, sequencer_id, SequenceNumber::new(3), 5);

        m.maybe_persist(&persister).await;

        assert!(persister.persist_called_for(partition_id));
        assert!(!persister.persist_called_for(PartitionId::new(2)));
        assert_eq!(
            persister.update_min_calls(),
            vec![(sequencer_id, SequenceNumber::new(2))]
        );

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 3);
        assert_eq!(stats.partition_stats.len(), 1);
        assert_eq!(stats.partition_stats[0].partition_id, PartitionId::new(2));
    }

    #[tokio::test]
    async fn persists_based_on_memory_size() {
        let config = LifecycleConfig {
            pause_ingest_size: 60,
            persist_memory_threshold: 20,
            partition_size_threshold: 20,
            partition_age_threshold: Duration::from_millis(1000),
        };
        let sequencer_id = SequencerId::new(1);
        let TestLifecycleManger { m, .. } = TestLifecycleManger::new(config);
        let partition_id = PartitionId::new(1);
        let persister = Arc::new(TestPersister::default());
        m.log_write(partition_id, sequencer_id, SequenceNumber::new(1), 8);
        m.log_write(
            PartitionId::new(2),
            sequencer_id,
            SequenceNumber::new(2),
            13,
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
            vec![(sequencer_id, SequenceNumber::new(1))]
        );

        // add that partition back in over size
        m.log_write(partition_id, sequencer_id, SequenceNumber::new(3), 20);
        m.log_write(
            PartitionId::new(2),
            sequencer_id,
            SequenceNumber::new(4),
            21,
        );

        // both partitions should now need to be persisted to bring us below the mem threshold of 20.
        m.maybe_persist(&persister).await;

        assert!(persister.persist_called_for(partition_id));
        assert!(persister.persist_called_for(PartitionId::new(2)));
        // because the persister is now empty, it has to make the last call with the last sequence number it
        // knows about. Even though this has been persisted, this fine since any already persisted
        // data will just be ignored on startup.
        assert_eq!(
            persister.update_min_calls(),
            vec![
                (sequencer_id, SequenceNumber::new(1)),
                (sequencer_id, SequenceNumber::new(4))
            ]
        );

        let stats = m.stats();
        assert_eq!(stats.total_bytes, 0);
        assert_eq!(stats.partition_stats.len(), 0);
    }

    #[tokio::test]
    async fn persist_based_on_partition_and_memory_size() {
        let config = LifecycleConfig {
            pause_ingest_size: 60,
            persist_memory_threshold: 6,
            partition_size_threshold: 5,
            partition_age_threshold: Duration::from_millis(1000),
        };
        let sequencer_id = SequencerId::new(1);
        let TestLifecycleManger { m, time_provider } = TestLifecycleManger::new(config);
        let persister = Arc::new(TestPersister::default());
        m.log_write(PartitionId::new(1), sequencer_id, SequenceNumber::new(1), 4);
        time_provider.inc(Duration::from_nanos(1));
        m.log_write(PartitionId::new(2), sequencer_id, SequenceNumber::new(2), 6);
        time_provider.inc(Duration::from_nanos(1));
        m.log_write(PartitionId::new(3), sequencer_id, SequenceNumber::new(3), 3);

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
            vec![(sequencer_id, SequenceNumber::new(3))]
        );
    }

    struct TestLifecycleManger {
        m: LifecycleManager,
        time_provider: Arc<MockProvider>,
    }

    impl TestLifecycleManger {
        fn new(config: LifecycleConfig) -> Self {
            let metric_registry = Arc::new(metric::Registry::new());
            let time_provider = Arc::new(MockProvider::new(Time::from_timestamp_nanos(0)));
            let m = LifecycleManager::new(
                config,
                metric_registry,
                Arc::<MockProvider>::clone(&time_provider),
            );
            Self { m, time_provider }
        }
    }
}
