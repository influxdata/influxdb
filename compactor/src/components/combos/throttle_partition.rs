//! Throttle partions that receive no commits.

use std::{
    collections::HashMap,
    fmt::Display,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use data_types::{CompactionLevel, ParquetFile, ParquetFileId, ParquetFileParams, PartitionId};
use futures::StreamExt;
use iox_time::{Time, TimeProvider};

use crate::components::{
    commit::Commit, partition_done_sink::PartitionDoneSink, partitions_source::PartitionsSource,
};

/// Ensures that partitions that do not receive any commits are throttled.
///
/// This may happen because our catalog query detects that the partition receives writes but the comapctor already
/// finished all the outstandign work.
///
/// This should be used as a wrapper around the actual [`PartitionsSource`] & [`Commit`] & [`PartitionDoneSink`] and will setup of
/// the following stream layout:
///
/// ```text
///         +--------------------------------------------+
///         |                                            |
///         |                                   (5)      |
///         |                                    ^       |
///         |                                    |       |
///         | +.................................(4)      |
///         | :                                  ^       |
///         | V                                  |       V
/// (1)====>(2)====>[concurrent processing]---->(3)---->(6)---->(7)
///          ^                                           :
///          :                                           :
///          :                                           :
///          +...........................................+
/// ```
///
/// | Step |  Name                 | Type                                                              | Description |
/// | ---- | --------------------- | ----------------------------------------------------------------- | ----------- |
/// | 1    | **Actual source**     | `inner_source`/`T1`/[`PartitionsSource`], wrapped                 | This is the actual source. |
/// | 2    | **Throttling source** | [`ThrottlePartitionsSourceWrapper`], wraps `inner_source`/`T1`    | Throttles partitions that do not receive any commits |
/// | 3    | **Critical section**  | --                           | The actual partition processing    |
/// | 4    | **Throttle commit**   | [`ThrottleCommitWrapper`], wraps `inner_commit`/`T2`              | Observes commits. |
/// | 5    | **Actual commit**     | `inner_commit`/`T2`/[`Commit`] | The actual commit implementation |
/// | 6    | **Throttle sink**     | [`ThrottlePartitionDoneSinkWrapper`], wraps `inner_sink`/`T3`     | Observes incoming IDs enables throttled if step (4) did not observe any commits. |
/// | 7    | **Actual sink**       | `inner_sink`/`T3`/[`PartitionDoneSink`], wrapped                  | The actual sink.  Directly receives all partitions filtered out at step 2. |
///
/// Note that partitions filtered out by [`ThrottlePartitionsSourceWrapper`] will directly be forwarded to `inner_sink`. No
/// partition is ever lost. This means that `inner_source` and `inner_sink` can perform proper accounting. The
/// concurrency of this bypass can be controlled via `bypass_concurrency`.
///
/// This setup relies on a fact that it does not process duplicate [`PartitionId`]. You may use
/// [`unique_partitions`](crate::components::combos::unique_partitions::unique_partitions) to achieve that.
pub fn throttle_partition<T1, T2, T3>(
    source: T1,
    commit: T2,
    sink: T3,
    time_provider: Arc<dyn TimeProvider>,
    throttle_duration: Duration,
    bypass_concurrency: usize,
) -> (
    ThrottlePartitionsSourceWrapper<T1, T3>,
    ThrottleCommitWrapper<T2>,
    ThrottlePartitionDoneSinkWrapper<T3>,
)
where
    T1: PartitionsSource,
    T2: Commit,
    T3: PartitionDoneSink,
{
    let state = SharedState::default();
    let inner_sink = Arc::new(sink);
    let source = ThrottlePartitionsSourceWrapper {
        inner_source: source,
        inner_sink: Arc::clone(&inner_sink),
        state: Arc::clone(&state),
        time_provider: Arc::clone(&time_provider),
        sink_concurrency: bypass_concurrency,
    };
    let commit = ThrottleCommitWrapper {
        inner: commit,
        state: Arc::clone(&state),
    };
    let sink = ThrottlePartitionDoneSinkWrapper {
        inner: inner_sink,
        state,
        time_provider,
        throttle_duration,
    };
    (source, commit, sink)
}

#[derive(Debug, Default)]
struct State {
    // Value is "true" while compaction task is in-flight, and "false" once complete.
    //
    // Completed compaction tasks are removed from the map each time the source fetch()
    // is called.
    in_flight: HashMap<PartitionId, bool>,
    throttled: HashMap<PartitionId, Time>,
}

type SharedState = Arc<Mutex<State>>;

#[derive(Debug)]
pub struct ThrottlePartitionsSourceWrapper<T1, T2>
where
    T1: PartitionsSource,
    T2: PartitionDoneSink,
{
    inner_source: T1,
    inner_sink: Arc<T2>,
    state: SharedState,
    time_provider: Arc<dyn TimeProvider>,
    sink_concurrency: usize,
}

impl<T1, T2> Display for ThrottlePartitionsSourceWrapper<T1, T2>
where
    T1: PartitionsSource,
    T2: PartitionDoneSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "throttle({}, {})", self.inner_source, self.inner_sink)
    }
}

#[async_trait]
impl<T1, T2> PartitionsSource for ThrottlePartitionsSourceWrapper<T1, T2>
where
    T1: PartitionsSource,
    T2: PartitionDoneSink,
{
    async fn fetch(&self) -> Vec<PartitionId> {
        let res = self.inner_source.fetch().await;

        let (pass, throttle) = {
            let mut guard = self.state.lock().expect("not poisoned");

            // ensure that in-flight data is non-overlapping
            for id in &res {
                if guard.in_flight.contains_key(id) {
                    drop(guard); // avoid poison
                    panic!("Partition already in-flight: {id}");
                }
            }

            // clean throttled states
            let now = self.time_provider.now();
            guard.throttled = guard
                .throttled
                .iter()
                .filter(|(_id, until)| **until > now)
                .map(|(k, v)| (*k, *v))
                .collect();

            // filter output
            let mut pass = Vec::with_capacity(res.len());
            let mut throttle = Vec::with_capacity(res.len());
            for id in res {
                if guard.throttled.contains_key(&id) {
                    throttle.push(id);
                } else {
                    pass.push(id);
                }
            }

            // set up in-flight
            for id in &pass {
                guard.in_flight.insert(*id, false);
            }

            (pass, throttle)
        };

        futures::stream::iter(throttle)
            .map(|id| self.inner_sink.record(id, Ok(())))
            .buffer_unordered(self.sink_concurrency)
            .collect::<()>()
            .await;

        pass
    }
}

#[derive(Debug)]
pub struct ThrottleCommitWrapper<T>
where
    T: Commit,
{
    inner: T,
    state: SharedState,
}

impl<T> Display for ThrottleCommitWrapper<T>
where
    T: Commit,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "throttle({})", self.inner)
    }
}

#[async_trait]
impl<T> Commit for ThrottleCommitWrapper<T>
where
    T: Commit,
{
    async fn commit(
        &self,
        partition_id: PartitionId,
        delete: &[ParquetFile],
        upgrade: &[ParquetFile],
        create: &[ParquetFileParams],
        target_level: CompactionLevel,
    ) -> Vec<ParquetFileId> {
        let known = {
            let mut guard = self.state.lock().expect("not poisoned");
            match guard.in_flight.get_mut(&partition_id) {
                Some(val) => {
                    *val = true;
                    true
                }
                None => false,
            }
        };
        // perform check when NOT holding the mutex to not poison it
        assert!(
            known,
            "Unknown or already done partition in commit: {partition_id}"
        );

        self.inner
            .commit(partition_id, delete, upgrade, create, target_level)
            .await
    }
}

#[derive(Debug)]
pub struct ThrottlePartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    inner: Arc<T>,
    state: SharedState,
    throttle_duration: Duration,
    time_provider: Arc<dyn TimeProvider>,
}

impl<T> Display for ThrottlePartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "throttle({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionDoneSink for ThrottlePartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    async fn record(
        &self,
        partition: PartitionId,
        res: Result<(), Box<dyn std::error::Error + Send + Sync>>,
    ) {
        let known = {
            let mut guard = self.state.lock().expect("not poisoned");
            match guard.in_flight.remove(&partition) {
                Some(val) => {
                    if !val {
                        guard
                            .throttled
                            .insert(partition, self.time_provider.now() + self.throttle_duration);
                    }
                    true
                }
                None => false,
            }
        };
        // perform check when NOT holding the mutex to not poison it
        assert!(
            known,
            "Unknown or already done partition in partition done sink: {partition}"
        );

        self.inner.record(partition, res).await;
    }
}

#[cfg(test)]
mod tests {
    use iox_time::MockProvider;

    use crate::components::{
        commit::mock::{CommitHistoryEntry, MockCommit},
        partition_done_sink::mock::MockPartitionDoneSink,
        partitions_source::mock::MockPartitionsSource,
    };

    use super::*;

    #[test]
    fn test_display() {
        let (source, commit, sink) = throttle_partition(
            MockPartitionsSource::new(vec![]),
            MockCommit::new(),
            MockPartitionDoneSink::new(),
            Arc::new(MockProvider::new(Time::MIN)),
            Duration::from_secs(0),
            1,
        );
        assert_eq!(source.to_string(), "throttle(mock, mock)");
        assert_eq!(commit.to_string(), "throttle(mock)");
        assert_eq!(sink.to_string(), "throttle(mock)");
    }

    #[tokio::test]
    async fn test_throttle() {
        let inner_source = Arc::new(MockPartitionsSource::new(vec![
            PartitionId::new(1),
            PartitionId::new(2),
            PartitionId::new(3),
            PartitionId::new(4),
        ]));
        let inner_commit = Arc::new(MockCommit::new());
        let inner_sink = Arc::new(MockPartitionDoneSink::new());
        let time_provider = Arc::new(MockProvider::new(Time::MIN));
        let (source, commit, sink) = throttle_partition(
            Arc::clone(&inner_source),
            Arc::clone(&inner_commit),
            Arc::clone(&inner_sink),
            Arc::clone(&time_provider) as _,
            Duration::from_secs(1),
            1,
        );

        // ========== Round 1 ==========
        // fetch
        assert_eq!(
            source.fetch().await,
            vec![
                PartitionId::new(1),
                PartitionId::new(2),
                PartitionId::new(3),
                PartitionId::new(4)
            ],
        );
        assert_eq!(inner_sink.results(), HashMap::from([]),);

        // commit
        commit
            .commit(PartitionId::new(1), &[], &[], &[], CompactionLevel::Initial)
            .await;
        commit
            .commit(PartitionId::new(2), &[], &[], &[], CompactionLevel::Initial)
            .await;

        // record
        sink.record(PartitionId::new(1), Ok(())).await;
        sink.record(PartitionId::new(3), Ok(())).await;
        assert_eq!(
            inner_sink.results(),
            HashMap::from([(PartitionId::new(1), Ok(())), (PartitionId::new(3), Ok(())),]),
        );

        // ========== Round 2 ==========
        // need to remove partition 2 and 4 because they weren't finished yet
        inner_source.set(vec![
            PartitionId::new(1),
            PartitionId::new(3),
            PartitionId::new(5),
        ]);

        // fetch
        assert_eq!(
            source.fetch().await,
            vec![
                // ID 1: commit in last round => pass
                PartitionId::new(1),
                // ID 3: no commit in last round => throttled
                // ID 5: new => pass
                PartitionId::new(5),
            ],
        );
        assert_eq!(
            inner_sink.results(),
            HashMap::from([(PartitionId::new(1), Ok(())), (PartitionId::new(3), Ok(())),]),
        );

        // ========== Round 3 ==========
        // advance time to "unthrottle" ID 3
        inner_source.set(vec![PartitionId::new(3)]);
        time_provider.inc(Duration::from_secs(1));

        // fetch
        assert_eq!(source.fetch().await, vec![PartitionId::new(3)],);

        // record
        // can still finish partition 2 and 4
        sink.record(PartitionId::new(2), Err(String::from("foo").into()))
            .await;
        sink.record(PartitionId::new(4), Err(String::from("bar").into()))
            .await;
        assert_eq!(
            inner_sink.results(),
            HashMap::from([
                (PartitionId::new(1), Ok(())),
                (PartitionId::new(2), Err(String::from("foo"))),
                (PartitionId::new(3), Ok(())),
                (PartitionId::new(4), Err(String::from("bar"))),
            ]),
        );

        // ========== Round 4 ==========
        inner_source.set(vec![PartitionId::new(2), PartitionId::new(4)]);

        // fetch
        assert_eq!(source.fetch().await, vec![PartitionId::new(2)],);

        assert_eq!(
            inner_sink.results(),
            HashMap::from([
                (PartitionId::new(1), Ok(())),
                (PartitionId::new(2), Err(String::from("foo"))),
                (PartitionId::new(3), Ok(())),
                (PartitionId::new(4), Ok(())),
            ]),
        );

        // commits are just forwarded to inner `Commit` impl
        assert_eq!(
            inner_commit.history(),
            vec![
                CommitHistoryEntry {
                    partition_id: PartitionId::new(1),
                    delete: vec![],
                    upgrade: vec![],
                    created: vec![],
                    target_level: CompactionLevel::Initial,
                },
                CommitHistoryEntry {
                    partition_id: PartitionId::new(2),
                    delete: vec![],
                    upgrade: vec![],
                    created: vec![],
                    target_level: CompactionLevel::Initial,
                },
            ]
        );
    }

    #[tokio::test]
    #[should_panic(expected = "Unknown or already done partition in commit: 1")]
    async fn test_panic_commit_unknown() {
        let (source, commit, sink) = throttle_partition(
            MockPartitionsSource::new(vec![PartitionId::new(1)]),
            MockCommit::new(),
            MockPartitionDoneSink::new(),
            Arc::new(MockProvider::new(Time::MIN)),
            Duration::from_secs(0),
            1,
        );

        source.fetch().await;
        sink.record(PartitionId::new(1), Ok(())).await;
        commit
            .commit(PartitionId::new(1), &[], &[], &[], CompactionLevel::Initial)
            .await;
    }

    #[tokio::test]
    #[should_panic(expected = "Unknown or already done partition in partition done sink: 1")]
    async fn test_panic_sink_unknown() {
        let (source, _commit, sink) = throttle_partition(
            MockPartitionsSource::new(vec![PartitionId::new(1)]),
            MockCommit::new(),
            MockPartitionDoneSink::new(),
            Arc::new(MockProvider::new(Time::MIN)),
            Duration::from_secs(0),
            1,
        );

        source.fetch().await;
        sink.record(PartitionId::new(1), Ok(())).await;
        sink.record(PartitionId::new(1), Ok(())).await;
    }

    #[tokio::test]
    #[should_panic(expected = "Partition already in-flight: 1")]
    async fn test_panic_duplicate_in_flight() {
        let (source, _commit, _sink) = throttle_partition(
            MockPartitionsSource::new(vec![PartitionId::new(1)]),
            MockCommit::new(),
            MockPartitionDoneSink::new(),
            Arc::new(MockProvider::new(Time::MIN)),
            Duration::from_secs(0),
            1,
        );

        source.fetch().await;
        source.fetch().await;
    }
}
