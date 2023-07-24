//! Ensure that partitions flowing through the pipeline are unique.

use std::{collections::HashSet, fmt::Display, sync::Arc};

use async_trait::async_trait;
use data_types::PartitionId;
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;

use crate::{local_scheduler::partition_done_sink::DynError, PartitionDoneSink, PartitionsSource};

/// Ensures that a unique set of partitions is flowing through the critical section of the compactor pipeline.
///
/// This should be used as a wrapper around the actual [`PartitionsSource`] and [`PartitionDoneSink`] and will setup of
/// the following stream layout:
///
/// ```text
///          +---------------------------------------------------+
///          |                                                   |
///          |                                                   |
///          |                                                   V
/// (1)====>(2)====>[concurrent processing]---->(3)---->(4)---->(5)
///          ^                                           :
///          :                                           :
///          :                                           :
///          +...........................................+
/// ```
///
/// | Step |  Name                 | Type                                                        | Description |
/// | ---- | --------------------- | ----------------------------------------------------------- | ----------- |
/// | 1    | **Actual source**     | `inner_source`/`T1`/[`PartitionsSource`], wrapped           | This is the actual source, e.g. a [schedule](crate::PartitionsSource) |
/// | 2    | **Unique IDs source** | [`UniquePartionsSourceWrapper`], wraps `inner_source`/`T1`  | Outputs that [`PartitionId`]s from the `inner_source` but filters out partitions that have not yet reached the uniqueness sink (step 4) |
/// | 3    | **Critical section**  | --                           | Here it is always ensured that a single [`PartitionId`] does NOT occur more than once. |
/// | 4    | **Unique IDs sink**   | [`UniquePartitionDoneSinkWrapper`], wraps `inner_sink`/`T2` | Observes incoming IDs and removes them from the filter applied in step 2. |
/// | 5    | **Actual sink**       | `inner_sink`/`T2`/[`PartitionDoneSink`], wrapped            | The actual sink. Directly receives all partitions filtered out at step 2. |
///
/// Note that partitions filtered out by [`UniquePartionsSourceWrapper`] will directly be forwarded to `inner_sink`. No
/// partition is ever lost. This means that `inner_source` and `inner_sink` can perform proper accounting. The
/// concurrency of this bypass can be controlled via `bypass_concurrency`.
pub(crate) fn unique_partitions<T1, T2>(
    inner_source: T1,
    inner_sink: T2,
    bypass_concurrency: usize,
) -> (
    UniquePartionsSourceWrapper<T1, T2>,
    UniquePartitionDoneSinkWrapper<T2>,
)
where
    T1: PartitionsSource,
    T2: PartitionDoneSink,
{
    let inner_sink = Arc::new(inner_sink);
    let in_flight = Arc::new(Mutex::new(HashSet::default()));
    let source = UniquePartionsSourceWrapper {
        inner_source,
        inner_sink: Arc::clone(&inner_sink),
        in_flight: Arc::clone(&in_flight),
        sink_concurrency: bypass_concurrency,
    };
    let sink = UniquePartitionDoneSinkWrapper {
        inner: inner_sink,
        in_flight,
    };
    (source, sink)
}

/// Error returned by uniqueness abstractions.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    /// Failed uniqueness check.
    #[error("Unknown or already done partition: {0}")]
    Uniqueness(PartitionId),
}

type InFlight = Arc<Mutex<HashSet<PartitionId>>>;

#[derive(Debug)]
pub(crate) struct UniquePartionsSourceWrapper<T1, T2>
where
    T1: PartitionsSource,
    T2: PartitionDoneSink,
{
    inner_source: T1,
    inner_sink: Arc<T2>,
    in_flight: InFlight,
    sink_concurrency: usize,
}

impl<T1, T2> Display for UniquePartionsSourceWrapper<T1, T2>
where
    T1: PartitionsSource,
    T2: PartitionDoneSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unique({}, {})", self.inner_source, self.inner_sink)
    }
}

#[async_trait]
impl<T1, T2> PartitionsSource for UniquePartionsSourceWrapper<T1, T2>
where
    T1: PartitionsSource,
    T2: PartitionDoneSink,
{
    async fn fetch(&self) -> Vec<PartitionId> {
        let res = self.inner_source.fetch().await;

        let (unique, duplicates) = {
            let mut guard = self.in_flight.lock();

            let mut unique = Vec::with_capacity(res.len());
            let mut duplicates = Vec::with_capacity(res.len());
            for id in res {
                if guard.insert(id) {
                    unique.push(id);
                } else {
                    duplicates.push(id)
                }
            }

            (unique, duplicates)
        };

        // pass through the removal from tracking, since it was marked as duplicate in this fetch() call
        let _ = futures::stream::iter(duplicates)
            .map(|id| self.inner_sink.record(id, Ok(())))
            .buffer_unordered(self.sink_concurrency)
            .try_collect::<()>()
            .await;

        unique
    }
}

#[derive(Debug)]
pub(crate) struct UniquePartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    inner: Arc<T>,
    in_flight: InFlight,
}

impl<T> Display for UniquePartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unique({})", self.inner)
    }
}

#[async_trait]
impl<T> PartitionDoneSink for UniquePartitionDoneSinkWrapper<T>
where
    T: PartitionDoneSink,
{
    async fn record(
        &self,
        partition: PartitionId,
        res: Result<(), DynError>,
    ) -> Result<(), crate::PartitionDoneSinkError> {
        let existing = {
            let mut guard = self.in_flight.lock();
            guard.remove(&partition)
        };
        // perform check when NOT holding the mutex to not poison it
        if !existing {
            return Err(Error::Uniqueness(partition).into());
        }

        // perform inner last, because the wrapping order is:
        //
        // - wrapped source
        // - unique source
        // - ...
        // - unique sink
        // - wrapped sink
        self.inner.record(partition, res).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert_matches::assert_matches;

    use crate::{MockPartitionDoneSink, MockPartitionsSource, PartitionDoneSinkError};

    use super::*;

    #[test]
    fn test_display() {
        let (source, sink) = unique_partitions(
            MockPartitionsSource::new(vec![]),
            MockPartitionDoneSink::new(),
            1,
        );
        assert_eq!(source.to_string(), "unique(mock, mock)");
        assert_eq!(sink.to_string(), "unique(mock)");
    }

    #[tokio::test]
    async fn test_unique() {
        let inner_source = Arc::new(MockPartitionsSource::new(vec![
            PartitionId::new(1),
            PartitionId::new(1),
            PartitionId::new(2),
            PartitionId::new(3),
            PartitionId::new(4),
        ]));
        let inner_sink = Arc::new(MockPartitionDoneSink::new());
        let (source, sink) =
            unique_partitions(Arc::clone(&inner_source), Arc::clone(&inner_sink), 1);

        // ========== Round 1 ==========
        // fetch
        assert_eq!(
            source.fetch().await,
            vec![
                PartitionId::new(1),
                PartitionId::new(2),
                PartitionId::new(3),
                PartitionId::new(4),
            ],
        );
        assert_eq!(
            inner_sink.results(),
            HashMap::from([(PartitionId::new(1), Ok(()))]),
        );

        // record
        sink.record(PartitionId::new(1), Ok(()))
            .await
            .expect("record failed");
        sink.record(PartitionId::new(2), Ok(()))
            .await
            .expect("record failed");

        assert_eq!(
            inner_sink.results(),
            HashMap::from([(PartitionId::new(1), Ok(())), (PartitionId::new(2), Ok(())),]),
        );

        assert_eq!(
            inner_sink.results(),
            HashMap::from([(PartitionId::new(1), Ok(())), (PartitionId::new(2), Ok(()))]),
        );

        // ========== Round 2 ==========
        inner_source.set(vec![
            PartitionId::new(1),
            PartitionId::new(3),
            PartitionId::new(5),
        ]);

        // fetch
        assert_eq!(
            source.fetch().await,
            vec![PartitionId::new(1), PartitionId::new(5)],
        );

        assert_eq!(
            inner_sink.results(),
            HashMap::from([
                (PartitionId::new(1), Ok(())),
                (PartitionId::new(2), Ok(())),
                (PartitionId::new(3), Ok(())),
            ]),
        );

        // record
        sink.record(PartitionId::new(1), Err(String::from("foo").into()))
            .await
            .expect("record failed");

        assert_eq!(
            inner_sink.results(),
            HashMap::from([
                (PartitionId::new(1), Err(String::from("foo"))),
                (PartitionId::new(2), Ok(())),
                (PartitionId::new(3), Ok(())),
            ]),
        );

        // ========== Round 3 ==========
        // fetch
        assert_eq!(source.fetch().await, vec![PartitionId::new(1)],);

        assert_eq!(
            inner_sink.results(),
            HashMap::from([
                (PartitionId::new(1), Err(String::from("foo"))),
                (PartitionId::new(2), Ok(())),
                (PartitionId::new(3), Ok(())),
                (PartitionId::new(5), Ok(())),
            ]),
        );
    }

    #[tokio::test]
    async fn test_panic_sink_unknown() {
        let (source, sink) = unique_partitions(
            MockPartitionsSource::new(vec![PartitionId::new(1)]),
            MockPartitionDoneSink::new(),
            1,
        );
        let ids = source.fetch().await;
        assert_eq!(ids.len(), 1);
        let id = ids[0];
        sink.record(id, Ok(())).await.expect("record failed");
        assert_matches!(
            sink.record(id, Ok(())).await,
            Err(PartitionDoneSinkError::UniquePartitions(Error::Uniqueness(partition))) if partition == PartitionId::new(1),
            "fails because partition 1 is already done"
        );
    }
}
