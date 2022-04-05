use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use data_types2::KafkaPartition;
use metric::U64Counter;
use observability_deps::tracing::*;
use tokio::task::JoinHandle;
use write_buffer::core::WriteBufferReading;

/// Periodically fetch and cache the maximum known write buffer offset
/// (watermark) from the write buffer for a given sequencer.
///
/// Uses [`WriteBufferReading::fetch_high_watermark()`] internally to obtain the
/// latest offset in the background, updating the cached value when it
/// successfully returns a value.
///
/// Emits an error metric named `write_buffer_watermark_fetch_errors` that
/// increments once per fetch error.
#[derive(Debug)]
pub struct PeriodicWatermarkFetcher {
    last_watermark: Arc<AtomicU64>,
    poll_handle: JoinHandle<()>,
}

impl PeriodicWatermarkFetcher {
    /// Instantiate a new [`PeriodicWatermarkFetcher`] that polls `write_buffer`
    /// every `interval` period for the maximum offset for `partition`.
    pub fn new(
        write_buffer: Arc<dyn WriteBufferReading>,
        partition: KafkaPartition,
        interval: Duration,
        metrics: &metric::Registry,
    ) -> Self {
        // Initialise a new poller to update the watermark in the background.
        let (poller, last_watermark) = Poller::new(write_buffer, partition, metrics);

        // Start the poller and retain a handle to stop it once this fetcher is
        // dropped.
        let poll_handle = tokio::spawn(async move {
            poller.run(interval).await;
        });

        Self {
            last_watermark,
            poll_handle,
        }
    }

    /// Returns the last observed watermark value, if available.
    ///
    /// The [`Instant`] in the returned tuple is the approximate instant the
    /// watermark was observed, allowing the caller to determine the staleness
    /// of the value.
    ///
    /// If the watermark value has yet to be observed (i.e. due to continuous
    /// fetch errors) `None` is returned.
    pub async fn cached_watermark(&self) -> Option<u64> {
        match self.last_watermark.load(Ordering::Relaxed) {
            // A value of 0 means "never observed a watermark".
            0 => None,
            v => Some(v),
        }
    }
}

impl Drop for PeriodicWatermarkFetcher {
    fn drop(&mut self) {
        // Stop the background poller task.
        self.poll_handle.abort();
    }
}

/// The internal poller half of the [`PeriodicWatermarkFetcher`] that performs
/// the write buffer request and updates the watermark.
///
/// Logs any errors that occur and emits an error count metric.
///
/// # Encoding
///
/// The atomic watermark value encodes "no watermark ever observed" as 0. All
/// non-zero values encode the last maximum watermark observed.
#[derive(Debug)]
struct Poller {
    write_buffer: Arc<dyn WriteBufferReading>,

    // The sequencer / kafka partition to ask the write buffer the max offset
    // for.
    sequencer_id: u32,

    // A metric tracking the number of max offset fetch errors.
    error_count: U64Counter,

    // The last observed maximum kafka offset, 0 if never observed (i.e. due to
    // error).
    last_watermark: Arc<AtomicU64>,
}

impl Poller {
    /// Initialise a new poller.
    ///
    /// The returned atomic will be updated with the most recent observed
    /// watermark value for `sequencer_id` after a successful poll, or 0 if
    /// never successfully polled.
    fn new(
        write_buffer: Arc<dyn WriteBufferReading>,
        partition: KafkaPartition,
        metrics: &metric::Registry,
    ) -> (Self, Arc<AtomicU64>) {
        let last_watermark = Arc::new(AtomicU64::new(0));
        let rx = Arc::clone(&last_watermark);

        let error_count = metrics
            .register_metric::<U64Counter>(
                "write_buffer_watermark_fetch_errors",
                "The total number of errors observed while trying to query for the high watermark",
            )
            .recorder([]);

        (
            Self {
                sequencer_id: partition
                    .get()
                    .try_into()
                    .expect("sequence ID out of range"),
                write_buffer,
                error_count,
                last_watermark,
            },
            rx,
        )
    }

    /// Block the current task and poll for the maximum watermark once every
    /// `interval`, publishing successful polls to the atomic returned from
    /// [`Self::new()`].
    async fn run(self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        let mut last_successful_poll = Instant::now();

        loop {
            ticker.tick().await;
            debug!("fetching write buffer watermark");

            let maybe_offset = self
                .write_buffer
                .fetch_high_watermark(self.sequencer_id)
                .await;

            let now = Instant::now();

            match maybe_offset {
                Ok(v) => {
                    // Publish the new offset
                    self.last_watermark.store(v, Ordering::Relaxed);
                    last_successful_poll = now;
                }
                Err(e) => {
                    // Log how long ago the watermark was last updated
                    let cache_staleness_seconds =
                        now.duration_since(last_successful_poll).as_secs();

                    warn!(
                        error=%e,
                        %cache_staleness_seconds,
                        "failed to read write buffer watermark"
                    );

                    self.error_count.inc(1);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use data_types2::Sequence;
    use metric::{Attributes, Metric};
    use test_helpers::timeout::FutureTimeout;
    use write_buffer::mock::{
        MockBufferForReading, MockBufferForReadingThatAlwaysErrors, MockBufferSharedState,
    };

    use super::*;

    #[tokio::test]
    async fn test_fetch_always_fails() {
        let write_buffer = Arc::new(MockBufferForReadingThatAlwaysErrors::default());
        let metrics = Arc::new(metric::Registry::default());
        let fetcher = PeriodicWatermarkFetcher::new(
            write_buffer,
            KafkaPartition::new(0),
            Duration::from_millis(10),
            &*metrics,
        );

        assert_eq!(fetcher.cached_watermark().await, None);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(fetcher.cached_watermark().await, None);

        let hits = metrics
            .get_instrument::<Metric<U64Counter>>("write_buffer_watermark_fetch_errors")
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch();
        assert!(hits > 0, "metric did not record any errors");
    }

    #[tokio::test]
    async fn test_fetch_ok() {
        let state = MockBufferSharedState::empty_with_n_sequencers(1.try_into().unwrap());
        state.push_lp(Sequence::new(0, 41), "cpu,t=1 v=2");

        let write_buffer = Arc::new(
            MockBufferForReading::new(state, None).expect("failed to init mock write buffer"),
        );

        let metrics = Arc::new(metric::Registry::default());
        let fetcher = PeriodicWatermarkFetcher::new(
            write_buffer,
            KafkaPartition::new(0),
            Duration::from_millis(10),
            &*metrics,
        );

        async {
            loop {
                // Read the watermark until the expected value is observed.
                match fetcher.cached_watermark().await {
                    Some(42) => break,
                    // The mock is configured to return 42 - any other value
                    // is incorrect.
                    Some(v) => panic!("observed unexpected value {}", v),
                    None => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
        }
        // Wait up to 1s for the correct value to be observed.
        .with_timeout_panic(Duration::from_secs(1))
        .await;

        let hits = metrics
            .get_instrument::<Metric<U64Counter>>("write_buffer_watermark_fetch_errors")
            .expect("failed to read metric")
            .get_observer(&Attributes::from([]))
            .expect("failed to get observer")
            .fetch();
        assert_eq!(hits, 0, "metric should not record any errors");
    }
}
