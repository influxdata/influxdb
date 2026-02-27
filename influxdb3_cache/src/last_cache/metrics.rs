use std::{borrow::Cow, sync::Arc};

use iox_time::{Time, TimeProvider};
use metric::{Attributes, DurationHistogram, Metric};

const LAST_VALUES_CACHE_QUERY_DURATION_METRIC_NAME: &str =
    "influxdb3_last_values_cache_query_duration";

/// Record various metrics on the last values cache
///
/// This holds `Metric<T>` instead of `T`, as we record metrics per database, and therefore
/// fetch metric recorders dynamically.
#[derive(Debug)]
pub(super) struct CacheMetrics {
    /// Record the duration of successful queries made to the last value cache.
    query_durations: Metric<DurationHistogram>,
    time_provider: Arc<dyn TimeProvider>,
}

impl CacheMetrics {
    pub(super) fn new(
        registry: Arc<metric::Registry>,
        time_provider: Arc<dyn TimeProvider>,
    ) -> Self {
        let query_durations = registry.register_metric(
            LAST_VALUES_CACHE_QUERY_DURATION_METRIC_NAME,
            "time to complete queries to the last values cache",
        );
        Self {
            query_durations,
            time_provider,
        }
    }

    pub(super) fn query_duration_recorder(
        &self,
        db: impl Into<Cow<'static, str>>,
    ) -> QueryDurationRecorder {
        let metric = self
            .query_durations
            .recorder(Attributes::from([("db", db.into())]));
        QueryDurationRecorder {
            metric,
            start_time: self.time_provider.now(),
            time_provider: Arc::clone(&self.time_provider),
            state: QueryState::DidNotComplete,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum QueryState {
    Success,
    DidNotComplete,
}

#[derive(Debug)]
pub(super) struct QueryDurationRecorder {
    metric: DurationHistogram,
    start_time: Time,
    time_provider: Arc<dyn TimeProvider>,
    state: QueryState,
}

impl QueryDurationRecorder {
    pub(super) fn set_success(&mut self) {
        self.state = QueryState::Success;
    }
}

impl Drop for QueryDurationRecorder {
    fn drop(&mut self) {
        if let QueryState::Success = self.state {
            let Some(elapsed) = self
                .time_provider
                .now()
                .checked_duration_since(self.start_time)
            else {
                // time went backwards, abort!
                return;
            };
            self.metric.record(elapsed);
        }
    }
}

#[cfg(test)]
mod tests;
