use std::{sync::Arc, time::Duration};

use iox_time::{MockProvider, Time};
use metric::{Attributes, DurationHistogram, Metric, Registry};

use super::*;

#[test]
fn test_query_duration_recorder() {
    let time = Arc::new(MockProvider::new(Time::MIN));
    let registry = Arc::new(Registry::new());
    let metrics = CacheMetrics::new(Arc::clone(&registry), Arc::clone(&time) as _);

    // open a recorder, but do not set it to `Success` state, which does not record a
    // measurement for the metric:
    {
        let _recorder = metrics.query_duration_recorder("test_db");
        time.inc(Duration::from_secs(1));
    }
    assert_duration_histogram_hits(
        &registry,
        LAST_VALUES_CACHE_QUERY_DURATION_METRIC_NAME,
        [("db", "test_db")],
        0,
    );

    // open a recorder and set it to `Success` state, so that a measurement is recorded:
    {
        let mut recorder = metrics.query_duration_recorder("test_db");
        time.inc(Duration::from_secs(1));
        recorder.set_success();
    }
    assert_duration_histogram_hits(
        &registry,
        LAST_VALUES_CACHE_QUERY_DURATION_METRIC_NAME,
        [("db", "test_db")],
        1,
    );
}

#[track_caller]
fn assert_duration_histogram_hits<const N: usize>(
    metrics: &metric::Registry,
    name: &'static str,
    attr: [(&'static str, &'static str); N],
    expected: u64,
) {
    let histogram = metrics
        .get_instrument::<Metric<DurationHistogram>>(name)
        .expect("failed to get DurationHistogram from registry")
        .get_observer(&Attributes::from(&attr))
        .expect("failed to get DurationHistogram for attributes")
        .fetch();
    let actual = histogram.sample_count();
    assert_eq!(
        expected, actual,
        "actual histogram hits did not match expectation"
    );
}
