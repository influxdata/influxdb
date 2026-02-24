use std::time::Duration;

use metric::{Attributes, DurationHistogram, Metric, U64Counter, U64Histogram};

/// Assert a [`DurationHistogram`] was hit.
#[track_caller]
pub(crate) fn assert_histogram_hit<const N: usize>(
    metrics: &metric::Registry,
    name: &'static str,
    attr: [(&'static str, &'static str); N],
) {
    let histogram = metrics
        .get_instrument::<Metric<DurationHistogram>>(name)
        .expect("failed to read histogram")
        .get_observer(&Attributes::from(&attr))
        .expect("failed to get observer")
        .fetch();

    let hit_count = histogram.sample_count();
    assert!(hit_count > 0, "metric {name} did not record any calls");
    assert!(hit_count < 2, "metric {name} did record MULTIPLE calls");
}

/// Assert a [`DurationHistogram`] was not hit.
#[track_caller]
pub(crate) fn assert_histogram_not_hit<const N: usize>(
    metrics: &metric::Registry,
    name: &'static str,
    attr: [(&'static str, &'static str); N],
) {
    let histogram = metrics
        .get_instrument::<Metric<DurationHistogram>>(name)
        .expect("failed to read histogram")
        .get_observer(&Attributes::from(&attr))
        .expect("failed to get observer")
        .fetch();

    let hit_count = histogram.sample_count();
    assert!(hit_count == 0, "metric {name} did record {hit_count} calls");
}

/// Assert a total aggregate duration on a [`DurationHistogram`].
#[track_caller]
pub(crate) fn assert_histogram_total<const N: usize>(
    metrics: &metric::Registry,
    name: &'static str,
    attr: [(&'static str, &'static str); N],
    value: Duration,
) {
    let total = metrics
        .get_instrument::<Metric<DurationHistogram>>(name)
        .expect("failed to read histogram")
        .get_observer(&Attributes::from(&attr))
        .expect("failed to get observer")
        .fetch()
        .total;

    assert_eq!(total, value);
}

/// Assert a total aggregate count on a [`U64Counter`].
#[track_caller]
pub(crate) fn assert_counter_value<const N: usize>(
    metrics: &metric::Registry,
    name: &'static str,
    attr: [(&'static str, &'static str); N],
    value: u64,
) {
    let count = metrics
        .get_instrument::<Metric<U64Counter>>(name)
        .expect("failed to read counter")
        .get_observer(&Attributes::from(&attr))
        .expect("failed to get observer")
        .fetch();
    assert_eq!(count, value);
}

/// Assert a number of records recorded on a [`U64Histogram`].
#[track_caller]
pub(crate) fn assert_u64histogram_hits<const N: usize>(
    metrics: &metric::Registry,
    name: &'static str,
    attr: [(&'static str, &'static str); N],
    expected_count: u64,
) {
    let histogram = metrics
        .get_instrument::<Metric<U64Histogram>>(name)
        .expect("failed to read histogram")
        .get_observer(&Attributes::from(&attr))
        .expect("failed to get observer")
        .fetch();

    let hit_count = histogram.sample_count();
    assert_eq!(hit_count, expected_count);
}

/// Assert the total recorded on a [`U64Histogram`].
#[track_caller]
pub(crate) fn assert_u64histogram_total<const N: usize>(
    metrics: &metric::Registry,
    name: &'static str,
    attr: [(&'static str, &'static str); N],
    expected_count: u64,
) {
    let histogram = metrics
        .get_instrument::<Metric<U64Histogram>>(name)
        .expect("failed to read histogram")
        .get_observer(&Attributes::from(&attr))
        .expect("failed to get observer")
        .fetch();

    let hit_count = histogram.total;
    assert_eq!(hit_count, expected_count);
}
