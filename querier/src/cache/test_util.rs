use metric::{Attributes, DurationHistogram, Metric};

#[track_caller]
pub fn assert_catalog_access_metric_count(metrics: &metric::Registry, name: &'static str, n: u64) {
    let histogram = metrics
        .get_instrument::<Metric<DurationHistogram>>("catalog_op_duration")
        .expect("failed to read metric")
        .get_observer(&Attributes::from(&[("op", name), ("result", "success")]))
        .expect("failed to get observer")
        .fetch();

    let hit_count = histogram.sample_count();
    assert_eq!(hit_count, n);
}

#[track_caller]
pub fn assert_cache_access_metric_count(metrics: &metric::Registry, name: &'static str, n: u64) {
    let metric = metrics
        .get_instrument::<Metric<DurationHistogram>>("iox_cache_get")
        .expect("failed to read metric");

    let mut total = 0;
    for status in ["hit", "miss", "miss_already_loading", "cancelled"] {
        let histogram = metric
            .get_observer(&Attributes::from(&[("name", name), ("status", status)]))
            .expect("failed to get observer")
            .fetch();
        total += histogram.sample_count();
    }

    assert_eq!(total, n);
}
