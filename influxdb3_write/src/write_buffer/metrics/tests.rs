use metric::{Attributes, Registry};

use super::WriteMetrics;

#[test]
fn record_lines() {
    let metric_registry = Registry::new();
    let metrics = WriteMetrics::new(&metric_registry);
    metrics.record_lines(64);
    metrics.record_lines(256);
    assert_eq!(
        320,
        metrics
            .write_lines_total
            .get_observer(&Attributes::from(&[]))
            .unwrap()
            .fetch()
    );
    // the per-database label set must no longer exist
    assert!(
        metrics
            .write_lines_total
            .get_observer(&Attributes::from(&[("db", "foo")]))
            .is_none()
    );
}

#[test]
fn record_lines_rejected() {
    let metric_registry = Registry::new();
    let metrics = WriteMetrics::new(&metric_registry);
    metrics.record_lines_rejected(64);
    metrics.record_lines_rejected(256);
    assert_eq!(
        320,
        metrics
            .write_lines_rejected_total
            .get_observer(&Attributes::from(&[]))
            .unwrap()
            .fetch()
    );
    assert!(
        metrics
            .write_lines_rejected_total
            .get_observer(&Attributes::from(&[("db", "foo")]))
            .is_none()
    );
}

#[test]
fn record_bytes() {
    let metric_registry = Registry::new();
    let metrics = WriteMetrics::new(&metric_registry);
    metrics.record_bytes(64);
    metrics.record_bytes(256);
    assert_eq!(
        320,
        metrics
            .write_bytes_total
            .get_observer(&Attributes::from(&[]))
            .unwrap()
            .fetch()
    );
    assert!(
        metrics
            .write_bytes_total
            .get_observer(&Attributes::from(&[("db", "foo")]))
            .is_none()
    );
}
