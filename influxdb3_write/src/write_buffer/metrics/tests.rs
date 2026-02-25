use metric::{Attributes, Registry};

use super::WriteMetrics;

#[test]
fn record_lines() {
    let metric_registry = Registry::new();
    let metrics = WriteMetrics::new(&metric_registry);
    metrics.record_lines("foo", 64);
    metrics.record_lines(String::from("bar"), 256);
    assert_eq!(
        64,
        metrics
            .write_lines_total
            .get_observer(&Attributes::from(&[("db", "foo")]))
            .unwrap()
            .fetch()
    );
    assert_eq!(
        256,
        metrics
            .write_lines_total
            .get_observer(&Attributes::from(&[("db", "bar")]))
            .unwrap()
            .fetch()
    );
}

#[test]
fn record_lines_rejected() {
    let metric_registry = Registry::new();
    let metrics = WriteMetrics::new(&metric_registry);
    metrics.record_lines_rejected("foo", 64);
    metrics.record_lines_rejected(String::from("bar"), 256);
    assert_eq!(
        64,
        metrics
            .write_lines_rejected_total
            .get_observer(&Attributes::from(&[("db", "foo")]))
            .unwrap()
            .fetch()
    );
    assert_eq!(
        256,
        metrics
            .write_lines_rejected_total
            .get_observer(&Attributes::from(&[("db", "bar")]))
            .unwrap()
            .fetch()
    );
}

#[test]
fn record_bytes() {
    let metric_registry = Registry::new();
    let metrics = WriteMetrics::new(&metric_registry);
    metrics.record_bytes("foo", 64);
    metrics.record_bytes(String::from("bar"), 256);
    assert_eq!(
        64,
        metrics
            .write_bytes_total
            .get_observer(&Attributes::from(&[("db", "foo")]))
            .unwrap()
            .fetch()
    );
    assert_eq!(
        256,
        metrics
            .write_bytes_total
            .get_observer(&Attributes::from(&[("db", "bar")]))
            .unwrap()
            .fetch()
    );
}
