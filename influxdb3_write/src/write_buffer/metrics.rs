use std::borrow::Cow;

use metric::{Metric, Registry, U64Counter};

#[derive(Debug)]
pub(super) struct WriteMetrics {
    write_lines_total: Metric<U64Counter>,
    write_lines_rejected_total: Metric<U64Counter>,
    write_bytes_total: Metric<U64Counter>,
}

pub(super) const WRITE_LINES_METRIC_NAME: &str = "influxdb3_write_lines";
pub(super) const WRITE_LINES_REJECTED_METRIC_NAME: &str = "influxdb3_write_lines_rejected";
pub(super) const WRITE_BYTES_METRIC_NAME: &str = "influxdb3_write_bytes";

impl WriteMetrics {
    pub(super) fn new(metric_registry: &Registry) -> Self {
        let write_lines_total = metric_registry.register_metric::<U64Counter>(
            WRITE_LINES_METRIC_NAME,
            "track total number of lines written to the database",
        );
        let write_lines_rejected_total = metric_registry.register_metric::<U64Counter>(
            WRITE_LINES_REJECTED_METRIC_NAME,
            "track total number of lines written to the database that were rejected",
        );
        let write_bytes_total = metric_registry.register_metric::<U64Counter>(
            WRITE_BYTES_METRIC_NAME,
            "track total number of bytes written to the database",
        );
        Self {
            write_lines_total,
            write_lines_rejected_total,
            write_bytes_total,
        }
    }

    pub(super) fn record_lines<D: Into<String>>(&self, db: D, lines: u64) {
        let db: Cow<'static, str> = Cow::from(db.into());
        self.write_lines_total.recorder([("db", db)]).inc(lines);
    }

    pub(super) fn record_lines_rejected<D: Into<String>>(&self, db: D, lines: u64) {
        let db: Cow<'static, str> = Cow::from(db.into());
        self.write_lines_rejected_total
            .recorder([("db", db)])
            .inc(lines);
    }

    pub(super) fn record_bytes<D: Into<String>>(&self, db: D, bytes: u64) {
        let db: Cow<'static, str> = Cow::from(db.into());
        self.write_bytes_total.recorder([("db", db)]).inc(bytes);
    }
}

#[cfg(test)]
mod tests {
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
}
