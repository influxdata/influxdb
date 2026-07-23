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
            "track total number of lines written",
        );
        let write_lines_rejected_total = metric_registry.register_metric::<U64Counter>(
            WRITE_LINES_REJECTED_METRIC_NAME,
            "track total number of lines that were rejected",
        );
        let write_bytes_total = metric_registry.register_metric::<U64Counter>(
            WRITE_BYTES_METRIC_NAME,
            "track total number of bytes written",
        );
        Self {
            write_lines_total,
            write_lines_rejected_total,
            write_bytes_total,
        }
    }

    pub(super) fn record_lines(&self, lines: u64) {
        self.write_lines_total.recorder([]).inc(lines);
    }

    pub(super) fn record_lines_rejected(&self, lines: u64) {
        self.write_lines_rejected_total.recorder([]).inc(lines);
    }

    pub(super) fn record_bytes(&self, bytes: u64) {
        self.write_bytes_total.recorder([]).inc(bytes);
    }
}

#[cfg(test)]
mod tests;
