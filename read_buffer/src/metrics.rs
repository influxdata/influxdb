use crate::column::Statistics;
use metric::{Attributes, CumulativeGauge, CumulativeRecorder, RecorderCollection};

/// The collection of metrics exposed by the Read Buffer. Note: several of these
/// be better represented as distributions, but the histogram story in IOx is not
/// yet figured out.
#[derive(Debug)]
pub struct Metrics {
    /// The base attributes to use for all metrics
    base_attributes: Attributes,

    /// The total number of row groups in the chunk.
    row_groups_total: CumulativeRecorder,

    /// This metric tracks the total number of columns in read buffer.
    columns_total: RecorderCollection<CumulativeGauge>,

    /// This metric tracks the total number of values stored in read buffer
    /// column encodings further segmented by nullness.
    column_values_total: RecorderCollection<CumulativeGauge>,

    /// This metric tracks the total number of bytes used by read buffer columns
    /// including any allocated but unused buffers.
    column_allocated_bytes_total: RecorderCollection<CumulativeGauge>,

    /// This metric tracks the minimal number of bytes required by read buffer
    /// columns but not including allocated but unused buffers. It's primarily
    /// of interest to the development of the Read Buffer.
    column_required_bytes_total: RecorderCollection<CumulativeGauge>,

    /// This metric tracks an estimated uncompressed data size for read buffer
    /// columns, further segmented by nullness. It is a building block for
    /// tracking a measure of overall compression.
    column_raw_bytes_total: RecorderCollection<CumulativeGauge>,
}

impl Metrics {
    pub fn new(registry: &metric::Registry, db_name: impl Into<String>) -> Self {
        let db_name = db_name.into();
        let base_attributes = Attributes::from([("db_name", db_name.into())]);

        Self {
            base_attributes: base_attributes.clone(),
            row_groups_total: registry.register_metric::<CumulativeGauge>(
                "read_buffer_row_group_total",
                "The number of row groups within the Read Buffer",
            ).recorder(base_attributes),
            columns_total: RecorderCollection::new(registry.register_metric(
                "read_buffer_column_total",
                "The number of columns within the Read Buffer",
            )),
            column_values_total: RecorderCollection::new(registry.register_metric(
                "read_buffer_column_values",
                "The number of values within columns in the Read Buffer",
            )),
            column_allocated_bytes_total: RecorderCollection::new(registry.register_metric(
                "read_buffer_column_allocated_bytes",
                "The number of bytes used by all data in the Read Buffer including allocated by unused buffers",
            )),
            column_required_bytes_total: RecorderCollection::new(registry.register_metric(
                "read_buffer_column_required_bytes",
                "The number of bytes currently required to store data in the Read Buffer excluding allocated by unused buffers",
            )),
            column_raw_bytes_total: RecorderCollection::new(registry.register_metric(
                "read_buffer_column_raw_bytes",
                "The number of bytes used by all columns if they were uncompressed in the Read Buffer",
            )),
        }
    }

    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metric registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metric registry
    pub fn new_unregistered() -> Self {
        Self {
            base_attributes: Attributes::from([]),
            row_groups_total: CumulativeRecorder::new_unregistered(),
            columns_total: RecorderCollection::new_unregistered(),
            column_values_total: RecorderCollection::new_unregistered(),
            column_allocated_bytes_total: RecorderCollection::new_unregistered(),
            column_required_bytes_total: RecorderCollection::new_unregistered(),
            column_raw_bytes_total: RecorderCollection::new_unregistered(),
        }
    }

    // Updates column storage statistics for the Read Buffer.
    pub(crate) fn update_column_storage_statistics(&mut self, statistics: &[Statistics]) {
        // increase number of row groups in chunk.
        self.row_groups_total.inc(1);

        for stat in statistics {
            let mut attributes = self.base_attributes.clone();
            attributes.insert("encoding", stat.enc_type.clone());
            attributes.insert("log_data_type", stat.log_data_type);

            // update number of columns
            self.columns_total.recorder(attributes.clone()).inc(1);

            // update bytes allocated associated with columns
            self.column_allocated_bytes_total
                .recorder(attributes.clone())
                .inc(stat.allocated_bytes as u64);

            // update bytes in use but excluded unused
            self.column_required_bytes_total
                .recorder(attributes.clone())
                .inc(stat.required_bytes as u64);

            attributes.insert("null", "true");

            // update raw estimated bytes of NULL values
            self.column_raw_bytes_total
                .recorder(attributes.clone())
                .inc((stat.raw_bytes - stat.raw_bytes_no_null) as u64);

            // update number of NULL values
            self.column_values_total
                .recorder(attributes.clone())
                .inc(stat.nulls as u64);

            attributes.insert("null", "false");

            // update raw estimated bytes of non-NULL values
            self.column_raw_bytes_total
                .recorder(attributes.clone())
                .inc(stat.raw_bytes_no_null as u64);

            // update number of non-NULL values
            self.column_values_total
                .recorder(attributes)
                .inc((stat.values - stat.nulls) as u64);
        }
    }
}
