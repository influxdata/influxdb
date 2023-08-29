use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use metric::{
    DurationHistogram, DurationHistogramOptions, U64Histogram, U64HistogramOptions, DURATION_MAX,
};

use super::completion_observer::{CompletedPersist, PersistCompletionObserver};

const MINUTES: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) struct ParquetFileInstrumentation<T> {
    inner: T,

    row_count: U64Histogram,
    column_count: U64Histogram,
    file_size_bytes: U64Histogram,
    file_time_range: DurationHistogram,
}

impl<T> ParquetFileInstrumentation<T> {
    pub(crate) fn new(inner: T, metrics: &metric::Registry) -> Self {
        // A metric capturing the duration difference between min & max
        // timestamps.
        let file_time_range: DurationHistogram = metrics
            .register_metric_with_options::<DurationHistogram, _>(
                "ingester_persist_parquet_file_time_range",
                "range from min to max timestamp in output parquet file",
                || {
                    DurationHistogramOptions::new([
                        30 * MINUTES,    // 30m
                        60 * MINUTES,    // 1h
                        120 * MINUTES,   // 2h
                        240 * MINUTES,   // 4h
                        480 * MINUTES,   // 8h
                        960 * MINUTES,   // 16h
                        1_920 * MINUTES, // 32h
                        DURATION_MAX,
                    ])
                },
            )
            .recorder(&[]);

        // File size distribution.
        let file_size_bytes: U64Histogram = metrics
            .register_metric_with_options::<U64Histogram, _>(
                "ingester_persist_parquet_file_size_bytes",
                "distribution of output parquet file size in bytes",
                || {
                    U64HistogramOptions::new([
                        4_u64.pow(5),  // 1 kibibyte
                        4_u64.pow(6),  // 4 kibibytes
                        4_u64.pow(7),  // 16 kibibytes
                        4_u64.pow(8),  // 64 kibibytes
                        4_u64.pow(9),  // 256 kibibytes
                        4_u64.pow(10), // 1 mebibyte
                        4_u64.pow(11), // 4 mebibytes
                        4_u64.pow(12), // 16 mebibytes
                        4_u64.pow(13), // 64 mebibytes
                        4_u64.pow(14), // 256 mebibytes
                        4_u64.pow(15), // 1 gibibyte
                        4_u64.pow(16), // 4 gibibytes
                        u64::MAX,
                    ])
                },
            )
            .recorder(&[]);

        // Row count distribution.
        let row_count: U64Histogram = metrics
            .register_metric_with_options::<U64Histogram, _>(
                "ingester_persist_parquet_file_row_count",
                "distribution of row count in output parquet files",
                || {
                    U64HistogramOptions::new([
                        4_u64.pow(3),  // 64
                        4_u64.pow(4),  // 256
                        4_u64.pow(5),  // 1,024
                        4_u64.pow(6),  // 4,096
                        4_u64.pow(7),  // 16,384
                        4_u64.pow(8),  // 65,536
                        4_u64.pow(9),  // 262,144
                        4_u64.pow(10), // 1,048,576
                        4_u64.pow(11), // 4,194,304
                        4_u64.pow(12), // 16,777,216
                        u64::MAX,
                    ])
                },
            )
            .recorder(&[]);

        // Column count distribution.
        //
        // Because the column count is, by default, limited per table, this
        // range should exceed that limit by some degree to discover overshoot
        // (limits are eventually consistent) and correctly measure workloads
        // that have been configured with a higher limit.
        let column_count: U64Histogram = metrics
            .register_metric_with_options::<U64Histogram, _>(
                "ingester_persist_parquet_file_column_count",
                "distribution of column count in output parquet files",
                || {
                    U64HistogramOptions::new([
                        2_u64.pow(1),  // 2
                        2_u64.pow(2),  // 4
                        2_u64.pow(3),  // 8
                        2_u64.pow(4),  // 16
                        2_u64.pow(5),  // 32
                        2_u64.pow(6),  // 64
                        2_u64.pow(7),  // 128
                        2_u64.pow(8),  // 256
                        2_u64.pow(9),  // 512
                        2_u64.pow(10), // 1,024
                        2_u64.pow(11), // 2,048
                        u64::MAX,
                    ])
                },
            )
            .recorder(&[]);

        Self {
            inner,
            row_count,
            column_count,
            file_size_bytes,
            file_time_range,
        }
    }
}

#[async_trait]
impl<T> PersistCompletionObserver for ParquetFileInstrumentation<T>
where
    T: PersistCompletionObserver,
{
    async fn persist_complete(&self, note: Arc<CompletedPersist>) {
        // Observe the persistence notification values.
        self.row_count.record(note.row_count() as _);
        self.column_count.record(note.column_count() as _);
        self.file_size_bytes.record(note.parquet_file_bytes() as _);
        self.file_time_range.record(note.timestamp_range());

        // Forward on the notification to the next handler.
        self.inner.persist_complete(note).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        persist::completion_observer::mock::MockCompletionObserver,
        test_util::{
            ARBITRARY_NAMESPACE_ID, ARBITRARY_TABLE_ID, ARBITRARY_TRANSITION_PARTITION_ID,
        },
    };
    use data_types::{
        sequence_number_set::SequenceNumberSet, ColumnId, ColumnSet, ParquetFile, ParquetFileId,
        Timestamp,
    };
    use metric::assert_histogram;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_persisted_file_metrics() {
        let inner = Arc::new(MockCompletionObserver::default());

        let metrics = metric::Registry::default();
        let decorator = ParquetFileInstrumentation::new(Arc::clone(&inner), &metrics);

        let meta = ParquetFile {
            id: ParquetFileId::new(42),
            to_delete: None,
            namespace_id: ARBITRARY_NAMESPACE_ID,
            table_id: ARBITRARY_TABLE_ID,
            partition_id: ARBITRARY_TRANSITION_PARTITION_ID.clone(),
            object_store_id: Default::default(),
            min_time: Timestamp::new(Duration::from_secs(1_000).as_nanos() as _),
            max_time: Timestamp::new(Duration::from_secs(1_042).as_nanos() as _), // 42 seconds later
            file_size_bytes: 42424242,
            row_count: 24,
            compaction_level: data_types::CompactionLevel::Initial,
            created_at: Timestamp::new(1234),
            column_set: ColumnSet::new([1, 2, 3, 4].into_iter().map(ColumnId::new)),
            max_l0_created_at: Timestamp::new(42),
        };

        decorator
            .persist_complete(Arc::new(CompletedPersist::new(
                meta.clone(),
                SequenceNumberSet::default(),
            )))
            .await;

        assert_histogram!(
            metrics,
            DurationHistogram,
            "ingester_persist_parquet_file_time_range",
            samples = 1,
            sum = Duration::from_secs(42),
        );

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_persist_parquet_file_size_bytes",
            samples = 1,
            sum = meta.file_size_bytes as u64,
        );

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_persist_parquet_file_row_count",
            samples = 1,
            sum = meta.row_count as u64,
        );

        assert_histogram!(
            metrics,
            U64Histogram,
            "ingester_persist_parquet_file_column_count",
            samples = 1,
            sum = meta.column_set.len() as u64,
        );
    }
}
