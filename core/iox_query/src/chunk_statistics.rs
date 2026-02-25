//! Tools to set up DataFusion statistics.

use std::sync::Arc;

use data_types::TimestampMinMax;
use datafusion::common::stats::Precision;
use datafusion::{
    physical_plan::{ColumnStatistics, Statistics},
    scalar::ScalarValue,
};
use datafusion_util::{option_to_precision, timestamptz_nano};
use schema::{InfluxColumnType, Schema};

use crate::pruning_oracle::{
    BucketInfo, BucketPartitionPruningOracle, BucketPartitionPruningOracleBuilder,
};

use crate::statistics::NULL_COLUMN_INDICATOR;

/// Represent known min/max values for a specific column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnRange {
    pub null_count: Precision<usize>,
    pub min_value: Precision<ScalarValue>,
    pub max_value: Precision<ScalarValue>,
    /// The server-side bucketing bucket ID of the column for this partition.
    ///
    /// None if this column or this table is not bucketed. e.g. the partition
    /// template of this table does not contain `TemplatePart::Bucket`.
    pub bucket_info: Option<BucketInfo>,
}

impl ColumnRange {
    /// No data / absent.
    pub const ABSENT: Self = Self {
        null_count: Precision::Absent,
        min_value: Precision::Absent,
        max_value: Precision::Absent,
        bucket_info: None,
    };

    pub fn size(&self) -> usize {
        let Self {
            null_count: _,
            min_value,
            max_value,
            bucket_info: _,
        } = self;

        std::mem::size_of::<Self>() + size_of_precision(min_value) + size_of_precision(max_value)
    }
}

/// Calculate the size of `datafusion::common::stats::Precision<Scalar>` in bytes.
fn size_of_precision(v: &Precision<ScalarValue>) -> usize {
    let size_of_value = std::mem::size_of_val(v);

    // account avoid double counting size of scalar
    let size_of_precision = size_of_value - std::mem::size_of::<ScalarValue>();

    match v {
        Precision::Exact(scalar) => size_of_precision + scalar.size(),
        Precision::Inexact(scalar) => size_of_precision + scalar.size(),
        Precision::Absent => size_of_value,
    }
}

/// Represents the known min/max values for a subset (not all) of the columns in a partition.
///
/// The values may not actually in any row.
///
/// These ranges apply to ALL rows (esp. in ALL files and ingester chunks) within in given partition.
pub trait ColumnRanges {
    /// Get range for column.
    fn get(&self, column: &str) -> Option<ColumnRange>;
}

/// Non-existing [`ColumnRanges`].
#[derive(Debug, Clone, Copy, Default)]
pub struct NoColumnRanges;

impl ColumnRanges for NoColumnRanges {
    fn get(&self, _column: &str) -> Option<ColumnRange> {
        None
    }
}

/// A container type to bundle the statistics and the server-side bucketing information
/// for a chunk.
///
/// This is a workaround on the limitation of DataFusion's `Statistics` type, which does not
/// has the entry to store the server-side bucketing information.
#[derive(Debug)]
pub struct ChunkStatistics {
    statistics: Arc<Statistics>,
    bucket_pruning_oracle: Option<Arc<BucketPartitionPruningOracle>>,
}

impl ChunkStatistics {
    pub fn new(
        statistics: Arc<Statistics>,
        bucket_pruning_oracle: Option<Arc<BucketPartitionPruningOracle>>,
    ) -> Self {
        Self {
            statistics,
            bucket_pruning_oracle,
        }
    }

    pub fn statistics(&self) -> Arc<Statistics> {
        Arc::clone(&self.statistics)
    }

    pub fn bucket_pruning_oracle(&self) -> Option<Arc<BucketPartitionPruningOracle>> {
        self.bucket_pruning_oracle.as_ref().cloned()
    }
}

/// Create chunk [statistics](Statistics).
pub fn create_chunk_statistics<R>(
    row_count: Option<usize>,
    schema: &Schema,
    ts_min_max: Option<TimestampMinMax>,
    ranges: &R,
) -> ChunkStatistics
where
    R: ColumnRanges,
{
    let mut columns = Vec::with_capacity(schema.len());
    let mut pruning_oracle_builder = BucketPartitionPruningOracleBuilder::default();
    let mut num_rows = option_to_precision(row_count);

    for (t, field) in schema.iter() {
        let stats = match t {
            InfluxColumnType::Timestamp => {
                // prefer explicitly given time range but fall back to column ranges
                let (min_value, max_value) = match ts_min_max {
                    Some(ts_min_max) => (
                        Precision::Exact(timestamptz_nano(ts_min_max.min)),
                        Precision::Exact(timestamptz_nano(ts_min_max.max)),
                    ),
                    None => {
                        let range = ranges.get(field.name().as_ref());
                        let ColumnRange {
                            min_value,
                            max_value,
                            ..
                        } = range.unwrap_or(ColumnRange::ABSENT);
                        (min_value, max_value)
                    }
                };
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value,
                    max_value,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                }
            }
            _ => {
                let range = ranges.get(field.name().as_ref());
                let ColumnRange {
                    mut null_count,
                    min_value,
                    max_value,
                    bucket_info,
                } = range.unwrap_or(ColumnRange::ABSENT);

                // If the column is NULL, make the null count and the row count identical
                // so that DataFusion can use this information to prune the partition.
                //
                // The way DataFusion works to prune out partitions with columns that
                // are entirely NULL is as follows:
                //
                // 1. For a given predicate, e.g. "x = 10", DataFusion will rewrite it to:
                //    ```
                //    CASE
                //      WHEN x_null_count = x_row_count THEN false
                //      ELSE x_min <= 10 AND 10 <= x_max
                //    END
                //    ```
                //
                // 2. For a given partition with statistics, DataFusion will compare the NULL
                //    count and row count values to determine whether to prune this partition
                //    or not.
                if null_count == NULL_COLUMN_INDICATOR {
                    match row_count {
                        Some(row_count) => {
                            null_count = Precision::Exact(row_count);
                        }
                        None => {
                            num_rows = NULL_COLUMN_INDICATOR;
                        }
                    }
                }

                // If the column is a tag, and there is any server-side bucketing information,
                // add the bucket info to the pruning oracle builder.
                if t == InfluxColumnType::Tag
                    && let Some(bucket_info) = bucket_info
                {
                    pruning_oracle_builder.insert(Arc::from(field.name().to_string()), bucket_info);
                }

                ColumnStatistics {
                    null_count,
                    min_value,
                    max_value,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                }
            }
        };
        columns.push(stats)
    }

    // If there is no server-side bucketing information,
    // don't include it in the statistics, i.e. None.
    let bucket_pruning_oracle = if pruning_oracle_builder.is_empty() {
        None
    } else {
        Some(Arc::new(pruning_oracle_builder.build(None)))
    };

    ChunkStatistics::new(
        Arc::new(Statistics {
            num_rows,
            total_byte_size: Precision::Absent,
            column_statistics: columns,
        }),
        bucket_pruning_oracle,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datafusion_util::dict;
    use schema::{InfluxFieldType, SchemaBuilder, TIME_COLUMN_NAME};

    use super::*;

    #[test]
    fn test_create_chunk_statistics_no_columns_no_rows() {
        let schema = SchemaBuilder::new().build().unwrap();
        let row_count = 0;

        let actual =
            create_chunk_statistics(Some(row_count), &schema, None, &NoColumnRanges).statistics();
        let expected = Statistics {
            num_rows: Precision::Exact(row_count),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        assert_eq!(actual, Arc::new(expected));
    }

    #[test]
    fn test_create_chunk_statistics_no_columns_null_rows() {
        let schema = SchemaBuilder::new().build().unwrap();

        let actual = create_chunk_statistics(None, &schema, None, &NoColumnRanges).statistics();
        let expected = Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        };
        assert_eq!(actual, Arc::new(expected));
    }

    #[test]
    fn test_create_chunk_statistics() {
        let schema = full_schema();
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };
        let ranges = TestColumnRanges(HashMap::from([
            (
                "tag1",
                ColumnRange {
                    null_count: Precision::Absent,
                    min_value: Precision::Exact(dict("aaa")),
                    max_value: Precision::Exact(dict("bbb")),
                    bucket_info: None,
                },
            ),
            (
                "tag3", // does not exist in schema
                ColumnRange {
                    null_count: Precision::Absent,
                    min_value: Precision::Exact(dict("ccc")),
                    max_value: Precision::Exact(dict("ddd")),
                    bucket_info: None,
                },
            ),
            (
                "field_integer",
                ColumnRange {
                    null_count: Precision::Absent,
                    min_value: Precision::Exact(ScalarValue::from(10i64)),
                    max_value: Precision::Exact(ScalarValue::from(20i64)),
                    bucket_info: None,
                },
            ),
        ]));

        for row_count in [0usize, 1337usize] {
            let actual =
                create_chunk_statistics(Some(row_count), &schema, Some(ts_min_max), &ranges)
                    .statistics();
            let expected = Statistics {
                num_rows: Precision::Exact(row_count),
                total_byte_size: Precision::Absent,
                column_statistics: vec![
                    // tag1
                    ColumnStatistics {
                        null_count: Precision::Absent,
                        min_value: Precision::Exact(dict("aaa")),
                        max_value: Precision::Exact(dict("bbb")),
                        distinct_count: Precision::Absent,
                        sum_value: Precision::Absent,
                    },
                    // tag2
                    ColumnStatistics::default(),
                    // field_bool
                    ColumnStatistics::default(),
                    // field_float
                    ColumnStatistics::default(),
                    // field_integer
                    ColumnStatistics {
                        null_count: Precision::Absent,
                        min_value: Precision::Exact(ScalarValue::from(10i64)),
                        max_value: Precision::Exact(ScalarValue::from(20i64)),
                        distinct_count: Precision::Absent,
                        sum_value: Precision::Absent,
                    },
                    // field_string
                    ColumnStatistics::default(),
                    // field_uinteger
                    ColumnStatistics::default(),
                    // time
                    ColumnStatistics {
                        null_count: Precision::Exact(0),
                        min_value: Precision::Exact(timestamptz_nano(10)),
                        max_value: Precision::Exact(timestamptz_nano(20)),
                        distinct_count: Precision::Absent,
                        sum_value: Precision::Absent,
                    },
                ],
            };
            assert_eq!(actual, Arc::new(expected));
        }
    }

    #[test]
    fn test_create_chunk_statistics_ts_min_max_overrides_column_range() {
        let schema = full_schema();
        let row_count = 42usize;
        let ts_min_max = TimestampMinMax { min: 10, max: 20 };
        let ranges = TestColumnRanges(HashMap::from([(
            TIME_COLUMN_NAME,
            ColumnRange {
                null_count: Precision::Absent,
                min_value: Precision::Exact(timestamptz_nano(12)),
                max_value: Precision::Exact(timestamptz_nano(22)),
                bucket_info: None,
            },
        )]));

        let actual = create_chunk_statistics(Some(row_count), &schema, Some(ts_min_max), &ranges)
            .statistics();
        let expected = Statistics {
            num_rows: Precision::Exact(row_count),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value: Precision::Exact(timestamptz_nano(10)),
                    max_value: Precision::Exact(timestamptz_nano(20)),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, Arc::new(expected));
    }

    #[test]
    fn test_create_chunk_statistics_ts_min_max_none_so_fallback_to_column_range() {
        let schema = full_schema();
        let row_count = 42usize;
        let ranges = TestColumnRanges(HashMap::from([(
            TIME_COLUMN_NAME,
            ColumnRange {
                null_count: Precision::Absent,
                min_value: Precision::Exact(timestamptz_nano(12)),
                max_value: Precision::Exact(timestamptz_nano(22)),
                bucket_info: None,
            },
        )]));

        let actual = create_chunk_statistics(Some(row_count), &schema, None, &ranges).statistics();
        let expected = Statistics {
            num_rows: Precision::Exact(row_count),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics::default(),
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    min_value: Precision::Exact(timestamptz_nano(12)),
                    max_value: Precision::Exact(timestamptz_nano(22)),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, Arc::new(expected));
    }

    fn full_schema() -> Schema {
        SchemaBuilder::new()
            .tag("tag1")
            .tag("tag2")
            .influx_field("field_bool", InfluxFieldType::Boolean)
            .influx_field("field_float", InfluxFieldType::Float)
            .influx_field("field_integer", InfluxFieldType::Integer)
            .influx_field("field_string", InfluxFieldType::String)
            .influx_field("field_uinteger", InfluxFieldType::UInteger)
            .timestamp()
            .build()
            .unwrap()
    }

    struct TestColumnRanges(HashMap<&'static str, ColumnRange>);

    impl ColumnRanges for TestColumnRanges {
        fn get(&self, column: &str) -> Option<ColumnRange> {
            self.0.get(column).cloned()
        }
    }
}
