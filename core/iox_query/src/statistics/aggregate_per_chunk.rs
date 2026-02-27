//! Code to translate IOx statistics to DataFusion statistics
//! for a given QueryChunk

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::{
    physical_plan::{ColumnStatistics, Statistics as DFStatistics},
    scalar::ScalarValue,
};

use crate::{CHUNK_ORDER_COLUMN_NAME, QueryChunk};

/// During the initial partition pruning pass (driving statistics from partition
/// keys), the row count and null count statistics for each partition
/// remain unknown.
///
/// In cases where "a column is known to be entirely NULL," the row count and
/// null count statistics should be identical. Use this constant to assign
/// identical values to both the row count and null count statistics for columns
/// identified as entirely NULL.
pub static NULL_COLUMN_INDICATOR: Precision<usize> = Precision::Inexact(1);

/// Aggregates DataFusion [statistics](DFStatistics).
#[derive(Debug)]
struct DFStatsAggregator<'a> {
    num_rows: Precision<usize>,
    total_byte_size: Precision<usize>,
    column_statistics: Vec<DFStatsAggregatorCol>,
    // Maps column name to index in column_statistics for all columns we are
    // aggregating
    col_idx_map: HashMap<&'a str, usize>,
}

impl<'a> DFStatsAggregator<'a> {
    /// Creates new aggregator the given schema.
    ///
    /// This will start with:
    ///
    /// - 0 rows
    /// - 0 bytes
    /// - for each column:
    ///   - 0 null values
    ///   - unknown min value
    ///   - unknown max value
    /// - exact representation
    fn new(schema: &'a Schema) -> Self {
        let col_idx_map = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| (f.name().as_str(), idx))
            .collect::<HashMap<_, _>>();

        Self {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: (0..col_idx_map.len())
                .map(|_| DFStatsAggregatorCol {
                    null_count: Precision::Exact(0),
                    max_value: None,
                    min_value: None,
                })
                .collect(),

            col_idx_map,
        }
    }

    /// Update given base statistics with the given schema.
    ///
    /// This only updates columns that were present when the aggregator was created. Column reordering is allowed.
    ///
    /// Updates are meant to be "additive", i.e. they only add data/rows. There is NOT way to remove/substract data from
    /// the accumulator.
    ///
    /// # Panics
    /// Panics when the number of columns in the statistics and the schema are different.
    fn update(&mut self, update_stats: &DFStatistics, update_schema: &Schema) {
        // decompose structs so we don't forget new fields
        let DFStatistics {
            num_rows: update_num_rows,
            total_byte_size: update_total_byte_size,
            column_statistics: update_column_statistics,
        } = update_stats;

        self.num_rows = self.num_rows.add(update_num_rows);
        self.total_byte_size = self.total_byte_size.add(update_total_byte_size);

        assert_eq!(self.column_statistics.len(), self.col_idx_map.len());
        assert_eq!(
            update_column_statistics.len(),
            update_schema.fields().len(),
            "stats ({}) and schema ({}) have different column count",
            update_column_statistics.len(),
            update_schema.fields().len(),
        );

        let mut used_cols = vec![false; self.col_idx_map.len()];

        for (update_field, update_col) in update_schema
            .fields()
            .iter()
            .zip(update_column_statistics.iter())
        {
            // Skip if not aggregating statitics for this field
            let Some(idx) = self.col_idx_map.get(update_field.name().as_str()) else {
                continue;
            };
            let base_col = &mut self.column_statistics[*idx];
            used_cols[*idx] = true;

            // decompose structs so we don't forget new fields
            let DFStatsAggregatorCol {
                null_count: base_null_count,
                max_value: base_max_value,
                min_value: base_min_value,
            } = base_col;
            let ColumnStatistics {
                null_count: update_null_count,
                max_value: update_max_value,
                min_value: update_min_value,
                distinct_count: _update_distinct_count,
                sum_value: _sum_value,
            } = update_col;

            *base_null_count = base_null_count.add(update_null_count);

            *base_max_value = Some(
                base_max_value
                    .take()
                    .map(|base_max_value| base_max_value.max(update_max_value))
                    .unwrap_or(update_max_value.clone()),
            );

            *base_min_value = Some(
                base_min_value
                    .take()
                    .map(|base_min_value| base_min_value.min(update_min_value))
                    .unwrap_or(update_min_value.clone()),
            );
        }

        // for unused cols, we need to assume all-NULL and hence invalidate the null counters
        for (used, base_col) in used_cols.into_iter().zip(&mut self.column_statistics) {
            if !used {
                base_col.null_count = Precision::Absent;
            }
        }
    }

    /// Build aggregated statistics.
    fn build(self) -> DFStatistics {
        DFStatistics {
            num_rows: self.num_rows,
            total_byte_size: self.total_byte_size,
            column_statistics: self
                .column_statistics
                .into_iter()
                .map(|col| ColumnStatistics {
                    null_count: col.null_count,
                    max_value: col.max_value.unwrap_or(Precision::Absent),
                    min_value: col.min_value.unwrap_or(Precision::Absent),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                })
                .collect(),
        }
    }
}

/// Similar to [`ColumnStatistics`] but uses `Option` to track min/max values so
/// we can differentiate between
///
/// 1. "uninitialized" (`None`)
/// 1. "initialized" (`Some(Precision::Exact(...))`)
/// 2. "initialized but invalid" (`Some(Precision::Absent)`).
///
/// It also does NOT contain a distinct count because we cannot aggregate these.
#[derive(Debug)]
struct DFStatsAggregatorCol {
    null_count: Precision<usize>,
    max_value: Option<Precision<ScalarValue>>,
    min_value: Option<Precision<ScalarValue>>,
}

/// build DF statistics for given chunks and a schema
pub fn build_statistics_for_chunks(
    chunks: &[Arc<dyn QueryChunk>],
    schema: SchemaRef,
) -> DFStatistics {
    let chunk_order_field = schema.field_with_name(CHUNK_ORDER_COLUMN_NAME).ok();
    let chunk_order_only_schema = chunk_order_field.map(|field| Schema::new(vec![field.clone()]));

    let chunks: Vec<_> = chunks.iter().collect();

    chunks
        .iter()
        .fold(DFStatsAggregator::new(&schema), |mut agg, chunk| {
            agg.update(&chunk.stats(), chunk.schema().as_arrow().as_ref());

            if let Some(schema) = chunk_order_only_schema.as_ref() {
                let order = chunk.order().get();
                let order = ScalarValue::from(order);

                agg.update(
                    &DFStatistics {
                        num_rows: Precision::Exact(0),
                        total_byte_size: Precision::Exact(0),
                        column_statistics: vec![ColumnStatistics {
                            null_count: Precision::Exact(0),
                            max_value: Precision::Exact(order.clone()),
                            min_value: Precision::Exact(order),
                            distinct_count: Precision::Exact(1),
                            sum_value: Precision::Absent,
                        }],
                    },
                    schema,
                );
            }

            agg
        })
        .build()
}

#[cfg(test)]
mod test {
    use crate::test::TestChunk;

    use super::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion::common::Statistics;
    use schema::{InfluxFieldType, SchemaBuilder};

    #[test]
    fn test_df_stats_agg_no_cols_no_updates() {
        let schema = Schema::new(Vec::<Field>::new());
        let agg = DFStatsAggregator::new(&schema);

        let actual = agg.build();
        let expected = DFStatistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: Statistics::unknown_column(&schema),
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_df_stats_agg_no_updates() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let agg = DFStatsAggregator::new(&schema);

        let actual = agg.build();
        let expected = DFStatistics {
            num_rows: Precision::Exact(0),
            total_byte_size: Precision::Exact(0),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_df_stats_agg_valid_update_partial() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let mut agg = DFStatsAggregator::new(&schema);

        let update_schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let update_stats = DFStatistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(100),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
            ],
        };
        agg.update(&update_stats, &update_schema);

        let update_schema = Schema::new(vec![Field::new("col2", DataType::Utf8, false)]);
        let update_stats = DFStatistics {
            num_rows: Precision::Exact(10_000),
            total_byte_size: Precision::Exact(100_000),
            column_statistics: vec![ColumnStatistics {
                null_count: Precision::Exact(1_000_000),
                max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                min_value: Precision::Exact(ScalarValue::Utf8(Some("c".to_owned()))),
                distinct_count: Precision::Exact(42),
                sum_value: Precision::Absent,
            }],
        };
        agg.update(&update_stats, &update_schema);

        let actual = agg.build();
        let expected = DFStatistics {
            num_rows: Precision::Exact(10_001),
            total_byte_size: Precision::Exact(100_010),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_001_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_df_stats_agg_valid_update_col_reorder() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let mut agg = DFStatsAggregator::new(&schema);

        let update_schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let update_stats = DFStatistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(100),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
            ],
        };
        agg.update(&update_stats, &update_schema);

        let update_schema = Schema::new(vec![
            Field::new("col2", DataType::Utf8, false),
            Field::new("col1", DataType::UInt64, true),
        ]);
        let update_stats = DFStatistics {
            num_rows: Precision::Exact(10_000),
            total_byte_size: Precision::Exact(100_000),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(1_000_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("c".to_owned()))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(10_000_000),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(99))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(40))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
            ],
        };
        agg.update(&update_stats, &update_schema);

        let actual = agg.build();
        let expected = DFStatistics {
            num_rows: Precision::Exact(10_001),
            total_byte_size: Precision::Exact(100_010),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(10_000_100),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(40))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_001_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_df_stats_agg_ignores_unknown_cols() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);
        let mut agg = DFStatsAggregator::new(&schema);

        let update_schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col3", DataType::Utf8, false),
        ]);
        let update_stats = DFStatistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(100),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
            ],
        };
        agg.update(&update_stats, &update_schema);

        let actual = agg.build();
        let expected = DFStatistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(100),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ],
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_df_stats_agg_invalidation() {
        let schema = Schema::new(vec![
            Field::new("col1", DataType::UInt64, true),
            Field::new("col2", DataType::Utf8, false),
        ]);

        let update_stats = DFStatistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(100),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Exact(42),
                    sum_value: Precision::Absent,
                },
            ],
        };
        let agg_stats = DFStatistics {
            num_rows: Precision::Exact(2),
            total_byte_size: Precision::Exact(20),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(200),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(100))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(50))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
                ColumnStatistics {
                    null_count: Precision::Exact(2_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Absent,
                    sum_value: Precision::Absent,
                },
            ],
        };

        #[derive(Debug, Clone, Copy)]
        enum ColMode {
            NullCount,
            MaxValue,
            MinValue,
        }

        #[derive(Debug, Clone, Copy)]
        enum Mode {
            NumRows,
            TotalByteSize,
            ColumnStatistics,
            Col(usize, ColMode),
        }

        impl Mode {
            fn mask(&self, mut stats: DFStatistics) -> DFStatistics {
                match self {
                    Self::NumRows => {
                        stats.num_rows = Precision::Absent;
                    }
                    Self::TotalByteSize => {
                        stats.total_byte_size = Precision::Absent;
                    }
                    Self::ColumnStatistics => {
                        let num_cols = stats.column_statistics.len();
                        stats.column_statistics = vec![ColumnStatistics::new_unknown(); num_cols]
                    }
                    Self::Col(idx, mode) => {
                        let stats = &mut stats.column_statistics[*idx];

                        match mode {
                            ColMode::NullCount => {
                                stats.null_count = Precision::Absent;
                            }
                            ColMode::MaxValue => {
                                stats.max_value = Precision::Absent;
                            }
                            ColMode::MinValue => {
                                stats.min_value = Precision::Absent;
                            }
                        }
                    }
                }
                stats
            }
        }

        for mode in [
            Mode::NumRows,
            Mode::TotalByteSize,
            Mode::ColumnStatistics,
            Mode::Col(0, ColMode::NullCount),
            Mode::Col(0, ColMode::MaxValue),
            Mode::Col(0, ColMode::MinValue),
            Mode::Col(1, ColMode::NullCount),
        ] {
            println!("mode: {mode:?}");

            for invalid_mask in [[false, true], [true, false], [true, true]] {
                println!("invalid_mask: {invalid_mask:?}");
                let mut agg = DFStatsAggregator::new(&schema);

                for invalid in invalid_mask {
                    let mut update_stats = update_stats.clone();
                    if invalid {
                        update_stats = mode.mask(update_stats);
                    }
                    agg.update(&update_stats, &schema);
                }

                let actual = agg.build();

                let expected = mode.mask(agg_stats.clone());
                assert_eq!(actual, expected);
            }
        }
    }

    #[test]
    #[should_panic(expected = "stats (0) and schema (1) have different column count")]
    fn test_df_stats_agg_asserts_schema_stats_match() {
        let schema = Schema::new(vec![Field::new("col1", DataType::UInt64, true)]);
        let mut agg = DFStatsAggregator::new(&schema);

        let update_schema = Schema::new(vec![Field::new("col1", DataType::UInt64, true)]);
        let update_stats = DFStatistics {
            num_rows: Precision::Exact(1),
            total_byte_size: Precision::Exact(10),
            column_statistics: vec![],
        };
        agg.update(&update_stats, &update_schema);
    }

    #[test]
    fn test_stats_for_one_chunk() {
        // schema with one tag, one field, time and CHUNK_ORDER_COLUMN_NAME
        let schema: SchemaRef = SchemaBuilder::new()
            .tag("tag")
            .influx_field("field", InfluxFieldType::Float)
            .timestamp()
            .influx_field(CHUNK_ORDER_COLUMN_NAME, InfluxFieldType::Integer)
            .build()
            .unwrap()
            .into();

        // create a test chunk with one tag, one filed, time and CHUNK_ORDER_COLUMN_NAME
        let record_batch_chunk = Arc::new(
            TestChunk::new("t")
                .with_tag_column_with_stats("tag", Some("AL"), Some("MT"))
                .with_time_column_with_stats(Some(10), Some(20))
                .with_i64_field_column_with_stats("field", Some(0), Some(100))
                .with_i64_field_column_with_stats(CHUNK_ORDER_COLUMN_NAME, Some(5), Some(6)),
        );

        // create them same test chunk but with a parquet file
        let parquet_chunk = Arc::new(
            TestChunk::new("t")
                .with_tag_column_with_stats("tag", Some("AL"), Some("MT"))
                .with_i64_field_column_with_stats("field", Some(0), Some(100))
                .with_time_column_with_stats(Some(10), Some(20))
                .with_i64_field_column_with_stats(CHUNK_ORDER_COLUMN_NAME, Some(5), Some(6))
                .with_dummy_parquet_file(),
        );

        let expected_stats = [
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some("MT".to_string()))),
                )),
                min_value: Precision::Exact(ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some("AL".to_string()))),
                )),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(100))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(20), None)),
                min_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(10), None)),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(6))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            },
        ];

        let record_batch_stats =
            build_statistics_for_chunks(&[record_batch_chunk], Arc::clone(&schema));
        assert_eq!(record_batch_stats.column_statistics, expected_stats);

        let parquet_stats = build_statistics_for_chunks(&[parquet_chunk], schema);
        assert_eq!(parquet_stats.column_statistics, expected_stats);
    }

    #[test]
    fn test_stats_for_two_chunks() {
        // schema with one tag, one field, time and CHUNK_ORDER_COLUMN_NAME
        let schema: SchemaRef = SchemaBuilder::new()
            .tag("tag")
            .influx_field("field", InfluxFieldType::Float)
            .timestamp()
            .influx_field(CHUNK_ORDER_COLUMN_NAME, InfluxFieldType::Integer)
            .build()
            .unwrap()
            .into();

        // create a test chunk with one tag, one filed, time and CHUNK_ORDER_COLUMN_NAME
        let record_batch_chunk_1 = Arc::new(
            TestChunk::new("t1")
                .with_tag_column_with_stats("tag", Some("AL"), Some("MT"))
                .with_time_column_with_stats(Some(10), Some(20))
                .with_i64_field_column_with_stats("field", Some(0), Some(100))
                .with_i64_field_column_with_stats(CHUNK_ORDER_COLUMN_NAME, Some(5), Some(6)),
        );

        let record_batch_chunk_2 = Arc::new(
            TestChunk::new("t2")
                .with_tag_column_with_stats("tag", Some("MI"), Some("WA"))
                .with_time_column_with_stats(Some(50), Some(80))
                .with_i64_field_column_with_stats("field", Some(0), Some(70))
                .with_i64_field_column_with_stats(CHUNK_ORDER_COLUMN_NAME, Some(7), Some(15)),
        );

        // create them same test chunk but with a parquet file
        let parquet_chunk_1 = Arc::new(
            TestChunk::new("t1")
                .with_tag_column_with_stats("tag", Some("AL"), Some("MT"))
                .with_i64_field_column_with_stats("field", Some(0), Some(100))
                .with_time_column_with_stats(Some(10), Some(20))
                .with_i64_field_column_with_stats(CHUNK_ORDER_COLUMN_NAME, Some(5), Some(6))
                .with_dummy_parquet_file(),
        );

        let parquet_chunk_2 = Arc::new(
            TestChunk::new("t2")
                .with_tag_column_with_stats("tag", Some("MI"), Some("WA"))
                .with_i64_field_column_with_stats("field", Some(0), Some(70))
                .with_time_column_with_stats(Some(50), Some(80))
                .with_i64_field_column_with_stats(CHUNK_ORDER_COLUMN_NAME, Some(7), Some(15))
                .with_dummy_parquet_file(),
        );

        let expected_stats = [
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some("WA".to_string()))),
                )),
                min_value: Precision::Exact(ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some("AL".to_string()))),
                )),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(100))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(80), None)),
                min_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(10), None)),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(15))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                distinct_count: Precision::Absent,
                sum_value: Precision::Absent,
            },
        ];

        let record_batch_stats = build_statistics_for_chunks(
            &[record_batch_chunk_1, record_batch_chunk_2],
            Arc::clone(&schema),
        );
        assert_eq!(record_batch_stats.column_statistics, expected_stats);

        let parquet_stats =
            build_statistics_for_chunks(&[parquet_chunk_1, parquet_chunk_2], schema);
        assert_eq!(parquet_stats.column_statistics, expected_stats);
    }
}
