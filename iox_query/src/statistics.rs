//! Code to translate IOx statistics to DataFusion statistics

use std::{cmp::Ordering, collections::HashMap};

use arrow::datatypes::Schema;
use datafusion::{
    physical_plan::{ColumnStatistics, Statistics as DFStatistics},
    scalar::ScalarValue,
};

/// Aggregates DataFusion [statistics](DFStatistics).
#[derive(Debug)]
pub struct DFStatsAggregator<'a> {
    num_rows: Option<usize>,
    total_byte_size: Option<usize>,
    column_statistics: Option<Vec<DFStatsAggregatorCol>>,
    is_exact: bool,
    col_idx_map: HashMap<&'a str, usize>,
}

impl<'a> DFStatsAggregator<'a> {
    /// Creates new aggregator the the given schema.
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
    pub fn new(schema: &'a Schema) -> Self {
        let col_idx_map = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| (f.name().as_str(), idx))
            .collect::<HashMap<_, _>>();

        Self {
            num_rows: Some(0),
            total_byte_size: Some(0),
            column_statistics: Some(
                (0..col_idx_map.len())
                    .map(|_| DFStatsAggregatorCol {
                        null_count: Some(0),
                        max_value: TriStateScalar::Uninit,
                        min_value: TriStateScalar::Uninit,
                    })
                    .collect(),
            ),
            is_exact: true,
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
    pub fn update(&mut self, update_stats: &DFStatistics, update_schema: &Schema) {
        // decompose structs so we don't forget new fields
        let DFStatistics {
            num_rows: update_num_rows,
            total_byte_size: update_total_byte_size,
            column_statistics: update_column_statistics,
            is_exact: update_is_exact,
        } = update_stats;

        self.num_rows = self
            .num_rows
            .zip(*update_num_rows)
            .map(|(base, update)| base + update);
        self.total_byte_size = self
            .total_byte_size
            .zip(*update_total_byte_size)
            .map(|(base, update)| base + update);
        self.column_statistics = self
            .column_statistics
            .take()
            .zip(update_column_statistics.as_ref())
            .map(|(mut base_cols, update_cols)| {
                assert_eq!(base_cols.len(), self.col_idx_map.len());
                assert!(
                    update_cols.len() == update_schema.fields().len(),
                    "stats ({}) and schema ({}) have different column count",
                    update_cols.len(),
                    update_schema.fields().len(),
                );

                let mut used_cols = vec![false; self.col_idx_map.len()];

                for (update_field, update_col) in update_schema.fields().iter().zip(update_cols) {
                    let Some(idx) = self.col_idx_map
                        .get(update_field.name().as_str()) else {continue;};
                    let base_col = &mut base_cols[*idx];
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
                    } = update_col;

                    *base_null_count = base_null_count
                        .zip(*update_null_count)
                        .map(|(base, update)| base + update);
                    base_max_value.update(update_max_value, |base, update| {
                        match base.partial_cmp(update) {
                            None => None,
                            Some(Ordering::Less) => Some(update.clone()),
                            Some(Ordering::Equal | Ordering::Greater) => Some(base),
                        }
                    });
                    base_min_value.update(update_min_value, |base, update| {
                        match base.partial_cmp(update) {
                            None => None,
                            Some(Ordering::Less | Ordering::Equal) => Some(base),
                            Some(Ordering::Greater) => Some(update.clone()),
                        }
                    });
                }

                // for unused cols, we need to assume all-NULL and hence invalidate the null counters
                for (used, base_col) in used_cols.into_iter().zip(&mut base_cols) {
                    if !used {
                        base_col.null_count = None;
                    }
                }

                base_cols
            });
        self.is_exact &= update_is_exact;
    }

    /// Build aggregated statistics.
    pub fn build(self) -> DFStatistics {
        DFStatistics {
            num_rows: self.num_rows,
            total_byte_size: self.total_byte_size,
            column_statistics: self.column_statistics.map(|cols| {
                cols.into_iter()
                    .map(|col| ColumnStatistics {
                        null_count: col.null_count,
                        max_value: col.max_value.collapse(),
                        min_value: col.min_value.collapse(),
                        distinct_count: None,
                    })
                    .collect()
            }),
            is_exact: self.is_exact,
        }
    }
}

/// Similar to [`ColumnStatistics`] but has a tri-state for the min/max values so we can differentiate between
/// ["uninitialized"](TriStateScalar::Uninit) and ["invalid"](TriStateScalar::Invalid).
///
/// It also does NOT contain a distinct count because we cannot aggregate these.
#[derive(Debug)]
struct DFStatsAggregatorCol {
    null_count: Option<usize>,
    max_value: TriStateScalar,
    min_value: TriStateScalar,
}

#[derive(Debug)]
enum TriStateScalar {
    /// Scalar has valid state.
    Valid(ScalarValue),

    /// Scalar was not yet initialized.
    Uninit,

    /// Scalar was poisoned and is invalid.
    Invalid,
}

impl TriStateScalar {
    fn update<'a, F>(&mut self, update: &'a Option<ScalarValue>, f: F)
    where
        F: FnOnce(ScalarValue, &'a ScalarValue) -> Option<ScalarValue>,
    {
        match (self, update.as_ref()) {
            // invalid acts as a poison value
            (Self::Invalid, _) => {}
            // update w/o invalid invalidates aggregate
            (this, None) => {
                *this = Self::Invalid;
            }
            // uninit w/ first value just clones the value
            (this @ Self::Uninit, Some(update)) => {
                *this = Self::Valid(update.clone());
            }
            // updating a valid value with something requires a folding function
            (this @ Self::Valid(_), Some(update)) => {
                let mut base = Self::Invalid;
                std::mem::swap(this, &mut base);
                let Self::Valid(base) = base else {unreachable!()};
                *this = match f(base, update) {
                    Some(val) => Self::Valid(val),
                    None => Self::Invalid,
                };
            }
        }
    }

    fn collapse(self) -> Option<ScalarValue> {
        match self {
            Self::Invalid | Self::Uninit => None,
            Self::Valid(val) => Some(val),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_df_stats_agg_no_cols_no_updates() {
        let schema = Schema::new(Vec::<Field>::new());
        let agg = DFStatsAggregator::new(&schema);

        let actual = agg.build();
        let expected = DFStatistics {
            num_rows: Some(0),
            total_byte_size: Some(0),
            column_statistics: Some(vec![]),
            is_exact: true,
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
            num_rows: Some(0),
            total_byte_size: Some(0),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
            ]),
            is_exact: true,
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
            num_rows: Some(1),
            total_byte_size: Some(10),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(100),
                    max_value: Some(ScalarValue::UInt64(Some(100))),
                    min_value: Some(ScalarValue::UInt64(Some(50))),
                    distinct_count: Some(42),
                },
                ColumnStatistics {
                    null_count: Some(1_000),
                    max_value: Some(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Some(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Some(42),
                },
            ]),
            is_exact: true,
        };
        agg.update(&update_stats, &update_schema);

        let update_schema = Schema::new(vec![Field::new("col2", DataType::Utf8, false)]);
        let update_stats = DFStatistics {
            num_rows: Some(10_000),
            total_byte_size: Some(100_000),
            column_statistics: Some(vec![ColumnStatistics {
                null_count: Some(1_000_000),
                max_value: Some(ScalarValue::Utf8(Some("g".to_owned()))),
                min_value: Some(ScalarValue::Utf8(Some("c".to_owned()))),
                distinct_count: Some(42),
            }]),
            is_exact: true,
        };
        agg.update(&update_stats, &update_schema);

        let actual = agg.build();
        let expected = DFStatistics {
            num_rows: Some(10_001),
            total_byte_size: Some(100_010),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: None,
                    max_value: Some(ScalarValue::UInt64(Some(100))),
                    min_value: Some(ScalarValue::UInt64(Some(50))),
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(1_001_000),
                    max_value: Some(ScalarValue::Utf8(Some("g".to_owned()))),
                    min_value: Some(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: None,
                },
            ]),
            is_exact: true,
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
            num_rows: Some(1),
            total_byte_size: Some(10),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(100),
                    max_value: Some(ScalarValue::UInt64(Some(100))),
                    min_value: Some(ScalarValue::UInt64(Some(50))),
                    distinct_count: Some(42),
                },
                ColumnStatistics {
                    null_count: Some(1_000),
                    max_value: Some(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Some(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Some(42),
                },
            ]),
            is_exact: true,
        };
        agg.update(&update_stats, &update_schema);

        let update_schema = Schema::new(vec![
            Field::new("col2", DataType::Utf8, false),
            Field::new("col1", DataType::UInt64, true),
        ]);
        let update_stats = DFStatistics {
            num_rows: Some(10_000),
            total_byte_size: Some(100_000),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(1_000_000),
                    max_value: Some(ScalarValue::Utf8(Some("g".to_owned()))),
                    min_value: Some(ScalarValue::Utf8(Some("c".to_owned()))),
                    distinct_count: Some(42),
                },
                ColumnStatistics {
                    null_count: Some(10_000_000),
                    max_value: Some(ScalarValue::UInt64(Some(99))),
                    min_value: Some(ScalarValue::UInt64(Some(40))),
                    distinct_count: Some(42),
                },
            ]),
            is_exact: true,
        };
        agg.update(&update_stats, &update_schema);

        let actual = agg.build();
        let expected = DFStatistics {
            num_rows: Some(10_001),
            total_byte_size: Some(100_010),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(10_000_100),
                    max_value: Some(ScalarValue::UInt64(Some(100))),
                    min_value: Some(ScalarValue::UInt64(Some(40))),
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(1_001_000),
                    max_value: Some(ScalarValue::Utf8(Some("g".to_owned()))),
                    min_value: Some(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: None,
                },
            ]),
            is_exact: true,
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
            num_rows: Some(1),
            total_byte_size: Some(10),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(100),
                    max_value: Some(ScalarValue::UInt64(Some(100))),
                    min_value: Some(ScalarValue::UInt64(Some(50))),
                    distinct_count: Some(42),
                },
                ColumnStatistics {
                    null_count: Some(1_000),
                    max_value: Some(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Some(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Some(42),
                },
            ]),
            is_exact: true,
        };
        agg.update(&update_stats, &update_schema);

        let actual = agg.build();
        let expected = DFStatistics {
            num_rows: Some(1),
            total_byte_size: Some(10),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(100),
                    max_value: Some(ScalarValue::UInt64(Some(100))),
                    min_value: Some(ScalarValue::UInt64(Some(50))),
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: None,
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
            ]),
            is_exact: true,
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
            num_rows: Some(1),
            total_byte_size: Some(10),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(100),
                    max_value: Some(ScalarValue::UInt64(Some(100))),
                    min_value: Some(ScalarValue::UInt64(Some(50))),
                    distinct_count: Some(42),
                },
                ColumnStatistics {
                    null_count: Some(1_000),
                    max_value: Some(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Some(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Some(42),
                },
            ]),
            is_exact: true,
        };
        let agg_stats = DFStatistics {
            num_rows: Some(2),
            total_byte_size: Some(20),
            column_statistics: Some(vec![
                ColumnStatistics {
                    null_count: Some(200),
                    max_value: Some(ScalarValue::UInt64(Some(100))),
                    min_value: Some(ScalarValue::UInt64(Some(50))),
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(2_000),
                    max_value: Some(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Some(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: None,
                },
            ]),
            is_exact: true,
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
            IsExact,
        }

        impl Mode {
            fn mask(&self, mut stats: DFStatistics) -> DFStatistics {
                match self {
                    Self::NumRows => {
                        stats.num_rows = None;
                    }
                    Self::TotalByteSize => {
                        stats.total_byte_size = None;
                    }
                    Self::ColumnStatistics => {
                        stats.column_statistics = None;
                    }
                    Self::Col(idx, mode) => {
                        if let Some(stats) = stats.column_statistics.as_mut() {
                            let stats = &mut stats[*idx];

                            match mode {
                                ColMode::NullCount => {
                                    stats.null_count = None;
                                }
                                ColMode::MaxValue => {
                                    stats.max_value = None;
                                }
                                ColMode::MinValue => {
                                    stats.min_value = None;
                                }
                            }
                        }
                    }
                    Self::IsExact => {
                        stats.is_exact = false;
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
            Mode::IsExact,
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
            num_rows: Some(1),
            total_byte_size: Some(10),
            column_statistics: Some(vec![]),
            is_exact: true,
        };
        agg.update(&update_stats, &update_schema);
    }
}
