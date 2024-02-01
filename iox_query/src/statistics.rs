//! Code to translate IOx statistics to DataFusion statistics

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use arrow::compute::rank;
use arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::datasource::physical_plan::ParquetExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{visit_execution_plan, ExecutionPlan, ExecutionPlanVisitor};
use datafusion::{
    physical_plan::{ColumnStatistics, Statistics as DFStatistics},
    scalar::ScalarValue,
};
use observability_deps::tracing::trace;

use crate::provider::{DeduplicateExec, RecordBatchesExec};
use crate::{QueryChunk, CHUNK_ORDER_COLUMN_NAME};

/// Aggregates DataFusion [statistics](DFStatistics).
#[derive(Debug)]
pub struct DFStatsAggregator<'a> {
    num_rows: Precision<usize>,
    total_byte_size: Precision<usize>,
    column_statistics: Vec<DFStatsAggregatorCol>,
    // Maps column name to index in column_statistics for all columns we are
    // aggregating
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
    pub fn update(&mut self, update_stats: &DFStatistics, update_schema: &Schema) {
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
    pub fn build(self) -> DFStatistics {
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

/// build DF statitics for given chunks and a schema
pub fn build_statistics_for_chunks(
    chunks: &[Arc<dyn QueryChunk>],
    schema: SchemaRef,
) -> DFStatistics {
    let chunk_order_field = schema.field_with_name(CHUNK_ORDER_COLUMN_NAME).ok();
    let chunk_order_only_schema = chunk_order_field.map(|field| Schema::new(vec![field.clone()]));

    let chunks: Vec<_> = chunks.iter().collect();

    let statistics = chunks
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
                        }],
                    },
                    schema,
                );
            }

            agg
        })
        .build();

    statistics
}

/// Traverse the execution plan and build statistics min max for the given column
pub fn compute_stats_column_min_max(
    plan: &dyn ExecutionPlan,
    column_name: &str,
) -> Result<ColumnStatistics, DataFusionError> {
    let mut visitor = StatisticsVisitor::new(column_name);
    visit_execution_plan(plan, &mut visitor)?;

    // there must be only one statistics left in the stack
    if visitor.statistics.len() != 1 {
        return Err(DataFusionError::Internal(format!(
            "There must be only one statistics left in the stack, but find {}",
            visitor.statistics.len()
        )));
    }

    Ok(visitor.statistics.pop_back().unwrap())
}

/// Traverse the physical plan and build statistics min max for the given column each node
/// Note: This is a temproray solution until DF's statistics is more mature
/// <https://github.com/apache/arrow-datafusion/issues/8078>
struct StatisticsVisitor<'a> {
    column_name: &'a str, //String,  // todo: not sure enough
    statistics: VecDeque<ColumnStatistics>,
}

impl<'a> StatisticsVisitor<'a> {
    fn new(column_name: &'a str) -> Self {
        Self {
            column_name,
            statistics: VecDeque::new(),
        }
    }
}

impl ExecutionPlanVisitor for StatisticsVisitor<'_> {
    type Error = DataFusionError;

    fn pre_visit(&mut self, _plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        Ok(false)
    }

    fn post_visit(&mut self, plan: &dyn ExecutionPlan) -> Result<bool, Self::Error> {
        // If this is an EmptyExec / PlaceholderRowExec, we don't know about it
        if plan.as_any().downcast_ref::<EmptyExec>().is_some()
            || plan.as_any().downcast_ref::<PlaceholderRowExec>().is_some()
        {
            self.statistics.push_back(ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Absent,
                min_value: Precision::Absent,
                distinct_count: Precision::Absent,
            });
        }
        // If this is leaf node (ParquetExec or RecordBatchExec), compute its statistics and push it to the stack
        else if plan.as_any().downcast_ref::<ParquetExec>().is_some()
            || plan.as_any().downcast_ref::<RecordBatchesExec>().is_some()
        {
            // get index of the given column in the schema
            let statistics = match plan.schema().index_of(self.column_name) {
                Ok(col_index) => plan.statistics()?.column_statistics[col_index].clone(),
                // This is the case of alias, do not optimize by returning no statistics
                Err(_) => {
                    trace!(
                        " ------------------- No statistics for column {} in PQ/RB",
                        self.column_name
                    );
                    ColumnStatistics {
                        null_count: Precision::Absent,
                        max_value: Precision::Absent,
                        min_value: Precision::Absent,
                        distinct_count: Precision::Absent,
                    }
                }
            };
            self.statistics.push_back(statistics);
        }
        // Non leaf node
        else {
            // These are cases the stats will be unioned of their children's
            //   Sort, Dediplicate, Filter, Repartition, Union, SortPreservingMerge, CoalesceBatches
            let union_stats = if plan.as_any().downcast_ref::<SortExec>().is_some()
                || plan.as_any().downcast_ref::<DeduplicateExec>().is_some()
                || plan.as_any().downcast_ref::<FilterExec>().is_some()
                || plan.as_any().downcast_ref::<RepartitionExec>().is_some()
                || plan.as_any().downcast_ref::<UnionExec>().is_some()
                || plan
                    .as_any()
                    .downcast_ref::<SortPreservingMergeExec>()
                    .is_some()
                || plan
                    .as_any()
                    .downcast_ref::<CoalesceBatchesExec>()
                    .is_some()
            {
                true
            } else if plan.as_any().downcast_ref::<ProjectionExec>().is_some() {
                // ProjectionExec is a special case. Only union stats if it includes pure columns
                projection_includes_pure_columns(
                    plan.as_any().downcast_ref::<ProjectionExec>().unwrap(),
                )
            } else {
                false
            };

            // pop statistics of all inputs from the stack
            let num_inputs = plan.children().len();
            // num_input must > 0. Pop the first one
            let mut statistics = self
                .statistics
                .pop_back()
                .expect("No statistics for input plan");
            // pop the rest and update the min and max
            for _ in 1..num_inputs {
                let input_statistics = self
                    .statistics
                    .pop_back()
                    .expect("No statistics for input plan");

                if union_stats {
                    // Convervatively union min max
                    statistics.null_count = statistics.null_count.add(&input_statistics.null_count);
                    statistics.max_value = statistics.max_value.max(&input_statistics.max_value);
                    statistics.min_value = statistics.min_value.min(&input_statistics.min_value);
                    statistics.distinct_count = Precision::Absent;
                };
            }

            if union_stats {
                self.statistics.push_back(statistics);
            } else {
                trace!(
                    " ------ No statistics for column {} in non-leaf node",
                    self.column_name
                );
                // Make them absent for other cases
                self.statistics.push_back(ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    distinct_count: Precision::Absent,
                });
            }
        }

        Ok(true)
    }
}

fn projection_includes_pure_columns(projection: &ProjectionExec) -> bool {
    projection
        .expr()
        .iter()
        .all(|(expr, _col_name)| expr.as_any().downcast_ref::<Column>().is_some())
}

/// Return min max of a ColumnStatistics with precise values
pub fn column_statistics_min_max(
    column_statistics: &ColumnStatistics,
) -> Option<(ScalarValue, ScalarValue)> {
    match (&column_statistics.min_value, &column_statistics.max_value) {
        (Precision::Exact(min), Precision::Exact(max)) => Some((min.clone(), max.clone())),
        // the statistics values are absent or imprecise
        _ => None,
    }
}

/// Get statsistics min max of given column name on given plans
/// Return None if one of the inputs does not have statistics or  does not include the column
pub fn statistics_min_max(
    plans: &[Arc<dyn ExecutionPlan>],
    column_name: &str,
) -> Option<Vec<(ScalarValue, ScalarValue)>> {
    // Get statistics for each plan
    let plans_schema_and_stats = plans
        .iter()
        .map(|plan| Ok((Arc::clone(plan), plan.schema(), plan.statistics()?)))
        .collect::<Result<Vec<_>, DataFusionError>>();

    // If any without statistics, return none
    let Ok(plans_schema_and_stats) = plans_schema_and_stats else {
        return None;
    };

    // get value range of the sorted column for each input
    let mut min_max_ranges = Vec::with_capacity(plans_schema_and_stats.len());
    for (input, input_schema, input_stats) in plans_schema_and_stats {
        // get index of the sorted column in the schema
        let Ok(sorted_col_index) = input_schema.index_of(column_name) else {
            // panic that the sorted column is not in the schema
            panic!("sorted column {} is not in the schema", column_name);
        };

        let column_stats = input_stats.column_statistics;
        let sorted_col_stats = column_stats[sorted_col_index].clone();
        match (sorted_col_stats.min_value, sorted_col_stats.max_value) {
            (Precision::Exact(min), Precision::Exact(max)) => {
                min_max_ranges.push((min, max));
            }
            // WARNING: this may produce incorrect results until we use more precision
            // as `Inexact` is not guaranteed to cover the actual min and max values
            // https://github.com/apache/arrow-datafusion/issues/8078
            (Precision::Inexact(min), Precision::Inexact(max)) => {
                if let Some(_deduplicate_exec) = input.as_any().downcast_ref::<DeduplicateExec>() {
                    min_max_ranges.push((min, max));
                } else {
                    return None;
                };
            }
            // the statistics  values are absent
            _ => return None,
        }
    }

    Some(min_max_ranges)
}

/// Return true if at least 2 min_max ranges in the given array overlap
pub fn overlap(value_ranges: &[(ScalarValue, ScalarValue)]) -> Result<bool, DataFusionError> {
    // interleave min and max into one iterator
    let value_ranges_iter = value_ranges.iter().flat_map(|(min, max)| {
        // panics if min > max
        if min > max {
            panic!("min ({:?}) > max ({:?})", min, max);
        }
        vec![min.clone(), max.clone()]
    });

    let value_ranges = ScalarValue::iter_to_array(value_ranges_iter)?;

    // rank it
    let ranks = rank(&*value_ranges, None)?;

    // check overlap by checking if the max is rank right behind its corresponding min
    //  . non-overlap example: values of min-max pairs [3, 5,   9, 12,   1, 1,   6, 8]
    //     ranks:  [3, 4,   7, 8,  2, 2,  5, 6] : max (even index) = its correspnding min (odd index) for same min max OR min + 1
    //  . overlap example:  [3, 5,   9, 12,   1, 1,   4, 6] : pair [3, 5] interleaves with pair [4, 6]
    //     ranks:  [3, 5,   7, 8,  2, 2,  4, 6]
    for i in (0..ranks.len()).step_by(2) {
        if !((ranks[i] == ranks[i + 1]) || (ranks[i + 1] == ranks[i] + 1)) {
            return Ok(true);
        }
    }

    Ok(false)
}

#[cfg(test)]
mod test {
    use crate::{
        provider::chunks_to_physical_nodes,
        test::{format_execution_plan, TestChunk},
    };

    use super::*;
    use arrow::datatypes::{DataType, Field};
    use datafusion::{common::Statistics, error::DataFusionError};
    use itertools::Itertools;
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
                },
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    distinct_count: Precision::Absent,
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
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Exact(42),
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
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_001_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Absent,
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
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Exact(42),
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
                },
                ColumnStatistics {
                    null_count: Precision::Exact(10_000_000),
                    max_value: Precision::Exact(ScalarValue::UInt64(Some(99))),
                    min_value: Precision::Exact(ScalarValue::UInt64(Some(40))),
                    distinct_count: Precision::Exact(42),
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
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_001_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("g".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Absent,
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
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Exact(42),
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
                },
                ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Absent,
                    min_value: Precision::Absent,
                    distinct_count: Precision::Absent,
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
                },
                ColumnStatistics {
                    null_count: Precision::Exact(1_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Exact(42),
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
                },
                ColumnStatistics {
                    null_count: Precision::Exact(2_000),
                    max_value: Precision::Exact(ScalarValue::Utf8(Some("e".to_owned()))),
                    min_value: Precision::Exact(ScalarValue::Utf8(Some("b".to_owned()))),
                    distinct_count: Precision::Absent,
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
                max_value: Precision::Exact(ScalarValue::Utf8(Some("MT".to_string()))),
                min_value: Precision::Exact(ScalarValue::Utf8(Some("AL".to_string()))),
                distinct_count: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(100))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                distinct_count: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(20), None)),
                min_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(10), None)),
                distinct_count: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(6))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                distinct_count: Precision::Absent,
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
                max_value: Precision::Exact(ScalarValue::Utf8(Some("WA".to_string()))),
                min_value: Precision::Exact(ScalarValue::Utf8(Some("AL".to_string()))),
                distinct_count: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(100))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                distinct_count: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(80), None)),
                min_value: Precision::Exact(ScalarValue::TimestampNanosecond(Some(10), None)),
                distinct_count: Precision::Absent,
            },
            ColumnStatistics {
                null_count: Precision::Absent,
                max_value: Precision::Exact(ScalarValue::Int64(Some(15))),
                min_value: Precision::Exact(ScalarValue::Int64(Some(0))),
                distinct_count: Precision::Absent,
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

    #[test]
    fn test_compute_statistics_min_max() {
        // schema with one tag, one field, time and CHUNK_ORDER_COLUMN_NAME
        let schema: SchemaRef = SchemaBuilder::new()
            .tag("tag")
            .influx_field("float_field", InfluxFieldType::Float)
            .influx_field("int_field", InfluxFieldType::Integer)
            .influx_field("string_field", InfluxFieldType::String)
            .tag("tag_no_val") // no chunks have values for this
            .influx_field("field_no_val", InfluxFieldType::Integer)
            .timestamp()
            .build()
            .unwrap()
            .into();

        let parquet_chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(10), Some(100))
                .with_tag_column_with_stats("tag", Some("MA"), Some("VT"))
                .with_f64_field_column_with_stats("float_field", Some(10.1), Some(100.4))
                .with_i64_field_column_with_stats("int_field", Some(30), Some(50))
                .with_string_field_column_with_stats("string_field", Some("orange"), Some("plum"))
                // only this chunk has value for this field
                .with_i64_field_column_with_stats("field_no_val", Some(30), Some(50))
                .with_dummy_parquet_file(),
        ) as Arc<dyn QueryChunk>;

        let record_batch_chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(20), Some(200))
                .with_tag_column_with_stats("tag", Some("Boston"), Some("DC"))
                .with_f64_field_column_with_stats("float_field", Some(15.6), Some(30.0))
                .with_i64_field_column_with_stats("int_field", Some(1), Some(50))
                .with_string_field_column_with_stats("string_field", Some("banana"), Some("plum")),
        ) as Arc<dyn QueryChunk>;

        let plan_pq = chunks_to_physical_nodes(&schema, None, vec![parquet_chunk], 1);
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan_pq),
            @r###"
        ---
        - " UnionExec"
        - "   ParquetExec: file_groups={1 group: [[0.parquet]]}, projection=[tag, float_field, int_field, string_field, tag_no_val, field_no_val, time]"
        "###
        );

        let plan_rb = chunks_to_physical_nodes(&schema, None, vec![record_batch_chunk], 1);
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan_rb),
            @r###"
        ---
        - " UnionExec"
        - "   RecordBatchesExec: chunks=1, projection=[tag, float_field, int_field, string_field, tag_no_val, field_no_val, time]"
        "###
        );

        // Stats for time
        // parquet
        let time_stats = compute_stats_column_min_max(&*plan_pq, "time").unwrap();
        let min_max = column_statistics_min_max(&time_stats).unwrap();
        let expected_time_stats = (
            ScalarValue::TimestampNanosecond(Some(10), None),
            ScalarValue::TimestampNanosecond(Some(100), None),
        );
        assert_eq!(min_max, expected_time_stats);
        // record batch
        let time_stats = compute_stats_column_min_max(&*plan_rb, "time").unwrap();
        let min_max = column_statistics_min_max(&time_stats).unwrap();
        let expected_time_stats = (
            ScalarValue::TimestampNanosecond(Some(20), None),
            ScalarValue::TimestampNanosecond(Some(200), None),
        );
        assert_eq!(min_max, expected_time_stats);

        // Stats for tag
        // parquet
        let tag_stats = compute_stats_column_min_max(&*plan_pq, "tag").unwrap();
        let min_max = column_statistics_min_max(&tag_stats).unwrap();
        let expected_tag_stats = (
            ScalarValue::Utf8(Some("MA".to_string())),
            ScalarValue::Utf8(Some("VT".to_string())),
        );
        assert_eq!(min_max, expected_tag_stats);
        // record batch
        let tag_stats = compute_stats_column_min_max(&*plan_rb, "tag").unwrap();
        let min_max = column_statistics_min_max(&tag_stats).unwrap();
        let expected_tag_stats = (
            ScalarValue::Utf8(Some("Boston".to_string())),
            ScalarValue::Utf8(Some("DC".to_string())),
        );
        assert_eq!(min_max, expected_tag_stats);

        // Stats for field
        // parquet
        let float_stats = compute_stats_column_min_max(&*plan_pq, "float_field").unwrap();
        let min_max = column_statistics_min_max(&float_stats).unwrap();
        let expected_float_stats = (
            ScalarValue::Float64(Some(10.1)),
            ScalarValue::Float64(Some(100.4)),
        );
        assert_eq!(min_max, expected_float_stats);
        // record batch
        let float_stats = compute_stats_column_min_max(&*plan_rb, "float_field").unwrap();
        let min_max = column_statistics_min_max(&float_stats).unwrap();
        let expected_float_stats = (
            ScalarValue::Float64(Some(15.6)),
            ScalarValue::Float64(Some(30.0)),
        );
        assert_eq!(min_max, expected_float_stats);

        // Stats for int
        // parquet
        let int_stats = compute_stats_column_min_max(&*plan_pq, "int_field").unwrap();
        let min_max = column_statistics_min_max(&int_stats).unwrap();
        let expected_int_stats = (ScalarValue::Int64(Some(30)), ScalarValue::Int64(Some(50)));
        assert_eq!(min_max, expected_int_stats);
        // record batch
        let int_stats = compute_stats_column_min_max(&*plan_rb, "int_field").unwrap();
        let min_max = column_statistics_min_max(&int_stats).unwrap();
        let expected_int_stats = (ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(50)));
        assert_eq!(min_max, expected_int_stats);

        // Stats for string
        // parquet
        let string_stats = compute_stats_column_min_max(&*plan_pq, "string_field").unwrap();
        let min_max = column_statistics_min_max(&string_stats).unwrap();
        let expected_string_stats = (
            ScalarValue::Utf8(Some("orange".to_string())),
            ScalarValue::Utf8(Some("plum".to_string())),
        );
        assert_eq!(min_max, expected_string_stats);
        // record batch
        let string_stats = compute_stats_column_min_max(&*plan_rb, "string_field").unwrap();
        let min_max = column_statistics_min_max(&string_stats).unwrap();
        let expected_string_stats = (
            ScalarValue::Utf8(Some("banana".to_string())),
            ScalarValue::Utf8(Some("plum".to_string())),
        );
        assert_eq!(min_max, expected_string_stats);

        // no tats on parquet
        let tag_no_stats = compute_stats_column_min_max(&*plan_pq, "tag_no_val").unwrap();
        let min_max = column_statistics_min_max(&tag_no_stats);
        assert!(min_max.is_none());

        // no stats on record batch
        let field_no_stats = compute_stats_column_min_max(&*plan_rb, "field_no_val").unwrap();
        let min_max = column_statistics_min_max(&field_no_stats);
        assert!(min_max.is_none());
    }

    #[test]
    fn test_statistics_min_max() {
        // schema with one tag, one field, time and CHUNK_ORDER_COLUMN_NAME
        let schema: SchemaRef = SchemaBuilder::new()
            .tag("tag")
            .influx_field("float_field", InfluxFieldType::Float)
            .influx_field("int_field", InfluxFieldType::Integer)
            .influx_field("string_field", InfluxFieldType::String)
            .tag("tag_no_val") // no chunks have values for this
            .influx_field("field_no_val", InfluxFieldType::Integer)
            .timestamp()
            .build()
            .unwrap()
            .into();

        let parquet_chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(10), Some(100))
                .with_tag_column_with_stats("tag", Some("MA"), Some("VT"))
                .with_f64_field_column_with_stats("float_field", Some(10.1), Some(100.4))
                .with_i64_field_column_with_stats("int_field", Some(30), Some(50))
                .with_string_field_column_with_stats("string_field", Some("orange"), Some("plum"))
                // only this chunk has value for this field
                .with_i64_field_column_with_stats("field_no_val", Some(30), Some(50))
                .with_dummy_parquet_file(),
        ) as Arc<dyn QueryChunk>;

        let record_batch_chunk = Arc::new(
            TestChunk::new("t")
                .with_time_column_with_stats(Some(20), Some(200))
                .with_tag_column_with_stats("tag", Some("Boston"), Some("DC"))
                .with_f64_field_column_with_stats("float_field", Some(15.6), Some(30.0))
                .with_i64_field_column_with_stats("int_field", Some(1), Some(50))
                .with_string_field_column_with_stats("string_field", Some("banana"), Some("plum")),
        ) as Arc<dyn QueryChunk>;

        let plan1 = chunks_to_physical_nodes(&schema, None, vec![parquet_chunk], 1);
        let plan2 = chunks_to_physical_nodes(&schema, None, vec![record_batch_chunk], 1);

        let time_stats =
            statistics_min_max(&[Arc::clone(&plan1), Arc::clone(&plan2)], "time").unwrap();
        let expected_time_stats = [
            (
                ScalarValue::TimestampNanosecond(Some(10), None),
                ScalarValue::TimestampNanosecond(Some(100), None),
            ),
            (
                ScalarValue::TimestampNanosecond(Some(20), None),
                ScalarValue::TimestampNanosecond(Some(200), None),
            ),
        ];
        assert_eq!(time_stats, expected_time_stats);

        let tag_stats =
            statistics_min_max(&[Arc::clone(&plan1), Arc::clone(&plan2)], "tag").unwrap();
        let expected_tag_stats = [
            (
                ScalarValue::Utf8(Some("MA".to_string())),
                ScalarValue::Utf8(Some("VT".to_string())),
            ),
            (
                ScalarValue::Utf8(Some("Boston".to_string())),
                ScalarValue::Utf8(Some("DC".to_string())),
            ),
        ];
        assert_eq!(tag_stats, expected_tag_stats);

        let float_stats =
            statistics_min_max(&[Arc::clone(&plan1), Arc::clone(&plan2)], "float_field").unwrap();
        let expected_float_stats = [
            (
                ScalarValue::Float64(Some(10.1)),
                ScalarValue::Float64(Some(100.4)),
            ),
            (
                ScalarValue::Float64(Some(15.6)),
                ScalarValue::Float64(Some(30.0)),
            ),
        ];
        assert_eq!(float_stats, expected_float_stats);

        let int_stats =
            statistics_min_max(&[Arc::clone(&plan1), Arc::clone(&plan2)], "int_field").unwrap();
        let expected_int_stats = [
            (ScalarValue::Int64(Some(30)), ScalarValue::Int64(Some(50))),
            (ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(50))),
        ];
        assert_eq!(int_stats, expected_int_stats);

        let string_stats =
            statistics_min_max(&[Arc::clone(&plan1), Arc::clone(&plan2)], "string_field").unwrap();
        let expected_string_stats = [
            (
                ScalarValue::Utf8(Some("orange".to_string())),
                ScalarValue::Utf8(Some("plum".to_string())),
            ),
            (
                ScalarValue::Utf8(Some("banana".to_string())),
                ScalarValue::Utf8(Some("plum".to_string())),
            ),
        ];
        assert_eq!(string_stats, expected_string_stats);

        let tag_no_stat =
            statistics_min_max(&[Arc::clone(&plan1), Arc::clone(&plan2)], "tag_no_val");
        assert!(tag_no_stat.is_none());

        let field_no_stat =
            statistics_min_max(&[Arc::clone(&plan1), Arc::clone(&plan2)], "field_no_val");
        assert!(field_no_stat.is_none());
    }

    #[test]
    fn test_non_overlap_time() {
        let pair_1 = (
            ScalarValue::TimestampNanosecond(Some(10), None),
            ScalarValue::TimestampNanosecond(Some(20), None),
        );
        let pair_2 = (
            ScalarValue::TimestampNanosecond(Some(100), None),
            ScalarValue::TimestampNanosecond(Some(150), None),
        );
        let pair_3 = (
            ScalarValue::TimestampNanosecond(Some(60), None),
            ScalarValue::TimestampNanosecond(Some(65), None),
        );

        let overlap = overlap_all(&vec![pair_1, pair_2, pair_3]).unwrap();
        assert!(!overlap);
    }

    #[test]
    fn test_overlap_time() {
        let pair_1 = (
            ScalarValue::TimestampNanosecond(Some(10), None),
            ScalarValue::TimestampNanosecond(Some(20), None),
        );
        let pair_2 = (
            ScalarValue::TimestampNanosecond(Some(100), None),
            ScalarValue::TimestampNanosecond(Some(150), None),
        );
        let pair_3 = (
            ScalarValue::TimestampNanosecond(Some(8), None),
            ScalarValue::TimestampNanosecond(Some(10), None),
        );

        let overlap = overlap_all(&vec![pair_1, pair_2, pair_3]).unwrap();
        assert!(overlap);
    }

    #[test]
    fn test_non_overlap_integer() {
        // [3, 5,   9, 12,   1, 1,   6, 8]
        let pair_1 = (ScalarValue::Int16(Some(3)), ScalarValue::Int16(Some(5)));
        let pair_2 = (ScalarValue::Int16(Some(9)), ScalarValue::Int16(Some(12)));
        let pair_3 = (ScalarValue::Int16(Some(1)), ScalarValue::Int16(Some(1)));
        let pair_4 = (ScalarValue::Int16(Some(6)), ScalarValue::Int16(Some(8)));

        let overlap = overlap_all(&vec![pair_1, pair_2, pair_3, pair_4]).unwrap();
        assert!(!overlap);
    }

    #[test]
    fn test_overlap_integer() {
        // [3, 5,   9, 12,   1, 1,   4, 6]
        let pair_1 = (ScalarValue::Int16(Some(3)), ScalarValue::Int16(Some(5)));
        let pair_2 = (ScalarValue::Int16(Some(9)), ScalarValue::Int16(Some(12)));
        let pair_3 = (ScalarValue::Int16(Some(1)), ScalarValue::Int16(Some(1)));
        let pair_4 = (ScalarValue::Int16(Some(4)), ScalarValue::Int16(Some(6)));

        let overlap = overlap_all(&vec![pair_1, pair_2, pair_3, pair_4]).unwrap();
        assert!(overlap);
    }

    #[test]
    fn test_non_overlap_integer_ascending_null_first() {
        // [3, 5,   null, null,   1, 1,   6, 8]
        let pair_1 = (ScalarValue::Int16(Some(3)), ScalarValue::Int16(Some(5)));
        let pair_2 = (ScalarValue::Int16(None), ScalarValue::Int16(None));
        let pair_3 = (ScalarValue::Int16(Some(1)), ScalarValue::Int16(Some(2)));
        let pair_4 = (ScalarValue::Int16(Some(6)), ScalarValue::Int16(Some(8)));

        let overlap = overlap_all(&vec![pair_1, pair_2, pair_3, pair_4]).unwrap();
        assert!(!overlap);
    }

    #[test]
    fn test_overlap_integer_ascending_null_first() {
        // [3, 5,   null, null,   1, 1,   4, 6]
        let pair_1 = (ScalarValue::Int16(Some(3)), ScalarValue::Int16(Some(5)));
        let pair_2 = (ScalarValue::Int16(None), ScalarValue::Int16(None));
        let pair_3 = (ScalarValue::Int16(Some(1)), ScalarValue::Int16(Some(2)));
        let pair_4 = (ScalarValue::Int16(Some(4)), ScalarValue::Int16(Some(6)));

        let overlap = overlap_all(&vec![pair_1, pair_2, pair_3, pair_4]).unwrap();
        assert!(overlap);
    }

    #[test]
    fn test_non_overlap_string_ascending_null_first() {
        // ['e', 'h',   null, null,   'a', 'a',   'k', 'q']
        let pair_1 = (
            ScalarValue::Utf8(Some('e'.to_string())),
            ScalarValue::Utf8(Some('h'.to_string())),
        );
        let pair_2 = (ScalarValue::Utf8(None), ScalarValue::Utf8(None));
        let pair_3 = (
            ScalarValue::Utf8(Some('a'.to_string())),
            ScalarValue::Utf8(Some('a'.to_string())),
        );

        let overlap = overlap_all(&vec![pair_1, pair_2, pair_3]).unwrap();
        assert!(!overlap);
    }

    #[test]
    fn test_overlap_string_ascending_null_first() {
        // ['e', 'h',   null, null,   'a', 'f',   'k', 'q']
        let pair_1 = (
            ScalarValue::Utf8(Some('e'.to_string())),
            ScalarValue::Utf8(Some('h'.to_string())),
        );
        let pair_2 = (ScalarValue::Utf8(None), ScalarValue::Utf8(None));
        let pair_3 = (
            ScalarValue::Utf8(Some('a'.to_string())),
            ScalarValue::Utf8(Some('f'.to_string())),
        );

        let overlap = overlap_all(&vec![pair_1, pair_2, pair_3]).unwrap();
        assert!(overlap);
    }

    #[test]
    #[should_panic(expected = "Internal(\"Empty iterator passed to ScalarValue::iter_to_array\")")]
    fn test_overlap_empty() {
        let _overlap = overlap_all(&[]);
    }

    #[should_panic(expected = "min (Int16(3)) > max (Int16(2))")]
    #[test]
    fn test_overlap_panic() {
        // max < min
        let pair_1 = (ScalarValue::Int16(Some(3)), ScalarValue::Int16(Some(2)));
        let _overlap = overlap_all(&[pair_1]);
    }

    /// Runs `overlap` on all permutations of the given `value_range`es and asserts that the result is
    /// the same. Returns that result
    fn overlap_all(value_ranges: &[(ScalarValue, ScalarValue)]) -> Result<bool, DataFusionError> {
        let n = value_ranges.len();

        let mut overlaps_all_permutations = value_ranges
            .iter()
            .cloned()
            .permutations(n)
            .map(|v| overlap(&v));

        let Some(first) = overlaps_all_permutations.next() else {
            return overlap(value_ranges);
        };

        let first = first.unwrap();

        for result in overlaps_all_permutations {
            assert_eq!(&result.unwrap(), &first);
        }

        Ok(first)
    }
}
