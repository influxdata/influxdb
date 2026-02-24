//! Code to translate IOx statistics to DataFusion statistics
//! for a given DataFusion plan

use std::collections::VecDeque;
#[cfg(test)]
use std::sync::Arc;

use datafusion::common::stats::Precision;
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanVisitor, visit_execution_plan};
use datafusion::{physical_plan::ColumnStatistics, scalar::ScalarValue};
use tracing::trace;

use crate::provider::{DeduplicateExec, RecordBatchesExec};

use super::stats_utils::projection_includes_pure_columns;

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

    Ok(visitor.result().unwrap())
}

/// Traverse the physical plan and find the min max for the given column each node
///
/// Note on correctness: The statistics may not be exact, but the values in the plan
/// are guaranteed to be within the min/max.
///
/// Specifically, the values must fall within the range, but the range
/// itself may be larger than the actual values.
///
/// At the time of writing, DataFusion statistics can't distinguish "inexact"
/// from "known to be within the range" so this is somewhat of a temporary
/// solution until DF's statistics is more mature
///
/// See
/// <https://github.com/apache/arrow-datafusion/issues/8078>
pub(crate) struct StatisticsVisitor<'a> {
    column_name: &'a str, //String,  // todo: not sure enough
    statistics: VecDeque<ColumnStatistics>,
}

impl<'a> StatisticsVisitor<'a> {
    pub(crate) fn new(column_name: &'a str) -> Self {
        Self {
            column_name,
            statistics: VecDeque::new(),
        }
    }

    /// DataFusion statistics can't distinguish "inexact" from "known to be within the range"
    ///
    /// Converts inexact values to exact if possible for the purposes of
    /// analysis by [`StatisticsVisitor`]
    fn convert_min_max_to_exact(mut column_statistics: ColumnStatistics) -> ColumnStatistics {
        column_statistics.min_value = Self::to_exact(column_statistics.min_value);
        column_statistics.max_value = Self::to_exact(column_statistics.max_value);
        column_statistics
    }

    /// Convert [`Precision::Inexact`] to [`Precision::Exact`] if possible
    fn to_exact(v: Precision<ScalarValue>) -> Precision<ScalarValue> {
        match v {
            Precision::Exact(v) | Precision::Inexact(v) => Precision::Exact(v),
            Precision::Absent => Precision::Absent,
        }
    }

    /// Consumes and returns the result.
    pub(crate) fn result(mut self) -> Option<ColumnStatistics> {
        self.statistics.pop_back()
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
            self.statistics.push_back(ColumnStatistics::new_unknown());
        }
        // DataSourceExec (leaf): compute its statistics and push it to the stack
        else if plan.as_any().downcast_ref::<DataSourceExec>().is_some() {
            // get index of the given column in the schema
            let statistics = match plan.schema().index_of(self.column_name) {
                Ok(col_index) => {
                    let col_stats = plan
                        .partition_statistics(None)?
                        .column_statistics
                        .swap_remove(col_index);
                    // DataSourceExec statistics may not be exact due to filter
                    // pushdown, but the values in the plan are guaranteed to be
                    // within the min/max.
                    Self::convert_min_max_to_exact(col_stats)
                }
                // This is the case of alias, do not optimize by returning no statistics
                Err(_) => {
                    trace!(
                        " ------------------- No statistics for column {} in PQ/RB",
                        self.column_name
                    );
                    ColumnStatistics::new_unknown()
                }
            };
            self.statistics.push_back(statistics);
        }
        // RecordBatchesExec (leaf): compute its statistics and push it to the stack
        else if plan.as_any().downcast_ref::<RecordBatchesExec>().is_some() {
            // get index of the given column in the schema
            let statistics = match plan.schema().index_of(self.column_name) {
                Ok(col_index) => plan
                    .partition_statistics(None)?
                    .column_statistics
                    .swap_remove(col_index),
                // This is the case of alias, do not optimize by returning no statistics
                Err(_) => {
                    trace!(
                        " ------------------- No statistics for column {} in PQ/RB",
                        self.column_name
                    );
                    ColumnStatistics::new_unknown()
                }
            };
            self.statistics.push_back(statistics);
        }
        // Non leaf node
        else {
            // These are cases the stats will be the union of their children's
            //   Sort, Deduplicate, Filter, Repartition, Union, SortPreservingMerge, CoalesceBatches
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
                    // Conservatively union min max
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
                self.statistics.push_back(ColumnStatistics::new_unknown());
            }
        }

        Ok(true)
    }
}

/// Get statistics min max of given column name on given plans
/// Return None if one of the inputs does not have statistics or  does not include the column.
///
/// Currently not used in prod code. See comment in [`crate::physical_optimizer::sort::util::collect_statistics_min_max`].
#[cfg(test)]
fn statistics_min_max(
    plans: &[Arc<dyn ExecutionPlan>],
    column_name: &str,
) -> Option<Vec<(ScalarValue, ScalarValue)>> {
    // Get statistics for each plan
    let plans_schema_and_stats = plans
        .iter()
        .map(|plan| {
            Ok((
                Arc::clone(plan),
                plan.schema(),
                plan.partition_statistics(None)?,
            ))
        })
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
            panic!("sorted column {column_name} is not in the schema");
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

#[cfg(test)]
mod test {
    use crate::{
        QueryChunk,
        provider::chunks_to_physical_nodes,
        statistics::{column_statistics_min_max, overlap},
        test::{TestChunk, format_execution_plan},
    };

    use super::*;
    use arrow::datatypes::{DataType, SchemaRef};
    use datafusion::error::DataFusionError;
    use itertools::Itertools;
    use schema::{InfluxFieldType, SchemaBuilder};

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
            @r#"- " DataSourceExec: file_groups={1 group: [[0.parquet]]}, projection=[tag, float_field, int_field, string_field, tag_no_val, field_no_val, time], file_type=parquet""#
        );

        let plan_rb = chunks_to_physical_nodes(&schema, None, vec![record_batch_chunk], 1);
        insta::assert_yaml_snapshot!(
            format_execution_plan(&plan_rb),
            @r#"- " RecordBatchesExec: chunks=1, projection=[tag, float_field, int_field, string_field, tag_no_val, field_no_val, time]""#
        );

        // Stats for time
        // parquet
        let time_stats = compute_stats_column_min_max(&*plan_pq, "time").unwrap();
        let min_max = column_statistics_min_max(time_stats).unwrap();
        let expected_time_stats = (
            ScalarValue::TimestampNanosecond(Some(10), None),
            ScalarValue::TimestampNanosecond(Some(100), None),
        );
        assert_eq!(min_max, expected_time_stats);
        // record batch
        let time_stats = compute_stats_column_min_max(&*plan_rb, "time").unwrap();
        let min_max = column_statistics_min_max(time_stats).unwrap();
        let expected_time_stats = (
            ScalarValue::TimestampNanosecond(Some(20), None),
            ScalarValue::TimestampNanosecond(Some(200), None),
        );
        assert_eq!(min_max, expected_time_stats);

        // Stats for tag
        // parquet
        let tag_stats = compute_stats_column_min_max(&*plan_pq, "tag").unwrap();
        let min_max = column_statistics_min_max(tag_stats).unwrap();
        let expected_tag_stats = (
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("MA".to_string()))),
            ),
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("VT".to_string()))),
            ),
        );
        assert_eq!(min_max, expected_tag_stats);
        // record batch
        let tag_stats = compute_stats_column_min_max(&*plan_rb, "tag").unwrap();
        let min_max = column_statistics_min_max(tag_stats).unwrap();
        let expected_tag_stats = (
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("Boston".to_string()))),
            ),
            ScalarValue::Dictionary(
                Box::new(DataType::Int32),
                Box::new(ScalarValue::Utf8(Some("DC".to_string()))),
            ),
        );
        assert_eq!(min_max, expected_tag_stats);

        // Stats for field
        // parquet
        let float_stats = compute_stats_column_min_max(&*plan_pq, "float_field").unwrap();
        let min_max = column_statistics_min_max(float_stats).unwrap();
        let expected_float_stats = (
            ScalarValue::Float64(Some(10.1)),
            ScalarValue::Float64(Some(100.4)),
        );
        assert_eq!(min_max, expected_float_stats);
        // record batch
        let float_stats = compute_stats_column_min_max(&*plan_rb, "float_field").unwrap();
        let min_max = column_statistics_min_max(float_stats).unwrap();
        let expected_float_stats = (
            ScalarValue::Float64(Some(15.6)),
            ScalarValue::Float64(Some(30.0)),
        );
        assert_eq!(min_max, expected_float_stats);

        // Stats for int
        // parquet
        let int_stats = compute_stats_column_min_max(&*plan_pq, "int_field").unwrap();
        let min_max = column_statistics_min_max(int_stats).unwrap();
        let expected_int_stats = (ScalarValue::Int64(Some(30)), ScalarValue::Int64(Some(50)));
        assert_eq!(min_max, expected_int_stats);
        // record batch
        let int_stats = compute_stats_column_min_max(&*plan_rb, "int_field").unwrap();
        let min_max = column_statistics_min_max(int_stats).unwrap();
        let expected_int_stats = (ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(50)));
        assert_eq!(min_max, expected_int_stats);

        // Stats for string
        // parquet
        let string_stats = compute_stats_column_min_max(&*plan_pq, "string_field").unwrap();
        let min_max = column_statistics_min_max(string_stats).unwrap();
        let expected_string_stats = (
            ScalarValue::Utf8(Some("orange".to_string())),
            ScalarValue::Utf8(Some("plum".to_string())),
        );
        assert_eq!(min_max, expected_string_stats);
        // record batch
        let string_stats = compute_stats_column_min_max(&*plan_rb, "string_field").unwrap();
        let min_max = column_statistics_min_max(string_stats).unwrap();
        let expected_string_stats = (
            ScalarValue::Utf8(Some("banana".to_string())),
            ScalarValue::Utf8(Some("plum".to_string())),
        );
        assert_eq!(min_max, expected_string_stats);

        // no tats on parquet
        let tag_no_stats = compute_stats_column_min_max(&*plan_pq, "tag_no_val").unwrap();
        let min_max = column_statistics_min_max(tag_no_stats);
        assert!(min_max.is_none());

        // no stats on record batch
        let field_no_stats = compute_stats_column_min_max(&*plan_rb, "field_no_val").unwrap();
        let min_max = column_statistics_min_max(field_no_stats);
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
                ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some("MA".to_string()))),
                ),
                ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some("VT".to_string()))),
                ),
            ),
            (
                ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some("Boston".to_string()))),
                ),
                ScalarValue::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(ScalarValue::Utf8(Some("DC".to_string()))),
                ),
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

        let overlap = overlap_all(&[pair_1, pair_2, pair_3]).unwrap();
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

        let overlap = overlap_all(&[pair_1, pair_2, pair_3]).unwrap();
        assert!(overlap);
    }

    #[test]
    fn test_non_overlap_integer() {
        // [3, 5,   9, 12,   1, 1,   6, 8]
        let pair_1 = (ScalarValue::Int16(Some(3)), ScalarValue::Int16(Some(5)));
        let pair_2 = (ScalarValue::Int16(Some(9)), ScalarValue::Int16(Some(12)));
        let pair_3 = (ScalarValue::Int16(Some(1)), ScalarValue::Int16(Some(1)));
        let pair_4 = (ScalarValue::Int16(Some(6)), ScalarValue::Int16(Some(8)));

        let overlap = overlap_all(&[pair_1, pair_2, pair_3, pair_4]).unwrap();
        assert!(!overlap);
    }

    #[test]
    fn test_overlap_integer() {
        // [3, 5,   9, 12,   1, 1,   4, 6]
        let pair_1 = (ScalarValue::Int16(Some(3)), ScalarValue::Int16(Some(5)));
        let pair_2 = (ScalarValue::Int16(Some(9)), ScalarValue::Int16(Some(12)));
        let pair_3 = (ScalarValue::Int16(Some(1)), ScalarValue::Int16(Some(1)));
        let pair_4 = (ScalarValue::Int16(Some(4)), ScalarValue::Int16(Some(6)));

        let overlap = overlap_all(&[pair_1, pair_2, pair_3, pair_4]).unwrap();
        assert!(overlap);
    }

    #[test]
    fn test_non_overlap_integer_ascending_null_first() {
        // [3, 5,   null, null,   1, 1,   6, 8]
        let pair_1 = (ScalarValue::Int16(Some(3)), ScalarValue::Int16(Some(5)));
        let pair_2 = (ScalarValue::Int16(None), ScalarValue::Int16(None));
        let pair_3 = (ScalarValue::Int16(Some(1)), ScalarValue::Int16(Some(2)));
        let pair_4 = (ScalarValue::Int16(Some(6)), ScalarValue::Int16(Some(8)));

        let overlap = overlap_all(&[pair_1, pair_2, pair_3, pair_4]).unwrap();
        assert!(!overlap);
    }

    #[test]
    fn test_overlap_integer_ascending_null_first() {
        // [3, 5,   null, null,   1, 1,   4, 6]
        let pair_1 = (ScalarValue::Int16(Some(3)), ScalarValue::Int16(Some(5)));
        let pair_2 = (ScalarValue::Int16(None), ScalarValue::Int16(None));
        let pair_3 = (ScalarValue::Int16(Some(1)), ScalarValue::Int16(Some(2)));
        let pair_4 = (ScalarValue::Int16(Some(4)), ScalarValue::Int16(Some(6)));

        let overlap = overlap_all(&[pair_1, pair_2, pair_3, pair_4]).unwrap();
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

        let overlap = overlap_all(&[pair_1, pair_2, pair_3]).unwrap();
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

        let overlap = overlap_all(&[pair_1, pair_2, pair_3]).unwrap();
        assert!(overlap);
    }

    #[test]
    #[should_panic(expected = "Execution(\"Empty iterator passed to ScalarValue::iter_to_array\")")]
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
