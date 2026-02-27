use arrow::array::{Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::TimeDelta;
use datafusion::common::Result;
use datafusion::common::cast::as_timestamp_nanosecond_array;
use datafusion::scalar::ScalarValue;
use std::cmp::{max, min};
use std::ops::{Bound, Range};

use super::{ExpandedValue, GapExpander};

/// GapExpander that expands evenly spaced timestamps.
#[derive(Debug)]
pub struct DateBinGapExpander {
    stride_ns: i64,
}

impl DateBinGapExpander {
    pub fn new(stride_ns: i64) -> Self {
        Self { stride_ns }
    }

    fn get_bounds(
        &self,
        range: Range<Bound<ScalarValue>>,
        array: &TimestampNanosecondArray,
    ) -> Result<Range<i64>> {
        let (array_start, array_end) = if array.is_empty() {
            (None, None)
        } else {
            (
                Some(array.value(0)),
                Some(array.value(array.len() - 1) + self.stride_ns),
            )
        };
        let start = match range.start {
            Bound::Included(ScalarValue::TimestampNanosecond(Some(v), _)) => {
                if let Some(array_start) = array_start {
                    min(v, array_start)
                } else {
                    v
                }
            }
            Bound::Excluded(ScalarValue::TimestampNanosecond(Some(v), _)) => {
                if let Some(array_start) = array_start {
                    min(v + self.stride_ns, array_start)
                } else {
                    v + self.stride_ns
                }
            }
            Bound::Unbounded => {
                if let Some(array_start) = array_start {
                    array_start
                } else {
                    return Ok(Range { start: 0, end: 0 });
                }
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Invalid start bound".to_string(),
                ));
            }
        };

        let end = match range.end {
            Bound::Included(ScalarValue::TimestampNanosecond(Some(v), _)) => {
                if let Some(array_end) = array_end {
                    max(v + self.stride_ns, array_end)
                } else {
                    v + self.stride_ns
                }
            }
            Bound::Excluded(ScalarValue::TimestampNanosecond(Some(v), _)) => {
                if let Some(array_end) = array_end {
                    max(v, array_end)
                } else {
                    v
                }
            }
            Bound::Unbounded => {
                if let Some(array_end) = array_end {
                    array_end
                } else {
                    return Ok(Range { start: 0, end: 0 });
                }
            }
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Invalid end bound".to_string(),
                ));
            }
        };

        Ok(Range { start, end })
    }
}

impl std::fmt::Display for DateBinGapExpander {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DateBinGapExpander [stride={}]",
            TimeDelta::nanoseconds(self.stride_ns)
        )
    }
}

impl GapExpander for DateBinGapExpander {
    fn expand_gaps(
        &self,
        range: Range<Bound<ScalarValue>>,
        array: &dyn Array,
        max_output_rows: usize,
    ) -> Result<(Vec<ExpandedValue>, usize)> {
        let tz = match array.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => tz.clone(),
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "Invalid array data type".to_string(),
                ));
            }
        };
        let array = as_timestamp_nanosecond_array(array)?;
        let range = self.get_bounds(range, array)?;
        let cap = ((range.end - range.start) / self.stride_ns) as usize;
        let mut pairs = Vec::with_capacity(cap.min(max_output_rows));
        let mut ts = range.start;
        let mut input_row = 0;
        while ts < range.end && pairs.len() < max_output_rows {
            if input_row < array.len() && array.value(input_row) < ts {
                return Err(datafusion::error::DataFusionError::Execution(
                    "DATE_BIN_GAPFILL: unexpected time bin value".to_string(),
                ));
            } else if input_row < array.len() && array.value(input_row) == ts {
                pairs.push((
                    ScalarValue::TimestampNanosecond(Some(ts), tz.clone()),
                    Some(input_row),
                ));
                input_row += 1;
            } else {
                pairs.push((ScalarValue::TimestampNanosecond(Some(ts), tz.clone()), None));
            }
            ts += self.stride_ns;
        }

        Ok((pairs, input_row))
    }

    fn count_rows(&self, range: Range<Bound<ScalarValue>>, array: &dyn Array) -> Result<usize> {
        let array = as_timestamp_nanosecond_array(array)?;
        self.get_bounds(range, array)
            .map(|range| ((range.end - range.start) / self.stride_ns) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn empty_array_with_bounds() {
        let tz = Arc::from("Europe/London");
        let gap_finder = DateBinGapExpander::new(1_000_000_000);
        let array =
            arrow::array::TimestampNanosecondArray::from_value(0, 0).with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = gap_finder
            .expand_gaps(
                Range {
                    start: Bound::Included(ScalarValue::TimestampNanosecond(
                        Some(0),
                        Some(Arc::clone(&tz)),
                    )),
                    end: Bound::Excluded(ScalarValue::TimestampNanosecond(
                        Some(10_000_000_000),
                        Some(Arc::clone(&tz)),
                    )),
                },
                &array,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(input_rows, 0);
        assert_eq!(
            pairs,
            vec![
                (
                    ScalarValue::TimestampNanosecond(Some(0), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(1_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(2_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(3_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(4_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(5_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(6_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(7_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(8_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(9_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
            ]
        );
    }

    #[test]
    fn empty_array_missing_uppper_bound() {
        let tz = Arc::from("Europe/London");
        let gap_finder = DateBinGapExpander::new(1_000_000_000);
        let array =
            arrow::array::TimestampNanosecondArray::from_value(0, 0).with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = gap_finder
            .expand_gaps(
                Range {
                    start: Bound::Included(ScalarValue::TimestampNanosecond(
                        Some(0),
                        Some(Arc::clone(&tz)),
                    )),
                    end: Bound::Unbounded,
                },
                &array,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(pairs, vec![]);
        assert_eq!(input_rows, 0);
    }

    #[test]
    fn empty_array_missing_lower_bound() {
        let tz = Arc::from("Europe/London");
        let gap_finder = DateBinGapExpander::new(1_000_000_000);
        let array =
            arrow::array::TimestampNanosecondArray::from_value(0, 0).with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = gap_finder
            .expand_gaps(
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Excluded(ScalarValue::TimestampNanosecond(
                        Some(10_000_000_000),
                        Some(Arc::clone(&tz)),
                    )),
                },
                &array,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(pairs, vec![]);
        assert_eq!(input_rows, 0);
    }

    #[test]
    fn no_gaps() {
        let tz = Arc::from("Europe/London");
        let gap_finder = DateBinGapExpander::new(1_000_000_000);
        let array = arrow::array::TimestampNanosecondArray::from_iter_values(
            (0..10).map(|n| n * 1_000_000_000),
        )
        .with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = gap_finder
            .expand_gaps(
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Unbounded,
                },
                &array,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(
            pairs,
            vec![
                (
                    ScalarValue::TimestampNanosecond(Some(0), Some(Arc::clone(&tz))),
                    Some(0)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(1_000_000_000), Some(Arc::clone(&tz))),
                    Some(1)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(2_000_000_000), Some(Arc::clone(&tz))),
                    Some(2)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(3_000_000_000), Some(Arc::clone(&tz))),
                    Some(3)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(4_000_000_000), Some(Arc::clone(&tz))),
                    Some(4)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(5_000_000_000), Some(Arc::clone(&tz))),
                    Some(5)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(6_000_000_000), Some(Arc::clone(&tz))),
                    Some(6)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(7_000_000_000), Some(Arc::clone(&tz))),
                    Some(7)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(8_000_000_000), Some(Arc::clone(&tz))),
                    Some(8)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(9_000_000_000), Some(Arc::clone(&tz))),
                    Some(9)
                ),
            ]
        );
        assert_eq!(input_rows, 10);
    }

    #[test]
    fn internal_gaps() {
        let tz = Arc::from("Europe/London");
        let gap_finder = DateBinGapExpander::new(1_000_000_000);
        let array = arrow::array::TimestampNanosecondArray::from_iter_values(
            (0..5).map(|n| n * 2_000_000_000),
        )
        .with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = gap_finder
            .expand_gaps(
                Range {
                    start: Bound::Unbounded,
                    end: Bound::Unbounded,
                },
                &array,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(
            pairs,
            vec![
                (
                    ScalarValue::TimestampNanosecond(Some(0), Some(Arc::clone(&tz))),
                    Some(0)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(1_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(2_000_000_000), Some(Arc::clone(&tz))),
                    Some(1)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(3_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(4_000_000_000), Some(Arc::clone(&tz))),
                    Some(2)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(5_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(6_000_000_000), Some(Arc::clone(&tz))),
                    Some(3)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(7_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(8_000_000_000), Some(Arc::clone(&tz))),
                    Some(4)
                ),
            ]
        );
        assert_eq!(input_rows, 5);
    }

    #[test]
    fn gaps_on_either_end() {
        let tz = Arc::from("Europe/London");
        let gap_finder = DateBinGapExpander::new(1_000_000_000);
        let array = arrow::array::TimestampNanosecondArray::from_iter_values(
            (3..8).map(|n| n * 1_000_000_000),
        )
        .with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = gap_finder
            .expand_gaps(
                Range {
                    start: Bound::Included(ScalarValue::TimestampNanosecond(
                        Some(0),
                        Some(Arc::clone(&tz)),
                    )),
                    end: Bound::Excluded(ScalarValue::TimestampNanosecond(
                        Some(10_000_000_000),
                        Some(Arc::clone(&tz)),
                    )),
                },
                &array,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(
            pairs,
            vec![
                (
                    ScalarValue::TimestampNanosecond(Some(0), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(1_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(2_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(3_000_000_000), Some(Arc::clone(&tz))),
                    Some(0)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(4_000_000_000), Some(Arc::clone(&tz))),
                    Some(1)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(5_000_000_000), Some(Arc::clone(&tz))),
                    Some(2)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(6_000_000_000), Some(Arc::clone(&tz))),
                    Some(3)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(7_000_000_000), Some(Arc::clone(&tz))),
                    Some(4)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(8_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(9_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
            ]
        );
        assert_eq!(input_rows, 5);
    }

    #[test]
    fn limit_output_rows() {
        let tz = Arc::from("Europe/London");
        let gap_finder = DateBinGapExpander::new(1_000_000_000);
        let array = arrow::array::TimestampNanosecondArray::from_iter_values(
            (3..8).map(|n| n * 1_000_000_000),
        )
        .with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = gap_finder
            .expand_gaps(
                Range {
                    start: Bound::Included(ScalarValue::TimestampNanosecond(
                        Some(0),
                        Some(Arc::clone(&tz)),
                    )),
                    end: Bound::Excluded(ScalarValue::TimestampNanosecond(
                        Some(10_000_000_000),
                        Some(Arc::clone(&tz)),
                    )),
                },
                &array,
                5,
            )
            .unwrap();
        assert_eq!(
            pairs,
            vec![
                (
                    ScalarValue::TimestampNanosecond(Some(0), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(1_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(2_000_000_000), Some(Arc::clone(&tz))),
                    None
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(3_000_000_000), Some(Arc::clone(&tz))),
                    Some(0)
                ),
                (
                    ScalarValue::TimestampNanosecond(Some(4_000_000_000), Some(Arc::clone(&tz))),
                    Some(1)
                ),
            ]
        );
        assert_eq!(input_rows, 2);
    }
}
