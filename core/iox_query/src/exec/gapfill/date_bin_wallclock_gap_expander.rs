use arrow::array::{Array, TimestampNanosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use chrono::TimeDelta;
use datafusion::common::cast::as_timestamp_nanosecond_array;
use datafusion::common::{Result, exec_err, not_impl_err};
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;
use std::cmp::{Ordering, max, min};
use std::ops::{Bound, Range};

use super::{ExpandedValue, GapExpander};

use iter::TimestampIterator;

/// GapExpander that expands timestamps that are tied to local clocks.
#[derive(Debug)]
pub struct DateBinWallclockGapExpander {
    stride_ns: i64,
}

impl DateBinWallclockGapExpander {
    pub fn new(stride_ns: i64) -> Self {
        Self { stride_ns }
    }
}

impl DateBinWallclockGapExpander {
    pub fn try_from_df_args(args: &[ColumnarValue]) -> Result<Self> {
        let stride_ns = match args[0] {
            ColumnarValue::Scalar(ScalarValue::IntervalMonthDayNano(Some(v))) => {
                if v.months > 0 {
                    return not_impl_err!("Cannot use months in interval with gap filling");
                } else {
                    (v.days as i64 * 24 * 60 * 60 * 1_000_000_000) + v.nanoseconds
                }
            }
            _ => {
                return exec_err!("Invalid interval argument");
            }
        };

        Ok(Self::new(stride_ns))
    }
}

impl DateBinWallclockGapExpander {
    fn get_bounds(
        &self,
        range: Range<Bound<ScalarValue>>,
        array: &TimestampNanosecondArray,
    ) -> Result<Range<i64>> {
        let (array_start, array_end) = if array.is_empty() {
            (None, None)
        } else {
            let end_ns = array.value(array.len() - 1);
            let end_ns = self.next_timestamp(end_ns, &array.timezone())?;
            (Some(array.value(0)), end_ns)
        };
        let start = match range.start {
            Bound::Included(ScalarValue::TimestampNanosecond(Some(start_ns), _)) => {
                if let Some(array_start) = array_start {
                    min(start_ns, array_start)
                } else {
                    start_ns
                }
            }
            Bound::Excluded(ScalarValue::TimestampNanosecond(Some(start_ns), _)) => {
                let start_ns = self.next_timestamp(start_ns, &array.timezone())?.unwrap();
                if let Some(array_start) = array_start {
                    min(start_ns, array_start)
                } else {
                    start_ns
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
            Bound::Included(ScalarValue::TimestampNanosecond(Some(end_ns), _)) => {
                let end_ns = self.next_timestamp(end_ns, &array.timezone())?.unwrap();
                if let Some(array_end) = array_end {
                    max(end_ns, array_end)
                } else {
                    end_ns
                }
            }
            Bound::Excluded(ScalarValue::TimestampNanosecond(Some(end_ns), _)) => {
                if let Some(array_end) = array_end {
                    max(end_ns, array_end)
                } else {
                    end_ns
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

    fn next_timestamp(&self, ts_ns: i64, tz: &Option<impl AsRef<str>>) -> Result<Option<i64>> {
        TimestampIterator::try_new(self.stride_ns, ts_ns, tz).map(|it| it.take(2).last())
    }
}

impl std::fmt::Display for DateBinWallclockGapExpander {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DateBinWallclockGapExpander [stride={}]",
            TimeDelta::nanoseconds(self.stride_ns),
        )
    }
}

impl GapExpander for DateBinWallclockGapExpander {
    fn expand_gaps(
        &self,
        range: Range<Bound<ScalarValue>>,
        array: &dyn Array,
        max_output_rows: usize,
    ) -> Result<(Vec<ExpandedValue>, usize)> {
        let tz = match array.data_type() {
            DataType::Timestamp(TimeUnit::Nanosecond, tz) => tz.clone(),
            _ => {
                return exec_err!("DATE_BIN_WALLCLOCK_GAPFILL: invalid array data type");
            }
        };
        let array = as_timestamp_nanosecond_array(array)?;
        let Range {
            start: start_ns,
            end: end_ns,
        } = self.get_bounds(range, array)?;

        let mut input_row = 0;
        let mut pairs = vec![];
        for ts_ns in TimestampIterator::try_new(self.stride_ns, start_ns, &array.timezone())?
            .take(max_output_rows)
            .take_while(|ts_ns| *ts_ns < end_ns)
        {
            if input_row < array.len() {
                let its = array.value(input_row);
                match its.cmp(&ts_ns) {
                    Ordering::Less => {
                        return Err(datafusion::error::DataFusionError::Execution(
                            "DATE_BIN_WALLCLOCK_GAPFILL: unexpected time bin value".to_string(),
                        ));
                    }
                    Ordering::Equal => {
                        pairs.push((
                            ScalarValue::TimestampNanosecond(
                                Some(array.value(input_row)),
                                tz.clone(),
                            ),
                            Some(input_row),
                        ));
                        input_row += 1;
                        continue;
                    }
                    Ordering::Greater => {}
                }
            }

            pairs.push((
                ScalarValue::TimestampNanosecond(Some(ts_ns), tz.clone()),
                None,
            ));
        }

        Ok((pairs, input_row))
    }

    fn count_rows(&self, range: Range<Bound<ScalarValue>>, array: &dyn Array) -> Result<usize> {
        let array = as_timestamp_nanosecond_array(array)?;
        let Range {
            start: start_ns,
            end: end_ns,
        } = self.get_bounds(range, array)?;
        Ok((((end_ns + self.stride_ns - 1) - start_ns) / self.stride_ns) as usize)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::timezone::Tz;
    use arrow::datatypes::TimestampNanosecondType;
    use arrow::util::display::{ArrayFormatter, FormatOptions};
    use chrono::TimeZone;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn empty_array_with_bounds() {
        let tz = Arc::from("Europe/London");
        let gap_finder = DateBinWallclockGapExpander::new(1_000_000_000);
        let array =
            arrow::array::TimestampNanosecondArray::from_value(0, 0).with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = <DateBinWallclockGapExpander as GapExpander>::expand_gaps(
            &gap_finder,
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
        let gap_finder = DateBinWallclockGapExpander::new(1_000_000_000);
        let array =
            arrow::array::TimestampNanosecondArray::from_value(0, 0).with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = <DateBinWallclockGapExpander as GapExpander>::expand_gaps(
            &gap_finder,
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
        let gap_finder = DateBinWallclockGapExpander::new(1_000_000_000);
        let array =
            arrow::array::TimestampNanosecondArray::from_value(0, 0).with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = <DateBinWallclockGapExpander as GapExpander>::expand_gaps(
            &gap_finder,
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
        let gap_finder = DateBinWallclockGapExpander::new(1_000_000_000);
        let array = arrow::array::TimestampNanosecondArray::from_iter_values(
            (0..10).map(|n| n * 1_000_000_000),
        )
        .with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = <DateBinWallclockGapExpander as GapExpander>::expand_gaps(
            &gap_finder,
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
        let gap_finder = DateBinWallclockGapExpander::new(1_000_000_000);
        let array = arrow::array::TimestampNanosecondArray::from_iter_values(
            (0..5).map(|n| n * 2_000_000_000),
        )
        .with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = <DateBinWallclockGapExpander as GapExpander>::expand_gaps(
            &gap_finder,
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
        let gap_finder = DateBinWallclockGapExpander::new(1_000_000_000);
        let array = arrow::array::TimestampNanosecondArray::from_iter_values(
            (3..8).map(|n| n * 1_000_000_000),
        )
        .with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = <DateBinWallclockGapExpander as GapExpander>::expand_gaps(
            &gap_finder,
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
        let gap_finder = DateBinWallclockGapExpander::new(1_000_000_000);
        let array = arrow::array::TimestampNanosecondArray::from_iter_values(
            (3..8).map(|n| n * 1_000_000_000),
        )
        .with_timezone(Arc::clone(&tz));
        let (pairs, input_rows) = <DateBinWallclockGapExpander as GapExpander>::expand_gaps(
            &gap_finder,
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

    #[test]
    fn move_into_dst() {
        let tz: Arc<str> = Arc::from("Australia/Sydney");
        let atz = Tz::from_str(tz.as_ref()).unwrap();

        let gap_expander = DateBinWallclockGapExpander::new(24 * 60 * 60 * 1_000_000_000);
        let array =
            arrow::array::TimestampNanosecondArray::from_value(0, 0).with_timezone(Arc::clone(&tz));
        let start_ns = atz
            .with_ymd_and_hms(2022, 10, 1, 0, 0, 0)
            .single()
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();
        let end_ns = atz
            .with_ymd_and_hms(2022, 10, 5, 0, 0, 0)
            .single()
            .unwrap()
            .timestamp_nanos_opt()
            .unwrap();

        let start = ScalarValue::new_timestamp::<TimestampNanosecondType>(
            Some(start_ns),
            Some(Arc::clone(&tz)),
        );
        let end = ScalarValue::new_timestamp::<TimestampNanosecondType>(
            Some(end_ns),
            Some(Arc::clone(&tz)),
        );
        let (pairs, input_rows) = gap_expander
            .expand_gaps(
                Range {
                    start: Bound::Included(start),
                    end: Bound::Excluded(end),
                },
                &array,
                31,
            )
            .unwrap();

        assert_eq!(input_rows, 0);
        assert_eq!(
            format_times(pairs),
            vec![
                ("2022-10-01T00:00:00+10:00".into(), None),
                ("2022-10-02T00:00:00+10:00".into(), None),
                ("2022-10-03T00:00:00+11:00".into(), None),
                ("2022-10-04T00:00:00+11:00".into(), None),
            ]
        );
    }

    fn format_times(
        values: impl IntoIterator<Item = ExpandedValue>,
    ) -> Vec<(String, Option<usize>)> {
        let (svs, offsets) = values.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
        let arr = ScalarValue::iter_to_array(svs).unwrap();
        let fmt = ArrayFormatter::try_new(&arr, &FormatOptions::new()).unwrap();
        (0..arr.len())
            .map(|idx| fmt.value(idx).to_string())
            .zip(offsets)
            .collect()
    }
}

mod iter {
    use arrow::array::timezone::Tz;
    use datafusion::common::Result;
    use query_functions::date_bin_wallclock::offset_from_utc_ns;
    use std::str::FromStr;

    pub(super) struct TimestampIterator {
        stride_ns: i64,
        tz: Tz,

        ts_ns: i64,
        utc_offset_ns: i64,
    }

    impl TimestampIterator {
        pub(super) fn try_new(
            stride_ns: i64,
            ts_ns: i64,
            tz: &Option<impl AsRef<str>>,
        ) -> Result<Self> {
            let tz = match tz {
                None => Tz::from_str("UTC"), // Un-zoned time acts like UTC.
                Some(tz) => Tz::from_str(tz.as_ref()),
            }?;

            Ok(Self {
                stride_ns,
                tz,
                ts_ns,
                utc_offset_ns: offset_from_utc_ns(ts_ns, &tz),
            })
        }

        fn adjust_for_discontinuity(&mut self, next_utc_offset_ns: i64) {
            let offset_diff_ns = (self.utc_offset_ns - next_utc_offset_ns).abs();
            if offset_diff_ns >= self.stride_ns {
                // The offset change is larger than the stride, no need to adjust the timestamp.
                self.utc_offset_ns = next_utc_offset_ns;
                return;
            }

            if next_utc_offset_ns < self.utc_offset_ns {
                // Leaving DST time so adjust the timestamp forwards to make up for
                // the gained time.
                self.ts_ns += self.utc_offset_ns - next_utc_offset_ns;
                self.utc_offset_ns = next_utc_offset_ns;
                return;
            }

            let adjusted_utc_offset_ns = offset_from_utc_ns(self.ts_ns - offset_diff_ns, &self.tz);
            if adjusted_utc_offset_ns == next_utc_offset_ns {
                // Entered DST, but we aren't dear the discontinuity so
                // we can just adjust the timestamp and UTC offset.
                self.ts_ns += self.utc_offset_ns - next_utc_offset_ns;
                self.utc_offset_ns = next_utc_offset_ns;
            }

            // The time we are aiming for doesn't exist in the timezone.
            // Here we do not adjust anything and keep the stored UTC offset
            // of the previous timestamp. That way the value will be adjusted
            // on the next iteration.
        }
    }

    impl Iterator for TimestampIterator {
        type Item = i64;

        fn next(&mut self) -> Option<Self::Item> {
            let ts_ns = self.ts_ns;
            self.ts_ns += self.stride_ns;
            let utc_offset_ns = offset_from_utc_ns(self.ts_ns, &self.tz);
            if utc_offset_ns != self.utc_offset_ns {
                // The next timestamp we produce will cross a discontinuity, adjust as necessary.
                self.adjust_for_discontinuity(utc_offset_ns);
            }
            Some(ts_ns)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use chrono::{DateTime, TimeZone};
        use std::sync::Arc;

        #[test]
        fn into_dst() {
            let tz = Tz::from_str("Australia/Sydney").unwrap();
            let start_ns = tz
                .with_ymd_and_hms(2022, 10, 1, 0, 0, 0)
                .single()
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap();
            let it = TimestampIterator::try_new(
                24 * 60 * 60 * 1_000_000_000,
                start_ns,
                &Some(Arc::from("Australia/Sydney")),
            )
            .unwrap();
            let times = it
                .take(4)
                .map(|ts_ns| {
                    DateTime::from_timestamp_nanos(ts_ns)
                        .with_timezone(&tz)
                        .to_string()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                times,
                vec![
                    "2022-10-01 00:00:00 +10:00",
                    "2022-10-02 00:00:00 +10:00",
                    "2022-10-03 00:00:00 +11:00",
                    "2022-10-04 00:00:00 +11:00",
                ]
            );
        }

        #[test]
        fn into_dst_short_strides() {
            let tz = Tz::from_str("Australia/Sydney").unwrap();
            let start_ns = tz
                .with_ymd_and_hms(2022, 10, 2, 1, 0, 0)
                .single()
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap();
            let it = TimestampIterator::try_new(
                30 * 60 * 1_000_000_000,
                start_ns,
                &Some(Arc::from("Australia/Sydney")),
            )
            .unwrap();
            let times = it
                .take(4)
                .map(|ts_ns| {
                    DateTime::from_timestamp_nanos(ts_ns)
                        .with_timezone(&tz)
                        .to_string()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                times,
                vec![
                    "2022-10-02 01:00:00 +10:00",
                    "2022-10-02 01:30:00 +10:00",
                    "2022-10-02 03:00:00 +11:00",
                    "2022-10-02 03:30:00 +11:00",
                ]
            );
        }

        #[test]
        fn into_dst_avoid_discontinuity() {
            let tz = Tz::from_str("Australia/Sydney").unwrap();
            let start_ns = tz
                .with_ymd_and_hms(2022, 10, 1, 2, 30, 0)
                .single()
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap();
            let it = TimestampIterator::try_new(
                24 * 60 * 60 * 1_000_000_000,
                start_ns,
                &Some(Arc::from("Australia/Sydney")),
            )
            .unwrap();
            let times = it
                .take(4)
                .map(|ts_ns| {
                    DateTime::from_timestamp_nanos(ts_ns)
                        .with_timezone(&tz)
                        .to_string()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                times,
                vec![
                    "2022-10-01 02:30:00 +10:00",
                    "2022-10-02 03:30:00 +11:00",
                    "2022-10-03 02:30:00 +11:00",
                    "2022-10-04 02:30:00 +11:00",
                ]
            );
        }

        #[test]
        fn leave_dst() {
            let tz = Tz::from_str("Europe/Paris").unwrap();
            let start_ns = tz
                .with_ymd_and_hms(2022, 10, 29, 0, 0, 0)
                .single()
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap();
            let it = TimestampIterator::try_new(
                24 * 60 * 60 * 1_000_000_000,
                start_ns,
                &Some(Arc::from("Europe/Paris")),
            )
            .unwrap();
            let times = it
                .take(4)
                .map(|ts_ns| {
                    DateTime::from_timestamp_nanos(ts_ns)
                        .with_timezone(&tz)
                        .to_string()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                times,
                vec![
                    "2022-10-29 00:00:00 +02:00",
                    "2022-10-30 00:00:00 +02:00",
                    "2022-10-31 00:00:00 +01:00",
                    "2022-11-01 00:00:00 +01:00",
                ]
            );
        }

        #[test]
        fn leave_dst_short_strides() {
            let tz = Tz::from_str("Europe/Paris").unwrap();
            let start_ns = tz
                .with_ymd_and_hms(2022, 10, 30, 1, 30, 0)
                .single()
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap();
            let it = TimestampIterator::try_new(
                30 * 60 * 1_000_000_000,
                start_ns,
                &Some(Arc::from("Europe/Paris")),
            )
            .unwrap();
            let times = it
                .take(6)
                .map(|ts_ns| {
                    DateTime::from_timestamp_nanos(ts_ns)
                        .with_timezone(&tz)
                        .to_string()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                times,
                vec![
                    "2022-10-30 01:30:00 +02:00",
                    "2022-10-30 02:00:00 +02:00",
                    "2022-10-30 02:30:00 +02:00",
                    "2022-10-30 02:00:00 +01:00",
                    "2022-10-30 02:30:00 +01:00",
                    "2022-10-30 03:00:00 +01:00",
                ]
            );
        }

        #[test]
        fn leave_dst_avoid_discontinuity() {
            let tz = Tz::from_str("Europe/Paris").unwrap();
            let start_ns = tz
                .with_ymd_and_hms(2022, 10, 29, 3, 30, 0)
                .single()
                .unwrap()
                .timestamp_nanos_opt()
                .unwrap();
            let it = TimestampIterator::try_new(
                24 * 60 * 60 * 1_000_000_000,
                start_ns,
                &Some(Arc::from("Europe/Paris")),
            )
            .unwrap();
            let times = it
                .take(4)
                .map(|ts_ns| {
                    DateTime::from_timestamp_nanos(ts_ns)
                        .with_timezone(&tz)
                        .to_string()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                times,
                vec![
                    "2022-10-29 03:30:00 +02:00",
                    "2022-10-30 03:30:00 +01:00",
                    "2022-10-31 03:30:00 +01:00",
                    "2022-11-01 03:30:00 +01:00",
                ]
            );
        }
    }
}
