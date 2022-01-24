/// minimum time that can be represented.
///
/// 1677-09-21 00:12:43.145224194 +0000 UTC
///
/// The two lowest minimum integers are used as sentinel values.  The
/// minimum value needs to be used as a value lower than any other value for
/// comparisons and another separate value is needed to act as a sentinel
/// default value that is unusable by the user, but usable internally.
/// Because these two values need to be used for a special purpose, we do
/// not allow users to write points at these two times.
///
/// Source: [influxdb](https://github.com/influxdata/influxdb/blob/540bb66e1381a48a6d1ede4fc3e49c75a7d9f4af/models/time.go#L12-L34)
pub const MIN_NANO_TIME: i64 = i64::MIN + 2;

/// maximum time that can be represented.
///
/// 2262-04-11 23:47:16.854775806 +0000 UTC
///
/// The highest time represented by a nanosecond needs to be used for an
/// exclusive range in the shard group, so the maximum time needs to be one
/// less than the possible maximum number of nanoseconds representable by an
/// int64 so that we don't lose a point at that one time.
/// Source: [influxdb](https://github.com/influxdata/influxdb/blob/540bb66e1381a48a6d1ede4fc3e49c75a7d9f4af/models/time.go#L12-L34)
pub const MAX_NANO_TIME: i64 = i64::MAX - 1;

/// Specifies a continuous range of nanosecond timestamps. Timestamp
/// predicates are so common and critical to performance of timeseries
/// databases in general, and IOx in particular, that they are handled
/// specially
///
/// Timestamp ranges are defined such that a value `v` is within the
/// range iff:
///
/// ```text
///  range.start <= v < range.end
/// ```
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Copy, Debug, Hash)]
pub struct TimestampRange {
    /// Start defines the inclusive lower bound. Minimum value is [MIN_NANO_TIME]
    start: i64,
    /// End defines the exclusive upper bound. Maximum value is [MAX_NANO_TIME]
    end: i64,
}

impl TimestampRange {
    pub fn new(start: i64, end: i64) -> Self {
        debug_assert!(end >= start);
        let start = start.max(MIN_NANO_TIME);
        let end = end.min(MAX_NANO_TIME);
        Self { start, end }
    }

    #[inline]
    /// Returns true if this range contains the value v
    pub fn contains(&self, v: i64) -> bool {
        self.start <= v && v < self.end
    }

    #[inline]
    /// Returns true if this range contains the value v
    pub fn contains_opt(&self, v: Option<i64>) -> bool {
        Some(true) == v.map(|ts| self.contains(ts))
    }

    /// Return the timestamp range's end.
    pub fn end(&self) -> i64 {
        self.end
    }

    /// Return the timestamp range's start.
    pub fn start(&self) -> i64 {
        self.start
    }
}

/// Specifies a min/max timestamp value.
///
/// Note this differs subtlety (but critically) from a
/// `TimestampRange` as the minimum and maximum values are included
#[derive(Clone, Debug, Copy)]
pub struct TimestampMinMax {
    /// The minimum timestamp value
    pub min: i64,
    /// the maximum timestamp value
    pub max: i64,
}

impl TimestampMinMax {
    pub fn new(min: i64, max: i64) -> Self {
        assert!(min <= max, "expected min ({}) <= max ({})", min, max);
        Self { min, max }
    }

    #[inline]
    /// Returns true if any of the values between min / max
    /// (inclusive) are contained within the specified timestamp range
    pub fn overlaps(&self, range: TimestampRange) -> bool {
        range.contains(self.min)
            || range.contains(self.max)
            || (self.min <= range.start && self.max >= range.end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_nano_min_max() {
        let cases = vec![
            (
                "MIN/MAX Nanos",
                TimestampRange::new(MIN_NANO_TIME, MAX_NANO_TIME),
            ),
            ("MIN/MAX i64", TimestampRange::new(i64::MIN, i64::MAX)),
        ];

        for (name, range) in cases {
            println!("case: {}", name);
            assert!(!range.contains(i64::MIN));
            assert!(range.contains(MIN_NANO_TIME));
            assert!(range.contains(MIN_NANO_TIME + 1));
            assert!(range.contains(MAX_NANO_TIME - 1));
            assert!(!range.contains(MAX_NANO_TIME));
            assert!(!range.contains(i64::MAX));
        }
    }

    #[test]
    fn test_timestamp_i64_min_max_offset() {
        let range = TimestampRange::new(MIN_NANO_TIME + 1, MAX_NANO_TIME - 1);

        assert!(!range.contains(i64::MIN));
        assert!(!range.contains(MIN_NANO_TIME));
        assert!(range.contains(MIN_NANO_TIME + 1));
        assert!(range.contains(MAX_NANO_TIME - 2));
        assert!(!range.contains(MAX_NANO_TIME - 1));
        assert!(!range.contains(MAX_NANO_TIME));
        assert!(!range.contains(i64::MAX));
    }

    #[test]
    fn test_timestamp_range_contains() {
        let range = TimestampRange::new(100, 200);
        assert!(!range.contains(99));
        assert!(range.contains(100));
        assert!(range.contains(101));
        assert!(range.contains(199));
        assert!(!range.contains(200));
        assert!(!range.contains(201));
    }

    #[test]
    fn test_timestamp_range_contains_opt() {
        let range = TimestampRange::new(100, 200);
        assert!(!range.contains_opt(Some(99)));
        assert!(range.contains_opt(Some(100)));
        assert!(range.contains_opt(Some(101)));
        assert!(range.contains_opt(Some(199)));
        assert!(!range.contains_opt(Some(200)));
        assert!(!range.contains_opt(Some(201)));

        assert!(!range.contains_opt(None));
    }

    #[test]
    fn test_timestamp_range_overlaps() {
        let range = TimestampRange::new(100, 200);
        assert!(!TimestampMinMax::new(0, 99).overlaps(range));
        assert!(TimestampMinMax::new(0, 100).overlaps(range));
        assert!(TimestampMinMax::new(0, 101).overlaps(range));

        assert!(TimestampMinMax::new(0, 200).overlaps(range));
        assert!(TimestampMinMax::new(0, 201).overlaps(range));
        assert!(TimestampMinMax::new(0, 300).overlaps(range));

        assert!(TimestampMinMax::new(100, 101).overlaps(range));
        assert!(TimestampMinMax::new(100, 200).overlaps(range));
        assert!(TimestampMinMax::new(100, 201).overlaps(range));

        assert!(TimestampMinMax::new(101, 101).overlaps(range));
        assert!(TimestampMinMax::new(101, 200).overlaps(range));
        assert!(TimestampMinMax::new(101, 201).overlaps(range));

        assert!(!TimestampMinMax::new(200, 200).overlaps(range));
        assert!(!TimestampMinMax::new(200, 201).overlaps(range));

        assert!(!TimestampMinMax::new(201, 300).overlaps(range));
    }

    #[test]
    #[should_panic(expected = "expected min (2) <= max (1)")]
    fn test_timestamp_min_max_invalid() {
        TimestampMinMax::new(2, 1);
    }
}
