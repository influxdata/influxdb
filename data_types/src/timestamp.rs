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
    /// Start defines the inclusive lower bound.
    pub start: i64,
    /// End defines the exclusive upper bound.
    pub end: i64,
}

impl TimestampRange {
    pub fn new(start: i64, end: i64) -> Self {
        debug_assert!(end >= start);
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
