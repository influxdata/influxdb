/// Specifies a continuous range of nanosecond timestamps. Timestamp
/// predicates are so common and critical to performance of timeseries
/// databases in general, and IOx in particular, that they are handled
/// specially
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

    #[inline]
    /// Returns if this range is disjoint w.r.t the provided range
    pub fn disjoint(&self, other: &Self) -> bool {
        self.end <= other.start || self.start >= other.end
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
    fn test_disjoint() {
        let r1 = TimestampRange::new(100, 200);
        let r2 = TimestampRange::new(200, 300);
        let r3 = TimestampRange::new(150, 250);

        assert!(r1.disjoint(&r2));
        assert!(r2.disjoint(&r1));
        assert!(!r1.disjoint(&r3));
        assert!(!r3.disjoint(&r1));
        assert!(!r2.disjoint(&r3));
        assert!(!r3.disjoint(&r2));
    }
}
