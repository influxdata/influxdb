/// Specifies a continuous range of nanosecond timestamps. Timestamp
/// predicates are so common and critical to performance of timeseries
/// databases in general, and IOx in particular, that they are handled
/// specially
#[derive(Clone, PartialEq, Copy, Debug)]
pub struct TimestampRange {
    /// Start defines the inclusive lower bound.
    pub start: i64,
    /// End defines the exclusive upper bound.
    pub end: i64,
}

impl TimestampRange {
    pub fn new(start: i64, end: i64) -> Self {
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
}
