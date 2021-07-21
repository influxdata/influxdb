/// The minimum and maximum sequence numbers seen for a given sequencer.
///
/// **IMPORTANT: These ranges include their start and their end (aka `[start, end]`)!**
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct MinMaxSequence {
    min: u64,
    max: u64,
}

impl MinMaxSequence {
    /// Create new min-max sequence range.
    ///
    /// This panics if `min > max`.
    pub fn new(min: u64, max: u64) -> Self {
        assert!(
            min <= max,
            "min ({}) is greater than max ({}) sequence",
            min,
            max
        );
        Self { min, max }
    }

    pub fn min(&self) -> u64 {
        self.min
    }

    pub fn max(&self) -> u64 {
        self.max
    }
}

/// The optional minimum and maximum sequence numbers seen for a given sequencer.
///
/// **IMPORTANT: These ranges include their start and their end (aka `[start, end]`)!**
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct OptionalMinMaxSequence {
    min: Option<u64>,
    max: u64,
}

impl OptionalMinMaxSequence {
    /// Create new min-max sequence range.
    ///
    /// This panics if `min > max`.
    pub fn new(min: Option<u64>, max: u64) -> Self {
        if let Some(min) = min {
            assert!(
                min <= max,
                "min ({}) is greater than max ({}) sequence",
                min,
                max
            );
        }
        Self { min, max }
    }

    pub fn min(&self) -> Option<u64> {
        self.min
    }

    pub fn max(&self) -> u64 {
        self.max
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_min_max_getters() {
        let min_max = MinMaxSequence::new(10, 20);
        assert_eq!(min_max.min(), 10);
        assert_eq!(min_max.max(), 20);
    }

    #[test]
    fn test_opt_min_max_getters() {
        let min_max = OptionalMinMaxSequence::new(Some(10), 20);
        assert_eq!(min_max.min(), Some(10));
        assert_eq!(min_max.max(), 20);

        let min_max = OptionalMinMaxSequence::new(None, 20);
        assert_eq!(min_max.min(), None);
        assert_eq!(min_max.max(), 20);
    }

    #[test]
    fn test_min_max_accepts_equal_values() {
        MinMaxSequence::new(10, 10);
    }

    #[test]
    fn test_opt_min_max_accepts_equal_values() {
        OptionalMinMaxSequence::new(Some(10), 10);
    }

    #[test]
    #[should_panic(expected = "min (11) is greater than max (10) sequence")]
    fn test_min_max_checks_values() {
        MinMaxSequence::new(11, 10);
    }

    #[test]
    #[should_panic(expected = "min (11) is greater than max (10) sequence")]
    fn test_opt_min_max_checks_values() {
        OptionalMinMaxSequence::new(Some(11), 10);
    }
}
