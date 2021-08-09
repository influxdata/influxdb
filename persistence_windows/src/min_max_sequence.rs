use std::cmp::Ordering;

/// The minimum and maximum sequence numbers seen for a given sequencer.
///
/// **IMPORTANT: These ranges include their start and their end (aka `[min, max]`)!**
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

    /// Inclusive minimum.
    pub fn min(&self) -> u64 {
        self.min
    }

    /// Inclusive maximum.
    pub fn max(&self) -> u64 {
        self.max
    }
}

/// The optional minimum and maximum sequence numbers seen for a given sequencer.
///
/// **IMPORTANT: These ranges include their start and their end (aka `[min, max]`)!**
///
/// # Ordering
/// The sequences are partially ordered. If both the min and the max value agree on the order (less, equal, greater)
/// then two sequences can be compared to each other, otherwise no order can be established:
///
/// ```
/// # use std::cmp::Ordering;
/// # use persistence_windows::min_max_sequence::OptionalMinMaxSequence;
/// #
/// assert_eq!(
///     OptionalMinMaxSequence::new(Some(10), 15)
///         .partial_cmp(&OptionalMinMaxSequence::new(Some(10), 15)),
///     Some(Ordering::Equal),
/// );
/// assert_eq!(
///     OptionalMinMaxSequence::new(Some(15), 16)
///         .partial_cmp(&OptionalMinMaxSequence::new(Some(10), 12)),
///     Some(Ordering::Greater),
/// );
/// assert_eq!(
///     OptionalMinMaxSequence::new(None, 10)
///         .partial_cmp(&OptionalMinMaxSequence::new(Some(8), 10)),
///     Some(Ordering::Greater),
/// );
/// assert_eq!(
///     OptionalMinMaxSequence::new(Some(10), 10)
///         .partial_cmp(&OptionalMinMaxSequence::new(Some(9), 11)),
///     None,
/// );
/// ```
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct OptionalMinMaxSequence {
    /// The minimum sequence number. If None, this implies that data
    /// up to and including the max sequence number has been persisted
    /// (aka that the min == max)
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

    /// Inclusive minimum.
    ///
    /// If this is `None` then the range is empty.
    pub fn min(&self) -> Option<u64> {
        self.min
    }

    /// Inclusive maximum.
    pub fn max(&self) -> u64 {
        self.max
    }
}

impl std::fmt::Display for OptionalMinMaxSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(min) = self.min {
            write!(f, "[{}, {}]", min, self.max)
        } else {
            write!(f, "({}, {}]", self.max, self.max)
        }
    }
}

impl PartialOrd for OptionalMinMaxSequence {
    /// See [`OptionalMinMaxSequence`] for an explanation of how/when ranges are ordered.
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // upcast min values so that we can use `u64::MAX + 1`
        let min1 = self
            .min()
            .map(|min| min as u128)
            .unwrap_or_else(|| (self.max() as u128) + 1);
        let min2 = other
            .min()
            .map(|min| min as u128)
            .unwrap_or_else(|| (other.max() as u128) + 1);

        match (min1.cmp(&min2), self.max.cmp(&other.max)) {
            (Ordering::Equal, x) => Some(x),
            (x, Ordering::Equal) => Some(x),
            (x, y) if x == y => Some(x),
            _ => None,
        }
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

    #[test]
    fn test_opt_min_max_display() {
        assert_eq!(
            OptionalMinMaxSequence::new(Some(10), 20).to_string(),
            "[10, 20]".to_string()
        );
        assert_eq!(
            OptionalMinMaxSequence::new(Some(20), 20).to_string(),
            "[20, 20]".to_string()
        );
        assert_eq!(
            OptionalMinMaxSequence::new(None, 20).to_string(),
            "(20, 20]".to_string()
        );
    }

    #[test]
    fn test_opt_min_max_partial_cmp() {
        assert_eq!(
            OptionalMinMaxSequence::new(Some(10), 10)
                .partial_cmp(&OptionalMinMaxSequence::new(Some(10), 10)),
            Some(Ordering::Equal),
        );

        assert_eq!(
            OptionalMinMaxSequence::new(None, 10)
                .partial_cmp(&OptionalMinMaxSequence::new(Some(10), 10)),
            Some(Ordering::Greater),
        );
        assert_eq!(
            OptionalMinMaxSequence::new(Some(10), 10)
                .partial_cmp(&OptionalMinMaxSequence::new(None, 10)),
            Some(Ordering::Less),
        );

        assert_eq!(
            OptionalMinMaxSequence::new(None, 10)
                .partial_cmp(&OptionalMinMaxSequence::new(Some(11), 11)),
            Some(Ordering::Less),
        );

        assert_eq!(
            OptionalMinMaxSequence::new(Some(10), 10)
                .partial_cmp(&OptionalMinMaxSequence::new(Some(9), 11)),
            None,
        );
        assert_eq!(
            OptionalMinMaxSequence::new(None, 10)
                .partial_cmp(&OptionalMinMaxSequence::new(Some(9), 11)),
            None,
        );

        assert_eq!(
            OptionalMinMaxSequence::new(Some(u64::MAX), u64::MAX)
                .partial_cmp(&OptionalMinMaxSequence::new(Some(u64::MAX), u64::MAX)),
            Some(Ordering::Equal),
        );
        assert_eq!(
            OptionalMinMaxSequence::new(None, u64::MAX)
                .partial_cmp(&OptionalMinMaxSequence::new(None, u64::MAX)),
            Some(Ordering::Equal),
        );
        assert_eq!(
            OptionalMinMaxSequence::new(None, u64::MAX)
                .partial_cmp(&OptionalMinMaxSequence::new(Some(u64::MAX), u64::MAX)),
            Some(Ordering::Greater),
        );
        assert_eq!(
            OptionalMinMaxSequence::new(Some(u64::MAX), u64::MAX)
                .partial_cmp(&OptionalMinMaxSequence::new(None, u64::MAX)),
            Some(Ordering::Less),
        );
    }
}
