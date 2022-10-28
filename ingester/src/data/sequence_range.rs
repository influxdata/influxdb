use data_types::SequenceNumber;

/// A range of sequence numbers, both inclusive [min, max].
#[derive(Debug, Default, PartialEq, Eq, Clone)]
pub(crate) struct SequenceNumberRange {
    range: Option<(SequenceNumber, SequenceNumber)>,
}

impl SequenceNumberRange {
    pub(crate) fn observe(&mut self, n: SequenceNumber) {
        self.range = Some(match self.range {
            Some((min, max)) => {
                assert!(n > max, "monotonicity violation");
                (min, n)
            }
            None => (n, n),
        });
    }

    /// Returns the inclusive lower bound on [`SequenceNumber`] values observed.
    pub(crate) fn inclusive_min(&self) -> Option<SequenceNumber> {
        self.range.map(|v| v.0)
    }

    /// Returns the inclusive upper bound on [`SequenceNumber`] values observed.
    pub(crate) fn inclusive_max(&self) -> Option<SequenceNumber> {
        self.range.map(|v| v.1)
    }

    /// Merge two [`SequenceNumberRange`] instances, returning a new, merged
    /// instance.
    ///
    /// The merge result contains the minimum of [`Self::inclusive_min()`] from
    /// each instance, and the maximum of [`Self::inclusive_max()`].
    ///
    /// If both `self` and `other` contain no [`SequenceNumber`] observations,
    /// the returned instance contains no observations.
    pub(crate) fn merge(&self, other: &Self) -> Self {
        let merged_range = self
            .range
            .into_iter()
            .chain(other.range)
            .reduce(|a, b| (a.0.min(b.0), a.1.max(b.1)));

        Self {
            range: merged_range,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ranges() {
        let mut r = SequenceNumberRange::default();

        r.observe(SequenceNumber::new(0));
        r.observe(SequenceNumber::new(2));
        r.observe(SequenceNumber::new(3));

        assert_eq!(r.inclusive_min(), Some(SequenceNumber::new(0)));
        assert_eq!(r.inclusive_max(), Some(SequenceNumber::new(3)));
    }

    #[test]
    #[should_panic = "monotonicity violation"]
    fn test_monotonicity() {
        let mut r = SequenceNumberRange::default();

        r.observe(SequenceNumber::new(1));
        r.observe(SequenceNumber::new(3));
        r.observe(SequenceNumber::new(2));
    }

    #[test]
    #[should_panic = "monotonicity violation"]
    fn test_exactly_once() {
        let mut r = SequenceNumberRange::default();

        r.observe(SequenceNumber::new(1));
        r.observe(SequenceNumber::new(1));
    }

    #[test]
    fn test_merge() {
        let mut a = SequenceNumberRange::default();
        let mut b = SequenceNumberRange::default();

        a.observe(SequenceNumber::new(4));
        b.observe(SequenceNumber::new(2));

        let a_b = a.merge(&b);
        assert_eq!(a_b.inclusive_min(), Some(SequenceNumber::new(2)));
        assert_eq!(a_b.inclusive_max(), Some(SequenceNumber::new(4)));

        let b_a = b.merge(&a);
        assert_eq!(b_a.inclusive_min(), Some(SequenceNumber::new(2)));
        assert_eq!(b_a.inclusive_max(), Some(SequenceNumber::new(4)));

        assert_eq!(a_b, b_a);
    }

    #[test]
    fn test_merge_half_empty() {
        let mut a = SequenceNumberRange::default();
        let b = SequenceNumberRange::default();

        a.observe(SequenceNumber::new(4));
        // B observes nothing

        let a_b = a.merge(&b);
        assert_eq!(a_b.inclusive_min(), Some(SequenceNumber::new(4)));
        assert_eq!(a_b.inclusive_max(), Some(SequenceNumber::new(4)));

        let b_a = b.merge(&a);
        assert_eq!(b_a.inclusive_min(), Some(SequenceNumber::new(4)));
        assert_eq!(b_a.inclusive_max(), Some(SequenceNumber::new(4)));

        assert_eq!(a_b, b_a);
    }

    #[test]
    fn test_merge_both_empty() {
        let a = SequenceNumberRange::default();
        let b = SequenceNumberRange::default();

        // Neither observe anything

        let a_b = a.merge(&b);
        assert_eq!(a_b.inclusive_min(), None);
        assert_eq!(a_b.inclusive_max(), None);

        let b_a = b.merge(&a);
        assert_eq!(b_a.inclusive_min(), None);
        assert_eq!(b_a.inclusive_max(), None);

        assert_eq!(a_b, b_a);
    }
}
