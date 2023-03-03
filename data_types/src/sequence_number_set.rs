//! A set of [`SequenceNumber`] instances.

use crate::SequenceNumber;

/// A space-efficient encoded set of [`SequenceNumber`].
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SequenceNumberSet(croaring::Bitmap);

impl SequenceNumberSet {
    /// Add the specified [`SequenceNumber`] to the set.
    pub fn add(&mut self, n: SequenceNumber) {
        self.0.add(n.get() as _);
    }

    /// Remove the specified [`SequenceNumber`] to the set, if present.
    ///
    /// This is a no-op if `n` was not part of `self`.
    pub fn remove(&mut self, n: SequenceNumber) {
        self.0.remove(n.get() as _);
    }

    /// Add all the [`SequenceNumber`] in `other` to `self`.
    ///
    /// The result of this operation is the set union of both input sets.
    pub fn add_set(&mut self, other: &Self) {
        self.0.or_inplace(&other.0)
    }

    /// Remove all the [`SequenceNumber`] in `other` from `self`.
    pub fn remove_set(&mut self, other: &Self) {
        self.0.andnot_inplace(&other.0)
    }

    /// Reduce the memory usage of this set (trading off immediate CPU time) by
    /// efficiently re-encoding the set (using run-length encoding).
    pub fn run_optimise(&mut self) {
        self.0.run_optimize();
    }

    /// Serialise `self` into an array of bytes.
    ///
    /// [This document][spec] describes the serialised format.
    ///
    /// [spec]: https://github.com/RoaringBitmap/RoaringFormatSpec/
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.serialize()
    }

    /// Return true if the specified [`SequenceNumber`] has been added to
    /// `self`.
    pub fn contains(&self, n: SequenceNumber) -> bool {
        self.0.contains(n.get() as _)
    }

    /// Returns the number of [`SequenceNumber`] in this set.
    pub fn len(&self) -> u64 {
        self.0.cardinality()
    }

    /// Return `true` if there are no [`SequenceNumber`] in this set.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Return an iterator of all [`SequenceNumber`] in this set.
    pub fn iter(&self) -> impl Iterator<Item = SequenceNumber> + '_ {
        self.0.iter().map(|v| SequenceNumber::new(v as _))
    }

    /// Initialise a [`SequenceNumberSet`] that is pre-allocated to contain up
    /// to `n` elements without reallocating.
    pub fn with_capacity(n: u32) -> Self {
        Self(croaring::Bitmap::create_with_capacity(n))
    }
}

/// Deserialisation method.
impl TryFrom<&[u8]> for SequenceNumberSet {
    type Error = String;

    fn try_from(buffer: &[u8]) -> Result<Self, Self::Error> {
        croaring::Bitmap::try_deserialize(buffer)
            .map(SequenceNumberSet)
            .ok_or_else(|| "invalid bitmap bytes".to_string())
    }
}

impl Extend<SequenceNumber> for SequenceNumberSet {
    fn extend<T: IntoIterator<Item = SequenceNumber>>(&mut self, iter: T) {
        self.0.extend(iter.into_iter().map(|v| v.get() as _))
    }
}

impl FromIterator<SequenceNumber> for SequenceNumberSet {
    fn from_iter<T: IntoIterator<Item = SequenceNumber>>(iter: T) -> Self {
        Self(iter.into_iter().map(|v| v.get() as _).collect())
    }
}

/// Return the intersection of `self` and `other`.
pub fn intersect(a: &SequenceNumberSet, b: &SequenceNumberSet) -> SequenceNumberSet {
    SequenceNumberSet(a.0.and(&b.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_operations() {
        let mut a = SequenceNumberSet::default();
        let mut b = SequenceNumberSet::default();

        // Add an element and check it is readable
        a.add(SequenceNumber::new(1));
        assert!(a.contains(SequenceNumber::new(1)));
        assert_eq!(a.len(), 1);
        assert_eq!(a.iter().collect::<Vec<_>>(), vec![SequenceNumber::new(1)]);
        assert!(!a.contains(SequenceNumber::new(42)));

        // Merging an empty set into a should not change a
        a.add_set(&b);
        assert_eq!(a.len(), 1);
        assert!(a.contains(SequenceNumber::new(1)));

        // Merging a non-empty set should add the new elements
        b.add(SequenceNumber::new(2));
        a.add_set(&b);
        assert_eq!(a.len(), 2);
        assert!(a.contains(SequenceNumber::new(1)));
        assert!(a.contains(SequenceNumber::new(2)));

        // Removing the set should return it to the pre-merged state.
        a.remove_set(&b);
        assert_eq!(a.len(), 1);
        assert!(a.contains(SequenceNumber::new(1)));

        // Removing a non-existant element should be a NOP
        a.remove(SequenceNumber::new(42));
        assert_eq!(a.len(), 1);

        // Removing the last element should result in an empty set.
        a.remove(SequenceNumber::new(1));
        assert_eq!(a.len(), 0);
    }

    #[test]
    fn test_extend() {
        let mut a = SequenceNumberSet::default();
        a.add(SequenceNumber::new(42));

        let extend_set = [SequenceNumber::new(4), SequenceNumber::new(2)];

        assert!(a.contains(SequenceNumber::new(42)));
        assert!(!a.contains(SequenceNumber::new(4)));
        assert!(!a.contains(SequenceNumber::new(2)));

        a.extend(extend_set);

        assert!(a.contains(SequenceNumber::new(42)));
        assert!(a.contains(SequenceNumber::new(4)));
        assert!(a.contains(SequenceNumber::new(2)));
    }

    #[test]
    fn test_collect() {
        let collect_set = [SequenceNumber::new(4), SequenceNumber::new(2)];

        let a = collect_set.into_iter().collect::<SequenceNumberSet>();

        assert!(!a.contains(SequenceNumber::new(42)));
        assert!(a.contains(SequenceNumber::new(4)));
        assert!(a.contains(SequenceNumber::new(2)));
    }

    #[test]
    fn test_partial_eq() {
        let mut a = SequenceNumberSet::default();
        let mut b = SequenceNumberSet::default();

        assert_eq!(a, b);

        a.add(SequenceNumber::new(42));
        assert_ne!(a, b);

        b.add(SequenceNumber::new(42));
        assert_eq!(a, b);

        b.add(SequenceNumber::new(24));
        assert_ne!(a, b);

        a.add(SequenceNumber::new(24));
        assert_eq!(a, b);
    }

    #[test]
    fn test_intersect() {
        let a = [0, i64::MAX, 40, 41, 42, 43, 44, 45]
            .into_iter()
            .map(SequenceNumber::new)
            .collect::<SequenceNumberSet>();

        let b = [1, 5, i64::MAX, 42]
            .into_iter()
            .map(SequenceNumber::new)
            .collect::<SequenceNumberSet>();

        let intersection = intersect(&a, &b);
        let want = [i64::MAX, 42]
            .into_iter()
            .map(SequenceNumber::new)
            .collect::<SequenceNumberSet>();

        assert_eq!(intersection, want);
    }
}
