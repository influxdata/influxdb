//! A set of [`SequenceNumber`] instances.

use crate::SequenceNumber;

/// A space-efficient encoded set of [`SequenceNumber`].
#[derive(Debug, Default, Clone)]
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

    /// Serialise `self` into a set of bytes.
    ///
    /// [This document][spec] describes the serialised format.
    ///
    /// [spec]: https://github.com/RoaringBitmap/RoaringFormatSpec/
    pub fn as_bytes(&self) -> Vec<u8> {
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
}
