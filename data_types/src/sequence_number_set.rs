//! A set of [`SequenceNumber`] instances.

use std::collections::BTreeMap;

use croaring::treemap::NativeSerializer;

use crate::SequenceNumber;

/// A space-efficient encoded set of [`SequenceNumber`].
#[derive(Debug, Default, Clone, PartialEq)]
pub struct SequenceNumberSet(croaring::Treemap);

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
        self.0
            .serialize()
            .expect("failed to serialise sequence number set")
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
        let mut map = BTreeMap::new();
        map.insert(0, croaring::Bitmap::create_with_capacity(n));
        Self(croaring::Treemap { map })
    }
}

/// Deserialisation method.
impl TryFrom<&[u8]> for SequenceNumberSet {
    type Error = String;

    fn try_from(buffer: &[u8]) -> Result<Self, Self::Error> {
        croaring::Treemap::deserialize(buffer)
            .map(SequenceNumberSet)
            .map_err(|e| format!("failed to deserialise sequence number set: {e}"))
    }
}

impl Extend<SequenceNumber> for SequenceNumberSet {
    fn extend<T: IntoIterator<Item = SequenceNumber>>(&mut self, iter: T) {
        self.0.extend(iter.into_iter().map(|v| v.get() as _))
    }
}

impl Extend<SequenceNumberSet> for SequenceNumberSet {
    fn extend<T: IntoIterator<Item = SequenceNumberSet>>(&mut self, iter: T) {
        for new_set in iter {
            self.add_set(&new_set);
        }
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
    use std::collections::HashSet;

    use proptest::{prelude::prop, proptest, strategy::Strategy};

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
    fn test_extend_multiple_sets() {
        let mut a = SequenceNumberSet::default();
        a.add(SequenceNumber::new(7));

        let b = [SequenceNumber::new(13), SequenceNumber::new(76)];
        let c = [SequenceNumber::new(42), SequenceNumber::new(64)];

        assert!(a.contains(SequenceNumber::new(7)));
        for &num in [b, c].iter().flatten() {
            assert!(!a.contains(num));
        }

        a.extend([
            SequenceNumberSet::from_iter(b),
            SequenceNumberSet::from_iter(c),
        ]);
        assert!(a.contains(SequenceNumber::new(7)));
        for &num in [b, c].iter().flatten() {
            assert!(a.contains(num));
        }
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
        let a = [0, u64::MAX, 40, 41, 42, 43, 44, 45]
            .into_iter()
            .map(SequenceNumber::new)
            .collect::<SequenceNumberSet>();

        let b = [1, 5, u64::MAX, 42]
            .into_iter()
            .map(SequenceNumber::new)
            .collect::<SequenceNumberSet>();

        let intersection = intersect(&a, &b);
        let want = [u64::MAX, 42]
            .into_iter()
            .map(SequenceNumber::new)
            .collect::<SequenceNumberSet>();

        assert_eq!(intersection, want);
    }

    /// Yield vec's of [`SequenceNumber`] derived from u64 values.
    ///
    /// This matches how the ingester allocates [`SequenceNumber`] - from a u64
    /// source.
    fn sequence_number_vec() -> impl Strategy<Value = Vec<SequenceNumber>> {
        prop::collection::vec(0..u64::MAX, 0..1024)
            .prop_map(|vec| vec.into_iter().map(SequenceNumber::new).collect())
    }

    // The following tests compare to an order-independent HashSet, as the
    // SequenceNumber uses the PartialOrd impl of the inner u64 for ordering,
    // resulting in incorrect output when compared to an ordered set of cast as
    // u64.
    //
    //      https://github.com/influxdata/influxdb_iox/issues/7260
    //
    // These tests also cover, collect()-ing to a SequenceNumberSet, etc.
    proptest! {
        /// Perform a SequenceNumberSet intersection test comparing the results
        /// to the known-good stdlib HashSet intersection implementation.
        #[test]
        fn prop_set_intersection(
                a in sequence_number_vec(),
                b in sequence_number_vec()
            ) {
            let known_a = a.iter().cloned().collect::<HashSet<_>>();
            let known_b = b.iter().cloned().collect::<HashSet<_>>();
            let set_a = a.into_iter().collect::<SequenceNumberSet>();
            let set_b = b.into_iter().collect::<SequenceNumberSet>();

            // The sets should be equal
            assert_eq!(set_a.iter().collect::<HashSet<_>>(), known_a, "set a does not match");
            assert_eq!(set_b.iter().collect::<HashSet<_>>(), known_b, "set b does not match");

            let known_intersection = known_a.intersection(&known_b).cloned().collect::<HashSet<_>>();
            let set_intersection = intersect(&set_a, &set_b).iter().collect::<HashSet<_>>();

            // The set intersections should be equal.
            assert_eq!(set_intersection, known_intersection);
        }

        /// Perform a SequenceNumberSet remove_set test comparing the results to
        /// the known-good stdlib HashSet difference implementation.
        #[test]
        fn prop_set_difference(
            a in sequence_number_vec(),
            b in sequence_number_vec()
        ) {
            let known_a = a.iter().cloned().collect::<HashSet<_>>();
            let known_b = b.iter().cloned().collect::<HashSet<_>>();
            let mut set_a = a.into_iter().collect::<SequenceNumberSet>();
            let set_b = b.into_iter().collect::<SequenceNumberSet>();

            // The sets should be equal
            assert_eq!(set_a.iter().collect::<HashSet<_>>(), known_a, "set a does not match");
            assert_eq!(set_b.iter().collect::<HashSet<_>>(), known_b, "set b does not match");

            let known_a = known_a.difference(&known_b).cloned().collect::<HashSet<_>>();
            set_a.remove_set(&set_b);
            let set_a = set_a.iter().collect::<HashSet<_>>();

            // The set difference should be equal.
            assert_eq!(set_a, known_a);
        }

        /// Perform a SequenceNumberSet add_set test comparing the results to
        /// the known-good stdlib HashSet or implementation.
        #[test]
        fn prop_set_add(
            a in sequence_number_vec(),
            b in sequence_number_vec()
        ) {
            let known_a = a.iter().cloned().collect::<HashSet<_>>();
            let known_b = b.iter().cloned().collect::<HashSet<_>>();
            let mut set_a = a.into_iter().collect::<SequenceNumberSet>();
            let set_b = b.into_iter().collect::<SequenceNumberSet>();

            // The sets should be equal
            assert_eq!(set_a.iter().collect::<HashSet<_>>(), known_a, "set a does not match");
            assert_eq!(set_b.iter().collect::<HashSet<_>>(), known_b, "set b does not match");

            let known_a = known_a.union(&known_b).cloned().collect::<HashSet<_>>();
            set_a.add_set(&set_b);
            let set_a = set_a.iter().collect::<HashSet<_>>();

            // The sets should be equal.
            assert_eq!(set_a, known_a);
        }

        /// Assert that a SequenceNumberSet deserialised from its serialised
        /// representation is equal (round-trippable).
        #[test]
        fn prop_serialise_deserialise(
            a in sequence_number_vec()
        ) {
            let orig = a.iter().cloned().collect::<SequenceNumberSet>();

            let serialised = orig.to_bytes();
            let got = SequenceNumberSet::try_from(&*serialised).expect("failed to deserialise valid set");

            assert_eq!(got, orig);
        }
    }
}
