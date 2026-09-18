//! A cache of fixed size.

use std::marker::PhantomData;

/// A set-associative cache holding `V` against `K`.
///
/// The cache is allocated once and never grows: a key maps to one *set* of
/// `ways` slots and may only live there, so the number of entries follows the
/// size it was built with rather than the number of distinct keys it is asked
/// about. When a set is full the slot used longest ago makes way, which makes
/// exceeding the size a matter of losing entries rather than of taking more
/// memory.
///
/// `ways` is how many slots a set holds. A lookup scans the whole set, so it is
/// both the cost of every lookup and what decides how likely two keys in use at
/// the same time are to displace each other: one way is a direct-mapped cache,
/// where a collision lasts as long as both keys are live, while a handful of
/// ways makes that unlikely.
///
/// A key is used as the number it converts into, so [`Into<u64>`] must give a
/// different number for every key that should be held separately.
pub(super) struct NWayCache<K, V> {
    /// `sets * ways` slots, a set at a time.
    slots: Vec<Option<Entry<V>>>,

    /// One less than the number of sets, which is a power of two, so that a
    /// hash reaches its set by masking.
    mask: usize,

    /// How far to rotate a hash to bring the bits the mask keeps down to the
    /// bottom: the number of sets is `1 << bits`.
    bits: u32,

    ways: usize,

    /// Stamped onto a slot whenever it is used.
    clock: u64,

    key: PhantomData<K>,
}

struct Entry<V> {
    /// The key this was stored against, as the number it converts into.
    key: u64,

    value: V,

    /// When this slot was last used, for choosing which of a set to displace.
    used: u64,
}

impl<K, V> NWayCache<K, V> {
    /// A cache of at least `entries` entries, in sets of at least `ways` slots.
    ///
    /// Both are rounded up to a power of two, which is what lets a key reach
    /// its set by masking. The cache therefore holds at least what was asked
    /// for, and at most twice it. No entries, or no ways to hold them in, gives
    /// a cache that keeps nothing.
    pub(super) fn new(entries: usize, ways: usize) -> Self {
        let (sets, ways) = Self::shape(entries, ways);
        if sets == 0 {
            return Self {
                slots: Vec::new(),
                mask: 0,
                bits: 0,
                ways: 0,
                clock: 0,
                key: PhantomData,
            };
        }
        Self {
            slots: (0..sets * ways).map(|_| None).collect(),
            mask: sets - 1,
            bits: sets.trailing_zeros(),
            ways,
            clock: 0,
            key: PhantomData,
        }
    }

    /// The number of sets and the ways in each that [`new`](Self::new) builds
    /// for these arguments.
    fn shape(entries: usize, ways: usize) -> (usize, usize) {
        if entries == 0 || ways == 0 {
            return (0, 0);
        }
        let ways = ways.next_power_of_two();
        (entries.div_ceil(ways).next_power_of_two(), ways)
    }

    /// The bytes a cache built by [`new`](Self::new) with these arguments
    /// would occupy, known without building it.
    pub(super) fn bytes_for(entries: usize, ways: usize) -> usize {
        let (sets, ways) = Self::shape(entries, ways);
        sets * ways * size_of::<Option<Entry<V>>>()
    }

    /// The number of entries the cache can hold, which is zero when it keeps
    /// nothing.
    pub(super) fn capacity(&self) -> usize {
        self.slots.len()
    }
}

impl<K: Into<u64>, V> NWayCache<K, V> {
    /// Where `key`'s set begins.
    ///
    /// The multiplication is Fibonacci hashing, whose carries run upwards, so
    /// the top bits are the well mixed ones. The rotation brings exactly those
    /// down to where the mask keeps them, which is also what spreads runs of
    /// consecutive keys evenly rather than merely arbitrarily.
    fn set(&self, key: u64) -> usize {
        let hash = key.wrapping_mul(0x9E37_79B9_7F4A_7C15);
        (hash.rotate_left(self.bits) as usize & self.mask) * self.ways
    }

    pub(super) fn get(&mut self, key: K) -> Option<&V> {
        if self.slots.is_empty() {
            return None;
        }
        let key = key.into();
        let set = self.set(key);
        let way = self.slots[set..set + self.ways]
            .iter()
            .position(|slot| slot.as_ref().is_some_and(|entry| entry.key == key))?;

        self.clock += 1;
        let used = self.clock;
        let entry = self.slots[set + way]
            .as_mut()
            .expect("the slot just matched");
        entry.used = used;
        Some(&entry.value)
    }

    /// Take `key`'s value out of the cache, if it is there.
    pub(super) fn remove(&mut self, key: K) -> Option<V> {
        if self.slots.is_empty() {
            return None;
        }
        let key = key.into();
        let set = self.set(key);
        let way = self.slots[set..set + self.ways]
            .iter()
            .position(|slot| slot.as_ref().is_some_and(|entry| entry.key == key))?;
        self.slots[set + way].take().map(|entry| entry.value)
    }

    /// Hold `value` against `key`, returning whatever the cache no longer
    /// holds: the value displaced to make room, or `value` itself when the
    /// cache keeps nothing.
    pub(super) fn insert(&mut self, key: K, value: V) -> Option<V> {
        if self.slots.is_empty() {
            return Some(value);
        }
        let key = key.into();
        let set = self.set(key);
        self.clock += 1;
        let used = self.clock;

        // This key's own entry, or else the slot used longest ago. The clock
        // starts at one, so an empty slot scores zero and is taken first.
        let ways = &self.slots[set..set + self.ways];
        let way = ways
            .iter()
            .position(|slot| slot.as_ref().is_some_and(|entry| entry.key == key))
            .unwrap_or_else(|| {
                ways.iter()
                    .enumerate()
                    .min_by_key(|(_, slot)| slot.as_ref().map_or(0, |entry| entry.used))
                    .map(|(way, _)| way)
                    .expect("a set has at least one way")
            });

        self.slots[set + way]
            .replace(Entry { key, value, used })
            .map(|entry| entry.value)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use proptest::prelude::*;

    use super::*;

    /// A key whose number is its own value, so that tests can reason about
    /// which keys share a set.
    #[derive(Clone, Copy)]
    struct Key(u64);

    impl From<Key> for u64 {
        fn from(key: Key) -> Self {
            key.0
        }
    }

    fn cache(entries: usize, ways: usize) -> NWayCache<Key, u64> {
        NWayCache::new(entries, ways)
    }

    #[test]
    fn size_rounds_up_to_a_power_of_two_of_sets() {
        // Sizes that are already whole powers of two are taken as they are.
        assert_eq!(16, cache(16, 4).capacity());
        assert_eq!(8, cache(8, 1).capacity());
        assert_eq!(4, cache(1, 4).capacity());

        // Anything else rounds up, never down.
        assert_eq!(32, cache(17, 4).capacity());
        assert_eq!(8, cache(7, 1).capacity());

        // Ways round up too, so that a set is a power of two wide.
        assert_eq!(16, cache(16, 3).capacity());
        assert_eq!(32, cache(20, 5).capacity());
    }

    #[test]
    fn a_cache_of_no_entries_keeps_nothing() {
        for (entries, ways) in [(0, 4), (16, 0), (0, 0)] {
            assert_eq!(0, NWayCache::<Key, u64>::bytes_for(entries, ways));
            let mut empty = cache(entries, ways);
            assert_eq!(0, empty.capacity());
            // The value comes straight back, because it was not kept.
            assert_eq!(Some(7), empty.insert(Key(1), 7));
            assert_eq!(None, empty.get(Key(1)));
        }
    }

    #[test]
    fn a_value_can_be_read_back() {
        let mut cache = cache(64, 4);
        for key in 0..40 {
            assert_eq!(None, cache.insert(Key(key), key * 10));
        }
        for key in 0..40 {
            assert_eq!(Some(&(key * 10)), cache.get(Key(key)));
        }
    }

    #[test]
    fn inserting_the_same_key_again_displaces_its_own_value() {
        let mut cache = cache(64, 4);
        assert_eq!(None, cache.insert(Key(3), 30));
        assert_eq!(Some(30), cache.insert(Key(3), 31));
        assert_eq!(Some(&31), cache.get(Key(3)));
    }

    #[test]
    fn a_full_set_displaces_the_slot_used_longest_ago() {
        // One set, so every key competes for the same four slots.
        let mut cache = cache(4, 4);
        for key in 0..4 {
            assert_eq!(None, cache.insert(Key(key), key));
        }
        // Touch every key but 1, making it the one used longest ago.
        for key in [0, 2, 3] {
            assert_eq!(Some(&key), cache.get(Key(key)));
        }
        assert_eq!(Some(1), cache.insert(Key(4), 4));
        assert_eq!(None, cache.get(Key(1)));
        for key in [0, 2, 3, 4] {
            assert_eq!(Some(&key), cache.get(Key(key)));
        }
    }

    #[test]
    fn a_removed_value_is_gone_and_its_slot_reused() {
        // One set, so the removed key's slot is the only free one.
        let mut cache = cache(4, 4);
        for key in 0..4 {
            assert_eq!(None, cache.insert(Key(key), key));
        }
        assert_eq!(Some(2), cache.remove(Key(2)));
        assert_eq!(None, cache.remove(Key(2)));
        assert_eq!(None, cache.get(Key(2)));

        // The freed slot takes the next key without displacing anyone.
        assert_eq!(None, cache.insert(Key(9), 9));
        for key in [0, 1, 3, 9] {
            assert_eq!(Some(&key), cache.get(Key(key)));
        }
    }

    #[test]
    fn more_keys_than_slots_costs_entries_and_not_memory() {
        let mut cache = cache(16, 4);
        let mut held = 0;
        for key in 0..1000 {
            cache.insert(Key(key), key);
        }
        for key in 0..1000 {
            held += usize::from(cache.get(Key(key)).is_some());
        }
        assert!(
            held <= cache.capacity(),
            "{held} entries in {} slots",
            cache.capacity()
        );
        assert_eq!(16, cache.capacity(), "the cache never grows");
    }

    #[derive(Debug, Clone)]
    enum Op {
        Insert(u64),
        Get(u64),
        Remove(u64),
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            4 => (0u64..32).prop_map(Op::Insert),
            2 => (0u64..32).prop_map(Op::Get),
            1 => (0u64..32).prop_map(Op::Remove),
        ]
    }

    proptest! {
        /// Any sequence of operations leaves the cache holding exactly what a
        /// plain map would say it should.
        ///
        /// Each value records the key it was stored against and when, so that
        /// whatever the cache hands back can be traced to the entry it came
        /// from.
        #[test]
        fn behaves_like_a_map_that_forgets(
            entries in 0usize..12,
            ways in 0usize..5,
            ops in prop::collection::vec(op(), 0..200),
        ) {
            let mut cache = NWayCache::<Key, (u64, u64)>::new(entries, ways);
            let mut model = HashMap::<u64, u64>::new();

            for (stamp, op) in ops.into_iter().enumerate() {
                let stamp = stamp as u64;
                match op {
                    Op::Insert(key) => match cache.insert(Key(key), (key, stamp)) {
                        // Handed straight back: the cache keeps nothing.
                        Some((_, returned)) if returned == stamp => {
                            prop_assert_eq!(0, cache.capacity());
                        }
                        Some((displaced, when)) => {
                            prop_assert_eq!(Some(when), model.remove(&displaced));
                            model.insert(key, stamp);
                        }
                        None => {
                            prop_assert!(!model.contains_key(&key));
                            model.insert(key, stamp);
                        }
                    },
                    Op::Get(key) => {
                        let expected = model.get(&key).map(|&when| (key, when));
                        prop_assert_eq!(expected, cache.get(Key(key)).copied());
                    }
                    Op::Remove(key) => {
                        let expected = model.remove(&key).map(|when| (key, when));
                        prop_assert_eq!(expected, cache.remove(Key(key)));
                    }
                }
            }
        }
    }
}
