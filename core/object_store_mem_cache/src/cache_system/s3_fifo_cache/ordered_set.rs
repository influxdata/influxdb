use bincode::{Decode, Encode};
use indexmap::{Equivalent, IndexSet};
use std::hash::Hash;

use crate::cache_system::HasSize;

/// And ordered set with size accounting.
///
/// # Data Structure
/// This uses an [`IndexSet`] under the hood but instead of [removing](IndexSet::shift_remove) elements right away, it
/// replaces them with tombstones.
///
/// This prevents from [`remove`](Self::remove) from being in `O(n)` (worst case), but reduces it to an amortized
/// `O(1)` (on average).
///
/// In exchange, [`pop_front`](Self::pop_front) is not truly `O(1)` anymore but only in the amortized case. It's now
/// `O(n)` in the worst case but basically only for one request because it would remove all processed tombstones. Since
/// we limit the number of tombstones to 50% of the entries within the [`IndexSet`], the number of processed tombstones
/// amortizes.
///
/// This is a worthwile tradeoff though.
pub(crate) struct OrderedSet<T>
where
    T: Eq + Hash + HasSize,
{
    /// The core of this data structure.
    set: IndexSet<Entry<T>>,

    /// The memory sizes of all non-tomstone data contained in this set.
    ///
    /// This is affectively the sum of [`HasSize::size`].
    memory_size: usize,

    /// Ever-increasing counter to give every tombstone a unique identity.
    tombstone_counter: u64,

    /// Number of tombstones in the [set](Self::set).
    n_tombstones: usize,
}

impl<T> OrderedSet<T>
where
    T: Eq + Hash + HasSize,
{
    /// Current amount of memory used by all entries.
    ///
    /// This does NOT include the memory consumption of the underlying [`IndexSet`] or the tombstones. They are
    /// considered rather minor if you store actual data.
    ///
    /// # Runtime Complexity
    /// This is always `O(1)`.
    ///
    /// This data is pre-computed and cached, so this is just a memory read.
    pub(crate) fn memory_size(&self) -> usize {
        self.memory_size
    }

    /// Push new entry back to the set.
    ///
    /// # Panic
    /// The entry MUST NOT be part of the set yet.
    ///
    /// # Runtime Complexity
    /// This is always `O(1)`.
    pub(crate) fn push_back(&mut self, o: T) {
        self.memory_size += o.size();
        let is_new = self.set.insert(Entry::Data(o));
        assert!(is_new);
    }

    /// Pop entry from the front of the set.
    ///
    /// This may be called on and empty set.
    ///
    /// # Runtime Complexity
    /// This amortizes to `O(1)`.
    ///
    /// If there are only tombstones at the start of the set, this may be in `O(n)` though. The good thing is that the
    /// tombstoes will be gone afterwards, so that will be a one-time clean-up.
    pub(crate) fn pop_front(&mut self) -> Option<T> {
        loop {
            if self.set.is_empty() {
                return None;
            }

            match self.set.shift_remove_index(0).expect("should exist") {
                Entry::Data(o) => {
                    self.memory_size -= o.size();
                    return Some(o);
                }
                Entry::Tombstone(_) => {
                    self.n_tombstones -= 1;
                    // need to continue
                }
            }
        }
    }

    /// Remove element from the middle of the set.
    ///
    /// The relative order of the elements is preserved.
    ///
    /// Returns `true` if the entry was part of the set.
    ///
    /// # Runtime Complexity
    /// This amortizes to `O(1)`.
    ///
    /// If the number of tombstones after the removal would be larger than 50% of the entries within the [`IndexSet`],
    /// this will compact the data by removing all the tombstones. That will effectively rewrite the [`IndexSet`] and
    /// has `O(n)` complexity.
    pub(crate) fn remove(&mut self, o: &T) -> bool {
        match self.set.get_index_of(&Entry::Data(o)) {
            Some(idx) => {
                // replace entry w/ tombstone
                self.set.insert(Entry::Tombstone(self.tombstone_counter));
                self.tombstone_counter += 1;
                self.n_tombstones += 1;
                self.set
                    .swap_remove_index(idx)
                    .expect("just got this index");

                // account memory size
                self.memory_size -= o.size();

                // maybe compact data
                // NOTE: Use `>`, NOT `>=` here to prevent compactions for empty containers
                if self.n_tombstones * 2 > self.set.len() {
                    self.set.retain(|entry| matches!(entry, Entry::Data(_)));
                    self.n_tombstones = 0;
                }

                true
            }
            None => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.set.len()
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, o: &T) -> bool {
        self.set.contains(&Entry::Data(o))
    }
}

/// Encode implementation, with trait bounds for `T`.
impl<T> Encode for OrderedSet<T>
where
    T: Encode + Eq + Hash + HasSize,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        // Encode IndexSet entries as a hand-rolled vec
        // refer to https://github.com/bincode-org/bincode/blob/trunk/docs/spec.md#linear-collections-vec-arrays-etc
        Encode::encode(&(self.set.len() as u8), encoder)?;
        for entry in &self.set {
            Encode::encode(entry, encoder)?;
        }

        Encode::encode(&self.memory_size, encoder)?;
        Encode::encode(&self.tombstone_counter, encoder)?;
        Encode::encode(&self.n_tombstones, encoder)?;
        Ok(())
    }
}

impl<T, Q> Decode<Q> for OrderedSet<T>
where
    T: Decode<Q> + Encode + Hash + Eq + HasSize,
{
    fn decode<D: bincode::de::Decoder<Context = Q>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let set_len: u8 = Decode::decode(decoder)?;
        // Manually reconstruct the IndexSet from decoded entries
        // since IndexSet does not implement Decode.
        let mut set = IndexSet::new();
        for _ in 0..set_len {
            let entry: Entry<T> = Decode::decode(decoder)?;
            set.insert(entry);
        }

        let memory_size: usize = Decode::decode(decoder)?;
        let tombstone_counter: u64 = Decode::decode(decoder)?;
        let n_tombstones: usize = Decode::decode(decoder)?;

        Ok(Self {
            set,
            memory_size,
            tombstone_counter,
            n_tombstones,
        })
    }
}

impl<T> std::fmt::Debug for OrderedSet<T>
where
    T: Eq + Hash + HasSize,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OrderedSet")
            .field("len", &self.set.len())
            .field("memory_size", &self.memory_size)
            .field("n_tombstones", &self.n_tombstones)
            .finish_non_exhaustive()
    }
}

impl<T> Default for OrderedSet<T>
where
    T: Eq + Hash + HasSize,
{
    fn default() -> Self {
        Self {
            set: Default::default(),
            memory_size: 0,
            tombstone_counter: 0,
            n_tombstones: 0,
        }
    }
}

const DATA_ENTRY: u8 = 1;
const TOMBSTONE_ENTRY: u8 = 2;

/// Underlying entry of [`OrderedSet`].
#[derive(PartialEq, Eq, Hash)]
#[repr(u8)]
enum Entry<T>
where
    T: Eq + Hash,
{
    /// Actual data.
    Data(T) = DATA_ENTRY,

    /// A tombstone with a unique ID.
    ///
    /// The unique ID prevents two tombstones from being equal and also spreads the hash values.
    Tombstone(u64) = TOMBSTONE_ENTRY,
}

impl<T> Entry<T>
where
    T: Eq + Hash,
{
    fn discriminant(&self) -> u8 {
        unsafe { *(self as *const Self as *const u8) }
    }
}

impl<T> Equivalent<Entry<T>> for Entry<&T>
where
    T: Eq + Hash,
{
    fn equivalent(&self, key: &Entry<T>) -> bool {
        let key = match key {
            Entry::Data(o) => Entry::Data(o),
            Entry::Tombstone(idx) => Entry::Tombstone(*idx),
        };
        self.equivalent(&key)
    }
}

/// Encode implementation, with trait bounds for `T`.
impl<T> Encode for Entry<T>
where
    T: Encode + Eq + Hash,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        // Encode the variant discriminant first
        Encode::encode(&self.discriminant(), encoder)?;

        // Encode the actual data based on the variant
        match self {
            Self::Data(data) => Encode::encode(data, encoder)?,
            Self::Tombstone(id) => Encode::encode(id, encoder)?,
        }

        Ok(())
    }
}

impl<T, Q> Decode<Q> for Entry<T>
where
    T: Decode<Q> + Encode + Eq + Hash,
{
    fn decode<D: bincode::de::Decoder<Context = Q>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        // Read the variant discriminant first
        let variant: u8 = Decode::decode(decoder)?;

        match variant {
            DATA_ENTRY => {
                // Data variant
                let data: T = Decode::decode(decoder)?;
                Ok(Self::Data(data))
            }
            TOMBSTONE_ENTRY => {
                // Tombstone variant
                let id: u64 = Decode::decode(decoder)?;
                Ok(Self::Tombstone(id))
            }
            _ => Err(bincode::error::DecodeError::OtherString(format!(
                "Invalid variant {variant} for Entry enum, expected 0 or 1",
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{Decode, Encode};

    // Simple test type that implements all required traits
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
    struct TestData(String);

    impl HasSize for TestData {
        fn size(&self) -> usize {
            self.0.len()
        }
    }

    #[test]
    fn test_ordered_set_encode_decode_with_data_and_tombstones() {
        // Create an OrderedSet and populate it with data
        let mut ordered_set = OrderedSet::default();

        // Add some data entries
        ordered_set.push_back(TestData("first".to_string()));
        ordered_set.push_back(TestData("second".to_string()));
        ordered_set.push_back(TestData("third".to_string()));
        ordered_set.push_back(TestData("fourth".to_string()));

        // Remove some entries to create tombstones
        assert!(ordered_set.remove(&TestData("second".to_string())));
        assert!(ordered_set.remove(&TestData("fourth".to_string())));

        // Verify the state before encoding
        let original_memory_size = ordered_set.memory_size();
        let original_len = ordered_set.len();
        let original_n_tombstones = ordered_set.n_tombstones;
        let original_tombstone_counter = ordered_set.tombstone_counter;
        assert_eq!(original_memory_size, "first".len() + "third".len());
        assert_eq!(original_len, 4); // 2 data + 2 tombstones
        assert_eq!(original_n_tombstones, 2);
        assert_eq!(original_tombstone_counter, 2);

        // Encode the OrderedSet
        let encoded = bincode::encode_to_vec(&ordered_set, bincode::config::standard())
            .expect("Failed to encode OrderedSet");

        // Decode the OrderedSet
        let (decoded_set, _): (OrderedSet<TestData>, usize) =
            bincode::decode_from_slice(&encoded, bincode::config::standard())
                .expect("Failed to decode OrderedSet");

        // Verify the decoded state matches the original
        assert_eq!(decoded_set.memory_size(), original_memory_size);
        assert_eq!(decoded_set.len(), original_len);
        assert_eq!(decoded_set.n_tombstones, original_n_tombstones);
        assert_eq!(decoded_set.tombstone_counter, original_tombstone_counter);

        // Verify that Data & Tombstone entries are correctly decoded
        assert!(
            matches!(decoded_set.set[0], Entry::Data(ref data) if data == &TestData("first".to_string()))
        );
        assert!(matches!(decoded_set.set[1], Entry::Tombstone(_)));
    }
}
