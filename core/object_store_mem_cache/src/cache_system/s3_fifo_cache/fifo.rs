use std::collections::{VecDeque, vec_deque};

use crate::cache_system::HasSize;

#[derive(bincode::Encode)]
pub(crate) struct Fifo<T>
where
    T: HasSize,
{
    queue: VecDeque<T>,
    memory_size: usize,
}

impl<T> Fifo<T>
where
    T: HasSize,
{
    /// Create a new Fifo from a VecDeque.
    pub(crate) fn new(queue: VecDeque<T>) -> Self {
        let memory_size = queue.iter().map(|o| o.size()).sum();
        Self { queue, memory_size }
    }

    pub(crate) fn memory_size(&self) -> usize {
        self.memory_size
    }

    /// Return a count of items in the queue.
    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    pub(crate) fn iter(&self) -> vec_deque::Iter<'_, T> {
        self.queue.iter()
    }

    pub(crate) fn push_back(&mut self, o: T) {
        self.memory_size += o.size();
        self.queue.push_back(o);
    }

    pub(crate) fn pop_front(&mut self) -> Option<T> {
        match self.queue.pop_front() {
            Some(o) => {
                self.memory_size -= o.size();
                Some(o)
            }
            None => None,
        }
    }

    /// Drain all elements from the queue, consuming the underlying VecDeque
    /// and returning a iterator over the items.
    ///
    /// This preserves the ordering of elements and avoids re-allocation.
    pub(crate) fn drain(&mut self) -> impl Iterator<Item = T> {
        self.memory_size = 0;
        std::mem::take(&mut self.queue).into_iter()
    }
}

impl<T> Default for Fifo<T>
where
    T: HasSize,
{
    fn default() -> Self {
        Self {
            queue: Default::default(),
            memory_size: 0,
        }
    }
}

impl<T> std::fmt::Debug for Fifo<T>
where
    T: HasSize,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fifo")
            .field("memory_size", &self.memory_size)
            .finish_non_exhaustive()
    }
}

// bincode Decode implementation for Fifo
impl<T, Q> bincode::Decode<Q> for Fifo<T>
where
    T: bincode::Decode<Q> + HasSize + bincode::Encode,
{
    fn decode<D: bincode::de::Decoder<Context = Q>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let queue: std::collections::VecDeque<T> = bincode::Decode::decode(decoder)?;

        // This was encoded but we rather want to re-calculate it, just in case the `HasSize` implementation has changed.
        let _memory_size_decoded: usize = bincode::Decode::decode(decoder)?;

        Ok(Self::new(queue))
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
    fn test_new() {
        let fifo = Fifo::<TestData>::new(
            [TestData("foo".to_owned()), TestData("bar".to_owned())]
                .into_iter()
                .collect(),
        );
        assert_eq!(fifo.memory_size(), 6);
        assert_eq!(fifo.len(), 2);
    }

    #[test]
    fn test_push_pop() {
        let mut fifo = Fifo::<TestData>::new(Default::default());

        fifo.push_back(TestData("foo".to_owned()));
        assert_eq!(fifo.memory_size(), 3);
        assert_eq!(fifo.len(), 1);

        fifo.push_back(TestData("bar".to_owned()));
        assert_eq!(fifo.memory_size(), 6);
        assert_eq!(fifo.len(), 2);

        assert_eq!(fifo.pop_front(), Some(TestData("foo".to_owned())));
        assert_eq!(fifo.memory_size(), 3);
        assert_eq!(fifo.len(), 1);

        fifo.push_back(TestData("x".to_owned()));
        assert_eq!(fifo.memory_size(), 4);
        assert_eq!(fifo.len(), 2);

        assert_eq!(fifo.pop_front(), Some(TestData("bar".to_owned())));
        assert_eq!(fifo.memory_size(), 1);
        assert_eq!(fifo.len(), 1);

        assert_eq!(fifo.pop_front(), Some(TestData("x".to_owned())));
        assert_eq!(fifo.memory_size(), 0);
        assert_eq!(fifo.len(), 0);

        assert_eq!(fifo.pop_front(), None);
        assert_eq!(fifo.memory_size(), 0);
        assert_eq!(fifo.len(), 0);
    }

    #[test]
    fn test_drain() {
        let mut fifo = Fifo::<TestData>::new(Default::default());
        fifo.push_back(TestData("foo".to_owned()));
        fifo.push_back(TestData("bar".to_owned()));
        assert_eq!(fifo.memory_size(), 6);
        assert_eq!(fifo.len(), 2);

        let drained = fifo.drain().collect::<Vec<_>>();
        assert_eq!(
            drained,
            vec![TestData("foo".to_owned()), TestData("bar".to_owned())]
        );
        assert_eq!(fifo.len(), 0);
        assert_eq!(fifo.memory_size(), 0);
    }

    #[test]
    fn test_serde() {
        let mut fifo = Fifo::<TestData>::new(Default::default());
        fifo.push_back(TestData("foo".to_owned()));
        fifo.push_back(TestData("bar".to_owned()));

        let encoded = bincode::encode_to_vec(&fifo, bincode::config::standard())
            .expect("Failed to encode OrderedSet");

        // Ensure that our encoding is stable and backwards compatible.
        assert_eq!(encoded, vec![2, 3, 102, 111, 111, 3, 98, 97, 114, 6]);

        // Decode the OrderedSet
        let (decoded_fifo, bytes_decoded): (Fifo<TestData>, usize) =
            bincode::decode_from_slice(&encoded, bincode::config::standard())
                .expect("Failed to decode OrderedSet");
        assert_eq!(bytes_decoded, encoded.len(), "trailing data");

        assert_eq!(fifo.len(), decoded_fifo.len());
        assert_eq!(fifo.memory_size(), decoded_fifo.memory_size());
        let dump = decoded_fifo.iter().cloned().collect::<Vec<_>>();
        assert_eq!(
            dump,
            vec![TestData("foo".to_owned()), TestData("bar".to_owned())]
        );
    }
}
