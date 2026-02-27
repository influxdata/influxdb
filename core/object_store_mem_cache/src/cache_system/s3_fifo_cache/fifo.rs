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
        let memory_size: usize = bincode::Decode::decode(decoder)?;

        Ok(Self { queue, memory_size })
    }
}
