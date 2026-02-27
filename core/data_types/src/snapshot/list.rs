//! A list of [`Message`] supporting efficient skipping

use bytes::Bytes;
use prost::Message;
use snafu::{Snafu, ensure};
use std::{
    cmp::Ordering,
    marker::PhantomData,
    ops::{Deref, Range},
};

use generated_types::influxdata::iox::catalog_cache::v1 as generated;

/// Error type for [`MessageList`]
#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum Error {
    #[snafu(context(false), display("PackedList decode error: {source}"))]
    DecodeError { source: prost::DecodeError },

    #[snafu(context(false), display("PackedList encode error: {source}"))]
    EncodeError { source: prost::EncodeError },

    #[snafu(display("Invalid MessageList offsets: {start}..{end}"))]
    InvalidSlice { start: usize, end: usize },

    #[snafu(display("MessageList slice {start}..{end} out of bounds 0..{bounds}"))]
    SliceOutOfBounds {
        start: usize,
        end: usize,
        bounds: usize,
    },
}

/// How to get the `i64` identifier to enable `MessageList::get_by_id`
pub trait GetId {
    /// The ID value for this instance that may be used for lookup by `MessageList::get_by_id`
    fn id(&self) -> i64;
}

/// Ensures a list is sorted by ID, which is necessary for the binary search by ID
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SortedById<T: Message + Default + GetId> {
    values: Vec<T>,
}

impl<T: Message + Default + GetId> SortedById<T> {
    /// Create a new instance sorted by the ID of the items, which `MessageList` relies on for
    /// its implementation of `get_by_id`.
    pub fn new(mut values: Vec<T>) -> Self {
        values.sort_unstable_by_key(|v| v.id());

        SortedById { values }
    }
}

impl<T: Message + Default + GetId> FromIterator<T> for SortedById<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self::new(Vec::from_iter(iter))
    }
}

impl<T: Message + Default + GetId> Deref for SortedById<T> {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        self.values.deref()
    }
}

impl<T: Message + Default + GetId> IntoIterator for SortedById<T> {
    type Item = T;
    type IntoIter = std::vec::IntoIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

/// Error type for [`MessageList`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A packed list of [`Message`]
///
/// Normally protobuf encodes repeated fields by simply encoding the tag multiple times,
/// see [here](https://protobuf.dev/programming-guides/encoding/#optional).
///
/// Unfortunately this means it is not possible to locate a value at a given index without
/// decoding all prior records. [`MessageList`] therefore provides a list encoding, inspired
/// by arrow, that provides this and is designed to be combined with [`prost`]'s support
/// for zero-copy decoding of [`Bytes`]
#[derive(Clone, Eq, PartialEq)]
pub struct MessageList<T: Message + Default + GetId> {
    len: usize,
    offsets: Bytes,
    values: Bytes,
    phantom: PhantomData<T>,
}

impl<T: Message + Default + GetId> MessageList<T> {
    /// Encode `values` to a [`MessageList`]
    pub fn encode(values: SortedById<T>) -> Result<Self> {
        let cap = (values.len() + 1) * 4;
        let mut offsets: Vec<u8> = Vec::with_capacity(cap);
        offsets.extend_from_slice(&0_u32.to_le_bytes());

        let mut cap = 0;
        for x in values.iter() {
            cap += x.encoded_len();
            let offset = u32::try_from(cap).unwrap();
            offsets.extend_from_slice(&offset.to_le_bytes());
        }

        let mut data = Vec::with_capacity(cap);
        values.iter().try_for_each(|x| x.encode(&mut data))?;

        Ok(Self {
            len: values.len(),
            offsets: offsets.into(),
            values: data.into(),
            phantom: Default::default(),
        })
    }

    /// Returns true if this list is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of elements in this list
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns the element at index `idx`
    pub fn get(&self, idx: usize) -> Result<T> {
        let offset_start = idx * 4;
        let offset_slice = &self.offsets[offset_start..offset_start + 8];
        let start = u32::from_le_bytes(offset_slice[0..4].try_into().unwrap()) as usize;
        let end = u32::from_le_bytes(offset_slice[4..8].try_into().unwrap()) as usize;

        let bounds = self.values.len();
        ensure!(end >= start, InvalidSliceSnafu { start, end });
        ensure!(end <= bounds, SliceOutOfBoundsSnafu { start, end, bounds });

        // We slice `Bytes` to preserve zero-copy
        let data = self.values.slice(start..end);
        Ok(T::decode(data)?)
    }

    /// Returns the element with ID `id` found via binary search. Implemented by hand rather than
    /// calling `std::slice::binary_search_by` to avoid needing to allocate a vector to create a
    /// slice to call `binary_search_by` on. This code was largely copied from the stdlib's
    /// implementation of `binary_search_by` as of Rust 1.89.0.
    pub fn get_by_id(&self, id: i64) -> Result<Option<T>> {
        let mut size = self.len();
        if size == 0 {
            return Ok(None);
        }
        let mut base = 0usize;

        // This loop intentionally doesn't have an early exit if the comparison
        // returns Equal. We want the number of loop iterations to depend *only*
        // on the size of the input slice so that the CPU can reliably predict
        // the loop count.
        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            let current_item = self.get(mid)?;
            let cmp = current_item.id().cmp(&id);

            // Binary search interacts poorly with branch prediction, so force
            // the compiler to use conditional moves if supported by the target
            // architecture.
            base = std::hint::select_unpredictable(cmp == Ordering::Greater, base, mid);

            // This is imprecise in the case where `size` is odd and the
            // comparison returns Greater: the mid element still gets included
            // by `size` even though it's known to be larger than the element
            // being searched for.
            //
            // This is fine though: we gain more performance by keeping the
            // loop iteration count invariant (and thus predictable) than we
            // lose from considering one additional element.
            size -= half;
        }

        let base_item = self.get(base)?;
        let cmp = base_item.id().cmp(&id);
        if cmp == Ordering::Equal {
            Ok(Some(base_item))
        } else {
            Ok(None)
        }
    }
}

impl<T: Message + Default + GetId> std::fmt::Debug for MessageList<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut l = f.debug_list();
        for idx in 0..self.len() {
            l.entry(&self.get(idx));
        }
        l.finish()
    }
}

impl<T: Message + Default + GetId> From<generated::MessageList> for MessageList<T> {
    fn from(proto: generated::MessageList) -> Self {
        let len = (proto.offsets.len() / 4).saturating_sub(1);
        Self {
            len,
            offsets: proto.offsets,
            values: proto.values,
            phantom: Default::default(),
        }
    }
}

impl<T: Message + Default + GetId> From<MessageList<T>> for generated::MessageList {
    fn from(value: MessageList<T>) -> Self {
        Self {
            offsets: value.offsets,
            values: value.values,
        }
    }
}

impl<T: Message + Default + GetId> IntoIterator for MessageList<T> {
    type Item = Result<T>;
    type IntoIter = MessageListIter<T>;

    fn into_iter(self) -> Self::IntoIter {
        MessageListIter {
            iter: (0..self.len),
            list: self,
        }
    }
}

/// [`Iterator`] for [`MessageList`]
#[derive(Debug)]
pub struct MessageListIter<T: Message + Default + GetId> {
    iter: Range<usize>,
    list: MessageList<T>,
}

impl<T: Message + Default + GetId> Iterator for MessageListIter<T> {
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.list.get(self.iter.next()?))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// As a simple hack for ease of test setup, the ID of a string is its length.
    impl GetId for String {
        fn id(&self) -> i64 {
            self.len() as i64
        }
    }

    impl GetId for i32 {
        fn id(&self) -> i64 {
            *self as i64
        }
    }

    #[test]
    fn test_simple() {
        // Note this list is not sorted by ID (`len`)
        let strings = ["", "test", "foo", "abcde", "z", "skedaddle"];
        let strings: SortedById<_> = strings.into_iter().map(ToString::to_string).collect();

        let encoded = MessageList::encode(strings.clone()).unwrap();

        assert_eq!(encoded.get(0).unwrap().as_str(), "");
        assert_eq!(encoded.get_by_id(0).unwrap().unwrap().as_str(), "");

        assert_eq!(encoded.get(1).unwrap().as_str(), "z");
        assert_eq!(encoded.get_by_id(1).unwrap().unwrap().as_str(), "z");

        // there is no value with ID 2
        assert!(encoded.get_by_id(2).unwrap().is_none());

        assert_eq!(encoded.get(2).unwrap().as_str(), "foo");
        assert_eq!(encoded.get_by_id(3).unwrap().unwrap().as_str(), "foo");

        assert_eq!(encoded.get(3).unwrap().as_str(), "test");
        assert_eq!(encoded.get_by_id(4).unwrap().unwrap().as_str(), "test");

        assert_eq!(encoded.get(4).unwrap().as_str(), "abcde");
        assert_eq!(encoded.get_by_id(5).unwrap().unwrap().as_str(), "abcde");

        assert_eq!(encoded.get(5).unwrap().as_str(), "skedaddle");
        assert_eq!(encoded.get_by_id(9).unwrap().unwrap().as_str(), "skedaddle");

        let decoded: SortedById<_> = encoded.clone().into_iter().map(Result::unwrap).collect();
        assert_eq!(strings, decoded);

        let proto = generated::MessageList::from(encoded.clone());
        let back = MessageList::<String>::from(proto.clone());
        assert_eq!(encoded, back);

        // Invalid decode should return error not panic
        let invalid = MessageList::<i32>::from(proto);
        invalid.get(2).unwrap_err();

        let strings: SortedById<String> = SortedById::new(vec![]);
        let encoded = MessageList::encode(strings).unwrap();
        assert_eq!(encoded.len(), 0);
        assert!(encoded.is_empty());

        let proto = generated::MessageList::default();
        let encoded = MessageList::<String>::from(proto);
        assert_eq!(encoded.len(), 0);
        assert!(encoded.is_empty());
    }
}
