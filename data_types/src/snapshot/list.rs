//! A list of [`Message`] supporting efficient skipping

use bytes::Bytes;
use prost::Message;
use snafu::{ensure, Snafu};
use std::marker::PhantomData;
use std::ops::Range;

use generated_types::influxdata::iox::catalog_cache::v1 as generated;

/// Error type for [`MessageList`]
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
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
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct MessageList<T: Message + Default> {
    len: usize,
    offsets: Bytes,
    values: Bytes,
    phantom: PhantomData<T>,
}

impl<T: Message + Default> MessageList<T> {
    /// Encode `values` to a [`MessageList`]
    pub fn encode(values: &[T]) -> Result<Self> {
        let cap = (values.len() + 1) * 4;
        let mut offsets: Vec<u8> = Vec::with_capacity(cap);
        offsets.extend_from_slice(&0_u32.to_le_bytes());

        let mut cap = 0;
        for x in values {
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
}

impl<T: Message + Default> From<generated::MessageList> for MessageList<T> {
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

impl<T: Message + Default> From<MessageList<T>> for generated::MessageList {
    fn from(value: MessageList<T>) -> Self {
        Self {
            offsets: value.offsets,
            values: value.values,
        }
    }
}

impl<T: Message + Default> IntoIterator for MessageList<T> {
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
pub struct MessageListIter<T: Message + Default> {
    iter: Range<usize>,
    list: MessageList<T>,
}

impl<T: Message + Default> Iterator for MessageListIter<T> {
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

    #[test]
    fn test_simple() {
        let strings = ["", "test", "foo", "abc", "", "skd"];
        let strings: Vec<_> = strings.into_iter().map(ToString::to_string).collect();

        let encoded = MessageList::encode(&strings).unwrap();

        assert_eq!(encoded.get(5).unwrap().as_str(), "skd");
        assert_eq!(encoded.get(2).unwrap().as_str(), "foo");
        assert_eq!(encoded.get(0).unwrap().as_str(), "");

        let decoded: Vec<_> = encoded.clone().into_iter().map(Result::unwrap).collect();
        assert_eq!(strings, decoded);

        let proto = generated::MessageList::from(encoded.clone());
        let back = MessageList::<String>::from(proto.clone());
        assert_eq!(encoded, back);

        // Invalid decode should return error not panic
        let invalid = MessageList::<i32>::from(proto);
        invalid.get(2).unwrap_err();

        let strings: Vec<String> = vec![];
        let encoded = MessageList::encode(&strings).unwrap();
        assert_eq!(encoded.len(), 0);
        assert!(encoded.is_empty());

        let proto = generated::MessageList::default();
        let encoded = MessageList::<String>::from(proto);
        assert_eq!(encoded.len(), 0);
        assert!(encoded.is_empty());
    }
}
