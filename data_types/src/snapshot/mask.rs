//! A packed bitmask

use arrow_buffer::bit_iterator::BitIndexIterator;
use arrow_buffer::bit_util::{ceil, set_bit};
use bytes::Bytes;
use generated_types::influxdata::iox::catalog_cache::v1 as generated;

/// A packed bitmask
#[derive(Debug, Clone)]
pub struct BitMask {
    mask: Bytes,
    len: usize,
}

impl BitMask {
    /// Returns an iterator of the set indices in this mask
    pub fn set_indices(&self) -> BitIndexIterator<'_> {
        BitIndexIterator::new(&self.mask, 0, self.len)
    }
}

impl From<generated::BitMask> for BitMask {
    fn from(value: generated::BitMask) -> Self {
        Self {
            mask: value.mask,
            len: value.len as _,
        }
    }
}

impl From<BitMask> for generated::BitMask {
    fn from(value: BitMask) -> Self {
        Self {
            mask: value.mask,
            len: value.len as _,
        }
    }
}

/// A builder for [`BitMask`]
#[derive(Debug)]
pub struct BitMaskBuilder {
    values: Vec<u8>,
    len: usize,
}

impl BitMaskBuilder {
    /// Create a new bitmask able to store `len` boolean values
    #[inline]
    pub fn new(len: usize) -> Self {
        Self {
            values: vec![0; ceil(len, 8)],
            len,
        }
    }

    /// Set the bit at index `idx`
    #[inline]
    pub fn set_bit(&mut self, idx: usize) {
        set_bit(&mut self.values, idx)
    }

    /// Return the built [`BitMask`]
    #[inline]
    pub fn finish(self) -> BitMask {
        BitMask {
            mask: self.values.into(),
            len: self.len,
        }
    }
}
