use arrow::array::ArrayDataBuilder;
use arrow::array::StringArray;
use arrow::buffer::Buffer;
use num_traits::{AsPrimitive, FromPrimitive, Zero};
use std::fmt::Debug;

/// A packed string array that stores start and end indexes into
/// a contiguous string slice.
///
/// The type parameter K alters the type used to store the offsets
#[derive(Debug)]
pub struct PackedStringArray<K> {
    /// The start and end offsets of strings stored in storage
    offsets: Vec<K>,
    /// A contiguous array of string data
    storage: String,
}

impl<K: Zero> Default for PackedStringArray<K> {
    fn default() -> Self {
        Self {
            offsets: vec![K::zero()],
            storage: String::new(),
        }
    }
}

impl<K: AsPrimitive<usize> + FromPrimitive + Zero> PackedStringArray<K> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a value
    ///
    /// Returns the index of the appended data
    pub fn append(&mut self, data: &str) -> usize {
        let id = self.offsets.len() - 1;

        let offset = self.storage.len() + data.len();
        let offset = K::from_usize(offset).expect("failed to fit into offset type");

        self.offsets.push(offset);
        self.storage.push_str(data);

        id
    }

    /// Get the value at a given index
    pub fn get(&self, index: usize) -> Option<&str> {
        let start_offset = self.offsets.get(index)?.as_();
        let end_offset = self.offsets.get(index + 1)?.as_();

        Some(&self.storage[start_offset..end_offset])
    }

    pub fn iter(&self) -> PackedStringIterator<'_, K> {
        PackedStringIterator {
            array: &self,
            index: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.len() == 1
    }

    /// Return the amount of memory in bytes taken up by this array
    pub fn size(&self) -> usize {
        self.storage.len() + self.offsets.len() * std::mem::size_of::<K>()
    }
}

impl PackedStringArray<i32> {
    /// Convert to an arrow representation
    pub fn to_arrow(&self) -> StringArray {
        let len = self.offsets.len() - 1;
        let offsets = Buffer::from_slice_ref(&self.offsets);
        let values = Buffer::from(self.storage.as_bytes());

        let data = ArrayDataBuilder::new(arrow::datatypes::DataType::Utf8)
            .len(len)
            .add_buffer(offsets)
            .add_buffer(values)
            .build();

        StringArray::from(data)
    }
}

pub struct PackedStringIterator<'a, K> {
    array: &'a PackedStringArray<K>,
    index: usize,
}

impl<'a, K: AsPrimitive<usize> + FromPrimitive + Zero> Iterator for PackedStringIterator<'a, K> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.array.get(self.index)?;
        self.index += 1;
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.array.len() - self.index;
        (len, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use crate::string::PackedStringArray;

    #[test]
    fn test_storage() {
        let mut array = PackedStringArray::<i32>::new();

        array.append("hello");
        array.append("world");
        array.append("cupcake");

        assert_eq!(array.get(0).unwrap(), "hello");
        assert_eq!(array.get(1).unwrap(), "world");
        assert_eq!(array.get(2).unwrap(), "cupcake");
        assert!(array.get(-1_i32 as usize).is_none());
    }
}
