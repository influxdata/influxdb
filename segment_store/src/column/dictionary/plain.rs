use std::collections::{BTreeMap, BTreeSet};
use std::convert::From;

use croaring::Bitmap;

use arrow_deps::arrow::array::{Array, StringArray};

use crate::column::dictionary::NULL_ID;
use crate::column::{cmp, RowIDs};

pub struct Plain {
    // The sorted set of logical values that are contained within this column
    // encoding. Entries always contains None, which is used to reserve the
    // encoded id of `0` for NULL values.
    entries: Vec<Option<String>>,

    // A vector of encoded ids used to represent logical values within the
    // column encoding.
    encoded_data: Vec<u32>,

    // marker indicating if the encoding contains a NULL value in one or more
    // rows.
    contains_null: bool,
}

// The default initialisation of an Plain involves reserving the first id/index 0
// for the NULL value.
impl Default for Plain {
    fn default() -> Self {
        Self {
            entries: vec![None],
            encoded_data: vec![],
            contains_null: false,
        }
    }
}

impl Plain {
    /// Initialises an Plain encoding with a set of logical values.
    /// Creating an encoding using `with_dictionary` ensures that the dictionary
    /// is in the correct order, and will allow values to be inserted with any
    /// value in the dictionary.
    ///
    /// Callers are not required to provide a logical NULL value as part of the
    /// dictionary. The encoding already reserves a representation for that.
    pub fn with_dictionary(dictionary: BTreeSet<String>) -> Self {
        let mut _self = Self::default();
        _self.entries.extend(dictionary.into_iter().map(Some));
        _self
    }

    /// A reasonable estimation of the on-heap size this encoding takes up.
    pub fn size(&self) -> u64 {
        todo!()
    }

    /// Adds the provided string value to the encoded data. It is the caller's
    /// responsibility to ensure that the dictionary encoded remains sorted.
    pub fn push(&mut self, v: String) {
        self.push_additional(Some(v), 1);
    }

    /// Adds a NULL value to the encoded data. It is the caller's
    /// responsibility to ensure that the dictionary encoded remains sorted.
    pub fn push_none(&mut self) {
        self.push_encoded_values(NULL_ID, 1);
    }

    /// Adds additional repetitions of the provided value to the encoded data.
    /// It is the caller's responsibility to ensure that the dictionary remains
    /// sorted. `push_additional` will panic if that invariant is broken.
    pub fn push_additional(&mut self, v: Option<String>, additional: u32) {
        if v.is_none() {
            self.push_encoded_values(NULL_ID, additional);
            return;
        }

        match self.encoded_id(&v) {
            Ok(id) => self.push_encoded_values(id, additional),
            Err(idx) => {
                // check this value can be inserted into the dictionary whilst
                // maintaining order.
                assert_eq!(idx, self.entries.len() as u32);

                self.entries.push(v);
                self.push_encoded_values(idx, additional);
            }
        }
    }

    // Preferred to add values to the column. `id` is the encoded
    // representation of a logical string value.
    fn push_encoded_values(&mut self, id: u32, additional: u32) {
        self.encoded_data
            .extend(std::iter::repeat(id).take(additional as usize));
    }

    // Preferred way to lookup the encoded id for a given logical string value.
    // If the value exists then the `Ok` variant with the encoded id is returned,
    // otherwise the `Err` variant is returned with the appropriate ordinal id
    // for the value if that value did exist.
    //
    // The `Err` variant can be useful when applying predicates directly to the
    // encoded data.
    fn encoded_id(&self, value: &Option<String>) -> Result<u32, u32> {
        match self.entries.binary_search(value) {
            Ok(id) => Ok(id as u32),
            Err(id) => Err(id as u32),
        }
    }

    // correct way to determine next encoded id for a new value.
    fn next_encoded_id(&self) -> u32 {
        todo!()
    }

    /// The number of logical rows encoded in this column.
    pub fn num_rows(&self) -> u32 {
        self.entries.len() as u32
    }

    /// Determine if NULL is encoded in the column.
    pub fn contains_null(&self) -> bool {
        self.contains_null
    }

    //
    //
    // ---- Methods for getting row ids from values.
    //
    //

    /// Populates the provided destination container with the row ids satisfying
    /// the provided predicate.
    pub fn row_ids_filter(&self, value: &str, op: &cmp::Operator, dst: RowIDs) -> RowIDs {
        match op {
            cmp::Operator::Equal | cmp::Operator::NotEqual => self.row_ids_equal(value, op, dst),
            cmp::Operator::LT | cmp::Operator::LTE | cmp::Operator::GT | cmp::Operator::GTE => {
                self.row_ids_cmp(value, op, dst)
            }
        }
    }

    // Finds row ids based on = or != operator.
    fn row_ids_equal(&self, value: &str, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        dst
    }

    // Finds row ids based on <, <=, > or >= operator.
    fn row_ids_cmp(&self, value: &str, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        todo!()
    }

    /// Populates the provided destination container with the row ids for rows
    /// that null.
    pub fn row_ids_null(&self, dst: RowIDs) -> RowIDs {
        self.row_ids_is_null(true, dst)
    }

    /// Populates the provided destination container with the row ids for rows
    /// that are not null.
    pub fn row_ids_not_null(&self, dst: RowIDs) -> RowIDs {
        self.row_ids_is_null(false, dst)
    }

    // All row ids that have either NULL or not NULL values.
    fn row_ids_is_null(&self, is_null: bool, mut dst: RowIDs) -> RowIDs {
        dst.clear();
        todo!()
    }

    // The set of row ids for each distinct value in the column.
    pub fn group_row_ids(&self) -> &BTreeMap<u32, Bitmap> {
        todo!()
    }

    //
    //
    // ---- Methods for getting materialised values.
    //
    //

    pub fn dictionary(&self) -> &[String] {
        todo!()
    }

    /// Returns the logical value present at the provided row id.
    ///
    /// N.B right now this doesn't discern between an invalid row id and a NULL
    /// value at a valid location.
    pub fn value(&self, row_id: u32) -> Option<&String> {
        todo!()
    }

    /// Materialises the decoded value belonging to the provided encoded id.
    ///
    /// Panics if there is no decoded value for the provided id
    pub fn decode_id(&self, encoded_id: u32) -> Option<String> {
        todo!()
    }

    /// Materialises a vector of references to the decoded values in the
    /// provided row ids.
    ///
    /// NULL values are represented by None. It is the caller's responsibility
    /// to ensure row ids are a monotonically increasing set.
    pub fn values<'a>(
        &'a self,
        row_ids: &[u32],
        mut dst: Vec<Option<&'a str>>,
    ) -> Vec<Option<&'a str>> {
        dst.clear();
        dst.reserve(row_ids.len());

        todo!()
    }

    /// Returns the lexicographical minimum value for the provided set of row
    /// ids. NULL values are not considered the minimum value if any non-null
    /// value exists at any of the provided row ids.
    pub fn min<'a>(&'a self, row_ids: &[u32]) -> Option<&'a String> {
        todo!()
    }

    /// Returns the lexicographical maximum value for the provided set of row
    /// ids. NULL values are not considered the maximum value if any non-null
    /// value exists at any of the provided row ids.
    pub fn max<'a>(&'a self, row_ids: &[u32]) -> Option<&'a String> {
        todo!()
    }

    /// Returns the total number of non-null values found at the provided set of
    /// row ids.
    pub fn count(&self, row_ids: &[u32]) -> u32 {
        todo!()
    }

    /// Returns references to the logical (decoded) values for all the rows in
    /// the column.
    ///
    /// NULL values are represented by None.
    ///
    pub fn all_values<'a>(
        &'a mut self,
        mut dst: Vec<Option<&'a String>>,
    ) -> Vec<Option<&'a String>> {
        dst.clear();
        dst.reserve(self.entries.len());

        for chunks in self.encoded_data.chunks_exact(4) {
            dst.push(self.entries[chunks[0] as usize].as_ref());
            dst.push(self.entries[chunks[1] as usize].as_ref());
            dst.push(self.entries[chunks[2] as usize].as_ref());
            dst.push(self.entries[chunks[3] as usize].as_ref());
        }

        for &v in &self.encoded_data[dst.len()..self.encoded_data.len()] {
            dst.push(self.entries[v as usize].as_ref());
        }
        dst
    }

    /// Returns references to the unique set of values encoded at each of the
    /// provided ids.
    ///
    /// It is the caller's responsibility to ensure row ids are a monotonically
    /// increasing set.
    pub fn distinct_values<'a>(
        &'a self,
        row_ids: &[u32],
        mut dst: BTreeSet<Option<&'a String>>,
    ) -> BTreeSet<Option<&'a String>> {
        // TODO(edd): Perf... We can improve on this if we know the column is
        // totally ordered.
        dst.clear();

        todo!()
    }

    //
    //
    // ---- Methods for getting encoded values directly, typically to be used
    //      as part of group keys.
    //
    //

    /// Return the raw encoded values for the provided logical row ids.
    /// Encoded values for NULL values are included.
    ///
    pub fn encoded_values(&self, row_ids: &[u32], mut dst: Vec<u32>) -> Vec<u32> {
        dst.clear();
        dst.reserve(row_ids.len());

        todo!()
    }

    /// Returns all encoded values for the column including the encoded value
    /// for any NULL values.
    pub fn all_encoded_values(&self, mut dst: Vec<u32>) -> Vec<u32> {
        dst.clear();
        dst.reserve(self.entries.len());

        dst.extend(self.encoded_data.iter());
        dst
    }

    //
    //
    // ---- Methods for optimising schema exploration.
    //
    //

    /// Efficiently determines if this column contains non-null values that
    /// differ from the provided set of values.
    ///
    /// Informally, this method provides an efficient way of answering "is it
    /// worth spending time reading this column for values or do I already have
    /// all the values in a set".
    ///
    /// More formally, this method returns the relative complement of this
    /// column's values in the provided set of values.
    ///
    /// This method would be useful when the same column is being read across
    /// many segments, and one wants to determine to the total distinct set of
    /// values. By exposing the current result set to each column (as an
    /// argument to `contains_other_values`) columns can be short-circuited when
    /// they only contain values that have already been discovered.
    ///
    pub fn contains_other_values(&self, values: &BTreeSet<Option<&String>>) -> bool {
        todo!()
    }

    /// Determines if the column contains at least one non-null value at
    /// any of the provided row ids.
    ///
    /// It is the caller's responsibility to ensure row ids are a monotonically
    /// increasing set.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        todo!()
    }

    // Returns true if there exists an encoded non-null value at any of the row
    // ids.
    fn find_non_null_value(&self, row_ids: &[u32]) -> bool {
        todo!()
    }
}

impl<'a> From<Vec<&str>> for Plain {
    fn from(vec: Vec<&str>) -> Self {
        let mut enc = Self::default();
        for v in vec {
            enc.push(v.to_string());
        }
        enc
    }
}

impl<'a> From<Vec<String>> for Plain {
    fn from(vec: Vec<String>) -> Self {
        let mut drle = Self::default();
        for v in vec {
            drle.push(v);
        }
        drle
    }
}

impl<'a> From<Vec<Option<&str>>> for Plain {
    fn from(vec: Vec<Option<&str>>) -> Self {
        let mut drle = Self::default();
        for v in vec {
            match v {
                Some(x) => drle.push(x.to_string()),
                None => drle.push_none(),
            }
        }
        drle
    }
}

impl<'a> From<Vec<Option<String>>> for Plain {
    fn from(vec: Vec<Option<String>>) -> Self {
        let mut drle = Self::default();
        for v in vec {
            match v {
                Some(x) => drle.push(x),
                None => drle.push_none(),
            }
        }
        drle
    }
}

impl<'a> From<StringArray> for Plain {
    fn from(arr: StringArray) -> Self {
        let mut drle = Self::default();
        for i in 0..arr.len() {
            if arr.is_null(i) {
                drle.push_none();
            } else {
                drle.push(arr.value(i).to_string());
            }
        }
        drle
    }
}

impl std::fmt::Display for Plain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn with_dictionary() {
        let mut dictionary = BTreeSet::new();
        dictionary.insert("hello".to_string());
        dictionary.insert("world".to_string());

        let enc = Plain::with_dictionary(dictionary);
        assert_eq!(
            enc.entries,
            vec![None, Some("hello".to_string()), Some("world".to_string()),]
        );
    }

    #[test]
    #[should_panic]
    fn push_wrong_order() {
        let mut enc = Plain::default();
        enc.push("b".to_string());
        enc.push("a".to_string());
    }
}
