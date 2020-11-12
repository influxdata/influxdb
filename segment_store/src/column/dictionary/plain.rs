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
        self.push_additional(None, 1);
    }

    /// Adds additional repetitions of the provided value to the encoded data.
    /// It is the caller's responsibility to ensure that the dictionary remains
    /// sorted. `push_additional` will panic if that invariant is broken.
    pub fn push_additional(&mut self, v: Option<String>, additional: u32) {
        if v.is_none() {
            self.contains_null = true;
            self.push_encoded_values(NULL_ID, additional);
            return;
        }

        match self.encoded_id(v.as_deref()) {
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
    fn encoded_id(&self, value: Option<&str>) -> Result<u32, u32> {
        match self
            .entries
            .binary_search_by(|entry| entry.as_deref().cmp(&value))
        {
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
        self.encoded_data.len() as u32
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

        if let Ok(encoded_id) = self.encoded_id(Some(value)) {
            // N.B(edd): this is specifically split out like this
            // (with the duplication on the looping) because the common path
            // (== predicate) will mean that the loop can easily be
            // auto-vectorised by LLVM. I have verified the assembly and it does
            // indeed do that.
            match op {
                cmp::Operator::Equal => {
                    for (i, next) in self.encoded_data.iter().enumerate() {
                        if next == &encoded_id {
                            // N.B(edd): adding individual values like this to a bitset
                            // is expensive if there are many values. In other encodings
                            // such as the RLE one I instead build up ranges of
                            // values to add. Here though we expect this encoding to
                            // have higher cardinality and not many co-located
                            // matching rows.
                            dst.add(i as u32);
                        }
                    }
                }
                cmp::Operator::NotEqual => {
                    for (i, next) in self.encoded_data.iter().enumerate() {
                        if next != &NULL_ID && next != &encoded_id {
                            dst.add(i as u32);
                        }
                    }
                }
                _ => unreachable!(format!("operator {:?} not supported for row_ids_equal", op)),
            }
        } else if let cmp::Operator::NotEqual = op {
            // special case - the column does not contain the value in the
            // predicate, but the predicate is != so we must return all non-null
            // row ids.
            if !self.contains_null {
                // this could be optimised to not materialise all the encoded
                // values. I have a `RowIDsOption` enum that can represent this.
                dst.add_range(0, self.num_rows());
                return dst;
            }
            dst = self.row_ids_not_null(dst)
        }

        dst
    }

    // Finds row ids based on <, <=, > or >= operator.
    fn row_ids_cmp(&self, value: &str, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        match self.encoded_id(Some(value)) {
            // Happy path - the logical value on the predicate exists in the column
            // apply the predicate to each value in the column and identify all the
            // row ids that match.
            Ok(encoded_id) => self.row_ids_encoded_cmp(encoded_id, op, dst),

            // In this case the column does not contain the value on the
            // predicate. The returned id determines the ordinal relationship
            // between the encoded form of the value on the predicate and the
            // existing logical values in the column. With this information we
            // can work directly on the encoded representation in the column...
            Err(id) => match op {
                cmp::Operator::GT | cmp::Operator::GTE => {
                    if id == self.entries.len() as u32 {
                        // value is > or >= the maximum logical value in the column
                        // so no rows will match.
                        dst.clear();
                        return dst;
                    } else if id == NULL_ID + 1 {
                        // value would be ordered at the beginning of all
                        // logical values and the predicate is > or >=
                        // All non-null rows will match.
                        return self.row_ids_not_null(dst);
                    }

                    // value would be ordered somewhere in the middle of the
                    // logical values in the column. By apply the >=
                    // operator to the encoded value currently at that
                    // position, all matching rows will be returned.
                    self.row_ids_encoded_cmp(id, &cmp::Operator::GTE, dst)
                }
                cmp::Operator::LT | cmp::Operator::LTE => {
                    if id == NULL_ID + 1 {
                        // value is < or <= the minimum logical value in the
                        // column so no rows will match.
                        dst.clear();
                        return dst;
                    } else if id == self.entries.len() as u32 {
                        // value would be ordered at the end of all logical
                        // values and the predicate is < or <=.
                        // All non-null rows will match.
                        return self.row_ids_not_null(dst);
                    }

                    // value would be ordered somewhere in the middle of the
                    // logical values in the column. By apply the <
                    // operator to the encoded value currently at that
                    // position, all matching rows will be returned.
                    self.row_ids_encoded_cmp(id, &cmp::Operator::LT, dst)
                }
                _ => {
                    unreachable!(format!("operator {:?} not supported for row_ids_cmp", op));
                }
            },
        }
    }

    // Given an encoded id for a logical column value and a predicate matching
    // row ids are returned.
    fn row_ids_encoded_cmp(&self, encoded_id: u32, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        let cmp = match op {
            cmp::Operator::GT => PartialOrd::gt,
            cmp::Operator::GTE => PartialOrd::ge,
            cmp::Operator::LT => PartialOrd::lt,
            cmp::Operator::LTE => PartialOrd::le,
            _ => unreachable!("operator not supported"),
        };

        let mut found = false;
        let mut count = 0_u32;
        for (i, next) in self.encoded_data.iter().enumerate() {
            let cmp_result = cmp(next, &encoded_id);

            if (next == &NULL_ID || !cmp_result) && found {
                let (min, max) = (i as u32 - count, i as u32);
                dst.add_range(min, max);
                found = false;
                count = 0;
                continue;
            } else if next == &NULL_ID || !cmp_result {
                continue;
            }

            if !found {
                found = true;
            }
            count += 1;
        }

        // add any remaining range.
        if found {
            let (min, max) = (
                self.encoded_data.len() as u32 - count,
                self.encoded_data.len() as u32,
            );
            dst.add_range(min, max);
        }
        dst
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

        if !self.contains_null {
            if is_null {
                return dst; // no NULL values in column so no rows will match
            }

            // no NULL values in column so all rows will match
            dst.add_range(0, self.num_rows());
            return dst;
        }

        let mut found = false;
        let mut count = 0;
        for (i, &next) in self.encoded_data.iter().enumerate() {
            let cmp_result = next == NULL_ID;

            if cmp_result != is_null && found {
                let (min, max) = (i as u32 - count, i as u32);
                dst.add_range(min, max);
                found = false;
                count = 0;
                continue;
            } else if cmp_result != is_null {
                continue;
            }

            if !found {
                found = true;
            }
            count += 1;
        }

        // add any remaining range.
        if found {
            let (min, max) = (
                self.encoded_data.len() as u32 - count,
                self.encoded_data.len() as u32,
            );
            dst.add_range(min, max);
        }

        dst
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

    // The dictionary of all non-null entries.
    //
    // TODO(edd): rethink returning `Vec<String>` by looking at if we can store
    // entries in a `Vec<String>` rather than a `Vec<Option<String>>`. It would
    // then allow us to return a `&[String]` here.
    pub fn dictionary(&self) -> Vec<&String> {
        if self.entries.len() == 1 {
            // no non-null entries.
            return vec![];
        }

        self.entries
            .iter()
            .skip(1)
            .filter_map(|v| v.as_ref())
            .collect()
    }

    /// Returns the logical value present at the provided row id. Panics if the
    /// encoding doesn't have a logical row at the id.
    pub fn value(&self, row_id: u32) -> Option<&String> {
        assert!(
            row_id < self.num_rows(),
            "row_id {:?} out of bounds for {:?} rows",
            row_id,
            self.num_rows()
        );

        let encoded_id = self.encoded_data[row_id as usize];
        self.entries[encoded_id as usize].as_ref()
    }

    /// Materialises the decoded value belonging to the provided encoded id.
    ///
    /// Panics if there is no decoded value for the provided id
    pub fn decode_id(&self, encoded_id: u32) -> Option<String> {
        self.entries[encoded_id as usize].clone()
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

        // TODO - not sure at all about this deref...
        for chunks in row_ids.chunks_exact(4) {
            dst.push(self.entries[self.encoded_data[chunks[0] as usize] as usize].as_deref());
            dst.push(self.entries[self.encoded_data[chunks[1] as usize] as usize].as_deref());
            dst.push(self.entries[self.encoded_data[chunks[2] as usize] as usize].as_deref());
            dst.push(self.entries[self.encoded_data[chunks[3] as usize] as usize].as_deref());
        }

        for &v in &row_ids[dst.len()..row_ids.len()] {
            dst.push(self.entries[self.encoded_data[v as usize] as usize].as_deref());
        }

        dst
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
