#[cfg(all(
    any(target_arch = "x86", target_arch = "x86_64"),
    target_feature = "avx2"
))]
#[cfg(target_arch = "x86")]
use std::arch::x86::*;
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
use std::arch::x86_64::*;

use std::collections::BTreeSet;
use std::convert::From;
use std::mem::size_of;

use arrow::array::{Array, StringArray};

use super::NULL_ID;
use crate::column::{cmp, RowIDs};

pub const ENCODING_NAME: &str = "DICT";
pub struct Dictionary {
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

// The default initialisation of this Dictionary involves reserving the first
// id/index `0`, which is the encoded representation of the NULL value.
impl Default for Dictionary {
    fn default() -> Self {
        // for this to make sense NULL_ID must be `0`.
        assert_eq!(NULL_ID, 0);
        Self {
            entries: vec![None],
            encoded_data: vec![],
            contains_null: false,
        }
    }
}

impl Dictionary {
    /// Initialises an Dictionar encoding with a set of logical values.
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
    pub fn size(&self) -> usize {
        // the total size of all decoded values in the column.
        let decoded_keys_size = self
            .entries
            .iter()
            .map(|k| match k {
                Some(v) =>  v.len(),
                None => 0,
            } + size_of::<Option<String>>())
            .sum::<usize>();

        let entries_size = size_of::<Vec<Option<String>>>() + decoded_keys_size;
        let encoded_ids_size = size_of::<Vec<u32>>() + (size_of::<u32>() * self.encoded_data.len());

        // + 1 for contains_null field
        entries_size + encoded_ids_size + 1
    }

    /// The number of distinct logical values in this column encoding.
    pub fn cardinality(&self) -> u32 {
        if self.contains_null {
            self.entries.len() as u32
        } else {
            self.entries.len() as u32 - 1
        }
    }

    /// The number of NULL values in this column.
    ///
    /// TODO(edd): this can be made O(1) by storing null_count on self.
    pub fn null_count(&self) -> u32 {
        self.encoded_data
            .iter()
            .filter(|&&id| id == NULL_ID)
            .count() as u32
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
                assert_eq!(
                    idx,
                    self.entries.len() as u32,
                    "out of order insertion for {:?}",
                    v
                );

                self.entries.push(v);
                self.push_encoded_values(idx, additional);
            }
        }
    }

    // Preferred method to add values to the column. `id` is the encoded
    // representation of a logical value.
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
            // (with the duplication on the looping) so that a specialised
            // function with some SIMD intrinsics can be used for the more
            // common == case.
            match op {
                cmp::Operator::Equal => {
                    #[cfg(all(
                        any(target_arch = "x86", target_arch = "x86_64"),
                        target_feature = "avx2"
                    ))]
                    return self.row_ids_equal_simd(encoded_id, dst);

                    #[cfg(any(
                        not(any(target_arch = "x86", target_arch = "x86_64")),
                        not(target_feature = "avx2")
                    ))]
                    for (i, next) in self.encoded_data.iter().enumerate() {
                        if next == &encoded_id {
                            // N.B(edd): adding individual values like this to a bitset
                            // can be expensive if there are many values. In other encodings
                            // such as the RLE one I instead build up ranges of
                            // values to add. Here though we expect this encoding to
                            // have higher cardinality and not many ranges of
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

    #[cfg(all(
        any(target_arch = "x86", target_arch = "x86_64"),
        target_feature = "avx2"
    ))]
    // An optimised method for finding all row ids within the column encoding
    // where the row contains the provided encoded id.
    //
    // This approach uses SIMD intrinsics to compare two registers with eight
    // lanes in parallel using a single CPU instruction. One register is packed
    // with the `encoded_id` we are looking for. The other register contains
    // eight consecutive values from the column we're searching. The comparioson
    // produces an 8 lane register (256 bits) where every 32 bit lane will be
    // `0` if the comparison was not equal.
    //
    // The fast path for this type of column encoding would be that most rows
    // will not match the provided encoded_id so it is efficient to check that
    // this 256-bit register is all zero and move onto the next eight rows in the
    // column.
    //
    // For cases where one or more of the eight values does match the encoded
    // id the exact matching row(s) is/are determined by examining the register.
    fn row_ids_equal_simd(&self, encoded_id: u32, mut dst: RowIDs) -> RowIDs {
        unsafe {
            // Pack an 8-lane register containing the encoded id we want to find.
            let id_register = _mm256_set1_epi32(encoded_id as i32);

            let mut i = 0_u32; // Track the total index.
            self.encoded_data.chunks_exact(8).for_each(|chunk| {
                // Load the entries we want to compare against into a SIMD register.
                let entries = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);

                // Compares the 32-bit packed values and returns a register
                // with the comparison results.
                let res = _mm256_cmpeq_epi32(entries, id_register);

                // Since we only care about determining if every result is 0
                // we can just unpack into two 128-bit integers and check they're
                // both zero.
                let (a, b) = std::mem::transmute::<_, (u128, u128)>(res);
                if a != 0 || b != 0 {
                    // slow path
                    // One or more of the rows checked contains the encoded id.
                    for (j, v) in chunk.iter().enumerate() {
                        if v == &encoded_id {
                            dst.add(i + j as u32);
                        }
                    }
                }

                i += 8;
            });

            // If encoded_data.len() % 8 != 0 then there will be remaining rows
            // to check.
            let rem = self.encoded_data.len() % 8;
            let all_rows = self.num_rows() as usize;
            for i in (all_rows - rem)..all_rows {
                if self.encoded_data[i] == encoded_id {
                    dst.add(i as u32);
                }
            }
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
    //
    // TODO(edd): add an alternative SIMD equivalent to `row_ids_equal_simd`
    // here.
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
    pub fn group_row_ids(&self) -> Vec<RowIDs> {
        let mut results = vec![];
        results.resize(self.entries.len(), RowIDs::new_bitmap());

        for (i, id) in self.encoded_data.iter().enumerate() {
            results[*id as usize].add(i as u32);
        }

        results
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
    pub fn decode_id(&self, encoded_id: u32) -> Option<&str> {
        self.entries[encoded_id as usize].as_deref()
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

        // The `as_deref` is needed to convert an `&Option<String>` into an
        // `Option<&str>`.
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

    /// Returns the lexicographical minimum value in the column. None is
    /// returned only if the column does not contain any non-null values.
    pub fn column_min(&self) -> Option<&'_ String> {
        if self.entries.len() > 1 {
            return self.entries.get(1).unwrap().as_ref();
        }
        None
    }

    /// Returns the lexicographical maximum value in the column. None is
    /// returned only if the column does not contain any non-null values.
    pub fn column_max(&self) -> Option<&'_ String> {
        if self.entries.len() > 1 {
            return self.entries.last().unwrap().as_ref();
        }
        None
    }

    /// Returns the lexicographical minimum value for the provided set of row
    /// ids. NULL values are not considered the minimum value if any non-null
    /// value exists at any of the provided row ids.
    pub fn min<'a>(&'a self, _row_ids: &[u32]) -> Option<&'a String> {
        todo!()
    }

    /// Returns the lexicographical maximum value for the provided set of row
    /// ids. NULL values are not considered the maximum value if any non-null
    /// value exists at any of the provided row ids.
    pub fn max<'a>(&'a self, _row_ids: &[u32]) -> Option<&'a String> {
        todo!()
    }

    /// Returns the total number of non-null values found at the provided set of
    /// row ids.
    pub fn count(&self, _row_ids: &[u32]) -> u32 {
        todo!()
    }

    /// Returns references to the logical (decoded) values for all the rows in
    /// the column.
    ///
    /// NULL values are represented by None.
    pub fn all_values<'a>(&'a self, mut dst: Vec<Option<&'a str>>) -> Vec<Option<&'a str>> {
        dst.clear();
        dst.reserve(self.entries.len());

        for chunks in self.encoded_data.chunks_exact(4) {
            dst.push(self.entries[chunks[0] as usize].as_deref());
            dst.push(self.entries[chunks[1] as usize].as_deref());
            dst.push(self.entries[chunks[2] as usize].as_deref());
            dst.push(self.entries[chunks[3] as usize].as_deref());
        }

        for &v in &self.encoded_data[dst.len()..self.encoded_data.len()] {
            dst.push(self.entries[v as usize].as_deref());
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
        row_ids: impl Iterator<Item = u32>,
        mut dst: BTreeSet<Option<&'a str>>,
    ) -> BTreeSet<Option<&'a str>> {
        // TODO(edd): Perf... We can improve on this if we know the column is
        // totally ordered.
        dst.clear();

        for row_id in row_ids {
            let encoded_id = self.encoded_data[row_id as usize];

            let value = self.entries[encoded_id as usize].as_deref();
            if !dst.contains(&value) {
                dst.insert(value);
            }

            if dst.len() as u32 == self.cardinality() {
                // no more distinct values to find.
                return dst;
            }
        }

        dst
    }

    //
    //
    // ---- Methods for getting encoded values directly, typically to be used
    //      as part of group keys.
    //
    //

    /// Return the raw encoded values for the provided logical row ids.
    /// Encoded values for NULL values are included.
    pub fn encoded_values(&self, row_ids: &[u32], mut dst: Vec<u32>) -> Vec<u32> {
        dst.clear();
        dst.reserve(row_ids.len());

        for row_id in row_ids {
            dst.push(self.encoded_data[*row_id as usize]);
        }
        dst
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
    /// worth spending time reading this column for distinct values that are not
    /// present in the provided set?".
    ///
    /// More formally, this method returns the relative complement of this
    /// column's dictionary in the provided set of values.
    pub fn has_other_non_null_values(&self, values: &BTreeSet<String>) -> bool {
        if self.cardinality() as usize > values.len() {
            return true;
        }

        // If any of the distinct values in this column are not present in
        // `values` then return `true`.
        self.entries.iter().any(|entry| {
            if let Some(value) = entry {
                if !values.contains(value) {
                    return true;
                }
            }
            false
        })
    }

    /// Determines if the column contains at least one non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        if !self.contains_null() {
            return true;
        }

        // If there is more than one dictionary entry then there is a non-null
        // value in the column.
        self.entries.len() > 1
    }

    /// Determines if the column contains at least one non-null value at
    /// any of the provided row ids.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        if !self.contains_null() {
            return true;
        }

        // If there are encoded values at any of the row IDs then there is at
        // least one non-null value in the column.
        row_ids
            .iter()
            .any(|id| self.encoded_data[*id as usize] != NULL_ID)
    }

    // Returns true if there exists an encoded non-null value at any of the row
    // ids.
    fn find_non_null_value(&self, _row_ids: &[u32]) -> bool {
        todo!()
    }
}

impl<'a> From<Vec<&str>> for Dictionary {
    fn from(vec: Vec<&str>) -> Self {
        let mut enc = Self::default();
        for v in vec {
            enc.push(v.to_string());
        }
        enc
    }
}

impl<'a> From<Vec<String>> for Dictionary {
    fn from(vec: Vec<String>) -> Self {
        let mut enc = Self::default();
        for v in vec {
            enc.push(v);
        }
        enc
    }
}

impl<'a> From<Vec<Option<&str>>> for Dictionary {
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

impl<'a> From<Vec<Option<String>>> for Dictionary {
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

impl<'a> From<StringArray> for Dictionary {
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

impl std::fmt::Display for Dictionary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] size: {:?} rows: {:?} cardinality: {}",
            ENCODING_NAME,
            self.size(),
            self.num_rows(),
            self.cardinality(),
        )
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

        let enc = Dictionary::with_dictionary(dictionary);
        assert_eq!(
            enc.entries,
            vec![None, Some("hello".to_string()), Some("world".to_string()),]
        );
    }

    #[test]
    fn size() {
        let mut enc = Dictionary::default();
        enc.push_additional(Some("east".to_string()), 3);
        enc.push_additional(Some("north".to_string()), 1);
        enc.push_additional(Some("east".to_string()), 5);
        enc.push_additional(Some("south".to_string()), 2);
        enc.push_none();
        enc.push_none();
        enc.push_none();
        enc.push_none();

        // keys - 14 bytes.

        // 3 string entries in dictionary
        // entries is 24 + (24*4) + 14 == 134

        // 15 rows.
        // encoded ids is 24 + (4 * 15) == 84

        // 134 + 84 + 1 == 219

        assert_eq!(enc.size(), 219);

        // check dictionary
        assert_eq!(
            enc.entries,
            vec![
                None,
                Some("east".to_string()),
                Some("north".to_string()),
                Some("south".to_string())
            ]
        );
        assert_eq!(
            enc.encoded_data,
            vec![1, 1, 1, 2, 1, 1, 1, 1, 1, 3, 3, NULL_ID, NULL_ID, NULL_ID, NULL_ID]
        );
    }

    #[test]
    fn null_count() {
        let mut enc = Dictionary::default();
        enc.push_additional(Some("east".to_string()), 3);
        assert_eq!(enc.null_count(), 0);

        enc.push_additional(Some("west".to_string()), 1);
        assert_eq!(enc.null_count(), 0);

        enc.push_none();
        assert_eq!(enc.null_count(), 1);

        enc.push_none();
        enc.push_none();
        assert_eq!(enc.null_count(), 3);
    }

    #[test]
    #[should_panic]
    fn push_wrong_order() {
        let mut enc = Dictionary::default();
        enc.push("b".to_string());
        enc.push("a".to_string());
    }

    #[test]
    fn has_non_null_value() {
        let mut enc = Dictionary::default();
        enc.push_none();
        enc.push_none();

        assert!(!enc.has_non_null_value(&[0, 1]));

        enc.push("b".to_string());
        assert!(enc.has_non_null_value(&[0, 1, 2]));
        assert!(enc.has_non_null_value(&[2]));
        assert!(!enc.has_non_null_value(&[0, 1]));
    }

    #[test]
    fn has_any_non_null_value() {
        let mut enc = Dictionary::default();
        enc.push_none();
        enc.push_none();

        assert!(!enc.has_any_non_null_value());

        enc.push("b".to_string());
        assert!(enc.has_any_non_null_value());
    }
}
