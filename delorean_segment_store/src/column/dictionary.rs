use std::collections::{BTreeMap, BTreeSet};
use std::convert::From;
use std::iter;

use croaring::Bitmap;

use arrow_deps::arrow::array::{Array, StringArray};

use crate::column::{cmp, RowIDs};

// The encoded id for a NULL value.
pub const NULL_ID: u32 = 0;

// `RLE` is a run-length encoding for dictionary columns, where all dictionary
// entries are utf-8 valid strings.
pub struct RLE {
    // TODO(edd): revisit choice of storing owned string versus references.

    // The mapping between non-null entries and their assigned ids. The id
    // `NULL_ID` is reserved for the NULL entry.
    entry_index: BTreeMap<String, u32>,

    // The mapping between an id (as an index) and its entry. The entry at index
    // `NULL_ID` is undefined because that id is reserved for the NULL value.
    index_entries: Vec<String>,

    // The set of rows that belong to each distinct value in the dictionary.
    // This allows essentially constant time grouping of rows on the column by
    // value.
    index_row_ids: BTreeMap<u32, Bitmap>,

    // stores tuples where each pair refers to a dictionary entry and the number
    // of times the entry repeats.
    run_lengths: Vec<(u32, u32)>,

    // marker indicating if the encoding contains a NULL value in one or more
    // rows.
    contains_null: bool,

    num_rows: u32,
}

// The default initialisation of an RLE involves reserving the first id/index 0
// for the NULL value.
impl Default for RLE {
    fn default() -> Self {
        let mut _self = Self {
            entry_index: BTreeMap::new(),
            index_entries: vec!["".to_string()],
            index_row_ids: BTreeMap::new(),
            run_lengths: Vec::new(),
            contains_null: false,
            num_rows: 0,
        };
        _self.index_row_ids.insert(NULL_ID, Bitmap::create());

        _self
    }
}

impl RLE {
    /// Initialises an RLE encoding with a set of column values, ensuring that
    /// the rows in the column can be inserted in any order and the correct
    /// ordinal relationship will exist between the encoded values.
    pub fn with_dictionary(dictionary: BTreeSet<String>) -> Self {
        let mut _self = Self::default();

        for entry in dictionary.into_iter() {
            let next_id = _self.next_encoded_id();

            _self.entry_index.insert(entry.clone(), next_id);
            _self.index_entries.push(entry);
            _self.index_row_ids.insert(next_id, Bitmap::create());
        }

        _self
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
    /// It is the caller's responsibility to ensure that the dictionary encoded
    /// remains sorted.
    pub fn push_additional(&mut self, v: Option<String>, additional: u32) {
        match v {
            Some(v) => self.push_additional_some(v, additional),
            None => self.push_additional_none(additional),
        }
    }

    fn push_additional_some(&mut self, v: String, additional: u32) {
        match self.entry_index.get(&v) {
            // existing dictionary entry for value.
            Some(id) => {
                match self.run_lengths.last_mut() {
                    Some((last_id, rl)) => {
                        if last_id == id {
                            // update the existing run-length
                            *rl += additional;
                        } else {
                            // start a new run-length for an existing id
                            self.run_lengths.push((*id, additional));
                        }
                    }
                    // very first run-length in column...
                    None => {
                        self.run_lengths.push((*id, additional));
                    }
                }

                // Update the rows associated with the value.
                self.index_row_ids
                    .get_mut(&id)
                    .unwrap()
                    .add_range(self.num_rows as u64..self.num_rows as u64 + additional as u64);
            }
            // no dictionary entry for value.
            None => {
                // New dictionary entry.
                let next_id = self.next_encoded_id();
                if next_id > 0
                    && self.index_entries[next_id as usize - 1].cmp(&v) != std::cmp::Ordering::Less
                {
                    panic!("out of order dictionary insertion");
                }
                self.index_entries.push(v.clone());

                self.entry_index.insert(v, next_id);
                self.index_row_ids.insert(next_id, Bitmap::create());

                // start a new run-length
                self.run_lengths.push((next_id, additional));

                // update the rows associated with the value.
                self.index_row_ids
                    .get_mut(&(next_id as u32))
                    .unwrap()
                    .add_range(self.num_rows as u64..self.num_rows as u64 + additional as u64);
            }
        }
        self.num_rows += additional;
    }

    fn push_additional_none(&mut self, additional: u32) {
        // existing dictionary entry
        if let Some((last_id, rl)) = self.run_lengths.last_mut() {
            if last_id == &NULL_ID {
                // update the existing run-length
                *rl += additional;
            } else {
                // start a new run-length
                self.run_lengths.push((NULL_ID, additional));
                self.contains_null = true; // set null marker.
            }

            // update the rows associated with the value.
            self.index_row_ids
                .get_mut(&NULL_ID)
                .unwrap()
                .add_range(self.num_rows as u64..self.num_rows as u64 + additional as u64);
        } else {
            // very first run-length in column...
            self.run_lengths.push((NULL_ID, additional));
            self.contains_null = true; // set null marker.

            // update the rows associated with the value.
            self.index_row_ids
                .get_mut(&NULL_ID)
                .unwrap()
                .add_range(self.num_rows as u64..self.num_rows as u64 + additional as u64);
        }

        self.num_rows += additional;
    }

    // correct way to determine next encoded id for a new value.
    fn next_encoded_id(&self) -> u32 {
        self.index_entries.len() as u32
    }

    /// The number of logical rows encoded in this column.
    pub fn num_rows(&self) -> u32 {
        self.num_rows
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
    pub fn row_ids_filter(&self, value: &str, op: cmp::Operator, dst: RowIDs) -> RowIDs {
        match op {
            cmp::Operator::Equal | cmp::Operator::NotEqual => self.row_ids_equal(value, op, dst),
            cmp::Operator::LT | cmp::Operator::LTE | cmp::Operator::GT | cmp::Operator::GTE => {
                self.row_ids_cmp(value, op, dst)
            }
        }
    }

    // Finds row ids based on = or != operator.
    fn row_ids_equal(&self, value: &str, op: cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();
        let include = match op {
            cmp::Operator::Equal => true,
            cmp::Operator::NotEqual => false,
            _ => unreachable!("invalid operator"),
        };

        if let Some(encoded_id) = self.entry_index.get(value) {
            let mut index: u32 = 0;
            for (other_encoded_id, other_rl) in &self.run_lengths {
                let start = index;
                index += *other_rl;
                if other_encoded_id == &NULL_ID {
                    continue; // skip NULL values
                } else if (other_encoded_id == encoded_id) == include {
                    // we found a row either matching (include == true) or not
                    // matching (include == false) `value`.
                    dst.add_range(start, index)
                }
            }
        } else if let cmp::Operator::NotEqual = op {
            // special case - the column does not contain the provided
            // value and the operator is != so we need to return all
            // row ids for non-null values.
            if !self.contains_null {
                // no null values in column so return all row ids
                dst.add_range(0, self.num_rows);
            } else {
                // some null values in column - determine matching non-null rows
                dst = self.row_ids_not_null(dst);
            }
        }

        dst
    }

    // Finds row ids based on <, <=, > or >= operator.
    fn row_ids_cmp(&self, value: &str, op: cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        // happy path - the value exists in the column
        if let Some(encoded_id) = self.entry_index.get(value) {
            let cmp = match op {
                cmp::Operator::GT => PartialOrd::gt,
                cmp::Operator::GTE => PartialOrd::ge,
                cmp::Operator::LT => PartialOrd::lt,
                cmp::Operator::LTE => PartialOrd::le,
                _ => unreachable!("operator not supported"),
            };

            let mut index: u32 = 0; // current position in the column.
            for (other_encoded_id, other_rl) in &self.run_lengths {
                let start = index;
                index += *other_rl;

                if other_encoded_id == &NULL_ID {
                    continue; // skip NULL values
                } else if cmp(other_encoded_id, encoded_id) {
                    dst.add_range(start, index)
                }
            }
            return dst;
        }

        match op {
            cmp::Operator::GT | cmp::Operator::GTE => {
                // find the first decoded value that satisfies the predicate.
                for (other, other_encoded_id) in &self.entry_index {
                    if other.as_str() > value {
                        // change filter from either `x > value` or `x >= value` to `x >= other`
                        return self.row_ids_cmp(other, cmp::Operator::GTE, dst);
                    }
                }
            }
            cmp::Operator::LT | cmp::Operator::LTE => {
                // find the first decoded value that satisfies the predicate.
                // Note iteration is in reverse
                for (other, other_encoded_id) in self.entry_index.iter().rev() {
                    if other.as_str() < value {
                        // change filter from either `x < value` or `x <= value` to `x <= other`
                        return self.row_ids_cmp(other, cmp::Operator::LTE, dst);
                    }
                }
            }
            _ => unreachable!("operator not supported"),
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

        let mut index: u32 = 0;
        for (other_encoded_id, other_rl) in &self.run_lengths {
            let start = index;
            index += *other_rl;

            if (other_encoded_id == &NULL_ID) == is_null {
                // we found a row that was either NULL (is_null == true) or not
                // NULL (is_null == false) `value`.
                dst.add_range(start, index)
            }
        }

        dst
    }

    // The set of row ids for each distinct value in the column.
    pub fn group_row_ids(&self) -> &BTreeMap<u32, Bitmap> {
        &self.index_row_ids
    }

    //
    //
    // ---- Methods for getting materialised values.
    //
    //

    pub fn dictionary(&self) -> &[String] {
        &self.index_entries[1..]
    }

    /// Returns the logical value present at the provided row id.
    ///
    /// N.B right now this doesn't discern between an invalid row id and a NULL
    /// value at a valid location.
    pub fn value(&self, row_id: u32) -> Option<&String> {
        if row_id < self.num_rows {
            let mut total = 0;
            for (encoded_id, rl) in &self.run_lengths {
                if total + rl > row_id {
                    // this run-length overlaps desired row id
                    match *encoded_id {
                        NULL_ID => return None,
                        _ => return Some(&self.index_entries[*encoded_id as usize]),
                    };
                }
                total += rl;
            }
        }
        None
    }

    /// Materialises the decoded value belonging to the provided encoded id.
    ///
    /// Panics if there is no decoded value for the provided id
    pub fn decode_id(&self, encoded_id: u32) -> Option<String> {
        match encoded_id {
            NULL_ID => None,
            _ => Some(self.index_entries[encoded_id as usize].clone()),
        }
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

        let mut curr_logical_row_id = 0;

        let (mut curr_entry_id, mut curr_entry_rl) = self.run_lengths[0];

        let mut i = 1;
        for row_id in row_ids {
            if row_id >= &self.num_rows {
                return dst; // row ids beyond length of column
            }

            while curr_logical_row_id + curr_entry_rl <= *row_id {
                // this encoded entry does not cover the row we need.
                // move on to next entry
                curr_logical_row_id += curr_entry_rl;
                curr_entry_id = self.run_lengths[i].0;
                curr_entry_rl = self.run_lengths[i].1;

                i += 1;
            }

            // this encoded entry covers the row_id we want.
            match curr_entry_id {
                NULL_ID => dst.push(None),
                _ => dst.push(Some(&self.index_entries[curr_entry_id as usize])),
            }

            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }

        assert_eq!(row_ids.len(), dst.len());
        dst
    }

    /// Returns the lexicographical minimum value for the provided set of row
    /// ids. NULL values are not considered the minimum value if any non-null
    /// value exists at any of the provided row ids.
    pub fn min<'a>(&'a self, row_ids: &[u32]) -> Option<&'a String> {
        // exit early if there is only NULL values in the column.
        let col_min = match self.entry_index.keys().next() {
            Some(entry) => entry,
            None => return None,
        };

        let mut curr_logical_row_id = 0;
        let (mut curr_entry_id, mut curr_entry_rl) = self.run_lengths[0];
        let mut min: Option<&String> = None;

        let mut i = 1;
        for row_id in row_ids {
            while curr_logical_row_id + curr_entry_rl <= *row_id {
                // this encoded entry does not cover the row we need.
                // move on to next entry
                curr_logical_row_id += curr_entry_rl;
                curr_entry_id = self.run_lengths[i].0;
                curr_entry_rl = self.run_lengths[i].1;

                i += 1;
            }

            // this encoded entry covers a candidate row_id if it's not the NULL
            // value
            match curr_entry_id {
                NULL_ID => {}
                _ => {
                    let candidate_min = &self.index_entries[curr_entry_id as usize];
                    match min {
                        None => min = Some(candidate_min),
                        Some(curr_min) => {
                            if candidate_min < curr_min {
                                min = Some(candidate_min);
                            } else if curr_min == col_min {
                                return min; // we can't find a lower min.
                            }
                        }
                    }
                }
            }

            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }
        min
    }

    /// Returns the lexicographical maximum value for the provided set of row
    /// ids. NULL values are not considered the maximum value if any non-null
    /// value exists at any of the provided row ids.
    pub fn max<'a>(&'a self, row_ids: &[u32]) -> Option<&'a String> {
        // exit early if there is only NULL values in the column.
        let col_max = match self.entry_index.keys().rev().next() {
            Some(entry) => entry,
            None => return None,
        };

        let mut curr_logical_row_id = 0;
        let (mut curr_entry_id, mut curr_entry_rl) = self.run_lengths[0];
        let mut max: Option<&String> = None;

        let mut i = 1;
        for row_id in row_ids {
            while curr_logical_row_id + curr_entry_rl <= *row_id {
                // this encoded entry does not cover the row we need.
                // move on to next entry
                curr_logical_row_id += curr_entry_rl;
                curr_entry_id = self.run_lengths[i].0;
                curr_entry_rl = self.run_lengths[i].1;

                i += 1;
            }

            // this encoded entry covers a candidate row_id if it's not the NULL
            // value
            match curr_entry_id {
                NULL_ID => {}
                _ => {
                    let candidate_min = &self.index_entries[curr_entry_id as usize];
                    match max {
                        None => max = Some(candidate_min),
                        Some(curr_min) => {
                            if candidate_min > curr_min {
                                max = Some(candidate_min);
                            } else if curr_min == col_max {
                                return max; // we can't find a bigger max.
                            }
                        }
                    }
                }
            }

            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }
        max
    }

    /// Returns the total number of non-null values found at the provided set of
    /// row ids.
    pub fn count(&self, row_ids: &[u32]) -> u32 {
        let mut curr_logical_row_id = 0;
        let (mut curr_entry_id, mut curr_entry_rl) = self.run_lengths[0];
        let mut count = 0;

        let mut i = 1;
        for row_id in row_ids {
            while curr_logical_row_id + curr_entry_rl <= *row_id {
                // this encoded entry does not cover the row we need.
                // move on to next entry
                curr_logical_row_id += curr_entry_rl;
                curr_entry_id = self.run_lengths[i].0;
                curr_entry_rl = self.run_lengths[i].1;

                i += 1;
            }

            // this encoded entry covers a candidate row_id
            match curr_entry_id {
                NULL_ID => {}
                _ => {
                    count += 1;
                }
            }

            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }
        count
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
        dst.reserve(self.num_rows as usize);

        for (id, rl) in &self.run_lengths {
            let v = match *id {
                NULL_ID => None,
                id => Some(&self.index_entries[id as usize]),
            };

            dst.extend(iter::repeat(v).take(*rl as usize));
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

        // Used to mark off when a decoded value has been added to the result
        // set. TODO(perf) - this might benefit from being pooled somehow.
        let mut encoded_values = Vec::with_capacity(self.index_entries.len());
        encoded_values.resize(self.index_entries.len(), false);

        let mut found = 0;
        // if the encoding doesn't contain any NULL values then we can mark
        // NULL off as "found"
        if !self.contains_null {
            encoded_values[NULL_ID as usize] = true;
            found += 1;
        }

        let mut curr_logical_row_id = 0;
        let (mut curr_entry_id, mut curr_entry_rl) = self.run_lengths[0];

        let mut i = 1;
        'by_row: for row_id in row_ids {
            if row_id >= &self.num_rows {
                return dst; // rows beyond the column size
            }

            while curr_logical_row_id + curr_entry_rl <= *row_id {
                // this encoded entry does not cover the row we need.
                // move on to next entry
                curr_logical_row_id += curr_entry_rl;
                curr_entry_id = self.run_lengths[i].0;
                curr_entry_rl = self.run_lengths[i].1;

                i += 1;
            }

            // encoded value not already in result set.
            if !encoded_values[curr_entry_id as usize] {
                match curr_entry_id {
                    NULL_ID => dst.insert(None),
                    _ => dst.insert(Some(&self.index_entries[curr_entry_id as usize])),
                };

                encoded_values[curr_entry_id as usize] = true;
                found += 1;
            }

            if found == encoded_values.len() {
                // all distinct values have been read
                break 'by_row;
            }

            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }

        assert!(dst.len() <= self.index_entries.len());
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
    ///
    pub fn encoded_values(&self, row_ids: &[u32], mut dst: Vec<u32>) -> Vec<u32> {
        dst.clear();
        dst.reserve(row_ids.len());

        let mut curr_logical_row_id = 0;

        let (mut curr_entry_id, mut curr_entry_rl) = self.run_lengths[0];

        let mut i = 1;
        for row_id in row_ids {
            while curr_logical_row_id + curr_entry_rl <= *row_id {
                // this encoded entry does not cover the row we need.
                // move on to next entry
                curr_logical_row_id += curr_entry_rl;
                curr_entry_id = self.run_lengths[i].0;
                curr_entry_rl = self.run_lengths[i].1;

                i += 1;
            }

            // this entry covers the row_id we want.
            dst.push(curr_entry_id);
            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }

        assert_eq!(row_ids.len(), dst.len());
        dst
    }

    /// Returns all encoded values for the column including the encoded value
    /// for any NULL values.
    pub fn all_encoded_values(&self, mut dst: Vec<u32>) -> Vec<u32> {
        dst.clear();
        dst.reserve(self.num_rows as usize);

        for (idx, rl) in &self.run_lengths {
            dst.extend(iter::repeat(*idx).take(*rl as usize));
        }
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
        let mut encoded_values = self.index_entries.len();
        if !self.contains_null {
            encoded_values -= 1; // this column doesn't encode NULL
        }

        if encoded_values > values.len() {
            return true;
        }

        for key in self.entry_index.keys() {
            if !values.contains(&Some(key)) {
                return true;
            }
        }

        if self.contains_null && !values.contains(&None) {
            return true;
        }
        false
    }

    /// Determines if the column contains at least one non-null value at
    /// any of the provided row ids.
    ///
    /// It is the caller's responsibility to ensure row ids are a monotonically
    /// increasing set.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        if self.contains_null {
            return self.find_non_null_value(row_ids);
        }

        // There are no NULL entries in this column so just find a row id
        // that falls on any row in the column.
        for &id in row_ids {
            if id < self.num_rows {
                return true;
            }
        }
        false
    }

    // Returns true if there exists an encoded non-null value at any of the row
    // ids.
    fn find_non_null_value(&self, row_ids: &[u32]) -> bool {
        let mut curr_logical_row_id = 0;

        let (mut curr_encoded_id, mut curr_entry_rl) = self.run_lengths[0];

        let mut i = 1;
        for &row_id in row_ids {
            if row_id >= self.num_rows {
                return false; // all other row ids beyond column.
            }

            while curr_logical_row_id + curr_entry_rl <= row_id {
                // this encoded entry does not cover the row we need.
                // move on to next encoded id
                curr_logical_row_id += curr_entry_rl;
                curr_encoded_id = self.run_lengths[i].0;
                curr_entry_rl = self.run_lengths[i].1;

                i += 1;
            }

            // this entry covers the row_id we want if it points to a non-null value.
            if curr_encoded_id != NULL_ID {
                return true;
            }
            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }

        false
    }
}

impl<'a> From<Vec<&str>> for RLE {
    fn from(vec: Vec<&str>) -> Self {
        let mut drle = Self::default();
        for v in vec {
            drle.push(v.to_string());
        }
        drle
    }
}

impl<'a> From<Vec<String>> for RLE {
    fn from(vec: Vec<String>) -> Self {
        let mut drle = Self::default();
        for v in vec {
            drle.push(v);
        }
        drle
    }
}

impl<'a> From<Vec<Option<&str>>> for RLE {
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

impl<'a> From<Vec<Option<String>>> for RLE {
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

impl<'a> From<StringArray> for RLE {
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

impl std::fmt::Display for RLE {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[RLE] rows: {:?} dict entries: {}, runs: {} ",
            self.num_rows,
            self.index_entries.len(),
            self.run_lengths.len()
        )
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use crate::column::{cmp, RowIDs};

    #[test]
    fn rle_with_dictionary() {
        let mut dictionary = BTreeSet::new();
        dictionary.insert("hello".to_string());
        dictionary.insert("world".to_string());

        let drle = super::RLE::with_dictionary(dictionary);
        assert_eq!(
            drle.entry_index.keys().cloned().collect::<Vec<String>>(),
            vec!["hello".to_string(), "world".to_string(),]
        );

        // The first id is `1` because `0` is reserved for the NULL entry.
        assert_eq!(
            drle.entry_index.values().cloned().collect::<Vec<u32>>(),
            vec![1, 2],
        );

        assert_eq!(
            drle.index_row_ids.keys().cloned().collect::<Vec<u32>>(),
            vec![0, 1, 2]
        )
    }

    #[test]
    fn rle_push() {
        let mut drle = super::RLE::from(vec!["hello", "hello", "hello", "hello"]);
        drle.push_additional(Some("hello".to_string()), 1);
        drle.push_additional(None, 3);
        drle.push("world".to_string());

        assert_eq!(
            drle.all_values(vec![]),
            [
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                None,
                None,
                None,
                Some(&"world".to_string()),
            ]
        );

        drle.push_additional(Some("zoo".to_string()), 3);
        drle.push_none();
        assert_eq!(
            drle.all_values(vec![]),
            [
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                None,
                None,
                None,
                Some(&"world".to_string()),
                Some(&"zoo".to_string()),
                Some(&"zoo".to_string()),
                Some(&"zoo".to_string()),
                None,
            ]
        );
    }

    // tests a defect I discovered.
    #[test]
    fn push_additional_first_run_length() {
        let arr = vec!["world".to_string(), "hello".to_string()];

        let mut drle = super::RLE::with_dictionary(arr.into_iter().collect::<BTreeSet<String>>());
        drle.push_additional(Some("world".to_string()), 1);
        drle.push_additional(Some("hello".to_string()), 1);

        assert_eq!(
            drle.all_values(vec![]),
            vec![Some(&"world".to_string()), Some(&"hello".to_string())]
        );
        assert_eq!(drle.all_encoded_values(vec![]), vec![2, 1]);

        drle = super::RLE::default();
        drle.push_additional(Some("hello".to_string()), 1);
        drle.push_additional(Some("world".to_string()), 1);

        assert_eq!(
            drle.all_values(vec![]),
            vec![Some(&"hello".to_string()), Some(&"world".to_string())]
        );
        assert_eq!(drle.all_encoded_values(vec![]), vec![1, 2]);
    }

    #[test]
    #[should_panic]
    fn rle_push_wrong_order() {
        let mut drle = super::RLE::default();
        drle.push("b".to_string());
        drle.push("a".to_string());
    }

    #[test]
    fn row_ids_filter_equal() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_none(); // 9
        drle.push_additional(Some("south".to_string()), 2); // 10, 11

        let ids = drle.row_ids_filter(&"east", cmp::Operator::Equal, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]));

        let ids = drle.row_ids_filter(&"south", cmp::Operator::Equal, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![10, 11]));

        let ids = drle.row_ids_filter(&"foo", cmp::Operator::Equal, RowIDs::Vector(vec![]));
        assert!(ids.is_empty());

        // != some value not in the column should exclude the NULL value.
        let ids = drle.row_ids_filter(&"foo", cmp::Operator::NotEqual, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11]));

        let ids = drle.row_ids_filter(&"east", cmp::Operator::NotEqual, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![3, 10, 11]));
    }

    #[test]
    fn row_ids_filter_equal_no_null() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 2);
        drle.push_additional(Some("west".to_string()), 1);

        let ids = drle.row_ids_filter(&"abba", cmp::Operator::NotEqual, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2]));
    }

    #[test]
    fn row_ids_filter_cmp() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_additional(Some("west".to_string()), 1); // 11
        drle.push_additional(Some("north".to_string()), 1); // 12
        drle.push_none(); // 13
        drle.push_additional(Some("west".to_string()), 5); // 14, 15, 16, 17, 18

        let ids = drle.row_ids_filter(&"east", cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]));

        let ids = drle.row_ids_filter(&"east", cmp::Operator::LT, RowIDs::Vector(vec![]));
        assert!(ids.is_empty());

        let ids = drle.row_ids_filter(&"north", cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![9, 10, 11, 14, 15, 16, 17, 18]));

        let ids = drle.row_ids_filter(&"north", cmp::Operator::GTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 14, 15, 16, 17, 18])
        );

        // The encoding also supports comparisons on values that don't directly exist in the column.
        let ids = drle.row_ids_filter(&"abba", cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18
            ])
        );

        let ids = drle.row_ids_filter(&"east1", cmp::Operator::GT, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 14, 15, 16, 17, 18])
        );

        let ids = drle.row_ids_filter(&"east1", cmp::Operator::GTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 14, 15, 16, 17, 18])
        );

        let ids = drle.row_ids_filter(&"east1", cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]));

        let ids = drle.row_ids_filter(&"region", cmp::Operator::LT, RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 12]));

        let ids = drle.row_ids_filter(&"zoo", cmp::Operator::LTE, RowIDs::Vector(vec![]));
        assert_eq!(
            ids,
            RowIDs::Vector(vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 14, 15, 16, 17, 18
            ])
        );
    }

    #[test]
    fn row_ids_not_null() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(None, 3); // 3, 4, 5
        drle.push_additional(Some("north".to_string()), 1); // 6
        drle.push_additional(None, 2); // 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10

        // essentially `WHERE value IS NULL`
        let ids = drle.row_ids_null(RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![3, 4, 5, 7, 8]));

        // essentially `WHERE value IS NOT NULL`
        let ids = drle.row_ids_not_null(RowIDs::Vector(vec![]));
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 6, 9, 10]));
    }

    #[test]
    fn value() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        assert_eq!(drle.value(3), Some(&"north".to_string()));
        assert_eq!(drle.value(0), Some(&"east".to_string()));
        assert_eq!(drle.value(10), Some(&"south".to_string()));

        assert_eq!(drle.value(11), None);
        assert_eq!(drle.value(22), None);
    }

    #[test]
    fn dictionary() {
        let mut drle = super::RLE::default();
        assert!(drle.dictionary().is_empty());

        drle.push_additional(Some("east".to_string()), 23);
        drle.push_additional(Some("west".to_string()), 2);
        drle.push_none();
        drle.push_additional(Some("zoo".to_string()), 1);

        assert_eq!(
            drle.dictionary(),
            &["east".to_string(), "west".to_string(), "zoo".to_string()]
        );
    }

    #[test]
    fn values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        let mut dst = Vec::with_capacity(1000);
        dst = drle.values(&[0, 1, 3, 4], dst);
        assert_eq!(
            dst,
            vec![Some("east"), Some("east"), Some("north"), Some("east"),]
        );

        dst = drle.values(&[8, 10, 11], dst);
        assert_eq!(dst, vec![Some("east"), Some("south"), None]);

        assert_eq!(dst.capacity(), 1000);

        assert!(drle.values(&[1000], dst).is_empty());
    }

    #[test]
    fn all_values() {
        let mut drle = super::RLE::from(vec!["hello", "zoo"]);
        drle.push_none();

        let zoo = "zoo".to_string();
        let dst = vec![Some(&zoo), Some(&zoo), Some(&zoo), Some(&zoo)];
        let got = drle.all_values(dst);

        assert_eq!(
            got,
            [Some(&"hello".to_string()), Some(&"zoo".to_string()), None]
        );
        assert_eq!(got.capacity(), 4);
    }

    #[test]
    fn distinct_values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 100);

        let values = drle.distinct_values((0..100).collect::<Vec<_>>().as_slice(), BTreeSet::new());
        assert_eq!(
            values,
            vec![Some(&"east".to_string())]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        let values = drle.distinct_values((0..12).collect::<Vec<_>>().as_slice(), BTreeSet::new());
        assert_eq!(
            values,
            vec![
                None,
                Some(&"east".to_string()),
                Some(&"north".to_string()),
                Some(&"south".to_string()),
            ]
            .into_iter()
            .collect::<BTreeSet<_>>()
        );

        let values = drle.distinct_values((0..4).collect::<Vec<_>>().as_slice(), BTreeSet::new());
        assert_eq!(
            values,
            vec![Some(&"east".to_string()), Some(&"north".to_string()),]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        let values = drle.distinct_values(&[3, 10], BTreeSet::new());
        assert_eq!(
            values,
            vec![Some(&"north".to_string()), Some(&"south".to_string()),]
                .into_iter()
                .collect::<BTreeSet<_>>()
        );

        let values = drle.distinct_values(&[100], BTreeSet::new());
        assert!(values.is_empty());
    }

    #[test]
    fn contains_other_values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        let east = &"east".to_string();
        let north = &"north".to_string();
        let south = &"south".to_string();

        let mut others = BTreeSet::new();
        others.insert(Some(east));
        others.insert(Some(north));

        assert!(drle.contains_other_values(&others));

        let f1 = "foo".to_string();
        others.insert(Some(&f1));
        assert!(drle.contains_other_values(&others));

        others.insert(Some(&south));
        others.insert(None);
        assert!(!drle.contains_other_values(&others));

        let f2 = "bar".to_string();
        others.insert(Some(&f2));
        assert!(!drle.contains_other_values(&others));

        assert!(drle.contains_other_values(&BTreeSet::new()));
    }

    #[test]
    fn has_non_null_value() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        assert!(drle.has_non_null_value(&[0]));
        assert!(drle.has_non_null_value(&[0, 1, 2]));
        assert!(drle.has_non_null_value(&[10]));

        assert!(!drle.has_non_null_value(&[11]));
        assert!(!drle.has_non_null_value(&[11, 12, 100]));

        // Pure NULL column...
        drle = super::RLE::default();
        drle.push_additional(None, 10);
        assert!(!drle.has_non_null_value(&[0]));
        assert!(!drle.has_non_null_value(&[4, 7]));
    }

    #[test]
    fn encoded_values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4, 5, 6, 7, 8
        drle.push_additional(Some("south".to_string()), 2); // 9, 10
        drle.push_none(); // 11

        let mut encoded = drle.encoded_values(&[0], vec![]);
        assert_eq!(encoded, vec![1]);

        encoded = drle.encoded_values(&[1, 3, 5, 6], vec![]);
        assert_eq!(encoded, vec![1, 2, 1, 1]);

        encoded = drle.encoded_values(&[9, 10, 11], vec![]);
        assert_eq!(encoded, vec![3, 3, 0]);
    }

    #[test]
    fn all_encoded_values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3);
        drle.push_additional_none(2);
        drle.push_additional(Some("north".to_string()), 2);

        let dst = Vec::with_capacity(100);
        let dst = drle.all_encoded_values(dst);
        assert_eq!(dst, vec![1, 1, 1, 0, 0, 2, 2]);
        assert_eq!(dst.capacity(), 100);
    }

    #[test]
    fn min() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional_none(2); // 3, 4
        drle.push_additional(Some("north".to_string()), 2); // 5, 6

        assert_eq!(drle.min(&[0, 1, 2]), Some(&"east".to_string()));
        assert_eq!(drle.min(&[0, 1, 2, 3, 4, 5, 6]), Some(&"east".to_string()));
        assert_eq!(drle.min(&[4, 5, 6]), Some(&"north".to_string()));
        assert_eq!(drle.min(&[3]), None);
        assert_eq!(drle.min(&[3, 4]), None);

        let mut drle = super::RLE::default();
        drle.push_additional_none(10);
        assert_eq!(drle.min(&[2, 3, 6, 8]), None);
    }

    #[test]
    fn max() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional_none(2); // 3, 4
        drle.push_additional(Some("north".to_string()), 2); // 5, 6

        assert_eq!(drle.max(&[0, 1, 2]), Some(&"east".to_string()));
        assert_eq!(drle.max(&[0, 1, 2, 3, 4, 5, 6]), Some(&"north".to_string()));
        assert_eq!(drle.max(&[4, 5, 6]), Some(&"north".to_string()));
        assert_eq!(drle.max(&[3]), None);
        assert_eq!(drle.max(&[3, 4]), None);

        let drle = super::RLE::default();
        assert_eq!(drle.max(&[0]), None);

        let mut drle = super::RLE::default();
        drle.push_additional_none(10);
        assert_eq!(drle.max(&[2, 3, 6, 8]), None);
    }

    #[test]
    fn count() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0, 1, 2
        drle.push_additional_none(2); // 3, 4
        drle.push_additional(Some("north".to_string()), 2); // 5, 6

        assert_eq!(drle.count(&[0, 1, 2]), 3);
        assert_eq!(drle.count(&[0, 1, 2, 3, 4, 5, 6]), 5);
        assert_eq!(drle.count(&[4, 5, 6]), 2);
        assert_eq!(drle.count(&[3]), 0);
        assert_eq!(drle.count(&[3, 4]), 0);

        let mut drle = super::RLE::default();
        drle.push_additional_none(10);
        assert_eq!(drle.count(&[2, 3, 6, 8]), 0);
    }
}
