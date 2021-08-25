use std::collections::{BTreeMap, BTreeSet};
use std::convert::From;
use std::iter;
use std::mem::size_of;

use arrow::array::{Array, StringArray};

use super::NULL_ID;
use crate::column::{cmp, RowIDs};

pub const ENCODING_NAME: &str = "RLE";

// `RLE` is a run-length encoding for dictionary columns, where all dictionary
// entries are utf-8 valid strings.
#[allow(clippy::upper_case_acronyms)] // this looks weird as `Rle`
pub struct RLE {
    // The mapping between an id (as an index) and its entry.
    // The first element is always "" representing NULL strings
    // and the elements are sorted
    index_entries: Vec<String>,

    // The set of rows that belong to each distinct value in the dictionary.
    // This allows essentially constant time grouping of rows on the column by
    // value.
    index_row_ids: BTreeMap<u32, RowIDs>,

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
        // for this to make sense NULL_ID must be `0`.
        assert_eq!(NULL_ID, 0);

        let mut _self = Self {
            index_entries: vec!["".to_string()],
            index_row_ids: BTreeMap::new(),
            run_lengths: Vec::new(),
            contains_null: false,
            num_rows: 0,
        };
        _self.index_row_ids.insert(NULL_ID, RowIDs::new_bitmap());

        _self
    }
}

impl RLE {
    /// Initialises an RLE encoding with a set of column values, ensuring that
    /// the rows in the column can be inserted in any order and the correct
    /// ordinal relationship will exist between the encoded values.
    pub fn with_dictionary(dictionary: BTreeSet<String>) -> Self {
        let mut index_entries = Vec::with_capacity(dictionary.len() + 1);
        index_entries.push(String::from(""));

        let mut _self = Self {
            index_entries,
            ..Self::default()
        };

        for mut entry in dictionary.into_iter() {
            let next_id = _self.next_encoded_id();

            // Depending on how the string was created, the backing vector might have more capacity than the actual
            // data. Shrink it (this is a no-op if the capacity already matches).
            entry.shrink_to_fit();

            _self.index_entries.push(entry);
            _self.index_row_ids.insert(next_id, RowIDs::new_bitmap());
        }

        _self
    }

    /// A reasonable estimation of the on-heap size this encoding takes up.
    /// If `buffers` is true then the size of all allocated buffers in the
    /// encoding are accounted for.
    pub fn size(&self, buffers: bool) -> usize {
        let base_size = size_of::<Self>();

        let mut index_entries_size = size_of::<String>()
            * match buffers {
                true => self.index_entries.capacity(),
                false => self.index_entries.len(),
            };
        // the total size of all decoded values in the column.
        index_entries_size += self
            .index_entries
            .iter()
            .map(|k| match buffers {
                true => k.capacity(),
                false => k.len(),
            })
            .sum::<usize>();

        // The total size (an upper bound estimate) of all the bitmaps
        // in the column - only set if including allocated capacity, otherwise
        // ignore because not required for RLE compression.
        let row_ids_bitmaps_size = match buffers {
            true => self
                .index_row_ids
                .values()
                .map(|row_ids| row_ids.size())
                .sum::<usize>(),
            false => 0,
        };

        let index_row_ids_size =
            (size_of::<u32>() * self.index_row_ids.len()) + row_ids_bitmaps_size;

        let run_lengths_size = size_of::<(u32, u32)>()
            * match buffers {
                true => self.run_lengths.capacity(),
                false => self.run_lengths.len(),
            };

        base_size + index_entries_size + index_row_ids_size + run_lengths_size
    }

    /// A reasonable estimation of the on-heap size of the underlying string
    /// values in this column if they were stored uncompressed contiguously.
    /// `include_nulls` determines whether to assign a pointer size to the null
    /// values or not.
    ///
    pub fn size_raw(&self, include_nulls: bool) -> usize {
        let mut total_size = 0;
        for (idx, rows) in &self.index_row_ids {
            // the length of the string value x number of times it appears
            // in the column.
            total_size += self.index_entries[*idx as usize].len() * rows.len();

            if include_nulls || idx != &NULL_ID {
                total_size += size_of::<String>() * rows.len()
            }
        }
        total_size + size_of::<Vec<String>>()
    }

    /// The number of distinct logical values in this column encoding.
    pub fn cardinality(&self) -> u32 {
        if self.contains_null {
            self.index_entries.len() as u32
        } else {
            self.index_entries.len() as u32 - 1
        }
    }

    /// The number of NULL values in this column.
    pub fn null_count(&self) -> u32 {
        self.index_row_ids
            .get(&NULL_ID)
            .map_or(0, |rows| rows.len() as u32)
    }

    /// Adds the provided string value to the encoded data. It is the caller's
    /// responsibility to ensure that the dictionary encoded remains sorted.
    pub fn push(&mut self, mut v: String) {
        v.shrink_to_fit();
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

    #[inline]
    fn lookup_entry(&self, entry: &str) -> Option<u32> {
        Some(
            self.index_entries[1..]
                .binary_search_by(|probe| probe.as_str().cmp(entry))
                .ok()? as u32
                + 1,
        )
    }

    #[inline]
    fn keys(&self) -> impl Iterator<Item = &String> + ExactSizeIterator + DoubleEndedIterator + '_ {
        // Skip the first NULL value
        self.index_entries.iter().skip(1)
    }

    fn push_additional_some(&mut self, mut v: String, additional: u32) {
        match self.lookup_entry(&v) {
            // existing dictionary entry for value.
            Some(id) => {
                match self.run_lengths.last_mut() {
                    Some((last_id, rl)) => {
                        if *last_id == id {
                            // update the existing run-length
                            *rl += additional;
                        } else {
                            // start a new run-length for an existing id
                            self.run_lengths.push((id, additional));
                        }
                    }
                    // very first run-length in column...
                    None => {
                        self.run_lengths.push((id, additional));
                    }
                }

                // Update the rows associated with the value.
                self.index_row_ids
                    .get_mut(&id)
                    .unwrap()
                    .add_range(self.num_rows, self.num_rows + additional);
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
                v.shrink_to_fit();
                self.index_entries.push(v);

                self.index_row_ids.insert(next_id, RowIDs::new_bitmap());

                // start a new run-length
                self.run_lengths.push((next_id, additional));

                // update the rows associated with the value.
                self.index_row_ids
                    .get_mut(&(next_id as u32))
                    .unwrap()
                    .add_range(self.num_rows, self.num_rows + additional);
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
                .add_range(self.num_rows, self.num_rows + additional);
        } else {
            // very first run-length in column...
            self.run_lengths.push((NULL_ID, additional));
            self.contains_null = true; // set null marker.

            // update the rows associated with the value.
            self.index_row_ids
                .get_mut(&NULL_ID)
                .unwrap()
                .add_range(self.num_rows, self.num_rows + additional);
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

        if let Some(encoded_id) = self.lookup_entry(value) {
            match op {
                cmp::Operator::Equal => {
                    let ids = self.index_row_ids.get(&encoded_id).unwrap();
                    dst.union(ids);
                    return dst;
                }
                cmp::Operator::NotEqual => {
                    // TODO(edd): perf - invert the bitset we know contains the
                    // row ids...
                    let mut index: u32 = 0;
                    for (other_encoded_id, other_rl) in &self.run_lengths {
                        let start = index;
                        index += *other_rl;
                        if other_encoded_id == &NULL_ID {
                            continue; // skip NULL values
                        } else if *other_encoded_id != encoded_id {
                            // we found a row that doesn't match the value
                            dst.add_range(start, index)
                        }
                    }
                }
                _ => unreachable!("invalid operator"),
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
    fn row_ids_cmp(&self, value: &str, op: &cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        // happy path - the value exists in the column
        if let Some(encoded_id) = self.lookup_entry(value) {
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
                } else if cmp(other_encoded_id, &encoded_id) {
                    dst.add_range(start, index)
                }
            }
            return dst;
        }

        match op {
            cmp::Operator::GT | cmp::Operator::GTE => {
                // find the first decoded value that satisfies the predicate.
                // Skip the first "" representing NULL
                for other in self.keys() {
                    if other.as_str() > value {
                        // change filter from either `x > value` or `x >= value` to `x >= other`
                        return self.row_ids_cmp(other, &cmp::Operator::GTE, dst);
                    }
                }
            }
            cmp::Operator::LT | cmp::Operator::LTE => {
                // find the first decoded value that satisfies the predicate.
                // Skip the "" at index 0 representing NULL
                // Note iteration is in reverse
                for other in self.keys().rev() {
                    if other.as_str() < value {
                        // change filter from either `x < value` or `x <= value` to `x <= other`
                        return self.row_ids_cmp(other, &cmp::Operator::LTE, dst);
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
    pub fn group_row_ids(&self) -> Vec<&RowIDs> {
        self.index_row_ids.values().collect()
    }

    //
    //
    // ---- Methods for getting materialised values.
    //
    //

    pub fn dictionary(&self) -> Vec<&String> {
        self.index_entries.iter().skip(1).collect()
    }

    /// Returns the logical value present at the provided row id.
    ///
    /// N.B right now this doesn't discern between an invalid row id and a NULL
    /// value at a valid location.
    pub fn value(&self, row_id: u32) -> Option<&String> {
        assert!(
            row_id < self.num_rows(),
            "row_id {:?} out of bounds for {:?} rows",
            row_id,
            self.num_rows()
        );

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
        unreachable!()
    }

    /// Materialises the decoded value belonging to the provided encoded id.
    ///
    /// Panics if there is no decoded value for the provided id
    pub fn decode_id(&self, encoded_id: u32) -> Option<&str> {
        match encoded_id {
            NULL_ID => None,
            _ => Some(self.index_entries[encoded_id as usize].as_str()),
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
        let col_min = self.keys().next()?;

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
        let col_max = self.keys().rev().next()?;

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

    /// Returns the lexicographical minimum value in the column. None is
    /// returned only if the column does not contain any non-null values.
    pub fn column_min(&self) -> Option<&'_ String> {
        if self.index_entries.len() > 1 {
            return Some(self.index_entries.get(1).unwrap());
        }
        None
    }

    /// Returns the lexicographical maximum value in the column. None is
    /// returned only if the column does not contain any non-null values.
    pub fn column_max(&self) -> Option<&'_ String> {
        if self.index_entries.len() > 1 {
            return Some(self.index_entries.last().unwrap());
        }
        None
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
    pub fn all_values<'a>(&'a self, mut dst: Vec<Option<&'a str>>) -> Vec<Option<&'a str>> {
        dst.clear();
        dst.reserve(self.num_rows as usize);

        for (id, rl) in &self.run_lengths {
            let v = match *id {
                NULL_ID => None,
                id => Some(self.index_entries[id as usize].as_str()),
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
        row_ids: impl Iterator<Item = u32>,
        mut dst: BTreeSet<Option<&'a str>>,
    ) -> BTreeSet<Option<&'a str>> {
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
            while curr_logical_row_id + curr_entry_rl <= row_id {
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
        self.keys().any(|entry| !values.contains(entry))
    }

    /// Determines if the column contains at least one non-null value.
    pub fn has_any_non_null_value(&self) -> bool {
        if !self.contains_null() {
            return true;
        }

        // If there are any non-null rows then there are entries in the
        // dictionary.
        self.index_entries.len() > 1
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
            "[{}] size: {:?} rows: {:?} cardinality: {}, nulls: {} runs: {} ",
            ENCODING_NAME,
            self.size(false),
            self.num_rows,
            self.cardinality(),
            self.null_count(),
            self.run_lengths.len()
        )
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn with_dictionary() {
        let mut dictionary = BTreeSet::new();
        dictionary.insert("hello".to_string());
        dictionary.insert("world".to_string());

        let drle = RLE::with_dictionary(dictionary);
        assert_eq!(drle.index_entries.as_slice(), &["", "hello", "world"]);

        assert_eq!(
            drle.index_row_ids.keys().cloned().collect::<Vec<u32>>(),
            vec![0, 1, 2]
        )
    }

    #[test]
    fn size() {
        let mut enc = RLE::default();
        enc.push_additional(Some("east".to_string()), 3);
        enc.push_additional(Some("north".to_string()), 1);
        enc.push_additional(Some("east".to_string()), 5);
        enc.push_additional(Some("south".to_string()), 2);
        enc.push_none();
        enc.push_none();
        enc.push_none();
        enc.push_none();

        // * Self: 24 + 24 + 24 + 1 + (padding 3b) + 4 = 80b
        // * index entries: (4) are is (24*4) + 14 == 110
        // * index row ids: (bitmaps) not included in calc
        // * run lengths: (8*5) == 40
        //
        assert_eq!(enc.size(false), 246);

        // check allocated size
        let mut enc = RLE::default();
        enc.index_entries.reserve_exact(39); // account for already-allocated NULL element
        enc.run_lengths.reserve_exact(40);

        enc.push_additional(Some("east".to_string()), 3);
        enc.push_additional(Some("north".to_string()), 1);
        enc.push_additional(Some("east".to_string()), 5);
        enc.push_additional(Some("south".to_string()), 2);
        enc.push_none();
        enc.push_none();
        enc.push_none();
        enc.push_none();

        // * Self: 24 + 24 + 24 + 1 + (padding 3b) + 4 = 80b
        // * index entries: (40 * 24) + 14 == 974
        // * index row ids: (bitmaps) is (4 * 4) + (204b for bitmaps) == 220
        // * run lengths: (40 * 8) == 320
        //
        assert_eq!(enc.size(true), 1544);
    }

    #[test]
    fn null_count() {
        let mut enc = RLE::default();
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
    fn size_raw() {
        let mut enc = RLE::default();
        enc.push_additional(Some("a longer string".to_string()), 3);
        enc.push_additional(Some("south".to_string()), 2);
        enc.push_none();
        enc.push_none();
        enc.push_none();
        enc.push_none();

        // Vec<String>       =                    24b
        // "a longer string" = (15b + 24b) * 3 = 117b
        // "south"           = (5b  + 24b) * 2 =  58b
        // NULL              = (0b  + 24b) * 4 =  96b
        assert_eq!(enc.size_raw(true), 295);

        // Vec<String>       =                    24b
        // "a longer string" = (15b + 24b) * 3 = 117b
        // "south"           = (5b  + 24b) * 2 =  58b
        assert_eq!(enc.size_raw(false), 199);

        let enc = RLE::default();
        assert_eq!(enc.size_raw(true), 24);
    }

    #[test]
    #[should_panic]
    fn push_wrong_order() {
        let mut enc = RLE::default();
        enc.push("b".to_string());
        enc.push("a".to_string());
    }
}
