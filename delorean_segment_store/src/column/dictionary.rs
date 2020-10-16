use std::collections::BTreeMap;
use std::convert::From;
use std::iter;

use croaring::Bitmap;

use delorean_arrow::arrow::array::{Array, StringArray};

use crate::column::{cmp, RowIDs};

// `RLE` is a run-length encoding for dictionary columns, where all dictionary
// entries are utf-8 valid strings.
#[derive(Default)]
pub struct RLE {
    // TODO(edd): revisit choice of storing owned string versus references.

    // The mapping between an entry and its assigned index.
    entry_index: BTreeMap<Option<String>, u32>,

    // The mapping between an index and its entry.
    index_entries: Vec<Option<String>>,

    // The set of rows that belong to each distinct value in the dictionary.
    // This allows essentially constant time grouping of rows on the column by
    // value.
    index_row_ids: BTreeMap<u32, Bitmap>,

    // stores tuples where each pair refers to a dictionary entry and the number
    // of times the entry repeats.
    run_lengths: Vec<(u32, u32)>,

    num_rows: u32,
}

impl RLE {
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
        let idx = self.entry_index.get(&v);
        match idx {
            Some(idx) => {
                if let Some((last_idx, rl)) = self.run_lengths.last_mut() {
                    if last_idx == idx {
                        // update the existing run-length
                        *rl += additional;
                    } else {
                        // start a new run-length
                        self.run_lengths.push((*idx, additional));
                    }
                    self.index_row_ids
                        .get_mut(&(*idx as u32))
                        .unwrap()
                        .add_range(self.num_rows as u64..self.num_rows as u64 + additional as u64);
                }
            }
            None => {
                // New dictionary entry.
                let idx = self.index_entries.len() as u32;
                if idx > 0 {
                    match (&self.index_entries[idx as usize - 1], &v) {
                        (None, Some(_)) => panic!("out of order dictionary insertion"),
                        (Some(_), None) => {}
                        (Some(a), Some(b)) => assert!(a < b),
                        (_, _) => unreachable!("multiple None values"),
                    }
                }
                self.index_entries.push(v.clone());

                self.entry_index.insert(v, idx);
                self.index_row_ids.insert(idx, Bitmap::create());

                self.run_lengths.push((idx, additional));
                self.index_row_ids
                    .get_mut(&(idx as u32))
                    .unwrap()
                    .add_range(self.num_rows as u64..self.num_rows as u64 + additional as u64);
            }
        }
        self.num_rows += additional;
    }

    //
    //
    // ---- Methods for getting row ids from values.
    //
    //

    /// Populates the provided destination container with the row ids satisfying
    /// the provided predicate.
    pub fn row_ids_filter(&self, value: Option<String>, op: cmp::Operator, dst: RowIDs) -> RowIDs {
        match op {
            cmp::Operator::Equal | cmp::Operator::NotEqual => self.row_ids_equal(value, op, dst),
            cmp::Operator::LT | cmp::Operator::LTE | cmp::Operator::GT | cmp::Operator::GTE => {
                self.row_ids_cmp(value, op, dst)
            }
        }
    }

    // Finds row ids based on = or != operator.
    fn row_ids_equal(&self, value: Option<String>, op: cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();
        let include = match op {
            cmp::Operator::Equal => true,
            cmp::Operator::NotEqual => false,
            _ => unreachable!("invalid operator"),
        };

        if let Some(encoded_id) = self.entry_index.get(&value) {
            let mut index: u32 = 0;
            for (other_encoded_id, other_rl) in &self.run_lengths {
                let start = index;
                index += *other_rl;
                if (other_encoded_id == encoded_id) == include {
                    dst.add_range(start, index)
                }
            }
        } else if let cmp::Operator::NotEqual = op {
            // special case - the column does not contain the provided
            // value and the operator is != so we need to return all
            // row ids.
            dst.add_range(0, self.num_rows)
        }

        dst
    }

    // Finds row ids based on <, <=, > or >= operator.
    fn row_ids_cmp(&self, value: Option<String>, op: cmp::Operator, mut dst: RowIDs) -> RowIDs {
        dst.clear();

        // happy path - the value exists in the column
        if let Some(encoded_id) = self.entry_index.get(&value) {
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
                if cmp(other_encoded_id, encoded_id) {
                    dst.add_range(start, index)
                }
            }
            return dst;
        }

        match op {
            cmp::Operator::GT | cmp::Operator::GTE => {
                // find the first decoded value that satisfies the predicate.
                for (other, other_encoded_id) in &self.entry_index {
                    if other > &value {
                        // change filter from either `x > value` or `x >= value` to `x >= other`
                        return self.row_ids_cmp(other.clone(), cmp::Operator::GTE, dst);
                    }
                }
            }
            cmp::Operator::LT | cmp::Operator::LTE => {
                // find the first decoded value that satisfies the predicate.
                // Note iteration is in reverse
                for (other, other_encoded_id) in self.entry_index.iter().rev() {
                    if other < &value {
                        // change filter from either `x < value` or `x <= value` to `x <= other`
                        return self.row_ids_cmp(other.clone(), cmp::Operator::LTE, dst);
                    }
                }
            }
            _ => unreachable!("operator not supported"),
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

    pub fn dictionary(&self) -> &[Option<String>] {
        &self.index_entries
    }

    /// Returns the logical value present at the provided row id.
    ///
    /// N.B right now this doesn't discern between an invalid row id and a NULL
    /// value at a valid location.
    pub fn value(&self, row_id: u32) -> &Option<String> {
        if row_id < self.num_rows {
            let mut total = 0;
            for (encoded_id, rl) in &self.run_lengths {
                if total + rl > row_id {
                    // this run-length overlaps desired row id
                    return &self.index_entries[*encoded_id as usize];
                }
                total += rl;
            }
        }
        &None
    }

    /// Materialises a vector of references to the decoded values in the
    /// provided row ids.
    ///
    /// NULL values are represented by None
    pub fn values<'a>(
        &'a self,
        row_ids: &[u32],
        mut dst: Vec<&'a Option<String>>,
    ) -> Vec<&'a Option<String>> {
        dst.clear();
        dst.reserve(row_ids.len());

        let mut curr_logical_row_id = 0;

        let mut run_lengths_iter = self.run_lengths.iter();
        let (mut curr_entry_id, mut curr_entry_rl) = run_lengths_iter.next().unwrap();

        for wanted_row_id in row_ids {
            while curr_logical_row_id + curr_entry_rl <= *wanted_row_id {
                // this encoded entry does not cover the row we need.
                // move on to next entry
                curr_logical_row_id += curr_entry_rl;
                match run_lengths_iter.next() {
                    Some(res) => {
                        curr_entry_id = res.0;
                        curr_entry_rl = res.1;
                    }
                    None => panic!("shouldn't get here"),
                }
            }

            // this encoded entry covers the row_id we want.
            // let value = &self.index_entries[curr_entry_id as usize];
            dst.push(&self.index_entries[curr_entry_id as usize]);
            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }

        assert_eq!(row_ids.len(), dst.len());
        dst
    }

    /// Returns references to the logical (decoded) values for all the rows in
    /// the column.
    ///
    /// NULL values are represented by None.
    ///
    pub fn all_values<'a>(
        &'a mut self,
        mut dst: Vec<&'a Option<String>>,
    ) -> Vec<&'a Option<String>> {
        dst.clear();
        dst.reserve(self.num_rows as usize);

        for (idx, rl) in &self.run_lengths {
            let v = &self.index_entries[*idx as usize];
            dst.extend(iter::repeat(v).take(*rl as usize));
        }
        dst
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

#[cfg(test)]
mod test {
    use crate::column::{cmp, RowIDs};

    #[test]
    fn rle_push() {
        let mut drle = super::RLE::from(vec!["hello", "hello", "hello", "hello"]);
        drle.push_additional(Some("hello".to_string()), 1);
        drle.push("world".to_string());

        assert_eq!(
            drle.all_values(vec![]),
            [
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("world".to_string()),
            ]
        );

        drle.push_additional(Some("zoo".to_string()), 3);
        drle.push_none();
        assert_eq!(
            drle.all_values(vec![]),
            [
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("world".to_string()),
                &Some("zoo".to_string()),
                &Some("zoo".to_string()),
                &Some("zoo".to_string()),
                &None,
            ]
        );
    }

    #[test]
    #[should_panic]
    fn rle_push_none_first() {
        let mut drle = super::RLE::default();
        drle.push_none();
        drle.push_additional(Some("hello".to_string()), 1);
    }

    #[test]
    #[should_panic]
    fn rle_push_wrong_order() {
        let mut drle = super::RLE::default();
        drle.push("b".to_string());
        drle.push("a".to_string());
    }

    #[test]
    fn all_values() {
        let mut drle = super::RLE::from(vec!["hello", "zoo"]);

        let zoo = Some("zoo".to_string());
        let dst = vec![&zoo, &zoo, &zoo, &zoo];
        let got = drle.all_values(dst);

        assert_eq!(got, [&Some("hello".to_string()), &Some("zoo".to_string()),]);
        assert_eq!(got.capacity(), 4);
    }

    #[test]
    fn row_ids_filter_equal() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3);
        drle.push_additional(Some("north".to_string()), 1);
        drle.push_additional(Some("east".to_string()), 5);
        drle.push_additional(Some("south".to_string()), 2);

        let ids = drle.row_ids_filter(
            Some("east".to_string()),
            cmp::Operator::Equal,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]));

        let ids = drle.row_ids_filter(
            Some("south".to_string()),
            cmp::Operator::Equal,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector(vec![9, 10]));

        let ids = drle.row_ids_filter(
            Some("foo".to_string()),
            cmp::Operator::Equal,
            RowIDs::Vector(vec![]),
        );
        assert!(ids.is_empty());

        let ids = drle.row_ids_filter(
            Some("foo".to_string()),
            cmp::Operator::NotEqual,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector((0..11).collect::<Vec<_>>()));

        let ids = drle.row_ids_filter(
            Some("east".to_string()),
            cmp::Operator::NotEqual,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector(vec![3, 9, 10]));
    }

    #[test]
    fn row_ids_filter_cmp() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3); // 0,1,2
        drle.push_additional(Some("north".to_string()), 1); // 3
        drle.push_additional(Some("east".to_string()), 5); // 4,5,6,7,8
        drle.push_additional(Some("south".to_string()), 2); // 9,10
        drle.push_additional(Some("west".to_string()), 1); // 11
        drle.push_additional(Some("north".to_string()), 1); // 12
        drle.push_additional(Some("west".to_string()), 5); // 13,14,15,16,17

        let ids = drle.row_ids_filter(
            Some("east".to_string()),
            cmp::Operator::LTE,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]));

        let ids = drle.row_ids_filter(
            Some("east".to_string()),
            cmp::Operator::LT,
            RowIDs::Vector(vec![]),
        );
        assert!(ids.is_empty());

        let ids = drle.row_ids_filter(
            Some("north".to_string()),
            cmp::Operator::GT,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector(vec![9, 10, 11, 13, 14, 15, 16, 17]));

        let ids = drle.row_ids_filter(
            Some("north".to_string()),
            cmp::Operator::GTE,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 13, 14, 15, 16, 17])
        );

        // The encoding also supports comparisons on values that don't directly exist in the column.
        let ids = drle.row_ids_filter(
            Some("abba".to_string()),
            cmp::Operator::GT,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector((0..18).collect::<Vec<u32>>()));

        let ids = drle.row_ids_filter(
            Some("east1".to_string()),
            cmp::Operator::GT,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 13, 14, 15, 16, 17])
        );

        let ids = drle.row_ids_filter(
            Some("east1".to_string()),
            cmp::Operator::GTE,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(
            ids,
            RowIDs::Vector(vec![3, 9, 10, 11, 12, 13, 14, 15, 16, 17])
        );

        let ids = drle.row_ids_filter(
            Some("east1".to_string()),
            cmp::Operator::LTE,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 4, 5, 6, 7, 8]));

        let ids = drle.row_ids_filter(
            Some("region".to_string()),
            cmp::Operator::LT,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 12]));

        let ids = drle.row_ids_filter(
            Some("zoo".to_string()),
            cmp::Operator::LTE,
            RowIDs::Vector(vec![]),
        );
        assert_eq!(ids, RowIDs::Vector((0..18).collect::<Vec<u32>>()));
    }

    #[test]
    fn value() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3);
        drle.push_additional(Some("north".to_string()), 1);
        drle.push_additional(Some("east".to_string()), 5);
        drle.push_additional(Some("south".to_string()), 2);

        assert_eq!(drle.value(3), &Some("north".to_string()));
        assert_eq!(drle.value(0), &Some("east".to_string()));
        assert_eq!(drle.value(10), &Some("south".to_string()));

        assert_eq!(drle.value(22), &None);
    }

    #[test]
    fn values() {
        let mut drle = super::RLE::default();
        drle.push_additional(Some("east".to_string()), 3);
        drle.push_additional(Some("north".to_string()), 1);
        drle.push_additional(Some("east".to_string()), 5);
        drle.push_additional(Some("south".to_string()), 2);
        drle.push_none();

        let mut dst = Vec::with_capacity(1000);
        dst = drle.values(&[0, 1, 3, 4], dst);
        assert_eq!(
            dst,
            vec![
                &Some("east".to_string()),
                &Some("east".to_string()),
                &Some("north".to_string()),
                &Some("east".to_string())
            ]
        );

        dst = drle.values(&[8, 10, 11], dst);
        assert_eq!(
            dst,
            vec![&Some("east".to_string()), &Some("south".to_string()), &None]
        );

        assert_eq!(dst.capacity(), 1000);
    }
}
