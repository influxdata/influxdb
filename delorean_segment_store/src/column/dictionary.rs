use std::collections::BTreeMap;
use std::convert::From;
use std::iter;

use croaring::Bitmap;

use delorean_arrow::arrow::array::{Array, StringArray};

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
    run_lengths: Vec<(u32, u64)>,

    num_rows: u64,
}

impl RLE {
    pub fn push(&mut self, v: String) {
        self.push_additional(Some(v), 1);
    }

    pub fn push_none(&mut self) {
        self.push_additional(None, 1);
    }

    pub fn push_additional(&mut self, v: Option<String>, additional: u64) {
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
                        .add_range(self.num_rows..self.num_rows + additional);
                }
            }
            None => {
                // New dictionary entry.
                let idx = self.index_entries.len() as u32;
                self.index_entries.push(v.clone());

                self.entry_index.insert(v, idx);
                self.index_row_ids.insert(idx, Bitmap::create());

                self.run_lengths.push((idx, additional));
                self.index_row_ids
                    .get_mut(&(idx as u32))
                    .unwrap()
                    .add_range(self.num_rows..self.num_rows + additional);
            }
        }
        self.num_rows += additional;
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
    #[test]
    fn rle_push() {
        let mut drle = super::RLE::from(vec!["hello", "hello", "world", "hello", "hello"]);
        drle.push_none();
        drle.push_additional(Some("hello".to_string()), 1);

        assert_eq!(
            drle.all_values(vec![]),
            [
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("world".to_string()),
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &None,
                &Some("hello".to_string())
            ]
        );

        drle.push_additional(Some("zoo".to_string()), 3);
        assert_eq!(
            drle.all_values(vec![]),
            [
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &Some("world".to_string()),
                &Some("hello".to_string()),
                &Some("hello".to_string()),
                &None,
                &Some("hello".to_string()),
                &Some("zoo".to_string()),
                &Some("zoo".to_string()),
                &Some("zoo".to_string()),
            ]
        );

        // assert_eq!(drle.value(0).unwrap(), "hello");
        // assert_eq!(drle.value(1).unwrap(), "hello");
        // assert_eq!(drle.value(2).unwrap(), "world");
        // assert_eq!(drle.value(3).unwrap(), "hello");
        // assert_eq!(drle.value(4).unwrap(), "hello");
        // assert_eq!(drle.value(5).unwrap(), "hello");
        // assert_eq!(drle.value(6).unwrap(), "zoo");
        // assert_eq!(drle.value(7).unwrap(), "zoo");
        // assert_eq!(drle.value(8).unwrap(), "zoo");

        // let row_ids = drle.index_row_ids.get(&0).unwrap().to_vec();
        // assert_eq!(row_ids, vec![0, 1, 3, 4, 5]);

        // let row_ids = drle.index_row_ids.get(&1).unwrap().to_vec();
        // assert_eq!(row_ids, vec![2]);

        // let row_ids = drle.index_row_ids.get(&2).unwrap().to_vec();
        // assert_eq!(row_ids, vec![6, 7, 8]);
    }
}
