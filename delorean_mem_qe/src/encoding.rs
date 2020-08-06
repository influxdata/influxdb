use std::collections::{BTreeMap, BTreeSet};
use std::iter;

// TODO(edd): this is just for convenience. In reality one would store nulls
// separately and not use `Option<T>`.
#[derive(Debug, Default)]
pub struct PlainFixedOption<T> {
    values: Vec<Option<T>>,
}

impl<T> PlainFixedOption<T> {
    pub fn size(&self) -> usize {
        self.values.len() * std::mem::size_of::<Option<T>>()
    }
}

#[derive(Debug, Default)]
// No compression
pub struct PlainFixed<T> {
    values: Vec<T>,
    total_order: bool, // if true the column is totally ordered ascending.
}

impl<T> PlainFixed<T>
where
    T: Default + PartialEq + PartialOrd + Copy + std::fmt::Debug + std::ops::AddAssign,
{
    pub fn size(&self) -> usize {
        self.values.len() * std::mem::size_of::<T>()
    }

    pub fn row_id_eq_value(&self, v: T) -> Option<usize> {
        self.values.iter().position(|x| *x == v)
    }

    pub fn row_id_ge_value(&self, v: T) -> Option<usize> {
        self.values.iter().position(|x| *x >= v)
    }

    // get value at row_id. Panics if out of bounds.
    pub fn value(&self, row_id: usize) -> T {
        self.values[row_id]
    }

    // TODO(edd): fix this when added NULL support
    pub fn scan_from_until_some(&self, row_id: usize) -> Option<T> {
        unreachable!("to remove");
        // for v in self.values.iter().skip(row_id) {
        //     return Some(*v);
        // }
        // None
    }

    pub fn scan_from(&self, row_id: usize) -> &[T] {
        &self.values[row_id..]
    }

    /// returns a set of row ids that match an ordering on a desired value
    pub fn row_ids_roaring(&self, wanted: &T, order: std::cmp::Ordering) -> croaring::Bitmap {
        let mut bm = croaring::Bitmap::create();

        let mut found = false; //self.values[0];
        let mut count = 0;
        for (i, next) in self.values.iter().enumerate() {
            if next.partial_cmp(wanted) != Some(order) && found {
                let (min, max) = (i as u64 - count as u64, i as u64);
                bm.add_range(min..max);
                found = false;
                count = 0;
                continue;
            } else if next.partial_cmp(wanted) != Some(order) {
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
                (self.values.len()) as u64 - count as u64,
                (self.values.len()) as u64,
            );
            bm.add_range(min..max);
        }
        bm
    }

    // TODO(edd): make faster
    pub fn sum_by_ids(&self, row_ids: &croaring::Bitmap) -> T {
        let mut res = T::default();
        row_ids.iter().for_each(|x| res += self.value(x as usize));
        res
    }

    pub fn count_by_ids(&self, row_ids: &croaring::Bitmap) -> u64 {
        row_ids.cardinality()
    }
}

impl From<&[i64]> for PlainFixed<i64> {
    fn from(v: &[i64]) -> Self {
        Self {
            values: v.to_vec(),
            total_order: false,
        }
    }
}

impl From<&[f64]> for PlainFixed<f64> {
    fn from(v: &[f64]) -> Self {
        Self {
            values: v.to_vec(),
            total_order: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct DictionaryRLE {
    // stores the mapping between an entry and its assigned index.
    entry_index: BTreeMap<Option<String>, usize>,

    // Experiment - store rows that each entry has a value for
    entry_row_ids: BTreeMap<Option<String>, croaring::Bitmap>,

    // stores the mapping between an index and its entry.
    index_entry: BTreeMap<usize, Option<String>>,

    map_size: usize, // TODO(edd) this isn't perfect at all

    // stores tuples where each pair refers to a dictionary entry and the number
    // of times the entry repeats.
    run_lengths: Vec<(usize, u64)>,
    run_length_size: usize,

    total: u64,
}

impl DictionaryRLE {
    pub fn new() -> Self {
        Self {
            entry_index: BTreeMap::new(),
            entry_row_ids: BTreeMap::new(),
            index_entry: BTreeMap::new(),
            map_size: 0,
            run_lengths: Vec::new(),
            run_length_size: 0,
            total: 0,
        }
    }

    pub fn push(&mut self, v: &str) {
        self.push_additional(Some(v.to_owned()), 1);
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
                        self.run_length_size += std::mem::size_of::<(usize, u64)>();
                    }
                    self.entry_row_ids
                        .get_mut(&v)
                        .unwrap()
                        .add_range(self.total..self.total + additional);
                }
            }
            None => {
                // New dictionary entry.
                if idx.is_none() {
                    let idx = self.entry_index.len();

                    self.entry_index.insert(v.clone(), idx);
                    self.entry_row_ids
                        .insert(v.clone(), croaring::Bitmap::create());
                    if let Some(value) = &v {
                        self.map_size += value.len();
                    }
                    self.index_entry.insert(idx, v.clone());
                    self.map_size += 8 + std::mem::size_of::<usize>(); // TODO(edd): clean this option size up

                    self.run_lengths.push((idx, additional));
                    self.entry_row_ids
                        .get_mut(&v)
                        .unwrap()
                        .add_range(self.total..self.total + additional);
                    self.run_length_size += std::mem::size_of::<(usize, u64)>();
                }
            }
        }
        self.total += additional;
    }

    // row_ids returns an iterator over the set of row ids matching the provided
    // value.
    pub fn row_ids(&self, value: Option<String>) -> impl iter::Iterator<Item = usize> {
        let mut out: Vec<usize> = vec![];
        if let Some(idx) = self.entry_index.get(&value) {
            let mut index: usize = 0;
            for (other_idx, other_rl) in &self.run_lengths {
                let start = index;
                index += *other_rl as usize;
                if other_idx == idx {
                    out.extend(start..index)
                }
            }
        }
        out.into_iter()
    }

    // row_ids returns an iterator over the set of row ids matching the provided
    // value.
    pub fn row_ids_roaring(&self, value: Option<String>) -> croaring::Bitmap {
        let mut bm = croaring::Bitmap::create();
        if let Some(idx) = self.entry_index.get(&value) {
            let mut index: u64 = 0;
            for (other_idx, other_rl) in &self.run_lengths {
                let start = index;
                index += other_rl;
                if other_idx == idx {
                    bm.add_range(start..index);
                }
            }
        }
        bm
    }

    // get the set of row ids for each distinct value
    pub fn group_row_ids(&self) -> &BTreeMap<Option<String>, croaring::Bitmap> {
        &self.entry_row_ids
    }

    // row_ids returns an iterator over the set of row ids matching the provided
    // value
    // pub fn row_ids(&'a self, value: &str) -> impl iter::Iterator<Item = usize> {
    //     if let Some(idx) = self.map.get(value) {
    //         let mut index: usize = 0;
    //         return self.run_lengths.iter().flat_map(|(other_idx, other_rl)| {
    //             let start = index;
    //             index += *other_rl as usize;

    //             if other_idx != idx {
    //                 let iter: Box<dyn Iterator<Item = usize>> = Box::new(iter::empty::<usize>());
    //                 return iter;
    //             }
    //             Box::new(start..index)
    //         });
    //     }

    //     // I need to return the same type as flatten_map or box the flatten_map return and this one??
    //     unreachable!("for now");
    // }

    pub fn dictionary(&self) -> BTreeSet<Option<String>> {
        self.entry_index
            .keys()
            .cloned()
            .collect::<BTreeSet<Option<String>>>()
    }

    // get the logical value at the provided index, or None if there is no value
    // at index.
    pub fn value(&self, index: usize) -> Option<&String> {
        if index < self.total as usize {
            let mut total = 0;
            for (idx, rl) in &self.run_lengths {
                if total + rl > index as u64 {
                    // TODO(edd): Can this really be idiomatic???
                    match self.index_entry.get(idx) {
                        Some(&Some(ref result)) => return Some(result),
                        Some(&None) => return None,
                        None => return None,
                    }
                }
                total += rl;
            }
        }
        None
    }

    // materialise a slice of rows starting from index.
    pub fn scan_from(&self, index: usize) -> Vec<&Option<String>> {
        let mut result = vec![];
        if index >= self.total as usize {
            return result;
        }

        let start_row_id = index as u64;

        let mut curr_row_id = 0_u64; // this tracks the logical row id.
        for (idx, rl) in &self.run_lengths {
            // Fast path - at this point we are just materialising the RLE
            // contents.
            if curr_row_id > start_row_id {
                let row_entry = self.index_entry.get(idx).unwrap();
                result.extend(vec![row_entry; *rl as usize]);
                curr_row_id += rl;
                continue;
            }

            // Once we have reached the desired starting row_id we can emit values.
            if (curr_row_id + *rl) >= start_row_id {
                // Since it's unlikely that the desired row falls on a new RLE
                // boundary we need to account for a partial RLE entry and only
                // populate some of the remaining entry
                let remainder = (curr_row_id + rl) - start_row_id;
                let row_entry = self.index_entry.get(idx).unwrap();
                result.extend(vec![row_entry; remainder as usize]);
            }

            // move onto next RLE entry.
            curr_row_id += *rl;
        }
        result
    }

    // // get the logical value at the provided index, or scan to the next value
    // // that is non-null.
    // pub fn scan_from_until_some(&self, index: usize) -> Option<&String> {
    //     if index < self.total as usize {
    //         let mut total = 0;
    //         for (idx, rl) in &self.run_lengths {
    //             if total + rl > index as u64 {
    //                 // If there is a value then return otherwise continue.
    //                 if let Some(v) = self.index_entry.get(idx) {
    //                     return v.as_ref();
    //                 }
    //             }
    //             total += rl;
    //         }
    //     }
    //     None
    // }

    // values materialises a vector of references to all logical values in the
    // encoding.
    pub fn values(&mut self) -> Vec<Option<&String>> {
        let mut out: Vec<Option<&String>> = Vec::with_capacity(self.total as usize);

        // build reverse mapping.
        let mut idx_value = BTreeMap::new();
        for (k, v) in &self.entry_index {
            idx_value.insert(v, k);
        }
        assert_eq!(idx_value.len(), self.entry_index.len());

        for (idx, rl) in &self.run_lengths {
            // TODO(edd): fix unwrap - we know that the value exists in map...
            let v = idx_value.get(&idx).unwrap().as_ref();
            out.extend(iter::repeat(v).take(*rl as usize));
        }
        out
    }

    pub fn size(&self) -> usize {
        // mapping and reverse mapping then the rles
        2 * self.map_size + self.run_length_size
    }
}

// TODO(edd): improve perf here....
impl std::convert::From<Vec<&str>> for DictionaryRLE {
    fn from(vec: Vec<&str>) -> Self {
        let mut drle = Self::new();
        for v in vec {
            drle.push(v);
        }
        drle
    }
}

// TODO(edd): improve perf here....
impl std::convert::From<&delorean_table::Packer<delorean_table::ByteArray>> for DictionaryRLE {
    fn from(p: &delorean_table::Packer<delorean_table::ByteArray>) -> Self {
        let mut drle = Self::new();
        for v in p.values() {
            let s = v
                .clone()
                .unwrap_or_else(|| delorean_table::ByteArray::from("NULL"));
            drle.push(s.as_utf8().unwrap());
        }
        drle
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn plain_row_ids_roaring_eq() {
        let input = vec![1, 1, 1, 1, 3, 4, 4, 5, 6, 5, 5, 5, 1, 5];
        let col = super::PlainFixed::from(input.as_slice());

        let bm = col.row_ids_roaring(&4, std::cmp::Ordering::Equal);
        assert_eq!(bm.to_vec(), vec![5, 6]);

        let bm = col.row_ids_roaring(&1, std::cmp::Ordering::Equal);
        assert_eq!(bm.to_vec(), vec![0, 1, 2, 3, 12]);

        let bm = col.row_ids_roaring(&6, std::cmp::Ordering::Equal);
        assert_eq!(bm.to_vec(), vec![8]);

        let bm = col.row_ids_roaring(&5, std::cmp::Ordering::Equal);
        assert_eq!(bm.to_vec(), vec![7, 9, 10, 11, 13]);

        let bm = col.row_ids_roaring(&20, std::cmp::Ordering::Equal);
        assert_eq!(bm.to_vec(), vec![]);
    }

    #[test]
    fn plain_row_ids_roaring_gt() {
        let input = vec![1, 1, 1, 1, 3, 4, 4, 5, 6, 5, 5, 5, 1, 5];
        let col = super::PlainFixed::from(input.as_slice());

        let bm = col.row_ids_roaring(&0, std::cmp::Ordering::Greater);
        let exp: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
        assert_eq!(bm.to_vec(), exp);

        let bm = col.row_ids_roaring(&4, std::cmp::Ordering::Greater);
        let exp: Vec<u32> = vec![7, 8, 9, 10, 11, 13];
        assert_eq!(bm.to_vec(), exp);

        let bm = col.row_ids_roaring(&5, std::cmp::Ordering::Greater);
        let exp: Vec<u32> = vec![8];
        assert_eq!(bm.to_vec(), exp);
    }

    #[test]
    fn dict_rle() {
        let mut drle = super::DictionaryRLE::new();
        drle.push("hello");
        drle.push("hello");
        drle.push("world");
        drle.push("hello");
        drle.push("hello");
        drle.push_additional(Some("hello".to_string()), 1);

        assert_eq!(
            drle.values(),
            [
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"world".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string())
            ]
        );

        drle.push_additional(Some("zoo".to_string()), 3);
        assert_eq!(
            drle.values(),
            [
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"world".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"hello".to_string()),
                Some(&"zoo".to_string()),
                Some(&"zoo".to_string()),
                Some(&"zoo".to_string()),
            ]
        );

        assert_eq!(drle.value(0).unwrap(), "hello");
        assert_eq!(drle.value(1).unwrap(), "hello");
        assert_eq!(drle.value(2).unwrap(), "world");
        assert_eq!(drle.value(3).unwrap(), "hello");
        assert_eq!(drle.value(4).unwrap(), "hello");
        assert_eq!(drle.value(5).unwrap(), "hello");
        assert_eq!(drle.value(6).unwrap(), "zoo");
        assert_eq!(drle.value(7).unwrap(), "zoo");
        assert_eq!(drle.value(8).unwrap(), "zoo");

        let row_ids = drle
            .entry_row_ids
            .get(&Some("hello".to_string()))
            .unwrap()
            .to_vec();
        assert_eq!(row_ids, vec![0, 1, 3, 4, 5]);

        let row_ids = drle
            .entry_row_ids
            .get(&Some("world".to_string()))
            .unwrap()
            .to_vec();
        assert_eq!(row_ids, vec![2]);

        let row_ids = drle
            .entry_row_ids
            .get(&Some("zoo".to_string()))
            .unwrap()
            .to_vec();
        assert_eq!(row_ids, vec![6, 7, 8]);
    }

    #[test]
    fn dict_rle_scan_from() {
        let mut drle = super::DictionaryRLE::new();
        let west = Some("west".to_string());
        let east = Some("east".to_string());
        let north = Some("north".to_string());
        drle.push_additional(west.clone(), 3);
        drle.push_additional(east.clone(), 2);
        drle.push_additional(north.clone(), 4);

        // all entries
        let results = drle.scan_from(0);
        let mut exp = vec![&west; 3];
        exp.extend(vec![&east; 2].iter());
        exp.extend(vec![&north; 4].iter());
        assert_eq!(results, exp);

        // partial results from an RLE entry
        let results = drle.scan_from(2);
        let mut exp = vec![&west; 1]; // notice partial results
        exp.extend(vec![&east; 2].iter());
        exp.extend(vec![&north; 4].iter());
        assert_eq!(results, exp);

        // right on a boundary
        let results = drle.scan_from(3);
        let mut exp = vec![&east; 2];
        exp.extend(vec![&north; 4].iter());
        assert_eq!(results, exp);

        // partial final result
        let results = drle.scan_from(6);
        assert_eq!(results, vec![&north; 3]);

        // out of bounds
        let results = drle.scan_from(9);
        let exp: Vec<&Option<String>> = vec![];
        assert_eq!(results, exp);
    }

    #[test]
    fn rle_dict_row_ids() {
        let mut drle = super::DictionaryRLE::new();
        drle.push_additional(Some("abc".to_string()), 3);
        drle.push_additional(Some("dre".to_string()), 2);
        drle.push("abc");

        let ids = drle
            .row_ids(Some("abc".to_string()))
            .collect::<Vec<usize>>();
        assert_eq!(ids, vec![0, 1, 2, 5]);

        let ids = drle
            .row_ids(Some("dre".to_string()))
            .collect::<Vec<usize>>();
        assert_eq!(ids, vec![3, 4]);

        let ids = drle
            .row_ids(Some("foo".to_string()))
            .collect::<Vec<usize>>();
        let empty: Vec<usize> = vec![];
        assert_eq!(ids, empty);
    }

    #[test]
    fn dict_rle_row_ids_roaring() {
        let mut drle = super::DictionaryRLE::new();
        drle.push_additional(Some("abc".to_string()), 3);
        drle.push_additional(Some("dre".to_string()), 2);
        drle.push("abc");

        let ids = drle
            .row_ids_roaring(Some("abc".to_string()))
            .iter()
            .collect::<Vec<u32>>();
        assert_eq!(ids, vec![0, 1, 2, 5]);

        let ids = drle
            .row_ids_roaring(Some("dre".to_string()))
            .iter()
            .collect::<Vec<u32>>();
        assert_eq!(ids, vec![3, 4]);

        let ids = drle
            .row_ids_roaring(Some("foo".to_string()))
            .iter()
            .collect::<Vec<u32>>();
        let empty: Vec<u32> = vec![];
        assert_eq!(ids, empty);
    }
}
