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
    T: PartialEq + Copy,
{
    pub fn size(&self) -> usize {
        self.values.len() * std::mem::size_of::<T>()
    }

    pub fn row_id_for_value(&self, v: T) -> Option<usize> {
        self.values.iter().position(|x| *x == v)
    }

    // get value at row_id. Panics if out of bounds.
    pub fn value(&self, row_id: usize) -> T {
        self.values[row_id]
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
        self.total += additional;
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
                }
            }
            None => {
                // New dictionary entry.
                if idx.is_none() {
                    let idx = self.entry_index.len();

                    self.entry_index.insert(v.clone(), idx);
                    if let Some(value) = &v {
                        self.map_size += value.len();
                    }
                    self.index_entry.insert(idx, v);
                    self.map_size += 8 + std::mem::size_of::<usize>(); // TODO(edd): clean this option size up

                    self.run_lengths.push((idx, additional));
                    self.run_length_size += std::mem::size_of::<(usize, u64)>();
                    return;
                }
            }
        }
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
    }

    #[test]
    fn row_ids() {
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
        assert_eq!(ids, vec![]);
    }

    #[test]
    fn row_ids_roaring() {
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
        assert_eq!(ids, vec![]);
    }
}
