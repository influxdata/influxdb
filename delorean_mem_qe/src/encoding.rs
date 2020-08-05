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
}

impl<T> PlainFixed<T> {
    pub fn size(&self) -> usize {
        self.values.len() * std::mem::size_of::<T>()
    }
}

impl From<&[i64]> for PlainFixed<i64> {
    fn from(v: &[i64]) -> Self {
        Self { values: v.to_vec() }
    }
}

impl From<&[f64]> for PlainFixed<f64> {
    fn from(v: &[f64]) -> Self {
        Self { values: v.to_vec() }
    }
}

#[derive(Debug, Default)]
pub struct DictionaryRLE {
    // stores the mapping between an entry and its assigned index.
    map: BTreeMap<String, usize>,
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
            map: BTreeMap::new(),
            map_size: 0,
            run_lengths: Vec::new(),
            run_length_size: 0,
            total: 0,
        }
    }

    pub fn push(&mut self, v: &str) {
        self.push_additional(v, 1);
    }

    pub fn push_additional(&mut self, v: &str, additional: u64) {
        self.total += additional;
        let idx = self.map.get(v);
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
                    let idx = self.map.len();

                    self.map.insert(String::from(v), idx);
                    self.map_size += v.len() + std::mem::size_of::<usize>();

                    self.run_lengths.push((idx, additional));
                    self.run_length_size += std::mem::size_of::<(usize, u64)>();
                    return;
                }
            }
        }
    }

    // row_ids returns an iterator over the set of row ids matching the provided
    // value.
    pub fn row_ids(&self, value: &str) -> impl iter::Iterator<Item = usize> {
        let mut out: Vec<usize> = vec![];
        if let Some(idx) = self.map.get(value) {
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
    pub fn row_ids_roaring(&self, value: &str) -> croaring::Bitmap {
        let mut bm = croaring::Bitmap::create();
        if let Some(idx) = self.map.get(value) {
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

    pub fn dictionary(&self) -> BTreeSet<String> {
        self.map.keys().cloned().collect::<BTreeSet<String>>()
    }

    // get the logical value at the provided index, or None if there is no value
    // at index.
    pub fn value(&self, index: usize) -> Option<&str> {
        if index < self.total as usize {
            // build reverse mapping.
            let mut idx_value = BTreeMap::new();
            for (k, v) in &self.map {
                idx_value.insert(v, k.as_str());
            }
            assert_eq!(idx_value.len(), self.map.len());

            let mut total = 0;
            for (idx, rl) in &self.run_lengths {
                if total + rl > index as u64 {
                    return idx_value.get(idx).cloned();
                }
                total += rl;
            }
        }
        None
    }

    // values materialises a vector of references to all logical values in the
    // encoding.
    pub fn values(&mut self) -> Vec<&str> {
        let mut out = Vec::with_capacity(self.total as usize);

        // build reverse mapping.
        let mut idx_value = BTreeMap::new();
        for (k, v) in &self.map {
            idx_value.insert(v, k.as_str());
        }
        assert_eq!(idx_value.len(), self.map.len());

        for (idx, rl) in &self.run_lengths {
            let &v = idx_value.get(&idx).unwrap();
            out.extend(iter::repeat(&v).take(*rl as usize));
        }
        out
    }

    pub fn size(&self) -> usize {
        self.map_size + self.run_length_size
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
        drle.push_additional("hello", 1);

        assert_eq!(
            drle.values(),
            ["hello", "hello", "world", "hello", "hello", "hello",]
        );

        drle.push_additional("zoo", 3);
        assert_eq!(
            drle.values(),
            ["hello", "hello", "world", "hello", "hello", "hello", "zoo", "zoo", "zoo"]
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
        drle.push_additional("abc", 3);
        drle.push_additional("dre", 2);
        drle.push("abc");

        let ids = drle.row_ids("abc").collect::<Vec<usize>>();
        assert_eq!(ids, vec![0, 1, 2, 5]);

        let ids = drle.row_ids("dre").collect::<Vec<usize>>();
        assert_eq!(ids, vec![3, 4]);

        let ids = drle.row_ids("foo").collect::<Vec<usize>>();
        assert_eq!(ids, vec![]);
    }

    #[test]
    fn row_ids_roaring() {
        let mut drle = super::DictionaryRLE::new();
        drle.push_additional("abc", 3);
        drle.push_additional("dre", 2);
        drle.push("abc");

        let ids = drle.row_ids_roaring("abc").iter().collect::<Vec<u32>>();
        assert_eq!(ids, vec![0, 1, 2, 5]);

        let ids = drle.row_ids_roaring("dre").iter().collect::<Vec<u32>>();
        assert_eq!(ids, vec![3, 4]);

        let ids = drle.row_ids_roaring("foo").iter().collect::<Vec<u32>>();
        assert_eq!(ids, vec![]);
    }
}
