use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::iter;
use std::mem::size_of;

use arrow::array::{Array, PrimitiveArray};
use arrow::datatypes::ArrowNumericType;

pub trait NumericEncoding: Send + Sync + std::fmt::Display + std::fmt::Debug {
    type Item;

    fn size(&self) -> usize;
    fn value(&self, row_id: usize) -> Option<Self::Item>;
    fn values(&self, row_ids: &[usize]) -> Vec<Option<Self::Item>>;

    fn encoded_values(&self, row_ids: &[usize]) -> Vec<Self::Item>;
    fn all_encoded_values(&self) -> Vec<Self::Item>;

    fn scan_from(&self, row_id: usize) -> &[Option<Self::Item>];

    fn sum_by_ids(&self, row_ids: &mut croaring::Bitmap) -> Option<Self::Item>;
    fn sum_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> Option<Self::Item>;

    fn count_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> u64;
    fn count_by_ids(&self, row_ids: &croaring::Bitmap) -> u64;

    // Returns the index of the first value equal to `v`
    fn row_id_eq_value(&self, v: Self::Item) -> Option<usize>;

    // Returns the index of the first value greater or equal to `v`
    fn row_id_ge_value(&self, v: Self::Item) -> Option<usize>;

    fn row_ids_single_cmp_roaring(
        &self,
        wanted: &Self::Item,
        order: std::cmp::Ordering,
    ) -> croaring::Bitmap;
    fn row_ids_gte_lt_roaring(&self, from: &Self::Item, to: &Self::Item) -> croaring::Bitmap;
}

#[derive(Debug)]
pub struct PlainArrow<T>
where
    T: ArrowNumericType,
    T::Native: Default
        + PartialEq
        + PartialOrd
        + Copy
        + std::fmt::Debug
        + std::ops::Add<Output = T::Native>,
{
    arr: PrimitiveArray<T>,
}

impl<T> PlainArrow<T>
where
    T: ArrowNumericType + std::fmt::Debug,
    T::Native: Default
        + PartialEq
        + PartialOrd
        + Copy
        + std::fmt::Debug
        + std::ops::Add<Output = T::Native>,
{
    pub fn new(arr: PrimitiveArray<T>) -> Self {
        Self { arr }
    }
}

impl<T> NumericEncoding for PlainArrow<T>
where
    T: ArrowNumericType + std::fmt::Debug,
    T::Native: Default
        + PartialEq
        + PartialOrd
        + Copy
        + std::fmt::Debug
        + std::ops::Add<Output = T::Native>,
{
    type Item = T::Native;

    fn size(&self) -> usize {
        self.arr.len()
    }

    fn value(&self, row_id: usize) -> Option<T::Native> {
        if self.arr.is_null(row_id) {
            return None;
        }
        Some(self.arr.value(row_id))
    }

    fn values(&self, row_ids: &[usize]) -> Vec<Option<T::Native>> {
        let mut out = Vec::with_capacity(row_ids.len());
        for &row_id in row_ids {
            if self.arr.is_null(row_id) {
                out.push(None)
            } else {
                out.push(Some(self.arr.value(row_id)))
            }
        }
        assert_eq!(out.len(), row_ids.len());
        out
    }

    /// encoded_values returns encoded values for the encoding. If the encoding
    /// supports null values then the values returned are undefined.
    ///
    /// encoded_values should not be called on nullable columns.
    fn encoded_values(&self, _: &[usize]) -> Vec<T::Native> {
        todo!();
    }

    fn all_encoded_values(&self) -> Vec<T::Native> {
        todo!();
    }

    // TODO(edd): problem here is returning a slice because we need to own the
    // backing vector.
    fn scan_from(&self, _: usize) -> &[Option<T::Native>] {
        unimplemented!("need to figure out returning a slice");
        // let mut out = Vec::with_capacity(self.arr.len() - row_id);
        // for i in row_id..self.arr.len() {
        //     if self.arr.is_null(i) {
        //         out.push(None)
        //     } else {
        //         out.push(Some(self.arr.value(i)))
        //     }
        // }
        // assert_eq!(out.len(), self.arr.len());
        // out.as_slice()
    }

    fn sum_by_ids(&self, row_ids: &mut croaring::Bitmap) -> Option<T::Native> {
        // TODO(edd): this is expensive - may pay to expose method to do this
        // where you accept an array.
        let mut res = T::Native::default();
        let vec = row_ids.to_vec();
        let mut non_null = false;
        for row_id in vec {
            let i = row_id as usize;
            if self.arr.is_null(i) {
                continue; // skip NULL values
            }
            non_null = true;
            res = res + self.arr.value(i);
        }

        // TODO: ghetto.
        if non_null {
            Some(res)
        } else {
            None
        }
    }

    fn sum_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> Option<T::Native> {
        let mut res = T::Native::default();
        let mut non_null = false;

        for i in from_row_id..to_row_id {
            if self.arr.is_null(i) {
                continue;
            }
            non_null = true;
            res = res + self.arr.value(i);
        }

        if non_null {
            Some(res)
        } else {
            None
        }
    }

    fn count_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> u64 {
        // TODO - count values that are not null in the row range.
        let mut count = 0;
        for i in from_row_id..to_row_id {
            if self.arr.is_null(i) {
                continue;
            }
            count += 1;
        }
        count // if there are no non-null rows the result is 0 rather than NULL
    }

    fn count_by_ids(&self, _: &croaring::Bitmap) -> u64 {
        todo!()
    }

    fn row_id_eq_value(&self, v: Self::Item) -> Option<usize> {
        for i in 0..self.arr.len() {
            if self.arr.is_null(i) {
                continue;
            } else if self.arr.value(i) == v {
                return Some(i);
            }
        }
        None
    }

    fn row_id_ge_value(&self, v: Self::Item) -> Option<usize> {
        for i in 0..self.arr.len() {
            if self.arr.is_null(i) {
                continue;
            } else if self.arr.value(i) >= v {
                return Some(i);
            }
        }
        None
    }

    fn row_ids_single_cmp_roaring(
        &self,
        _: &Self::Item,
        _: std::cmp::Ordering,
    ) -> croaring::Bitmap {
        todo!()
    }

    fn row_ids_gte_lt_roaring(&self, from: &Self::Item, to: &Self::Item) -> croaring::Bitmap {
        let mut bm = croaring::Bitmap::create();

        let mut found = false; //self.values[0];
        let mut count = 0;
        for i in 0..self.arr.len() {
            let next = &self.arr.value(i);
            if (self.arr.is_null(i) || next < from || next >= to) && found {
                let (min, max) = (i as u64 - count as u64, i as u64);
                bm.add_range(min..max);
                found = false;
                count = 0;
                continue;
            } else if self.arr.is_null(i) || next < from || next >= to {
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
                (self.arr.len()) as u64 - count as u64,
                (self.arr.len()) as u64,
            );
            bm.add_range(min..max);
        }
        bm
    }
}

impl<T: ArrowNumericType> std::fmt::Display for PlainArrow<T>
where
    T: ArrowNumericType + std::fmt::Debug,
    T::Native: Default
        + PartialEq
        + PartialOrd
        + Copy
        + std::fmt::Debug
        + std::ops::Add<Output = T::Native>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[PlainArrow<T>] rows: {:?}, nulls: {:?}, size: {}",
            self.arr.len(),
            self.arr.null_count(),
            self.size()
        )
    }
}

#[derive(Debug, Default)]
// No compression
pub struct PlainFixed<T> {
    values: Vec<T>,
    // total_order can be used as a hint to stop scanning the column early when
    // applying a comparison predicate to the column.
    total_order: bool,

    size: usize,
}

impl<T> std::fmt::Display for PlainFixed<T>
where
    T: Default
        + PartialEq
        + PartialOrd
        + Copy
        + std::fmt::Debug
        + std::fmt::Display
        + Sync
        + Send
        + std::ops::AddAssign,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[PlainFixed<T>] rows: {:?}, size: {}",
            self.values.len(),
            self.size()
        )
    }
}

impl<T> NumericEncoding for PlainFixed<T>
where
    T: Default
        + PartialEq
        + PartialOrd
        + Copy
        + std::fmt::Debug
        + std::fmt::Display
        + Sync
        + Send
        + std::ops::AddAssign,
{
    type Item = T;

    fn size(&self) -> usize {
        self.size
    }

    fn row_id_eq_value(&self, v: T) -> Option<usize> {
        self.values.iter().position(|x| *x == v)
    }

    fn row_id_ge_value(&self, v: T) -> Option<usize> {
        self.values.iter().position(|x| *x >= v)
    }

    // get value at row_id. Panics if out of bounds.
    fn value(&self, row_id: usize) -> Option<Self::Item> {
        Some(self.values[row_id])
    }

    /// Return the decoded values for the provided logical row ids.
    fn values(&self, row_ids: &[usize]) -> Vec<Option<Self::Item>> {
        let mut out = Vec::with_capacity(row_ids.len());
        for chunks in row_ids.chunks_exact(4) {
            out.push(Some(self.values[chunks[3]]));
            out.push(Some(self.values[chunks[2]]));
            out.push(Some(self.values[chunks[1]]));
            out.push(Some(self.values[chunks[0]]));
        }

        let rem = row_ids.len() % 4;
        for &i in &row_ids[row_ids.len() - rem..row_ids.len()] {
            out.push(Some(self.values[i]));
        }

        assert_eq!(out.len(), row_ids.len());
        out
    }

    /// Return the raw encoded values for the provided logical row ids. For
    /// Plain encoding this is just the decoded values.
    fn encoded_values(&self, row_ids: &[usize]) -> Vec<T> {
        let mut out = Vec::with_capacity(row_ids.len());
        for chunks in row_ids.chunks_exact(4) {
            out.push(self.values[chunks[3]]);
            out.push(self.values[chunks[2]]);
            out.push(self.values[chunks[1]]);
            out.push(self.values[chunks[0]]);
        }

        let rem = row_ids.len() % 4;
        for &i in &row_ids[row_ids.len() - rem..row_ids.len()] {
            out.push(self.values[i]);
        }

        assert_eq!(out.len(), row_ids.len());
        out
    }

    /// Return all encoded values. For this encoding this is just the decoded
    /// values
    fn all_encoded_values(&self) -> Vec<T> {
        self.values.clone() // TODO(edd):perf probably can return reference to
                            // vec.
    }

    fn scan_from(&self, _: usize) -> &[Option<Self::Item>] {
        unimplemented!("this should probably take a destination vector or maybe a closure");
        // &self.values[row_id..]
    }

    /// returns a set of row ids that match a single ordering on a desired value
    ///
    /// This supports `value = x` , `value < x` or `value > x`.
    fn row_ids_single_cmp_roaring(
        &self,
        wanted: &T,
        order: std::cmp::Ordering,
    ) -> croaring::Bitmap {
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

    /// returns a set of row ids that match the half open interval `[from, to)`.
    ///
    /// The main use-case for this is time range filtering.
    fn row_ids_gte_lt_roaring(&self, from: &T, to: &T) -> croaring::Bitmap {
        let mut bm = croaring::Bitmap::create();

        let mut found = false; //self.values[0];
        let mut count = 0;
        for (i, next) in self.values.iter().enumerate() {
            if (next < from || next >= to) && found {
                let (min, max) = (i as u64 - count as u64, i as u64);
                bm.add_range(min..max);
                found = false;
                count = 0;
                continue;
            } else if next < from || next >= to {
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

    fn sum_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> Option<Self::Item> {
        let mut res = T::default();
        for v in self.values[from_row_id..to_row_id].iter() {
            res += *v;
        }
        Some(res)
    }

    fn count_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> u64 {
        (to_row_id - from_row_id) as u64
    }

    // TODO(edd): make faster
    fn sum_by_ids(&self, row_ids: &mut croaring::Bitmap) -> Option<Self::Item> {
        let mut res = T::default();

        // Consider accepting a vec of ids if those ids need to be used again
        // across other columns.
        let vec = row_ids.to_vec();
        for v in vec {
            //  Todo(edd): this could benefit from unrolling (maybe)
            res += self.values[v as usize];
        }

        Some(res)
    }

    fn count_by_ids(&self, row_ids: &croaring::Bitmap) -> u64 {
        row_ids.cardinality()
    }
}

impl From<&[i64]> for PlainFixed<i64> {
    fn from(v: &[i64]) -> Self {
        Self {
            values: v.to_vec(),
            total_order: false,
            size: size_of::<Vec<i64>>()
                + (size_of::<i64>() * v.len())
                + size_of::<bool>()
                + size_of::<usize>(),
        }
    }
}

impl From<&[f64]> for PlainFixed<f64> {
    fn from(v: &[f64]) -> Self {
        Self {
            values: v.to_vec(),
            total_order: false,
            size: size_of::<Vec<f64>>()
                + (size_of::<f64>() * v.len())
                + size_of::<bool>()
                + size_of::<usize>(),
        }
    }
}

#[derive(Debug, Default)]
pub struct DictionaryRle {
    // stores the mapping between an entry and its assigned index.
    entry_index: BTreeMap<Option<String>, usize>,

    // stores the mapping between an index and its entry.
    index_entry: BTreeMap<usize, Option<String>>,

    // Experiment - store rows that each entry has a value for
    index_row_ids: BTreeMap<u32, croaring::Bitmap>,

    map_size: usize, // TODO(edd) this isn't perfect at all

    // stores tuples where each pair refers to a dictionary entry and the number
    // of times the entry repeats.
    run_lengths: Vec<(usize, u64)>,

    nulls: u64,
    total: u64,
}

impl DictionaryRle {
    pub fn new() -> Self {
        Self {
            entry_index: BTreeMap::new(),
            index_row_ids: BTreeMap::new(),
            index_entry: BTreeMap::new(),
            map_size: 0,
            run_lengths: Vec::new(),
            nulls: 0,
            total: 0,
        }
    }

    pub fn with_dictionary(dictionary: BTreeSet<Option<String>>) -> Self {
        let mut _self = Self {
            entry_index: BTreeMap::new(),
            index_row_ids: BTreeMap::new(),
            index_entry: BTreeMap::new(),
            map_size: 0,
            run_lengths: Vec::new(),
            nulls: 0,
            total: 0,
        };

        for (next_idx, k) in dictionary.iter().enumerate() {
            _self.entry_index.insert(k.to_owned(), next_idx);
            _self.index_entry.insert(next_idx, k.to_owned());

            _self
                .index_row_ids
                .insert(next_idx as u32, croaring::Bitmap::create());

            _self.run_lengths.push((next_idx, 0)); // could this cause a bug?ta
        }
        _self
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
                    }
                    self.index_row_ids
                        .get_mut(&(*idx as u32))
                        .unwrap()
                        .add_range(self.total..self.total + additional);
                }
            }
            None => {
                // New dictionary entry.
                if idx.is_none() {
                    let idx = self.entry_index.len();

                    self.entry_index.insert(v.clone(), idx);
                    self.index_row_ids
                        .insert(idx as u32, croaring::Bitmap::create());
                    if let Some(value) = &v {
                        self.map_size += value.len();
                    }
                    self.index_entry.insert(idx, v.clone());
                    self.map_size += 8 + std::mem::size_of::<usize>(); // TODO(edd): clean this option size up

                    self.run_lengths.push((idx, additional));
                    self.index_row_ids
                        .get_mut(&(idx as u32))
                        .unwrap()
                        .add_range(self.total..self.total + additional);
                }
            }
        }
        self.total += additional;
        if v.is_none() {
            self.nulls += additional;
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
    pub fn row_ids_eq_roaring(&self, value: Option<String>) -> croaring::Bitmap {
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
    pub fn group_row_ids(&self) -> &BTreeMap<u32, croaring::Bitmap> {
        &self.index_row_ids
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
    //                 let iter: Box<dyn Iterator<Item = usize>> =
    // Box::new(iter::empty::<usize>());                 return iter;
    //             }
    //             Box::new(start..index)
    //         });
    //     }

    //     // I need to return the same type as flatten_map or box the flatten_map
    // return and this one??     unreachable!("for now");
    // }

    pub fn dictionary(&self) -> BTreeMap<Option<String>, usize> {
        self.entry_index.clone()
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

    // materialises a vector of references to the decoded values in the
    // each provided row_id.
    pub fn values(&self, row_ids: &[usize]) -> Vec<&Option<String>> {
        let mut out: Vec<&Option<String>> = Vec::with_capacity(row_ids.len());

        let mut curr_logical_row_id = 0;

        let mut run_lengths_iter = self.run_lengths.iter();
        let (mut curr_entry_id, mut curr_entry_rl) = run_lengths_iter.next().unwrap();

        for wanted_row_id in row_ids {
            while curr_logical_row_id + curr_entry_rl <= *wanted_row_id as u64 {
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
            let value = self.index_entry.get(&curr_entry_id).unwrap();
            out.push(value);
            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }

        assert_eq!(row_ids.len(), out.len());
        out
    }

    // values materialises a vector of references to all logical values in the
    // encoding.
    pub fn all_values(&mut self) -> Vec<Option<&String>> {
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

    /// Returns the unique set of values encoded at each of the provided ids.
    /// NULL values are not returned.
    pub fn distinct_values(&self, row_ids: &[usize]) -> BTreeSet<&String> {
        // TODO(edd): can improve on this if we know encoded data is totally
        // ordered.
        let mut encoded_values = HashSet::new();

        let mut curr_logical_row_id = 0;
        let mut run_lengths_iter = self.run_lengths.iter();
        let (mut curr_entry_id, mut curr_entry_rl) = run_lengths_iter.next().unwrap();

        'by_row: for row_id in row_ids {
            while curr_logical_row_id + curr_entry_rl <= *row_id as u64 {
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

            // track encoded value
            encoded_values.insert(curr_entry_id);
            if encoded_values.len() == self.index_entry.len() {
                // all distinct values have been read
                break 'by_row;
            }

            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }

        assert!(encoded_values.len() <= self.index_entry.len());

        // Finally, materialise the decoded values for the encoded set.
        let mut results = BTreeSet::new();
        for id in encoded_values.iter() {
            let decoded_value = self.index_entry.get(id).unwrap();
            if let Some(value) = decoded_value {
                results.insert(value);
            }
        }
        results
    }

    /// Returns true if the encoding contains values other than those provided
    /// in `values`.
    pub fn contains_other_values(&self, values: &BTreeSet<&String>) -> bool {
        let mut encoded_values = self.entry_index.len();
        if self.entry_index.contains_key(&None) {
            encoded_values -= 1;
        }

        if encoded_values > values.len() {
            return true;
        }

        for key in self.entry_index.keys() {
            match key {
                Some(key) => {
                    if !values.contains(key) {
                        return true;
                    }
                }
                None => continue, // skip NULL
            }
        }
        false
    }

    /// Determines if the encoded data contains at least one non-null value at
    /// any of the provided row ids.
    pub fn has_non_null_value_in_row_ids(&self, row_ids: &[usize]) -> bool {
        let null_encoded_value = self.entry_index.get(&None);
        if null_encoded_value.is_none() {
            // there are no NULL entries in this encoded column so return true
            // as soon a row_id is found that's < the number of rows encoded in
            // the column.
            for &id in row_ids {
                if (id as u64) < self.total {
                    return true;
                }
            }
            return false;
        }
        let null_encoded_value = *null_encoded_value.unwrap();

        // Return true if there exists an encoded value at any of the row ids
        // that is not equal to `null_encoded_value`. In such a case the column
        // contains a non-NULL value at one of the row ids.
        let mut curr_logical_row_id = 0;
        let mut run_lengths_iter = self.run_lengths.iter();
        let (mut curr_encoded_id, mut curr_entry_rl) = run_lengths_iter.next().unwrap();

        for &row_id in row_ids {
            if (row_id as u64) >= self.total {
                continue; // can't possibly have a value at this row id.
            }

            while curr_logical_row_id + curr_entry_rl <= row_id as u64 {
                // this encoded entry does not cover the row we need.
                // move on to next encoded id
                curr_logical_row_id += curr_entry_rl;
                match run_lengths_iter.next() {
                    Some(res) => {
                        curr_encoded_id = res.0;
                        curr_entry_rl = res.1;
                    }
                    // TODO(edd): deal with this properly.
                    None => panic!("shouldn't get here"),
                }
            }

            // this entry covers the row_id we want.
            if curr_encoded_id != null_encoded_value {
                return true;
            }
            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }

        false
    }

    /// Return the decoded value for an encoded ID.
    ///
    /// Panics if there is no decoded value for the provided id
    pub fn decode_id(&self, encoded_id: usize) -> Option<String> {
        self.index_entry.get(&encoded_id).unwrap().clone()
    }

    /// Return the raw encoded values for the provided logical row ids.
    ///
    /// TODO(edd): return type is wrong but I'm making it fit
    pub fn encoded_values(&self, row_ids: &[usize]) -> Vec<u32> {
        let mut out = Vec::with_capacity(row_ids.len());

        let mut curr_logical_row_id = 0;

        let mut run_lengths_iter = self.run_lengths.iter();
        let (mut curr_entry_id, mut curr_entry_rl) = run_lengths_iter.next().unwrap();

        for wanted_row_id in row_ids {
            while curr_logical_row_id + curr_entry_rl <= *wanted_row_id as u64 {
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

            // this entry covers the row_id we want.
            out.push(curr_entry_id as u32);
            curr_logical_row_id += 1;
            curr_entry_rl -= 1;
        }

        assert_eq!(row_ids.len(), out.len());
        out
    }

    // all_encoded_values materialises a vector of all encoded values for the
    // column.
    pub fn all_encoded_values(&self) -> Vec<u32> {
        let mut out = Vec::with_capacity(self.total as usize);

        for (idx, rl) in &self.run_lengths {
            out.extend(iter::repeat(*idx as u32).take(*rl as usize));
        }
        out
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

    pub fn size(&self) -> usize {
        // entry_index: BTreeMap<Option<String>, usize>,

        // // stores the mapping between an index and its entry.
        // index_entry: BTreeMap<usize, Option<String>>,

        (self.index_entry.len() * size_of::<BTreeMap<usize, Option<String>>>())
            + (self.index_row_ids.len() * size_of::<BTreeMap<u32, croaring::Bitmap>>())
            + size_of::<usize>()
            + (self.run_lengths.len() * size_of::<Vec<(usize, u64)>>())
            + size_of::<u64>()
    }
}

impl std::fmt::Display for DictionaryRle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[DictionaryRLE] rows: {:?} nulls: {:?}, size: {}, dict entries: {}, runs: {} ",
            self.total,
            self.nulls,
            self.size(),
            self.index_entry.len(),
            self.run_lengths.len()
        )
    }
}

// TODO(edd): improve perf here....
impl std::convert::From<Vec<&str>> for DictionaryRle {
    fn from(vec: Vec<&str>) -> Self {
        let mut drle = Self::new();
        for v in vec {
            drle.push(v);
        }
        drle
    }
}

// TODO(edd): improve perf here....
impl std::convert::From<&packers::Packer<packers::ByteArray>> for DictionaryRle {
    fn from(p: &packers::Packer<packers::ByteArray>) -> Self {
        let mut drle = Self::new();
        for v in p.values() {
            let s = v
                .clone()
                .unwrap_or_else(|| packers::ByteArray::from("NULL"));
            drle.push(s.as_utf8().unwrap());
        }
        drle
    }
}

#[cfg(test)]
mod test {
    use super::NumericEncoding;

    #[test]
    fn plain_arrow() {
        let col = super::PlainArrow {
            arr: super::PrimitiveArray::from(vec![Some(2.3), Some(44.56), None]),
        };

        let sum = col.sum_by_id_range(0, 2);
        assert_eq!(sum, Some(46.86));
    }

    #[test]
    fn plain_row_ids_roaring_eq() {
        let input = vec![1, 1, 1, 1, 3, 4, 4, 5, 6, 5, 5, 5, 1, 5];
        let col = super::PlainFixed::from(input.as_slice());

        let bm = col.row_ids_single_cmp_roaring(&4, std::cmp::Ordering::Equal);
        assert_eq!(bm.to_vec(), vec![5, 6]);

        let bm = col.row_ids_single_cmp_roaring(&1, std::cmp::Ordering::Equal);
        assert_eq!(bm.to_vec(), vec![0, 1, 2, 3, 12]);

        let bm = col.row_ids_single_cmp_roaring(&6, std::cmp::Ordering::Equal);
        assert_eq!(bm.to_vec(), vec![8]);

        let bm = col.row_ids_single_cmp_roaring(&5, std::cmp::Ordering::Equal);
        assert_eq!(bm.to_vec(), vec![7, 9, 10, 11, 13]);

        let bm = col.row_ids_single_cmp_roaring(&20, std::cmp::Ordering::Equal);
        let exp: Vec<u32> = Vec::new();
        assert_eq!(bm.to_vec(), exp);
    }

    #[test]
    fn plain_row_ids_cmp_roaring_gt() {
        let input = vec![1, 1, 1, 1, 3, 4, 4, 5, 6, 5, 5, 5, 1, 5];
        let col = super::PlainFixed::from(input.as_slice());

        let bm = col.row_ids_single_cmp_roaring(&0, std::cmp::Ordering::Greater);
        let exp: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
        assert_eq!(bm.to_vec(), exp);

        let bm = col.row_ids_single_cmp_roaring(&4, std::cmp::Ordering::Greater);
        let exp: Vec<u32> = vec![7, 8, 9, 10, 11, 13];
        assert_eq!(bm.to_vec(), exp);

        let bm = col.row_ids_single_cmp_roaring(&5, std::cmp::Ordering::Greater);
        let exp: Vec<u32> = vec![8];
        assert_eq!(bm.to_vec(), exp);
    }

    #[test]
    fn plain_row_ids_gte_lt_roaring() {
        let input = vec![1, 1, 1, 1, 3, 4, 4, 5, 6, 5, 5, 5, 1, 5];
        let col = super::PlainFixed::from(input.as_slice());

        let bm = col.row_ids_gte_lt_roaring(&-1, &7);
        let exp: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
        assert_eq!(bm.to_vec(), exp);

        let bm = col.row_ids_gte_lt_roaring(&1, &5);
        let exp: Vec<u32> = vec![0, 1, 2, 3, 4, 5, 6, 12];
        assert_eq!(bm.to_vec(), exp);

        let bm = col.row_ids_gte_lt_roaring(&0, &1);
        let exp: Vec<u32> = vec![];
        assert_eq!(bm.to_vec(), exp);

        let bm = col.row_ids_gte_lt_roaring(&1, &2);
        let exp: Vec<u32> = vec![0, 1, 2, 3, 12];
        assert_eq!(bm.to_vec(), exp);
    }

    #[test]
    fn dict_rle() {
        let mut drle = super::DictionaryRle::new();
        drle.push("hello");
        drle.push("hello");
        drle.push("world");
        drle.push("hello");
        drle.push("hello");
        drle.push_additional(Some("hello".to_string()), 1);

        assert_eq!(
            drle.all_values(),
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
            drle.all_values(),
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

        let row_ids = drle.index_row_ids.get(&0).unwrap().to_vec();
        assert_eq!(row_ids, vec![0, 1, 3, 4, 5]);

        let row_ids = drle.index_row_ids.get(&1).unwrap().to_vec();
        assert_eq!(row_ids, vec![2]);

        let row_ids = drle.index_row_ids.get(&2).unwrap().to_vec();
        assert_eq!(row_ids, vec![6, 7, 8]);
    }

    #[test]
    fn dict_rle_scan_from() {
        let mut drle = super::DictionaryRle::new();
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
    fn dict_rle_has_value_no_null() {
        let mut drle = super::DictionaryRle::new();
        let west = Some("west".to_string());
        let east = Some("east".to_string());
        let north = Some("north".to_string());
        drle.push_additional(west, 3);
        drle.push_additional(east, 2);
        drle.push_additional(north, 4);

        // w,w,w,e,e,n,n,n,n
        // 0 1 2 3 4 5 6 7 8
        assert_eq!(drle.has_non_null_value_in_row_ids(&[0]), true);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[1, 3]), true);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[8]), true);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[12, 132]), false);
    }

    #[test]
    fn dict_rle_has_value() {
        let mut drle = super::DictionaryRle::new();
        let west = Some("west".to_string());
        let east = Some("east".to_string());
        let north = Some("north".to_string());
        drle.push_additional(west.clone(), 3);
        drle.push_additional(None, 1);
        drle.push_additional(east, 2);
        drle.push_additional(north, 4);
        drle.push_additional(None, 4);
        drle.push_additional(west, 3);

        // w,w,w,?,e,e,n,n,n,n, ?, ?,  ?,  ?,  w,  w,  w
        // 0 1 2 3 4 5 6 7 8 9 10 11, 12, 13, 14, 15, 16
        assert_eq!(drle.has_non_null_value_in_row_ids(&[0]), true);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[2, 3]), true);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[2, 3]), true);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[3, 4, 10]), true);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[16, 19]), true);

        assert_eq!(drle.has_non_null_value_in_row_ids(&[3]), false);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[3, 10]), false);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[17]), false);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[17, 19]), false);
        assert_eq!(drle.has_non_null_value_in_row_ids(&[12, 19]), false);
    }

    #[test]
    fn dict_rle_values() {
        let mut drle = super::DictionaryRle::new();
        let west = Some("west".to_string());
        let east = Some("east".to_string());
        let north = Some("north".to_string());
        drle.push_additional(west.clone(), 3);
        drle.push_additional(east.clone(), 2);
        drle.push_additional(north.clone(), 4);
        drle.push_additional(west.clone(), 3);

        let results = drle.values(&[0, 1, 4, 5]);

        // w,w,w,e,e,n,n,n,n,w, w, w
        // 0 1 2 3 4 5 6 7 8 9 10 11
        let exp = vec![&west, &west, &east, &north];
        assert_eq!(results, exp);

        let results = drle.values(&[10, 11]);
        let exp = vec![&west, &west];
        assert_eq!(results, exp);

        let results = drle.values(&[0, 3, 5, 11]);
        let exp = vec![&west, &east, &north, &west];
        assert_eq!(results, exp);

        let results = drle.values(&[0]);
        let exp = vec![&west];
        assert_eq!(results, exp);

        let results = drle.values(&[0, 9]);
        let exp = vec![&west, &west];
        assert_eq!(results, exp);
    }

    #[test]
    fn dict_rle_encoded_values() {
        let mut drle = super::DictionaryRle::new();
        let west = Some("west".to_string());
        let east = Some("east".to_string());
        let north = Some("north".to_string());
        drle.push_additional(west.clone(), 3);
        drle.push_additional(east, 2);
        drle.push_additional(north, 4);
        drle.push_additional(west, 3);

        let results = drle.encoded_values(&[0, 1, 4, 5]);

        // w,w,w,e,e,n,n,n,n,w,w,w
        // 0,0,0,1,1,2,2,2,2,0,0,0
        let exp = vec![0, 0, 1, 2];
        assert_eq!(results, exp);

        let results = drle.encoded_values(&[10, 11]);
        let exp = vec![0, 0];
        assert_eq!(results, exp);

        let results = drle.encoded_values(&[0, 3, 5, 11]);
        let exp = vec![0, 1, 2, 0];
        assert_eq!(results, exp);

        let results = drle.encoded_values(&[0]);
        let exp = vec![0];
        assert_eq!(results, exp);

        let results = drle.encoded_values(&[0, 9]);
        let exp = vec![0, 0];
        assert_eq!(results, exp);
    }

    #[test]
    fn rle_dict_row_ids() {
        let mut drle = super::DictionaryRle::new();
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
        let mut drle = super::DictionaryRle::new();
        drle.push_additional(Some("abc".to_string()), 3);
        drle.push_additional(Some("dre".to_string()), 2);
        drle.push("abc");

        let ids = drle
            .row_ids_eq_roaring(Some("abc".to_string()))
            .iter()
            .collect::<Vec<u32>>();
        assert_eq!(ids, vec![0, 1, 2, 5]);

        let ids = drle
            .row_ids_eq_roaring(Some("dre".to_string()))
            .iter()
            .collect::<Vec<u32>>();
        assert_eq!(ids, vec![3, 4]);

        let ids = drle
            .row_ids_eq_roaring(Some("foo".to_string()))
            .iter()
            .collect::<Vec<u32>>();
        let empty: Vec<u32> = vec![];
        assert_eq!(ids, empty);
    }
}
