use crate::delorean::{Node, Predicate, TimestampRange};
use crate::line_parser::{self, Point, PointType};
use crate::storage::inverted_index::{InvertedIndex, SeriesFilter};
use crate::storage::predicate::{Evaluate, EvaluateVisitor};
use crate::storage::series_store::{ReadPoint, SeriesStore};
use crate::storage::{SeriesDataType, StorageError};

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use croaring::Treemap;
/// memdb implements an in memory database for the InvertedIndex and SeriesStore traits. It
/// works with a ring buffer. Currently, it does not limit the number of series that can be
/// created. It also assumes that data per series arrives in time acending order.

// TODO: return errors if trying to insert data out of order in an individual series

#[derive(Default)]
pub struct MemDB {
    default_ring_buffer_size: usize,
    bucket_id_to_series_data: Arc<RwLock<HashMap<u32, Mutex<SeriesData>>>>,
    bucket_id_to_series_map: Arc<RwLock<HashMap<u32, RwLock<SeriesMap>>>>,
}

struct SeriesData {
    ring_buffer_size: usize,
    i64_series: HashMap<u64, SeriesRingBuffer<i64>>,
    f64_series: HashMap<u64, SeriesRingBuffer<f64>>,
}

trait StoreInSeriesData {
    fn write(&self, series_data: &mut SeriesData);
}

impl StoreInSeriesData for PointType {
    fn write(&self, series_data: &mut SeriesData) {
        match self {
            PointType::I64(inner) => inner.write(series_data),
            PointType::F64(inner) => inner.write(series_data),
        }
    }
}

impl StoreInSeriesData for Point<i64> {
    fn write(&self, series_data: &mut SeriesData) {
        match series_data.i64_series.get_mut(&self.series_id.unwrap()) {
            Some(buff) => buff.write(&self),
            None => {
                let mut buff = new_i64_ring_buffer(series_data.ring_buffer_size);
                buff.write(&self);
                series_data.i64_series.insert(self.series_id.unwrap(), buff);
            }
        }
    }
}

impl StoreInSeriesData for Point<f64> {
    fn write(&self, series_data: &mut SeriesData) {
        match series_data.f64_series.get_mut(&self.series_id.unwrap()) {
            Some(buff) => buff.write(&self),
            None => {
                let mut buff = new_f64_ring_buffer(series_data.ring_buffer_size);
                buff.write(&self);
                series_data.f64_series.insert(self.series_id.unwrap(), buff);
            }
        }
    }
}

impl SeriesData {
    fn write_points(&mut self, points: &[PointType]) {
        for p in points {
            p.write(self);
        }
    }
}

struct SeriesRingBuffer<T: Clone> {
    next_position: usize,
    data: Vec<ReadPoint<T>>,
}

fn new_i64_ring_buffer(size: usize) -> SeriesRingBuffer<i64> {
    let mut data = Vec::with_capacity(size);
    for _ in 0..size {
        data.push(ReadPoint {
            time: std::i64::MAX,
            value: 0 as i64,
        })
    }

    SeriesRingBuffer {
        next_position: 0,
        data,
    }
}

fn new_f64_ring_buffer(size: usize) -> SeriesRingBuffer<f64> {
    let mut data = Vec::with_capacity(size);
    for _ in 0..size {
        data.push(ReadPoint {
            time: std::i64::MAX,
            value: 0 as f64,
        })
    }

    SeriesRingBuffer {
        next_position: 0,
        data,
    }
}

impl<T: Clone> SeriesRingBuffer<T> {
    fn write(&mut self, point: &Point<T>) {
        if self.next_position == self.data.len() {
            self.next_position = 0;
        }
        self.data[self.next_position].time = point.time;
        self.data[self.next_position].value = point.value.clone();
        self.next_position += 1;
    }

    fn get_range(&self, range: &TimestampRange) -> Vec<ReadPoint<T>> {
        let (_, pos) = self.oldest_time_and_position();

        let mut values = Vec::new();

        for i in pos..self.data.len() {
            if self.data[i].time > range.end {
                return values;
            } else if self.data[i].time >= range.start {
                values.push(self.data[i].clone());
            }
        }

        for i in 0..self.next_position {
            if self.data[i].time > range.end {
                return values;
            } else if self.data[i].time >= range.start {
                values.push(self.data[i].clone());
            }
        }

        values
    }

    fn oldest_time_and_position(&self) -> (i64, usize) {
        let mut pos = self.next_position;
        if self.next_position == self.data.len() || self.data[pos].time == std::i64::MAX {
            pos = 0;
        }

        (self.data[pos].time, pos)
    }
}

struct SeriesMap {
    last_id: u64,
    series_key_to_id: HashMap<String, u64>,
    series_id_to_key_and_type: HashMap<u64, (String, SeriesDataType)>,
    tag_keys: BTreeMap<String, BTreeMap<String, bool>>,
    posting_list: HashMap<Vec<u8>, Treemap>,
}

impl SeriesMap {
    fn new() -> SeriesMap {
        SeriesMap {
            last_id: 0,
            series_key_to_id: HashMap::new(),
            series_id_to_key_and_type: HashMap::new(),
            tag_keys: BTreeMap::new(),
            posting_list: HashMap::new(),
        }
    }

    fn insert_series(&mut self, point: &mut PointType) -> line_parser::Result<()> {
        if let Some(id) = self.series_key_to_id.get(point.series()) {
            point.set_series_id(*id);
            return Ok(());
        }

        // insert the series id
        self.last_id += 1;
        point.set_series_id(self.last_id);
        self.series_key_to_id
            .insert(point.series().clone(), self.last_id);

        let series_type = match point {
            PointType::I64(_) => SeriesDataType::I64,
            PointType::F64(_) => SeriesDataType::F64,
        };
        self.series_id_to_key_and_type
            .insert(self.last_id, (point.series().clone(), series_type));

        for pair in point.index_pairs()? {
            // insert this id into the posting list
            let list_key = list_key(&pair.key, &pair.value);

            let posting_list = self
                .posting_list
                .entry(list_key)
                .or_insert_with(Treemap::create);
            posting_list.add(self.last_id);

            // insert the tag key value mapping
            let tag_values = self.tag_keys.entry(pair.key).or_insert_with(BTreeMap::new);
            tag_values.insert(pair.value, true);
        }

        Ok(())
    }

    fn posting_list_for_key_value(&self, key: &str, value: &str) -> Treemap {
        let list_key = list_key(key, value);
        match self.posting_list.get(&list_key) {
            Some(m) => m.clone(),
            None => Treemap::create(),
        }
    }
}

fn list_key(key: &str, value: &str) -> Vec<u8> {
    let mut list_key = key.as_bytes().to_vec();
    list_key.push(0 as u8);
    list_key.append(&mut value.as_bytes().to_vec());
    list_key
}

impl MemDB {
    pub fn new() -> MemDB {
        MemDB {
            default_ring_buffer_size: 10000,
            bucket_id_to_series_data: Arc::new(RwLock::new(HashMap::new())),
            bucket_id_to_series_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn get_or_create_series_ids_for_points(
        &self,
        bucket_id: u32,
        points: &mut [PointType],
    ) -> Result<(), StorageError> {
        // first try to do everything with just a read lock
        if self.get_series_ids_for_points(bucket_id, points) {
            return Ok(());
        }

        // if we got this far, we have new series to insert
        let buckets = self.bucket_id_to_series_map.read().unwrap();
        let mut series_map = buckets.get(&bucket_id).unwrap().write().unwrap();

        for p in points {
            match p.series_id() {
                Some(_) => (),
                None => match series_map.insert_series(p) {
                    Ok(_) => (),
                    Err(e) => {
                        return Err(StorageError {
                            description: format!("error parsing line protocol metadata {}", e),
                        })
                    }
                },
            }
        }

        Ok(())
    }

    // get_series_ids_for_points attempts to fill the series ids for all points in the passed in
    // collection using only a read lock. If no SeriesMap exists for the bucket, it will be inserted.
    // It will return true if all points have series ids filled in.
    fn get_series_ids_for_points(&self, bucket_id: u32, points: &mut [PointType]) -> bool {
        let buckets = self.bucket_id_to_series_map.read().unwrap();
        match buckets.get(&bucket_id) {
            Some(b) => {
                let b = b.read().unwrap();
                let mut all_have_id = true;
                for p in points {
                    match b.series_key_to_id.get(p.series()) {
                        Some(id) => p.set_series_id(*id),
                        None => all_have_id = false,
                    }
                }

                if all_have_id {
                    return true;
                }
            }
            None => {
                // we've never even seen this bucket. insert it and then we'll get to inserting the series
                drop(buckets);
                self.bucket_id_to_series_map
                    .write()
                    .unwrap()
                    .insert(bucket_id, RwLock::new(SeriesMap::new()));
            }
        }

        false
    }

    fn get_tag_keys(
        &self,
        bucket_id: u32,
        _predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError> {
        match self.bucket_id_to_series_map.read().unwrap().get(&bucket_id) {
            Some(map) => {
                let keys: Vec<String> = map.read().unwrap().tag_keys.keys().cloned().collect();
                Ok(Box::new(keys.into_iter()))
            }
            None => Err(StorageError {
                description: format!("bucket {} not found", bucket_id),
            }),
        }
    }

    fn get_tag_values(
        &self,
        bucket_id: u32,
        tag_key: &str,
        _predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError> {
        match self.bucket_id_to_series_map.read().unwrap().get(&bucket_id) {
            Some(map) => match map.read().unwrap().tag_keys.get(tag_key) {
                Some(values) => {
                    let values: Vec<String> = values.keys().cloned().collect();
                    Ok(Box::new(values.into_iter()))
                }
                None => Ok(Box::new(vec![].into_iter())),
            },
            None => Err(StorageError {
                description: format!("bucket {} not found", bucket_id),
            }),
        }
    }

    fn read_series_matching(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = SeriesFilter> + Send>, StorageError> {
        let pred = match predicate {
            Some(p) => p,
            None => {
                return Err(StorageError {
                    description: "unable to list all series".to_string(),
                })
            }
        };

        let root = match &pred.root {
            Some(r) => r,
            None => {
                return Err(StorageError {
                    description: "expected root node to evaluate".to_string(),
                })
            }
        };

        let bucket_map = self.bucket_id_to_series_map.read().unwrap();
        let series_map = match bucket_map.get(&bucket_id) {
            Some(m) => m.read().unwrap(),
            None => {
                return Err(StorageError {
                    description: format!("no series written for bucket {}", bucket_id),
                })
            }
        };

        let map = evaluate_node(&series_map, &root)?;
        let mut filters = Vec::with_capacity(map.cardinality() as usize);

        for id in map.iter() {
            let (key, series_type) = series_map.series_id_to_key_and_type.get(&id).unwrap();
            filters.push(SeriesFilter {
                id,
                key: key.clone(),
                value_predicate: None,
                series_type: *series_type,
            });
        }

        Ok(Box::new(filters.into_iter()))
    }

    fn write_points_with_series_ids(
        &self,
        bucket_id: u32,
        points: &[PointType],
    ) -> Result<(), StorageError> {
        let bucket_data = self.bucket_id_to_series_data.read().unwrap();

        // ensure the the bucket has a series data struct to write into
        let series_data = match bucket_data.get(&bucket_id) {
            Some(d) => d,
            None => {
                drop(bucket_data);
                let mut bucket_data = self.bucket_id_to_series_data.write().unwrap();
                let series_data = SeriesData {
                    ring_buffer_size: self.default_ring_buffer_size,
                    i64_series: HashMap::new(),
                    f64_series: HashMap::new(),
                };
                bucket_data.insert(bucket_id, Mutex::new(series_data));
                drop(bucket_data);
                return self.write_points_with_series_ids(bucket_id, points);
            }
        };

        let mut series_data = series_data.lock().unwrap();
        series_data.write_points(points);
        Ok(())
    }

    fn read_range<T: 'static + Clone + FromSeries + Send>(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<T>>> + Send>, StorageError> {
        let buckets = self.bucket_id_to_series_data.read().unwrap();
        let data = match buckets.get(&bucket_id) {
            Some(d) => d,
            None => {
                return Err(StorageError {
                    description: format!("bucket {} not found", bucket_id),
                })
            }
        };

        let data = data.lock().unwrap();
        let buff = match FromSeries::from_series(&data, series_id) {
            Some(b) => b,
            None => {
                return Err(StorageError {
                    description: format!("series {} not found", series_id),
                })
            }
        };

        let values = buff.get_range(&range);
        Ok(Box::new(PointsIterator {
            values: Some(values),
            batch_size,
        }))
    }
}

trait FromSeries: Clone {
    fn from_series<'a>(data: &'a SeriesData, series_id: u64) -> Option<&'a SeriesRingBuffer<Self>>;
}

impl FromSeries for i64 {
    fn from_series<'a>(data: &'a SeriesData, series_id: u64) -> Option<&'a SeriesRingBuffer<i64>> {
        data.i64_series.get(&series_id)
    }
}

impl FromSeries for f64 {
    fn from_series<'a>(data: &'a SeriesData, series_id: u64) -> Option<&'a SeriesRingBuffer<f64>> {
        data.f64_series.get(&series_id)
    }
}

struct PointsIterator<T: Clone> {
    values: Option<Vec<ReadPoint<T>>>,
    batch_size: usize,
}

impl<T: Clone> Iterator for PointsIterator<T> {
    type Item = Vec<ReadPoint<T>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut values) = self.values.take() {
            if self.batch_size > values.len() {
                return Some(values);
            }

            let remaining = values.split_off(self.batch_size);

            if !remaining.is_empty() {
                self.values = Some(remaining);
            }

            return Some(values);
        }

        None
    }
}

fn evaluate_node(series_map: &SeriesMap, n: &Node) -> Result<Treemap, StorageError> {
    struct Visitor<'a>(&'a SeriesMap);

    impl EvaluateVisitor for Visitor<'_> {
        fn equal(&mut self, left: &str, right: &str) -> Result<Treemap, StorageError> {
            Ok(self.0.posting_list_for_key_value(left, right))
        }
    }

    Evaluate::evaluate(Visitor(series_map), n)
}

impl InvertedIndex for MemDB {
    fn get_or_create_series_ids_for_points(
        &self,
        bucket_id: u32,
        points: &mut [PointType],
    ) -> Result<(), StorageError> {
        self.get_or_create_series_ids_for_points(bucket_id, points)
    }

    fn read_series_matching(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = SeriesFilter> + Send>, StorageError> {
        self.read_series_matching(bucket_id, predicate)
    }

    fn get_tag_keys(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError> {
        self.get_tag_keys(bucket_id, predicate)
    }

    fn get_tag_values(
        &self,
        bucket_id: u32,
        tag_key: &str,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError> {
        self.get_tag_values(bucket_id, tag_key, predicate)
    }
}

impl SeriesStore for MemDB {
    fn write_points_with_series_ids(
        &self,
        bucket_id: u32,
        points: &[PointType],
    ) -> Result<(), StorageError> {
        self.write_points_with_series_ids(bucket_id, points)
    }

    fn read_i64_range(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<i64>>> + Send>, StorageError> {
        self.read_range(bucket_id, series_id, range, batch_size)
    }

    fn read_f64_range(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<f64>>> + Send>, StorageError> {
        self.read_range(bucket_id, series_id, range, batch_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::inverted_index;
    use crate::storage::series_store;

    #[test]
    fn write_and_read_i64() {
        let db = Box::new(MemDB::new());
        series_store::tests::write_and_read_i64(db);
    }

    #[test]
    fn write_and_read_f64() {
        let db = Box::new(MemDB::new());
        series_store::tests::write_and_read_f64(db);
    }

    #[test]
    fn series_id_indexing() {
        let db = Box::new(MemDB::new());
        inverted_index::tests::series_id_indexing(db)
    }

    #[test]
    fn series_metadata_indexing() {
        let db = Box::new(MemDB::new());
        inverted_index::tests::series_metadata_indexing(db);
    }
}
