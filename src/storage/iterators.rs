use crate::delorean::Predicate;
use crate::storage::rocksdb::{Database, SeriesFilter, StorageError, Range, PointsIterator};

use rocksdb::{DB, IteratorMode, DBIterator};

pub struct SeriesIterator {
    pub org_id: u32,
    pub bucket_id: u32,
    series_filters: Vec<SeriesFilter>,
    next_filter: usize,
}

impl SeriesIterator {
    pub fn new(org_id: u32, bucket_id: u32, series_filters: Vec<SeriesFilter>) -> SeriesIterator {
        SeriesIterator{
            org_id,
            bucket_id,
            next_filter: 0,
            series_filters: series_filters,
        }
    }
}

impl Iterator for SeriesIterator {
    type Item = SeriesFilter;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_filter += 1;
        match self.series_filters.get(self.next_filter - 1) {
            Some(f) => Some(f.clone()),
            None => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct ReadSeries<T> {
    pub id: u64,
    pub key: String,
    pub points: Vec<ReadPoint<T>>,
}

#[derive(Debug, PartialEq)]
pub struct ReadPoint<T> {
    pub time: i64,
    pub value: T,
}