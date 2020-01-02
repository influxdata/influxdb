use crate::delorean::Predicate;
use crate::storage::rocksdb::{Database, SeriesFilter, StorageError, Range, PointsIterator};

use rocksdb::{DB, IteratorMode, DBIterator};

pub struct SeriesIterator<'a> {
    range: &'a Range,
    batch_size: usize,
    predicate: &'a Predicate,
    org_id: u32,
    bucket_id: u32,
    series_filters: Vec<SeriesFilter>,
    next_filter: usize,
}

impl SeriesIterator<'_> {
    pub fn new<'a>(range: &'a Range, batch_size: usize, predicate: &'a Predicate, org_id: u32, bucket_id: u32, series_filters: Vec<SeriesFilter>) -> SeriesIterator<'a> {
        SeriesIterator{
            range,
            batch_size,
            predicate,
            org_id,
            bucket_id,
            next_filter: 0,
            series_filters: series_filters,
        }
    }

    pub fn points_iterator<'a>(&self, db: &'a Database, series_filter: &'a SeriesFilter) -> Result<PointsIterator<'a>, StorageError> {
        db.get_db_points_iter(self.org_id, self.bucket_id, series_filter.id, self.range, self.batch_size)
    }
}

impl Iterator for SeriesIterator<'_> {
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