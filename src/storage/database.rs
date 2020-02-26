use crate::delorean::{Bucket, Predicate, TimestampRange};
use crate::line_parser::PointType;
use crate::storage::config_store::ConfigStore;
use crate::storage::inverted_index::{InvertedIndex, SeriesFilter};
use crate::storage::rocksdb::RocksDB;
use crate::storage::series_store::{ReadPoint, SeriesStore};
use crate::storage::StorageError;

use std::sync::Arc;

pub struct Database {
    local_index: Arc<dyn InvertedIndex>,
    local_series_store: Arc<dyn SeriesStore>,
    local_config_store: Arc<dyn ConfigStore>,
}

impl Database {
    pub fn new(dir: &str) -> Database {
        let db = Arc::new(RocksDB::new(dir));

        Database {
            local_index: db.clone(),
            local_config_store: db.clone(),
            local_series_store: db,
        }
    }

    pub fn write_points(
        &self,
        _org_id: u32,
        bucket: &Bucket,
        points: &mut [PointType],
    ) -> Result<(), StorageError> {
        self.local_index
            .get_or_create_series_ids_for_points(bucket.id, points)?;
        self.local_series_store
            .write_points_with_series_ids(bucket.id, points)
    }

    pub fn get_bucket_by_name(
        &self,
        org_id: u32,
        bucket_name: &str,
    ) -> Result<Option<Arc<Bucket>>, StorageError> {
        self.local_config_store
            .get_bucket_by_name(org_id, bucket_name)
    }

    pub fn get_bucket_by_id(&self, bucket_id: u32) -> Result<Option<Arc<Bucket>>, StorageError> {
        self.local_config_store.get_bucket_by_id(bucket_id)
    }

    pub fn create_bucket_if_not_exists(
        &self,
        org_id: u32,
        bucket: &Bucket,
    ) -> Result<u32, StorageError> {
        self.local_config_store
            .create_bucket_if_not_exists(org_id, bucket)
    }

    pub fn read_series_matching_predicate_and_range(
        &self,
        bucket: &Bucket,
        predicate: Option<&Predicate>,
        _range: Option<&TimestampRange>,
    ) -> Result<Box<dyn Iterator<Item = SeriesFilter> + Send>, StorageError> {
        self.local_index.read_series_matching(bucket.id, predicate)
    }

    pub fn read_i64_range(
        &self,
        bucket: &Bucket,
        series_filter: &SeriesFilter,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<i64>>> + Send>, StorageError> {
        self.local_series_store
            .read_i64_range(bucket.id, series_filter.id, range, batch_size)
    }

    pub fn read_f64_range(
        &self,
        bucket: &Bucket,
        series_filter: &SeriesFilter,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<f64>>> + Send>, StorageError> {
        self.local_series_store
            .read_f64_range(bucket.id, series_filter.id, range, batch_size)
    }

    pub fn get_tag_keys(
        &self,
        bucket: &Bucket,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError> {
        self.local_index.get_tag_keys(bucket.id, predicate)
    }

    pub fn get_tag_values(
        &self,
        bucket: &Bucket,
        tag_key: &str,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String> + Send>, StorageError> {
        self.local_index
            .get_tag_values(bucket.id, tag_key, predicate)
    }
}
