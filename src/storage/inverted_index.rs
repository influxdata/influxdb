use crate::delorean::Predicate;
use crate::line_parser::PointType;
use crate::storage::{SeriesDataType, StorageError};

pub trait InvertedIndex: Sync + Send {
    fn get_or_create_series_ids_for_points(
        &self,
        bucket_id: u32,
        points: &mut Vec<PointType>,
    ) -> Result<(), StorageError>;

    fn read_series_matching(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = SeriesFilter>>, StorageError>;

    fn get_tag_keys(
        &self,
        bucket_id: u32,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String>>, StorageError>;

    fn get_tag_values(
        &self,
        bucket_id: u32,
        tag_key: &str,
        predicate: Option<&Predicate>,
    ) -> Result<Box<dyn Iterator<Item = String>>, StorageError>;
}

#[derive(Debug, PartialEq, Clone)]
pub struct SeriesFilter {
    pub id: u64,
    pub key: String,
    pub value_predicate: Option<Predicate>,
    pub series_type: SeriesDataType,
}
