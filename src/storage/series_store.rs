use crate::line_parser::PointType;
use crate::storage::{Range, StorageError};

pub trait SeriesStore: Sync + Send {
    fn write_points_with_series_ids(
        &self,
        bucket_id: u32,
        points: &Vec<PointType>,
    ) -> Result<(), StorageError>;

    fn read_i64_range(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &Range,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<i64>>>>, StorageError>;

    fn read_f64_range(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &Range,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<f64>>>>, StorageError>;
}

#[derive(Debug, PartialEq)]
pub struct ReadPoint<T> {
    pub time: i64,
    pub value: T,
}
