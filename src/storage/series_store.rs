use crate::delorean::TimestampRange;
use crate::line_parser::PointType;
use crate::storage::{ReadPoint, StorageError};

pub trait SeriesStore: Sync + Send {
    fn write_points_with_series_ids(
        &self,
        bucket_id: u32,
        points: &[PointType],
    ) -> Result<(), StorageError>;

    fn read_i64_range(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<i64>>> + Send>, StorageError>;

    fn read_f64_range(
        &self,
        bucket_id: u32,
        series_id: u64,
        range: &TimestampRange,
        batch_size: usize,
    ) -> Result<Box<dyn Iterator<Item = Vec<ReadPoint<f64>>> + Send>, StorageError>;
}

// Test helpers for other implementations to run
#[cfg(test)]
pub mod tests {
    use crate::delorean::TimestampRange;
    use crate::line_parser::PointType;
    use crate::storage::series_store::{ReadPoint, SeriesStore};

    pub fn write_and_read_i64(store: Box<dyn SeriesStore>) {
        let b1_id = 1;
        let b2_id = 2;
        let mut p1 = PointType::new_i64("cpu,host=b,region=west\tusage_system".to_string(), 1, 1);
        p1.set_series_id(1);
        let mut p2 = PointType::new_i64("cpu,host=b,region=west\tusage_system".to_string(), 1, 2);
        p2.set_series_id(1);
        let mut p3 = PointType::new_i64("mem,host=b,region=west\tfree".to_string(), 1, 2);
        p3.set_series_id(2);
        let mut p4 = PointType::new_i64("mem,host=b,region=west\tfree".to_string(), 1, 4);
        p4.set_series_id(2);

        let b1_points = vec![p1.clone(), p2.clone()];
        store
            .write_points_with_series_ids(b1_id, &b1_points)
            .unwrap();

        let b2_points = vec![p1.clone(), p2, p3.clone(), p4];
        store
            .write_points_with_series_ids(b2_id, &b2_points)
            .unwrap();

        // test that we'll only read from the bucket we wrote points into
        let range = TimestampRange { start: 1, end: 4 };
        let mut points_iter = store
            .read_i64_range(b1_id, p1.series_id().unwrap(), &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(
            points,
            vec![
                ReadPoint { time: 1, value: 1 },
                ReadPoint { time: 2, value: 1 },
            ]
        );
        assert_eq!(points_iter.next(), None);

        // test that we'll read multiple series
        let mut points_iter = store
            .read_i64_range(b2_id, p1.series_id().unwrap(), &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(
            points,
            vec![
                ReadPoint { time: 1, value: 1 },
                ReadPoint { time: 2, value: 1 },
            ]
        );

        let mut points_iter = store
            .read_i64_range(b2_id, p3.series_id().unwrap(), &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(
            points,
            vec![
                ReadPoint { time: 2, value: 1 },
                ReadPoint { time: 4, value: 1 },
            ]
        );

        // test that the batch size is honored
        let mut points_iter = store
            .read_i64_range(b1_id, p1.series_id().unwrap(), &range, 1)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![ReadPoint { time: 1, value: 1 },]);
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![ReadPoint { time: 2, value: 1 },]);
        assert_eq!(points_iter.next(), None);

        // test that the time range is properly limiting
        let range = TimestampRange { start: 2, end: 3 };
        let mut points_iter = store
            .read_i64_range(b2_id, p1.series_id().unwrap(), &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![ReadPoint { time: 2, value: 1 },]);

        let mut points_iter = store
            .read_i64_range(b2_id, p3.series_id().unwrap(), &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(points, vec![ReadPoint { time: 2, value: 1 },]);
    }

    pub fn write_and_read_f64(store: Box<dyn SeriesStore>) {
        let bucket_id = 1;
        let mut p1 = PointType::new_f64("cpu,host=b,region=west\tusage_system".to_string(), 1.0, 1);
        p1.set_series_id(1);
        let mut p2 = PointType::new_f64("cpu,host=b,region=west\tusage_system".to_string(), 2.2, 2);
        p2.set_series_id(1);

        let points = vec![p1.clone(), p2];
        store
            .write_points_with_series_ids(bucket_id, &points)
            .unwrap();

        // test that we'll only read from the bucket we wrote points into
        let range = TimestampRange { start: 0, end: 4 };
        let mut points_iter = store
            .read_f64_range(bucket_id, p1.series_id().unwrap(), &range, 10)
            .unwrap();
        let points = points_iter.next().unwrap();
        assert_eq!(
            points,
            vec![
                ReadPoint {
                    time: 1,
                    value: 1.0
                },
                ReadPoint {
                    time: 2,
                    value: 2.2
                },
            ]
        );
        assert_eq!(points_iter.next(), None);
    }
}
