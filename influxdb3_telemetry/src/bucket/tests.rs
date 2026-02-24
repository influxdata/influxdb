use observability_deps::tracing::info;

use super::*;

#[test_log::test(test)]
fn test_bucket_for_writes() {
    let mut bucket = EventsBucket::new();
    info!(bucket = ?bucket, "Events bucket empty");
    bucket.add_write_sample(1, 1);
    info!(bucket = ?bucket, "Events bucket added 1");
    bucket.add_write_sample(2, 2);
    info!(bucket = ?bucket, "Events bucket added 2");
    bucket.add_write_sample(3, 3);
    info!(bucket = ?bucket, "Events bucket added 3");
    bucket.add_write_sample(4, 4);
    info!(bucket = ?bucket, "Events bucket added 4");

    assert_eq!(4, bucket.num_writes);
    assert_eq!(1, bucket.writes.lines.avg);
    assert_eq!(1, bucket.writes.size_bytes.avg);

    assert_eq!(1, bucket.writes.lines.min);
    assert_eq!(1, bucket.writes.size_bytes.min);

    assert_eq!(4, bucket.writes.lines.max);
    assert_eq!(4, bucket.writes.size_bytes.max);

    bucket.add_write_sample(20, 20);
    info!(bucket = ?bucket, "Events bucket added 20");
    assert_eq!(4, bucket.writes.lines.avg);
    assert_eq!(4, bucket.writes.size_bytes.avg);

    bucket.update_num_queries();
    bucket.update_num_queries();
    bucket.update_num_queries();
    assert_eq!(3, bucket.num_queries);

    bucket.reset();
    assert_eq!(0, bucket.writes.lines.min);
    assert_eq!(0, bucket.writes.lines.max);
    assert_eq!(0, bucket.writes.lines.avg);
    assert_eq!(0, bucket.writes.size_bytes.min);
    assert_eq!(0, bucket.writes.size_bytes.max);
    assert_eq!(0, bucket.writes.size_bytes.avg);
    assert_eq!(0, bucket.num_writes);
    assert_eq!(0, bucket.num_queries);
}
