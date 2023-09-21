use arrow_util::assert_batches_eq;
use data_types::{StatValues, Statistics};
use mutable_batch::{writer::Writer, MutableBatch, TimestampSummary};
use schema::Projection;
use std::num::NonZeroU64;

fn get_stats(batch: &MutableBatch) -> Vec<(&str, Statistics)> {
    let mut stats: Vec<_> = batch
        .columns()
        .map(|(name, col)| (name.as_str(), col.stats()))
        .collect();

    stats.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
    stats
}

#[test]
fn test_basic() {
    let mut batch = MutableBatch::new();

    let mut writer = Writer::new(&mut batch, 5);

    writer
        .write_bool(
            "b1",
            None,
            vec![true, true, false, false, false].into_iter(),
        )
        .unwrap();

    writer
        .write_bool(
            "b2",
            Some(&[0b00011101]),
            vec![true, false, false, true].into_iter(),
        )
        .unwrap();

    writer
        .write_f64(
            "f64",
            Some(&[0b00011011]),
            vec![343.3, 443., 477., -24.].into_iter(),
        )
        .unwrap();

    writer
        .write_i64("i64", None, vec![234, 6, 2, 6, -3].into_iter())
        .unwrap();

    writer
        .write_i64("i64_2", Some(&[0b00000001]), vec![-8].into_iter())
        .unwrap();

    writer
        .write_u64("u64", Some(&[0b00001001]), vec![23, 5].into_iter())
        .unwrap();

    writer
        .write_time("time", vec![7, 5, 7, 3, 5].into_iter())
        .unwrap();

    writer
        .write_tag("tag1", None, vec!["v1", "v1", "v2", "v2", "v1"].into_iter())
        .unwrap();

    writer
        .write_tag(
            "tag2",
            Some(&[0b00001011]),
            vec!["v1", "v2", "v2"].into_iter(),
        )
        .unwrap();

    writer
        .write_tag_dict(
            "tag3",
            Some(&[0b00011011]),
            vec![1, 0, 0, 1].into_iter(),
            vec!["v1", "v2"].into_iter(),
        )
        .unwrap();

    writer.commit();

    let stats: Vec<_> = get_stats(&batch);

    let expected_data = &[
        "+-------+-------+-------+-----+-------+------+------+------+--------------------------------+-----+",
        "| b1    | b2    | f64   | i64 | i64_2 | tag1 | tag2 | tag3 | time                           | u64 |",
        "+-------+-------+-------+-----+-------+------+------+------+--------------------------------+-----+",
        "| true  | true  | 343.3 | 234 | -8    | v1   | v1   | v2   | 1970-01-01T00:00:00.000000007Z | 23  |",
        "| true  |       | 443.0 | 6   |       | v1   | v2   | v1   | 1970-01-01T00:00:00.000000005Z |     |",
        "| false | false |       | 2   |       | v2   |      |      | 1970-01-01T00:00:00.000000007Z |     |",
        "| false | false | 477.0 | 6   |       | v2   | v2   | v1   | 1970-01-01T00:00:00.000000003Z | 5   |",
        "| false | true  | -24.0 | -3  |       | v1   |      | v2   | 1970-01-01T00:00:00.000000005Z |     |",
        "+-------+-------+-------+-----+-------+------+------+------+--------------------------------+-----+",
    ];

    let expected_stats = vec![
        (
            "b1",
            Statistics::Bool(StatValues::new(Some(false), Some(true), 5, Some(0))),
        ),
        (
            "b2",
            Statistics::Bool(StatValues::new(Some(false), Some(true), 5, Some(1))),
        ),
        (
            "f64",
            Statistics::F64(StatValues::new(Some(-24.), Some(477.), 5, Some(1))),
        ),
        (
            "i64",
            Statistics::I64(StatValues::new(Some(-3), Some(234), 5, Some(0))),
        ),
        (
            "i64_2",
            Statistics::I64(StatValues::new(Some(-8), Some(-8), 5, Some(4))),
        ),
        (
            "tag1",
            Statistics::String(StatValues::new_with_distinct(
                Some("v1".to_string()),
                Some("v2".to_string()),
                5,
                Some(0),
                Some(NonZeroU64::new(2).unwrap()),
            )),
        ),
        (
            "tag2",
            Statistics::String(StatValues::new_with_distinct(
                Some("v1".to_string()),
                Some("v2".to_string()),
                5,
                Some(2),
                Some(NonZeroU64::new(3).unwrap()),
            )),
        ),
        (
            "tag3",
            Statistics::String(StatValues::new_with_distinct(
                Some("v1".to_string()),
                Some("v2".to_string()),
                5,
                Some(1),
                Some(NonZeroU64::new(3).unwrap()),
            )),
        ),
        (
            "time",
            Statistics::I64(StatValues::new(Some(3), Some(7), 5, Some(0))),
        ),
        (
            "u64",
            Statistics::U64(StatValues::new(Some(5), Some(23), 5, Some(3))),
        ),
    ];

    assert_batches_eq!(expected_data, &[batch.to_arrow(Projection::All).unwrap()]);
    assert_eq!(stats, expected_stats);

    let mut writer = Writer::new(&mut batch, 4);
    writer
        .write_time("time", vec![4, 6, 21, 7].into_iter())
        .unwrap();

    writer
        .write_tag("tag1", None, vec!["v6", "v7", "v8", "v4"].into_iter())
        .unwrap();

    std::mem::drop(writer);

    let stats: Vec<_> = get_stats(&batch);

    // Writer dropped, should not impact stats or data
    assert_batches_eq!(expected_data, &[batch.to_arrow(Projection::All).unwrap()]);
    assert_eq!(stats, expected_stats);

    let err = Writer::new(&mut batch, 1)
        .write_tag("b1", None, vec!["err"].into_iter())
        .unwrap_err()
        .to_string();
    assert_eq!(err.as_str(), "Unable to insert iox::column_type::tag type into column b1 with type iox::column_type::field::boolean");

    let err = Writer::new(&mut batch, 1)
        .write_i64("f64", None, vec![3].into_iter())
        .unwrap_err()
        .to_string();

    assert_eq!(err.as_str(), "Unable to insert iox::column_type::field::integer type into column f64 with type iox::column_type::field::float");

    let err = Writer::new(&mut batch, 1)
        .write_string("tag3", None, vec!["sd"].into_iter())
        .unwrap_err()
        .to_string();

    assert_eq!(err.as_str(), "Unable to insert iox::column_type::field::string type into column tag3 with type iox::column_type::tag");

    let err = Writer::new(&mut batch, 1)
        .write_tag_dict("tag3", None, vec![1].into_iter(), vec!["v1"].into_iter())
        .unwrap_err()
        .to_string();

    assert_eq!(err.as_str(), "Key not found in dictionary: 1");

    let stats: Vec<_> = get_stats(&batch);

    // Writer not committed, should not impact stats or data
    assert_batches_eq!(expected_data, &[batch.to_arrow(Projection::All).unwrap()]);
    assert_eq!(stats, expected_stats);

    let mut writer = Writer::new(&mut batch, 17);

    writer.write_time("time", 0..17).unwrap();

    writer
        .write_f64(
            "f64",
            Some(&[0b01000010, 0b00100100, 0b00000001]),
            vec![4., 945., -222., 4., 7.].into_iter(),
        )
        .unwrap();

    writer
        .write_tag("tag3", None, std::iter::repeat("v2"))
        .unwrap();

    writer
        .write_tag_dict(
            "tag2",
            Some(&[0b11011111, 0b11011101, 0b00000000]),
            vec![0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1].into_iter(),
            vec!["v4", "v1", "v7"].into_iter(), // Intentional extra key
        )
        .unwrap();

    writer.commit();

    let stats: Vec<_> = get_stats(&batch);

    let expected_data = &[
        "+-------+-------+--------+-----+-------+------+------+------+--------------------------------+-----+",
        "| b1    | b2    | f64    | i64 | i64_2 | tag1 | tag2 | tag3 | time                           | u64 |",
        "+-------+-------+--------+-----+-------+------+------+------+--------------------------------+-----+",
        "| true  | true  | 343.3  | 234 | -8    | v1   | v1   | v2   | 1970-01-01T00:00:00.000000007Z | 23  |",
        "| true  |       | 443.0  | 6   |       | v1   | v2   | v1   | 1970-01-01T00:00:00.000000005Z |     |",
        "| false | false |        | 2   |       | v2   |      |      | 1970-01-01T00:00:00.000000007Z |     |",
        "| false | false | 477.0  | 6   |       | v2   | v2   | v1   | 1970-01-01T00:00:00.000000003Z | 5   |",
        "| false | true  | -24.0  | -3  |       | v1   |      | v2   | 1970-01-01T00:00:00.000000005Z |     |",
        "|       |       |        |     |       |      | v4   | v2   | 1970-01-01T00:00:00Z           |     |",
        "|       |       | 4.0    |     |       |      | v1   | v2   | 1970-01-01T00:00:00.000000001Z |     |",
        "|       |       |        |     |       |      | v1   | v2   | 1970-01-01T00:00:00.000000002Z |     |",
        "|       |       |        |     |       |      | v4   | v2   | 1970-01-01T00:00:00.000000003Z |     |",
        "|       |       |        |     |       |      | v1   | v2   | 1970-01-01T00:00:00.000000004Z |     |",
        "|       |       |        |     |       |      |      | v2   | 1970-01-01T00:00:00.000000005Z |     |",
        "|       |       | 945.0  |     |       |      | v1   | v2   | 1970-01-01T00:00:00.000000006Z |     |",
        "|       |       |        |     |       |      | v1   | v2   | 1970-01-01T00:00:00.000000007Z |     |",
        "|       |       |        |     |       |      | v4   | v2   | 1970-01-01T00:00:00.000000008Z |     |",
        "|       |       |        |     |       |      |      | v2   | 1970-01-01T00:00:00.000000009Z |     |",
        "|       |       | -222.0 |     |       |      | v4   | v2   | 1970-01-01T00:00:00.000000010Z |     |",
        "|       |       |        |     |       |      | v4   | v2   | 1970-01-01T00:00:00.000000011Z |     |",
        "|       |       |        |     |       |      | v4   | v2   | 1970-01-01T00:00:00.000000012Z |     |",
        "|       |       | 4.0    |     |       |      |      | v2   | 1970-01-01T00:00:00.000000013Z |     |",
        "|       |       |        |     |       |      | v1   | v2   | 1970-01-01T00:00:00.000000014Z |     |",
        "|       |       |        |     |       |      | v1   | v2   | 1970-01-01T00:00:00.000000015Z |     |",
        "|       |       | 7.0    |     |       |      |      | v2   | 1970-01-01T00:00:00.000000016Z |     |",
        "+-------+-------+--------+-----+-------+------+------+------+--------------------------------+-----+",
    ];

    let expected_stats = vec![
        (
            "b1",
            Statistics::Bool(StatValues::new(Some(false), Some(true), 22, Some(17))),
        ),
        (
            "b2",
            Statistics::Bool(StatValues::new(Some(false), Some(true), 22, Some(18))),
        ),
        (
            "f64",
            Statistics::F64(StatValues::new(Some(-222.), Some(945.), 22, Some(13))),
        ),
        (
            "i64",
            Statistics::I64(StatValues::new(Some(-3), Some(234), 22, Some(17))),
        ),
        (
            "i64_2",
            Statistics::I64(StatValues::new(Some(-8), Some(-8), 22, Some(21))),
        ),
        (
            "tag1",
            Statistics::String(StatValues::new_with_distinct(
                Some("v1".to_string()),
                Some("v2".to_string()),
                22,
                Some(17),
                Some(NonZeroU64::new(3).unwrap()),
            )),
        ),
        (
            "tag2",
            Statistics::String(StatValues::new_with_distinct(
                Some("v1".to_string()),
                Some("v4".to_string()),
                22,
                Some(6),
                Some(NonZeroU64::new(4).unwrap()),
            )),
        ),
        (
            "tag3",
            Statistics::String(StatValues::new_with_distinct(
                Some("v1".to_string()),
                Some("v2".to_string()),
                22,
                Some(1),
                Some(NonZeroU64::new(3).unwrap()),
            )),
        ),
        (
            "time",
            Statistics::I64(StatValues::new(Some(0), Some(16), 22, Some(0))),
        ),
        (
            "u64",
            Statistics::U64(StatValues::new(Some(5), Some(23), 22, Some(20))),
        ),
    ];

    assert_batches_eq!(expected_data, &[batch.to_arrow(Projection::All).unwrap()]);
    assert_eq!(stats, expected_stats);

    let mut expected_timestamps = TimestampSummary::default();
    for t in [
        7, 5, 7, 3, 5, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    ] {
        expected_timestamps.record_nanos(t)
    }

    let timestamps = batch.timestamp_summary().unwrap();
    assert_eq!(timestamps, expected_timestamps);
}
