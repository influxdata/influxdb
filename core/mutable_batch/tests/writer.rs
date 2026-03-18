// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use arrow_util::assert_batches_eq;
use core::iter::empty;
use data_types::TimestampMinMax;
use mutable_batch::{MutableBatch, writer::Writer};
use schema::Projection;

#[test]
fn test_basic() {
    let mut batch = MutableBatch::new();

    let mut writer = Writer::new(&mut batch, 5);

    writer
        .write_bool("b1", None, [true, true, false, false, false].into_iter())
        .unwrap();

    writer
        .write_bool(
            "b2",
            Some(&[0b00011101]),
            [true, false, false, true].into_iter(),
        )
        .unwrap();

    writer
        .write_f64("f64", &[0b00011011], [343.3, 443., 477., -24.].into_iter())
        .unwrap();

    writer
        .write_i64s_from_slice("i64", &[234, 6, 2, 6, -3])
        .unwrap();

    writer
        .write_i64("i64_2", &[0b00000001], [-8].into_iter())
        .unwrap();

    writer
        .write_u64("u64", &[0b00001001], [23, 5].into_iter())
        .unwrap();

    writer
        .write_time("time", [7, 5, 7, 3, 5].into_iter())
        .unwrap();

    writer
        .write_tag("tag1", None, ["v1", "v1", "v2", "v2", "v1"].into_iter())
        .unwrap();

    writer
        .write_tag("tag2", Some(&[0b00001011]), ["v1", "v2", "v2"].into_iter())
        .unwrap();

    writer
        .write_tag_dict(
            "tag3",
            Some(&[0b00011011]),
            [1, 0, 0, 1].into_iter(),
            ["v1", "v2"].into_iter(),
        )
        .unwrap();

    writer.commit();

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

    assert_batches_eq!(
        expected_data,
        &[batch.clone().try_into_arrow(Projection::All).unwrap()]
    );

    let mut writer = Writer::new(&mut batch, 4);
    writer
        .write_time("time", vec![4, 6, 21, 7].into_iter())
        .unwrap();

    writer
        .write_tag("tag1", None, vec!["v6", "v7", "v8", "v4"].into_iter())
        .unwrap();

    std::mem::drop(writer);

    // Writer dropped, should not impact data
    assert_batches_eq!(
        expected_data,
        &[batch.clone().try_into_arrow(Projection::All).unwrap()]
    );

    let err = Writer::new(&mut batch, 1)
        .write_tag("b1", None, vec!["err"].into_iter())
        .unwrap_err()
        .to_string();
    assert_eq!(
        err.as_str(),
        "Unable to insert iox::column_type::tag type into column b1 with type iox::column_type::field::boolean"
    );

    let err = Writer::new(&mut batch, 1)
        .write_i64s_from_slice("f64", &[3])
        .unwrap_err()
        .to_string();

    assert_eq!(
        err.as_str(),
        "Unable to insert iox::column_type::field::integer type into column f64 with type iox::column_type::field::float"
    );

    let err = Writer::new(&mut batch, 1)
        .write_string("tag3", None, vec!["sd"].into_iter())
        .unwrap_err()
        .to_string();

    assert_eq!(
        err.as_str(),
        "Unable to insert iox::column_type::field::string type into column tag3 with type iox::column_type::tag"
    );

    let err = Writer::new(&mut batch, 1)
        .write_tag_dict("tag3", None, vec![1].into_iter(), vec!["v1"].into_iter())
        .unwrap_err()
        .to_string();

    assert_eq!(err.as_str(), "Key not found in dictionary: 1");

    // Writer not committed, should not impact stats or data
    assert_batches_eq!(
        expected_data,
        &[batch.clone().try_into_arrow(Projection::All).unwrap()]
    );

    let mut writer = Writer::new(&mut batch, 17);

    writer.write_time("time", 0..17).unwrap();

    writer
        .write_f64(
            "f64",
            &[0b01000010, 0b00100100, 0b00000001],
            [4., 945., -222., 4., 7.].into_iter(),
        )
        .unwrap();

    writer
        .write_tag("tag3", None, std::iter::repeat("v2"))
        .unwrap();

    writer
        .write_tag_dict(
            "tag2",
            Some(&[0b11011111, 0b11011101, 0b00000000]),
            [0, 1, 1, 0, 1, 1, 1, 0, 0, 0, 0, 1, 1].into_iter(),
            ["v4", "v1", "v7"].into_iter(), // Intentional extra key
        )
        .unwrap();

    writer.commit();

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

    assert_batches_eq!(
        expected_data,
        &[batch.clone().try_into_arrow(Projection::All).unwrap()]
    );

    let timestamps = batch.timestamp_minmax().unwrap();
    assert_eq!(timestamps, TimestampMinMax { min: 0, max: 16 });
}

#[test]
fn test_null_only() {
    let mut batch = MutableBatch::new();

    let mut writer = Writer::new(&mut batch, 1);

    writer
        .write_bool("b1", Some(&[0b00000000]), empty())
        .unwrap();

    writer.write_f64("f64", &[0b00000000], empty()).unwrap();

    writer.write_i64("i64", &[0b00000000], empty()).unwrap();

    writer.write_u64("u64", &[0b00000000], empty()).unwrap();

    writer
        .write_string("string", Some(&[0b00000000]), empty())
        .unwrap();

    writer.write_time("time", vec![42].into_iter()).unwrap();

    writer
        .write_tag("tag1", Some(&[0b00000000]), empty())
        .unwrap();

    writer.commit();

    // let stats: Vec<_> = get_stats(&batch);

    let expected_data = &[
        "+----+-----+-----+--------+------+--------------------------------+-----+",
        "| b1 | f64 | i64 | string | tag1 | time                           | u64 |",
        "+----+-----+-----+--------+------+--------------------------------+-----+",
        "|    |     |     |        |      | 1970-01-01T00:00:00.000000042Z |     |",
        "+----+-----+-----+--------+------+--------------------------------+-----+",
    ];

    assert_batches_eq!(
        expected_data,
        &[batch.clone().try_into_arrow(Projection::All).unwrap()]
    );
}
