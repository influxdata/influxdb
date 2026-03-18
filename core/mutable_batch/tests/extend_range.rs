// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use arrow_util::assert_batches_eq;
use mutable_batch::{MutableBatch, writer::Writer};
use schema::Projection;

#[test]
fn test_extend_range() {
    let mut a = MutableBatch::new();
    let mut writer = Writer::new(&mut a, 5);

    writer
        .write_tag(
            "tag1",
            Some(&[0b00010101]),
            vec!["v2", "v2", "v2"].into_iter(),
        )
        .unwrap();

    writer
        .write_f64("f64", &[0b00001011], vec![23., 23., 5.].into_iter())
        .unwrap();

    writer
        .write_time("time", vec![0, 1, 2, 3, 4].into_iter())
        .unwrap();

    writer.commit();

    let mut b = MutableBatch::new();
    let mut writer = Writer::new(&mut b, 8);

    writer
        .write_tag(
            "tag1",
            Some(&[0b10010011]),
            vec!["v1", "v1", "v3", "v1"].into_iter(),
        )
        .unwrap();

    writer
        .write_tag(
            "tag3",
            None,
            vec!["v2", "v1", "v3", "v1", "v3", "v5", "v3", "v2"].into_iter(),
        )
        .unwrap();

    writer
        .write_bool("bool", Some(&[0b00010010]), vec![false, true].into_iter())
        .unwrap();

    writer
        .write_time("time", vec![5, 6, 7, 8, 9, 10, 11, 12].into_iter())
        .unwrap();

    writer.commit();

    assert_batches_eq!(
        &[
            "+------+------+--------------------------------+",
            "| f64  | tag1 | time                           |",
            "+------+------+--------------------------------+",
            "| 23.0 | v2   | 1970-01-01T00:00:00Z           |",
            "| 23.0 |      | 1970-01-01T00:00:00.000000001Z |",
            "|      | v2   | 1970-01-01T00:00:00.000000002Z |",
            "| 5.0  |      | 1970-01-01T00:00:00.000000003Z |",
            "|      | v2   | 1970-01-01T00:00:00.000000004Z |",
            "+------+------+--------------------------------+",
        ],
        &[a.clone().try_into_arrow(Projection::All).unwrap()]
    );

    assert_batches_eq!(
        &[
            "+-------+------+------+--------------------------------+",
            "| bool  | tag1 | tag3 | time                           |",
            "+-------+------+------+--------------------------------+",
            "|       | v1   | v2   | 1970-01-01T00:00:00.000000005Z |",
            "| false | v1   | v1   | 1970-01-01T00:00:00.000000006Z |",
            "|       |      | v3   | 1970-01-01T00:00:00.000000007Z |",
            "|       |      | v1   | 1970-01-01T00:00:00.000000008Z |",
            "| true  | v3   | v3   | 1970-01-01T00:00:00.000000009Z |",
            "|       |      | v5   | 1970-01-01T00:00:00.000000010Z |",
            "|       |      | v3   | 1970-01-01T00:00:00.000000011Z |",
            "|       | v1   | v2   | 1970-01-01T00:00:00.000000012Z |",
            "+-------+------+------+--------------------------------+",
        ],
        &[b.clone().try_into_arrow(Projection::All).unwrap()]
    );

    a.extend_from_range(&b, 1..4).unwrap();

    assert_batches_eq!(
        &[
            "+-------+------+------+------+--------------------------------+",
            "| bool  | f64  | tag1 | tag3 | time                           |",
            "+-------+------+------+------+--------------------------------+",
            "|       | 23.0 | v2   |      | 1970-01-01T00:00:00Z           |",
            "|       | 23.0 |      |      | 1970-01-01T00:00:00.000000001Z |",
            "|       |      | v2   |      | 1970-01-01T00:00:00.000000002Z |",
            "|       | 5.0  |      |      | 1970-01-01T00:00:00.000000003Z |",
            "|       |      | v2   |      | 1970-01-01T00:00:00.000000004Z |",
            "| false |      | v1   | v1   | 1970-01-01T00:00:00.000000006Z |",
            "|       |      |      | v3   | 1970-01-01T00:00:00.000000007Z |",
            "|       |      |      | v1   | 1970-01-01T00:00:00.000000008Z |",
            "+-------+------+------+------+--------------------------------+",
        ],
        &[a.clone().try_into_arrow(Projection::All).unwrap()]
    );
}
