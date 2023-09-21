use arrow_util::assert_batches_eq;
use data_types::{StatValues, Statistics};
use mutable_batch::{writer::Writer, MutableBatch};
use schema::Projection;
use std::{collections::BTreeMap, num::NonZeroU64};

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
        .write_f64("f64", Some(&[0b00001011]), vec![23., 23., 5.].into_iter())
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
        &[a.to_arrow(Projection::All).unwrap()]
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
        &[b.to_arrow(Projection::All).unwrap()]
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
        &[a.to_arrow(Projection::All).unwrap()]
    );

    let stats: BTreeMap<_, _> = a.columns().map(|(k, v)| (k.as_str(), v.stats())).collect();

    assert_eq!(
        stats["bool"],
        Statistics::Bool(StatValues {
            min: Some(false),
            max: Some(false),
            total_count: 8,
            null_count: Some(7),
            distinct_count: None
        })
    );

    assert_eq!(
        stats["f64"],
        Statistics::F64(StatValues {
            min: Some(5.),
            max: Some(23.),
            total_count: 8,
            null_count: Some(5),
            distinct_count: None
        })
    );

    assert_eq!(
        stats["tag1"],
        Statistics::String(StatValues {
            min: Some("v1".to_string()),
            max: Some("v2".to_string()),
            total_count: 8,
            null_count: Some(4),
            distinct_count: Some(NonZeroU64::new(3).unwrap())
        })
    );

    assert_eq!(
        stats["tag3"],
        Statistics::String(StatValues {
            min: Some("v1".to_string()),
            max: Some("v3".to_string()),
            total_count: 8,
            null_count: Some(5),
            distinct_count: Some(NonZeroU64::new(3).unwrap())
        })
    );

    assert_eq!(
        stats["time"],
        Statistics::I64(StatValues {
            min: Some(0),
            max: Some(8),
            total_count: 8,
            null_count: Some(0),
            distinct_count: None
        })
    )
}
